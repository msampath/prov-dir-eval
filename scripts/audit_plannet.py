"""IG-driven conformance audit of Plan-Net data: shred first, then query.

The audit has two halves.

SHRED. Every resource's ``resource`` JSONB is normalised into
``public.plannet_element_value`` -- one row per (resource, IG element, parent
occurrence, occurrence, coding) -- using ``jsonb_path_query`` so repeats
survive with their ordinal. The enumeration is HIERARCHICAL: a parent's
occurrences are enumerated first, and each of its children is then evaluated
against one parent occurrence at a time, so ``ordinal`` is the child's index
WITHIN its parent and ``parent_ordinal`` identifies which parent instance.
That is what FHIR cardinality is defined against. A flat shred cannot express
it, and a flat max test on this data flagged 28,992 of the 29,350 rows
publishing the flagship element as breaches -- 98.8%, none of them real.
Every element in the Plan-Net 1.2.0 snapshot surface is
addressable there as data rather than as a bespoke expression buried in a
query, so no later check can silently skip a path: if a path is missing from
the shred it is missing from the catalogue table too, and that is visible.

QUERY. Completeness, cardinality, type validity, terminology binding and
referential integrity are then ordinary SQL over the shred, joined to the
element catalogue ``public.plannet_element``. Results land in
``public.ig_audit`` / ``ig_audit_ref`` / ``ig_audit_code`` under one
``ig_audit_run``.

WHY THE JSONB AND NOT THE EXTRACTED COLUMNS. An earlier column-based audit
concluded PractitionerRole publishes no network reference at all. It does:
the linkage is `PractitionerRole.extension:newpatients.extension:fromNetwork`,
a nested extension the ETL never lifts into `network_refs`. Nothing here reads
an extracted column -- even the Organization/Network profile split comes from
`$.type[*].coding[*] ? (@.code == "ntwk")` on the JSONB (verified identical to
the `is_network` column: 36 rows either way).

REGRESSION INVARIANT. `PractitionerRole.extension:newpatients.extension:
fromNetwork.value[x]` must shred to 29,350 distinct resources out of 96,027 on
`bcbs_az_new`. If the shred disagrees, the shred is wrong.

READ-ONLY on payer schemas: the only writes are to public.* objects this
module owns.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from ig_elements import IGElement, _load as load_structure_definitions, build  # noqa: E402
from ig_closure import PLAN_NET, LazyIndex, resolve_closure  # noqa: E402

from provdir.db import get_engine, safe_ident  # noqa: E402
from provdir.models import RESOURCE_TABLES  # noqa: E402
from provdir.quality.bindings import DEFAULT_CACHE, expand  # noqa: E402

PKG_DIR = r"F:/fhir_cache/packages/hl7.fhir.us.davinci-pdex-plan-net#1.2.0/package"

# Plan-Net models a network as plannet-Network: an Organization carrying
# type=ntwk. Both profiles live in the same table and are separated by this
# predicate, evaluated on the JSONB (never on the is_network column).
NETWORK_PRED = """jsonb_path_exists({t}.resource, '$.type[*].coding[*] ? (@.code == "ntwk")')"""

PROFILE_SPEC: dict[str, tuple[str, Optional[str]]] = {
    "PlannetPractitionerRole": ("practitioner_role", None),
    "PlannetPractitioner": ("practitioner", None),
    "PlannetOrganization": ("organization", "not_network"),
    "PlannetNetwork": ("organization", "network"),
    "PlannetLocation": ("location", None),
    "PlannetHealthcareService": ("healthcare_service", None),
    "PlannetOrganizationAffiliation": ("organization_affiliation", None),
    "PlannetInsurancePlan": ("insurance_plan", None),
    "PlannetEndpoint": ("endpoint_resource", None),
}

# `urn:source_name:<lob>` in meta.source. azblue = ACA + Medicare Advantage,
# hcaz = Medicaid (Health Choice Arizona). `ods` is a SOURCE SYSTEM appearing
# only in Organization -- it is NOT a line of business, and neither LOB is
# "commercial".
LOB_SQL = "split_part({t}.resource->'meta'->>'source', ':', 3)"
LOB_LABELS = {
    "azblue": "azblue - ACA + Medicare Advantage",
    "hcaz": "hcaz - Medicaid / Health Choice Arizona",
    "ods": "ods - source system, not a line of business",
}

# A choice element declaring more than this many types is FHIR's open type
# (`Element.value[x]` on a complex extension), not a real choice. Plan-Net
# pins those to max=0, so the only meaningful test is "is any value* key
# present at all", which no single-type jsonpath can express.
OPEN_TYPE_THRESHOLD = 5

PRIMITIVE_ORDER = [
    "instant", "dateTime", "date", "time", "boolean", "positiveInt",
    "unsignedInt", "integer", "decimal", "url", "canonical", "uri", "oid",
    "uuid", "code", "id", "markdown", "string", "Reference",
]
TEMPORAL_TYPES = {"date", "dateTime", "instant", "time"}
CODED_TYPES = {"CodeableConcept", "Coding", "code"}

FHIR_DATE_RE = r"^([0-9]{4})(-(0[1-9]|1[0-2])(-(0[1-9]|[12][0-9]|3[01]))?)?$"
FHIR_DATETIME_RE = (
    r"^([0-9]{4})(-(0[1-9]|1[0-2])(-(0[1-9]|[12][0-9]|3[01])"
    r"(T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?"
    r"(Z|[+-]((0[0-9]|1[0-3]):[0-5][0-9]|14:00)))?)?)?$"
)
FHIR_INSTANT_RE = (
    r"^([0-9]{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
    r"T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?"
    r"(Z|[+-]((0[0-9]|1[0-3]):[0-5][0-9]|14:00))$"
)
FHIR_TIME_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?$"

REF_RE = r"(?:^|/)([A-Za-z]+)/([A-Za-z0-9._-]{1,64})(?:/_history/[A-Za-z0-9.-]+)?$"


# --------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------

# plannet_element_value is partitioned by resource_type and sub-partitioned by
# schema_name, so each leaf partition is exactly one (schema, resource type)
# shred unit. Two reasons, both practical at ~100M rows per cell: re-shredding
# one cell is a TRUNCATE of one leaf rather than a 100M-row DELETE, and every
# audit query names both columns, so it prunes to a single leaf instead of
# touching the whole shred.
#
# The LEAF partitions are UNLOGGED, deliberately (Postgres does not allow it on
# a partitioned parent, only on the tables that actually hold rows). The shred
# is derived data: every row is reproducible
# from the payer schemas by re-running this script, and ig_shred_progress makes
# that resumable per chunk. Writing ~270M rows through WAL onto the same
# spinning volume that holds the heap roughly doubles the IO for a table that a
# crash should simply invalidate rather than recover. The cost is that a server
# crash empties the partitions -- which is the correct outcome for a cache, and
# is why ig_shred_progress is a normal logged table (a crash must not leave it
# claiming chunks that no longer exist... so it is truncated alongside; see
# reset_shred()).
SHRED_DDL = """
CREATE OR REPLACE FUNCTION public.ig_try_ts(s text) RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $fn$
BEGIN RETURN s::timestamptz; EXCEPTION WHEN others THEN RETURN NULL; END $fn$;

CREATE TABLE IF NOT EXISTS public.plannet_element (
    elem_idx         smallint PRIMARY KEY,
    ig_package       text NOT NULL,
    profile          text NOT NULL,
    resource_type    text NOT NULL,
    table_name       text NOT NULL,
    cell             text,
    element_path     text NOT NULL,
    jsonpath         text NOT NULL,
    min_card         integer NOT NULL,
    max_card         text NOT NULL,
    max_n            integer,
    types            text[] NOT NULL,
    primary_type     text,
    must_support     boolean NOT NULL,
    is_reference     boolean NOT NULL,
    ref_targets      text[] NOT NULL,
    binding_strength text,
    binding_valueset text,
    parent_path      text,
    parent_idx       smallint,
    ancestor_repeats boolean NOT NULL,
    validity_kind    text,
    card_method      text NOT NULL,
    notes            text[] NOT NULL);

CREATE TABLE IF NOT EXISTS public.plannet_element_value (
    schema_name    text     NOT NULL,
    resource_type  text     NOT NULL,
    resource_id    text     NOT NULL,
    lob            text,
    profile        text     NOT NULL,
    elem_idx       smallint NOT NULL,
    element_path   text     NOT NULL,
    ordinal        integer  NOT NULL,
    parent_ordinal integer  NOT NULL,
    coding_ordinal smallint NOT NULL,
    value_text     text,
    value_num      numeric,
    value_bool     boolean,
    value_date     timestamptz,
    ref_type       text,
    ref_id         text,
    code_system    text,
    code_code      text,
    code_display   text
) PARTITION BY LIST (resource_type);

-- parent_ordinal post-dates the first shred shape. It is added nullable so the
-- migration itself cannot rewrite a 100M-row partition, and any row still
-- carrying NULL is a PRE-0a shred whose `ordinal` counts occurrences flat
-- across the whole resource rather than within one parent. Those two shapes
-- must never be measured together, so assert_shred_shape() refuses to read a
-- cell that mixes them (see its docstring).
ALTER TABLE public.plannet_element_value
    ADD COLUMN IF NOT EXISTS parent_ordinal integer;

CREATE TABLE IF NOT EXISTS public.ig_shred_progress (
    schema_name text NOT NULL,
    table_name  text NOT NULL,
    chunk_lo    bigint NOT NULL,
    chunk_hi    bigint NOT NULL,
    n_values    bigint NOT NULL,
    seconds     double precision NOT NULL,
    done_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name, chunk_lo));
ALTER TABLE public.ig_shred_progress
    ADD COLUMN IF NOT EXISTS postmaster_start timestamptz;
"""

AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS public.ig_audit_run (
    run_id        bigserial PRIMARY KEY,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    ig_package    text NOT NULL,
    schemas       text[] NOT NULL,
    n_elements    integer NOT NULL,
    notes         jsonb);
CREATE TABLE IF NOT EXISTS public.ig_audit (
    run_id            bigint NOT NULL REFERENCES public.ig_audit_run(run_id),
    schema_name       text NOT NULL,
    profile           text NOT NULL,
    elem_idx          smallint NOT NULL,
    element_path      text NOT NULL,
    lob               text NOT NULL,
    resource_type     text NOT NULL,
    n_rows            bigint NOT NULL,
    n_present         bigint NOT NULL,
    n_occurrences     bigint NOT NULL,
    n_parent_instances bigint,
    n_min_violation   bigint,
    n_max_violation   bigint,
    n_invalid_occ     bigint,
    n_invalid_rows    bigint,
    n_concept_no_coding bigint,
    n_concept_text_only bigint,
    measured_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, schema_name, profile, elem_idx, lob));
-- n_parent_instances is the honest denominator for a cardinality figure: the
-- number of PARENT OCCURRENCES the element could legally have appeared under,
-- which is n_rows only for elements parented directly by the resource. The
-- concept counters exist because a CodeableConcept carrying `text` and no
-- `coding` scores as fully populated yet never reaches the binding check --
-- under a required binding that is a conformance failure rendered as a pass.
ALTER TABLE public.ig_audit
    ADD COLUMN IF NOT EXISTS n_parent_instances bigint;
ALTER TABLE public.ig_audit
    ADD COLUMN IF NOT EXISTS n_concept_no_coding bigint;
ALTER TABLE public.ig_audit
    ADD COLUMN IF NOT EXISTS n_concept_text_only bigint;
CREATE INDEX IF NOT EXISTS ix_ig_audit_profile
    ON public.ig_audit (run_id, schema_name, profile);
CREATE TABLE IF NOT EXISTS public.ig_audit_ref (
    run_id        bigint NOT NULL REFERENCES public.ig_audit_run(run_id),
    schema_name   text NOT NULL,
    profile       text NOT NULL,
    elem_idx      smallint NOT NULL,
    element_path  text NOT NULL,
    lob           text NOT NULL,
    target_type   text NOT NULL,
    declared_ok   boolean NOT NULL,
    n_refs        bigint NOT NULL,
    n_distinct    bigint NOT NULL,
    n_resolved    bigint NOT NULL,
    n_dangling    bigint NOT NULL,
    n_to_network  bigint NOT NULL,
    n_to_ods      bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, schema_name, profile, elem_idx, lob, target_type));
-- n_to_ods answers "does anything actually point at the three quarters of
-- Organizations whose meta.source says ods?" -- the evidence for treating
-- `ods` as a provenance tag rather than a line of business.
ALTER TABLE public.ig_audit_ref
    ADD COLUMN IF NOT EXISTS n_to_ods bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS public.ig_audit_code (
    run_id        bigint NOT NULL REFERENCES public.ig_audit_run(run_id),
    schema_name   text NOT NULL,
    profile       text NOT NULL,
    elem_idx      smallint NOT NULL,
    element_path  text NOT NULL,
    lob           text NOT NULL,
    code          text NOT NULL,
    system        text NOT NULL,
    strength      text,
    n_occurrences bigint NOT NULL,
    n_rows        bigint NOT NULL,
    code_member   boolean,
    system_member boolean,
    PRIMARY KEY (run_id, schema_name, profile, elem_idx, lob, code, system));
"""

# The two tables the shred and the measurement pass read. Named rather than
# inlined so a test can point the SAME queries at a fixture pair; production
# never overrides them.
SHRED_TABLE = "public.plannet_element_value"
CATALOGUE_TABLE = "public.plannet_element"


# --------------------------------------------------------------------------
# compilation: IGElement -> ElementCheck
# --------------------------------------------------------------------------

@dataclass
class ElementCheck:
    idx: int
    profile: str
    resource_type: str
    table: str
    cell: Optional[str]              # 'network' | 'not_network' | None
    path: str
    jsonpaths: list[str]
    min: int
    max_card: str
    max_n: Optional[int]
    types: list[str]
    must_support: bool
    binding_strength: Optional[str]
    binding_valueset: Optional[str]
    is_reference: bool
    ref_targets: list[str]
    parent_path: Optional[str] = None
    parent_jsonpath: Optional[str] = None
    parent_idx: Optional[int] = None
    ancestor_repeats: bool = False
    open_value_parent: Optional[str] = None
    validity_kind: Optional[str] = None
    notes: list[str] = field(default_factory=list)

    @property
    def primary_type(self) -> Optional[str]:
        return next((t for t in PRIMITIVE_ORDER if t in self.types), None)

    @property
    def is_coded(self) -> bool:
        return bool(CODED_TYPES & set(self.types))

    @property
    def is_temporal(self) -> bool:
        return bool(TEMPORAL_TYPES & set(self.types))

    @property
    def is_root_parented(self) -> bool:
        """True when the element hangs directly off the resource root.

        Equivalently `parent_jsonpath == '$'`, and equivalently
        `parent_idx is None` -- verified identical across all 737 elements.
        For these the parent instance count IS the row count.
        """
        return (self.parent_jsonpath or "$") == "$"

    @property
    def card_method(self) -> str:
        """How cardinality is counted -- BOTH values are exact.

        `root` groups by resource, `per-parent` groups by (resource, parent
        occurrence). The old third meaning of this field -- "approximate,
        compared against min x parent totals" -- is gone: the shred now carries
        parent_ordinal, so every element's cardinality is counted against the
        parent instance FHIR defines it against.
        """
        if self.open_value_parent:
            return "open-value-key-probe"
        return "root" if self.is_root_parented else "per-parent"


def _lit(s: str) -> str:
    """A SQL string literal. jsonpaths come from IG canonicals, never user
    input, but quotes are doubled anyway so a future canonical containing one
    cannot break out of the literal."""
    return "'" + s.replace("'", "''") + "'"


def _seg_jsonpath(seg: str, types: list[str]) -> str:
    """The jsonpath fragment for one FHIR path segment (choice types only)."""
    if seg.endswith("[x]") and types:
        t = types[0]
        return seg[:-3] + t[0].upper() + t[1:]
    return seg


def _pattern_systems(sds_by_name: dict[str, dict]) -> dict[tuple[str, str], str]:
    """(profile, element id) -> the `system` a pattern-discriminated slice pins.

    ig_elements resolves EXTENSION slice discriminators only (via the type's
    profile url). A `pattern` slice on Identifier -- Organization.identifier:NPI
    and friends -- carries its discriminator in patternIdentifier.system;
    without this, every such slice compiles to a bare `$.identifier[*]` that
    matches every identifier of any kind.
    """
    out: dict[tuple[str, str], str] = {}
    for name, sd in sds_by_name.items():
        for e in (sd.get("snapshot") or {}).get("element", []) or []:
            if not e.get("sliceName"):
                continue
            for key in ("patternIdentifier", "patternCoding"):
                pat = e.get(key)
                if isinstance(pat, dict) and pat.get("system"):
                    out[(name, e["id"])] = pat["system"]
    return out


def valueset_systems(index: dict[str, dict], vs_url: str,
                     _seen: Optional[frozenset] = None) -> Optional[set[str]]:
    """The coding `system`s a ValueSet draws from, or None if undecidable.

    Companion to bindings.expand(), which deliberately returns codes only (see
    its module docstring's code-only caveat: the extracted columns it was
    written against discard the system). Reading the JSONB gives us the system
    alongside the code, so membership can finally be checked as the (system,
    code) pair FHIR actually defines it on. This supplies the missing half
    rather than re-expanding codes.
    """
    vs_url = vs_url.split("|", 1)[0]
    seen = _seen or frozenset()
    if vs_url in seen:
        return set()
    seen = seen | {vs_url}
    vs = index.get(vs_url)
    if vs is None or vs.get("resourceType") != "ValueSet":
        return None
    systems: set[str] = set()
    for inc in (vs.get("compose") or {}).get("include", []) or []:
        if inc.get("system"):
            systems.add(inc["system"])
        for sub in inc.get("valueSet", []) or []:
            part = valueset_systems(index, sub, seen)
            if part is None:
                return None
            systems |= part
        if not inc.get("system") and not inc.get("valueSet"):
            return None
    return systems or None


def _slice_filter(el: IGElement, base: str, index: dict[str, dict],
                  pattern_sys: dict[tuple[str, str], str]) -> tuple[str, Optional[str]]:
    """The jsonpath filter isolating one slice, plus a note when there is none.

    Three discriminators appear in Plan-Net 1.2.0:
      * extension slice        -> `@.url == <canonical, or plain sub-extension url>`
      * `pattern` slice        -> `@.system == <patternIdentifier.system>`
      * `value`-on-$this slice -> the CodeSystem its required binding draws from
    Anything else compiles to an undiscriminated `base[*]`, which over-matches;
    that is recorded as a note so the report can list it rather than quietly
    presenting the parent's numbers as the slice's.
    """
    sname = el.path.split(".")[-1].split(":", 1)[1]
    if base in ("extension", "modifierExtension"):
        # a top-level extension slice is pinned by its profile canonical; a
        # sub-extension inside a complex extension is keyed by its plain name
        return f'{base}[*] ? (@.url == "{el.slice_url or sname}")', None
    sysurl = pattern_sys.get((el.profile, el.path))
    if sysurl:
        return f'{base}[*] ? (@.system == "{sysurl}")', None
    if el.binding_valueset and el.binding_strength == "required":
        systems = valueset_systems(index, el.binding_valueset)
        if systems and len(systems) == 1:
            return (f'{base}[*] ? (exists(@.coding[*] ? '
                    f'(@.system == "{next(iter(systems))}")))'), None
    return f"{base}[*]", ("slice has no discriminator resolvable offline; "
                          "the path matches every sibling of the sliced element")


def dedupe_elements(elements: list[IGElement]) -> list[IGElement]:
    """One element per (profile, path), preferring the profile's own snapshot.

    A complex extension that a profile inlines is emitted TWICE by ig_elements:
    once from the profile snapshot, once from walking the extension's own
    StructureDefinition. In Plan-Net 1.2.0 that is 13 of the 750 raw records --
    the whole PractitionerRole qualification family. Shredding both is not
    merely wasteful (it doubled practitioner_role's shred, its single largest
    contributor, because one practitioner's qualification can carry 1,620
    whereValid occurrences); it also double-counts every occurrence the
    cardinality and reference checks then read.

    The two copies can disagree, and the profile's snapshot is the binding
    one: PlannetPractitionerRole narrows qualification.code.value[x] to
    IndividualSpecialtyAndDegreeLicenseCertificateVS, while the shared
    Qualification extension says SpecialtyAndDegreeLicenseCertificateVS.
    Keeping the expansion copy would audit against a value set the profile
    does not impose, so from_expansion=False wins. Original order is kept so
    elem_idx stays stable against the IG's own element order.
    """
    best: dict[tuple[str, str], IGElement] = {}
    order: list[tuple[str, str]] = []
    for el in elements:
        key = (el.profile, el.path)
        if key not in best:
            best[key] = el
            order.append(key)
        elif best[key].from_expansion and not el.from_expansion:
            best[key] = el
    return [best[k] for k in order]


def compile_checks(elements: list[IGElement], index: dict[str, dict],
                   sds_by_name: dict[str, dict]) -> list[ElementCheck]:
    """Normalise the raw IG element surface into shreddable checks.

    Every element's jsonpath is REBUILT from its ancestor chain rather than
    taken as-is, because ig_elements compiles each snapshot element in
    isolation and a slice's children therefore lose the slice's own
    discriminator. Concretely, PlannetPractitionerRole's snapshot inlines the
    entire qualification extension, so `PractitionerRole.extension:qualification
    .extension:identifier` arrives compiled as `$.extension[*].extension[*]` --
    a path matching ANY sub-extension of ANY extension. Rebuilding parent-first
    yields `$.extension[*] ? (@.url == ".../qualification").extension[*] ?
    (@.url == "identifier")`, and collapses the 13 duplicate (profile, path)
    pairs that inlining produces onto one correct expression.

    Two further repairs:
      * a real choice element (<= OPEN_TYPE_THRESHOLD declared types) is tested
        across ALL its types, not just types[0];
      * FHIR's open `value[x]` (50 declared types, always max=0 here) has no
        single-type path and is tested by key prefix on its parent.
    """
    elements = dedupe_elements(elements)
    pattern_sys = _pattern_systems(sds_by_name)
    by_key: dict[tuple[str, str], IGElement] = {}
    for el in elements:
        by_key.setdefault((el.profile, el.path), el)

    memo: dict[tuple[str, str], str] = {}

    def jp_for(profile: str, path: str) -> str:
        key = (profile, path)
        if key in memo:
            return memo[key]
        segs = path.split(".")
        if len(segs) <= 1:
            memo[key] = "$"
            return "$"
        el = by_key.get(key)
        parent_jp = jp_for(profile, ".".join(segs[:-1]))
        last = segs[-1]
        if el is None:
            # a path level the snapshot never emits on its own; the plain
            # segment is the only honest reading of it
            seg = last.split(":", 1)[0]
        elif ":" in last:
            seg, _ = _slice_filter(el, last.split(":", 1)[0], index, pattern_sys)
        else:
            seg = _seg_jsonpath(last, el.types)
        # A repeating element is a JSON ARRAY, and `$.location` matches that
        # array as ONE item -- so without `[*]` the whole repeat collapses into
        # a single shred row whose value is the array itself: ordinal is always
        # 1, ref_type/ref_id are NULL (an array has no `reference` key), and a
        # repeating CodeableConcept never reaches the coding fan-out, so its
        # codes never reach the binding check. Postgres jsonpath is lax, so
        # `[*]` also tolerates a payer publishing a single object where the IG
        # says a list. Slices already carry their own `[*] ? (...)`.
        if el is not None and el.max not in ("0", "1") and "[*]" not in seg:
            seg += "[*]"
        memo[key] = f"{parent_jp}.{seg}"
        return memo[key]

    out: list[ElementCheck] = []
    for idx, el in enumerate(elements):
        table, cell = PROFILE_SPEC[el.profile]
        notes: list[str] = []
        last = el.path.split(".")[-1]
        parent_path = ".".join(el.path.split(".")[:-1])
        parent = jp_for(el.profile, parent_path)

        if ":" in last:
            _, warn = _slice_filter(el, last.split(":", 1)[0], index, pattern_sys)
            if warn:
                notes.append(warn)

        jp = jp_for(el.profile, el.path)
        if jp != el.jsonpath:
            notes.append("jsonpath rebuilt from the ancestor chain "
                         f"(ig_elements emitted {el.jsonpath})")

        jsonpaths = [jp]
        open_parent = None
        n_types = len(el.types)
        if last.endswith("[x]"):
            if n_types > OPEN_TYPE_THRESHOLD:
                open_parent = parent
                notes.append(f"FHIR open type ({n_types} declared types); shredded "
                             "as 'any value* key on the parent element'")
            elif n_types > 1:
                stem = last[:-3]
                jsonpaths = [f"{parent}.{stem}{t[0].upper()}{t[1:]}" for t in el.types]
                notes.append("choice element; shredded as the union over "
                             + ", ".join(el.types))

        prim = next((t for t in PRIMITIVE_ORDER if t in el.types), None)
        out.append(ElementCheck(
            idx=idx, profile=el.profile, resource_type=el.resource_type,
            table=table, cell=cell, path=el.path, jsonpaths=jsonpaths,
            min=el.min, max_card=el.max,
            max_n=int(el.max) if el.max.isdigit() else None,
            types=el.types, must_support=el.must_support,
            binding_strength=el.binding_strength,
            binding_valueset=el.binding_valueset, is_reference=el.is_reference,
            ref_targets=el.ref_targets, parent_path=parent_path or None,
            parent_jsonpath=parent, open_value_parent=open_parent,
            validity_kind=prim if prim in VALIDITY_SQL else None,
            notes=notes,
        ))

    # Cardinality is per-parent-instance in FHIR but flat in a shred, so an
    # element under a repeating ancestor cannot have min/max read off its own
    # occurrence count alone -- flag those and resolve the parent's elem_idx so
    # the check can compare the two counts (card_method='per-parent').
    idx_by_key = {(c.profile, c.path): c.idx for c in out}
    max_by_path = {(el.profile, el.path): el.max for el in reversed(elements)}
    for c in out:
        c.parent_idx = idx_by_key.get((c.profile, c.parent_path)) if c.parent_path else None
        segs = c.path.split(".")
        for i in range(1, len(segs)):
            anc = ".".join(segs[:i + 1])
            if anc == c.path:
                continue
            m = max_by_path.get((c.profile, anc))
            if m is not None and m not in ("0", "1"):
                c.ancestor_repeats = True
                break
    return out


# --------------------------------------------------------------------------
# validity, expressed over shred columns
# --------------------------------------------------------------------------

def _txt_bad(extra: str = "") -> str:
    return ("CASE WHEN p.value_text IS NULL THEN true "
            "WHEN p.value_text = '' THEN true " + extra + " ELSE false END")


VALIDITY_SQL: dict[str, str] = {
    "date": _txt_bad(f"WHEN p.value_text !~ {_lit(FHIR_DATE_RE)} THEN true "),
    "dateTime": _txt_bad(f"WHEN p.value_text !~ {_lit(FHIR_DATETIME_RE)} THEN true "),
    "instant": _txt_bad(f"WHEN p.value_text !~ {_lit(FHIR_INSTANT_RE)} THEN true "),
    "time": _txt_bad(f"WHEN p.value_text !~ {_lit(FHIR_TIME_RE)} THEN true "),
    # FHIR's own `code` regex allows single internal spaces; the audit brief
    # asks for the stricter reading -- non-empty and whitespace-free -- so any
    # whitespace at all counts here, and the report says so.
    "code": _txt_bad(r"WHEN p.value_text ~ '\s' THEN true "),
    "uri": _txt_bad(r"WHEN p.value_text ~ '\s' THEN true "),
    "canonical": _txt_bad(r"WHEN p.value_text ~ '\s' THEN true "),
    "oid": _txt_bad(r"WHEN p.value_text !~ '^urn:oid:[0-2](\.(0|[1-9][0-9]*))+$' THEN true "),
    "uuid": _txt_bad(r"WHEN p.value_text !~ '^urn:uuid:[0-9a-fA-F]{8}-' THEN true "),
    "url": _txt_bad(r"WHEN p.value_text ~ '\s' THEN true "
                    r"WHEN p.value_text !~ '^[a-zA-Z][a-zA-Z0-9+.-]*:' THEN true "),
    "boolean": "CASE WHEN p.value_bool IS NULL THEN true ELSE false END",
    "positiveInt": ("CASE WHEN p.value_num IS NULL THEN true "
                    "WHEN p.value_num <= 0 THEN true "
                    "WHEN p.value_num <> trunc(p.value_num) THEN true ELSE false END"),
    "unsignedInt": ("CASE WHEN p.value_num IS NULL THEN true "
                    "WHEN p.value_num < 0 THEN true "
                    "WHEN p.value_num <> trunc(p.value_num) THEN true ELSE false END"),
    "integer": ("CASE WHEN p.value_num IS NULL THEN true "
                "WHEN p.value_num <> trunc(p.value_num) THEN true ELSE false END"),
    "decimal": "CASE WHEN p.value_num IS NULL THEN true ELSE false END",
    # For a Reference the shred stores reference / display / '(identifier)' in
    # value_text, so a NULL there means the Reference names nothing, displays
    # nothing and carries no logical identifier: it is inert.
    "Reference": "CASE WHEN p.value_text IS NULL THEN true ELSE false END",
}


# --------------------------------------------------------------------------
# shred SQL
# --------------------------------------------------------------------------

OPEN_VALUE_SQL = (
    "(SELECT coalesce(jsonb_agg(kv.value), '[]'::jsonb) "
    " FROM jsonb_array_elements(jsonb_path_query_array({col}, {parent}::jsonpath)) o(obj), "
    "      jsonb_each(o.obj) kv "
    " WHERE jsonb_typeof(o.obj) = 'object' AND starts_with(kv.key, 'value'))"
)

# The same probe evaluated against ONE parent occurrence rather than the array
# of all of them. The union over parents of this is the expression above, which
# is what makes the hierarchical shred a partition of the flat one.
REL_OPEN_VALUE_SQL = (
    "(SELECT coalesce(jsonb_agg(kv.value), '[]'::jsonb) "
    " FROM jsonb_each({col}) kv "
    " WHERE jsonb_typeof({col}) = 'object' AND starts_with(kv.key, 'value'))"
)


def arr_expr(c: ElementCheck, col: str = "t.resource") -> str:
    """The jsonb array of every value `c` matches in one row, in document order.

    jsonb_path_query_array, not _exists: the shred needs each repeat as its own
    row with an ordinal, which is what makes cardinality and reference counting
    possible downstream.

    This is the FLAT reading -- every occurrence anywhere in the resource, with
    no parent axis. The shred no longer writes rows this way (see
    build_shred_insert), but the flat total is exactly what the hierarchical
    enumeration partitions, so this stays as the independent recount
    verify_occurrences() checks the shred against.
    """
    if c.open_value_parent is not None:
        return OPEN_VALUE_SQL.format(parent=_lit(c.open_value_parent), col=col)
    parts = [f"jsonb_path_query_array({col}, {_lit(jp)}::jsonpath)" for jp in c.jsonpaths]
    return parts[0] if len(parts) == 1 else "(" + " || ".join(parts) + ")"


def rel_jsonpath(jp: str, parent_jp: str) -> str:
    """`jp` re-expressed relative to one occurrence of `parent_jp`.

    Every element's own jsonpath is built by appending one segment to its
    parent's (see compile_checks.jp_for), so the parent's path is a literal
    prefix of the child's in all 737 cases -- verified, 0 exceptions. Stripping
    it is therefore exact, not a heuristic, and the ValueError below is the
    tripwire if that ever stops holding.
    """
    if parent_jp == "$":
        return jp
    prefix = parent_jp + "."
    if not jp.startswith(prefix):
        raise ValueError(f"jsonpath {jp!r} is not under its parent {parent_jp!r}; "
                         "relative extraction would silently change what is matched")
    return "$." + jp[len(prefix):]


def arr_expr_rel(c: ElementCheck, col: str) -> str:
    """`c`'s value array evaluated against ONE occurrence of its parent."""
    parent_jp = c.parent_jsonpath or "$"
    if c.open_value_parent is not None:
        return (OPEN_VALUE_SQL.format(parent=_lit(c.open_value_parent), col=col)
                if parent_jp == "$" else REL_OPEN_VALUE_SQL.format(col=col))
    parts = [f"jsonb_path_query_array({col}, {_lit(rel_jsonpath(jp, parent_jp))}::jsonpath)"
             for jp in c.jsonpaths]
    return parts[0] if len(parts) == 1 else "(" + " || ".join(parts) + ")"


def cell_pred(cell: Optional[str], alias: str = "t") -> str:
    if cell == "network":
        return NETWORK_PRED.format(t=alias)
    if cell == "not_network":
        return f"NOT {NETWORK_PRED.format(t=alias)}"
    return "true"


def cell_flag(cell: Optional[str]) -> str:
    """cell_pred against the per-resource scalar hoisted into `tc`.

    The shred branches sit inside a LATERAL that runs once per PARENT
    occurrence, and a VALUES row is re-evaluated on every iteration. With the
    predicate written against t.resource that meant one jsonb_path_exists --
    one full detoast of the document -- per parent occurrence per element,
    which on a 25 MB Network Organization carrying 66k addresses turned a
    2-minute chunk into days. `tc.is_net` is computed once per resource row
    (see the fenced lateral in build_shred_select) and referencing it here is
    a plain Var.
    """
    if cell == "network":
        return "tc.is_net"
    if cell == "not_network":
        return "NOT tc.is_net"
    return "true"


_E_COLS = ("e(elem_idx, element_path, profile, arr, in_cell, is_coded, "
           "is_temporal, is_ref)")
_X_COLS = ("x(elem_idx, element_path, profile, in_cell, is_coded, is_temporal, "
           "is_ref, parent_ord, v, ord)")
_X_SELECT = ("e.elem_idx, e.element_path, e.profile, e.in_cell, e.is_coded, "
             "e.is_temporal, e.is_ref")


def _val_row(c: ElementCheck, arr: str) -> str:
    """One (VALUES) entry: the element's identity plus its value array."""
    return (f"({c.idx}, {_lit(c.path)}, {_lit(c.profile)}, ({arr}), "
            f"{cell_flag(c.cell)}, {str(c.is_coded).lower()}, "
            f"{str(c.is_temporal).lower()}, {str(c.is_reference).lower()})")


def shred_branches(checks: list[ElementCheck]) -> list[str]:
    """One UNION ALL branch per distinct parent jsonpath.

    The root branch (`parent_jsonpath = '$'`) evaluates each element's absolute
    path against the resource and stamps parent_ordinal 0 -- there is exactly
    one "parent instance", the resource itself.

    Every other branch enumerates ITS parent's occurrences once, WITH
    ORDINALITY, and then evaluates each of that parent's children against one
    parent occurrence at a time. That is what gives `ordinal` its FHIR meaning
    (the child's index WITHIN its parent) and `parent_ordinal` its identity
    (which parent instance). The parent enumeration is byte-for-byte the same
    expression as the parent element's own arr_expr -- verified for all 737
    elements -- so a child's parent_ordinal joins to the parent element's own
    `ordinal`, and the parent's occurrence count is exactly the denominator its
    children's cardinality must be scored against.

    Grouping by the jsonpath rather than by parent elem_idx is deliberate: two
    profiles sharing a table (PlannetOrganization / PlannetNetwork) compile the
    same parent path, so they share one enumeration. Their children stay
    separated by the per-element `in_cell` predicate.

    Every jsonpath is a plan-time literal. Carrying them as data --
    `e.rel_jp::jsonpath` from a VALUES list -- would re-parse the path text once
    per row per element, ~335M times on this corpus.
    """
    by_parent: dict[str, list[ElementCheck]] = {}
    for c in checks:
        by_parent.setdefault(c.parent_jsonpath or "$", []).append(c)

    out: list[str] = []
    for pjp, group in by_parent.items():
        if pjp == "$":
            vals = ", ".join(_val_row(c, arr_expr_rel(c, "t.resource")) for c in group)
            out.append(f"""
    SELECT {_X_SELECT}, 0::int AS parent_ord, o.val, o.ord
    FROM (VALUES {vals}) AS {_E_COLS}
    CROSS JOIN LATERAL jsonb_array_elements(e.arr) WITH ORDINALITY AS o(val, ord)""")
        else:
            vals = ", ".join(_val_row(c, arr_expr_rel(c, "pj.pv")) for c in group)
            out.append(f"""
    SELECT {_X_SELECT}, pj.ord::int AS parent_ord, o.val, o.ord
    FROM jsonb_array_elements(
             jsonb_path_query_array(t.resource, {_lit(pjp)}::jsonpath))
         WITH ORDINALITY AS pj(pv, ord)
    CROSS JOIN LATERAL (VALUES {vals}) AS {_E_COLS}
    CROSS JOIN LATERAL jsonb_array_elements(e.arr) WITH ORDINALITY AS o(val, ord)""")
    return out


def build_shred_select(schema: str, table: str, rtype: str,
                       checks: list[ElementCheck], where: str = "true") -> str:
    """The SELECT the shred INSERT is built from, as its own callable query.

    Split out so the generated expression can be run read-only -- against a
    ctid slice of a payer table, or a temp fixture -- and its per-element
    occurrence totals compared to an independent recount, without writing a row.

    The LEFT JOIN LATERAL over `coding` gives a CodeableConcept one row per
    coding (and one row at coding_ordinal 1 when there is none), because binding
    membership is defined per coding, not per concept.

    Reference splitting is a CASE in the target list rather than a LATERAL
    join: as a join it was evaluated once per output row (6s of an 18s trial
    chunk) even for the ~85% of rows that are not references at all. The
    `Type/id` fast path is split_part; the regex only runs on the absolute and
    versioned forms that do not match it.
    """
    lob = LOB_SQL.format(t="t")
    # Both `lob` and the cell predicate are constant per resource row, and both
    # used to be re-evaluated per OUTPUT row (lob, in the target list) or per
    # parent occurrence (the cell predicate, inside every VALUES row). Each
    # evaluation detoasts the whole document -- invisible on a 3 KB
    # organization, catastrophic on a 25 MB network whale (one org chunk ran
    # 55 min at ~8 KB/s before this fix). They are computed once per row in
    # the `tc` lateral below; OFFSET 0 is the planner fence that stops the
    # trivial subquery being pulled up and the expressions re-inlined at every
    # use site.
    has_cells = any(c.cell for c in checks)
    is_net = NETWORK_PRED.format(t="t") if has_cells else "false"
    branches = "\n    UNION ALL".join(shred_branches(checks))
    return f"""
SELECT {_lit(schema)} AS schema_name, {_lit(rtype)} AS resource_type,
       t.id AS resource_id, tc.lob AS lob, x.profile AS profile,
       x.elem_idx AS elem_idx, x.element_path AS element_path,
       x.ord::int AS ordinal,
       x.parent_ord AS parent_ordinal,
       coalesce(cd.ord, 1)::smallint AS coding_ordinal,
       CASE WHEN x.is_ref THEN coalesce(x.v ->> 'reference', x.v ->> 'display',
                                        CASE WHEN jsonb_exists(x.v, 'identifier')
                                             THEN '(identifier)' END)
            WHEN jsonb_typeof(x.v) = 'string' THEN x.v #>> '{{}}'
            WHEN x.is_coded AND jsonb_typeof(x.v) = 'object'
                 AND NOT jsonb_exists(x.v, 'coding') THEN x.v ->> 'text' END AS value_text,
       CASE WHEN jsonb_typeof(x.v) = 'number' THEN (x.v #>> '{{}}')::numeric END AS value_num,
       CASE WHEN jsonb_typeof(x.v) = 'boolean' THEN (x.v #>> '{{}}')::boolean END AS value_bool,
       CASE WHEN x.is_temporal AND jsonb_typeof(x.v) = 'string'
            THEN public.ig_try_ts(x.v #>> '{{}}') END AS value_date,
       CASE WHEN x.is_ref AND x.v ->> 'reference' ~ '^[A-Za-z]+/[A-Za-z0-9._-]+$'
                 THEN split_part(x.v ->> 'reference', '/', 1)
            WHEN x.is_ref AND x.v ->> 'reference' IS NOT NULL
                 THEN coalesce((regexp_match(x.v ->> 'reference', {_lit(REF_RE)}))[1],
                               '(unparseable)') END AS ref_type,
       CASE WHEN x.is_ref AND x.v ->> 'reference' ~ '^[A-Za-z]+/[A-Za-z0-9._-]+$'
                 THEN split_part(x.v ->> 'reference', '/', 2)
            WHEN x.is_ref AND x.v ->> 'reference' IS NOT NULL
                 THEN (regexp_match(x.v ->> 'reference', {_lit(REF_RE)}))[2] END AS ref_id,
       coalesce(cd.cv ->> 'system',
                CASE WHEN x.is_coded AND jsonb_typeof(x.v) = 'object'
                     THEN x.v ->> 'system' END) AS code_system,
       coalesce(cd.cv ->> 'code',
                CASE WHEN x.is_coded AND jsonb_typeof(x.v) = 'object'
                     THEN x.v ->> 'code'
                     WHEN x.is_coded AND jsonb_typeof(x.v) = 'string'
                     THEN x.v #>> '{{}}' END) AS code_code,
       coalesce(cd.cv ->> 'display',
                CASE WHEN x.is_coded AND jsonb_typeof(x.v) = 'object'
                     THEN x.v ->> 'display' END) AS code_display
FROM {safe_ident(schema)}.{safe_ident(table)} t
CROSS JOIN LATERAL (SELECT {lob} AS lob, {is_net} AS is_net OFFSET 0) tc
CROSS JOIN LATERAL ({branches}
) AS {_X_COLS}
LEFT JOIN LATERAL jsonb_array_elements(
      CASE WHEN x.is_coded AND jsonb_typeof(x.v) = 'object' AND jsonb_exists(x.v, 'coding')
           THEN x.v -> 'coding' ELSE '[]'::jsonb END)
    WITH ORDINALITY AS cd(cv, ord) ON true
WHERE ({where}) AND x.in_cell"""


SHRED_COLUMNS = ("schema_name, resource_type, resource_id, lob, profile, elem_idx, "
                 "element_path, ordinal, parent_ordinal, coding_ordinal, value_text, "
                 "value_num, value_bool, value_date, ref_type, ref_id, code_system, "
                 "code_code, code_display")


def build_shred_insert(schema: str, table: str, rtype: str,
                       checks: list[ElementCheck], where: str = "true") -> str:
    """One INSERT that shreds every element of one table in a single pass.

    All of a table's elements share ONE scan of the JSONB rather than one scan
    each, and all of a given parent's children share one enumeration of that
    parent. See shred_branches() for the shape and why it is hierarchical.
    """
    return (f"INSERT INTO public.plannet_element_value\n  ({SHRED_COLUMNS})"
            + build_shred_select(schema, table, rtype, checks, where))


# ix_pev_path (schema_name, resource_type, element_path) used to be here and is
# not any more: no audit query filters on element_path -- measure_presence,
# measure_codes and the catalogue join all key on elem_idx, and measure_presence
# is a full GROUP BY over an entire leaf partition that no index can help. It is
# dropped in build_indexes() rather than merely omitted, so a database carrying
# the old one stops paying to maintain it.
SHRED_INDEXES = [
    ("ix_pev_elem", "(schema_name, resource_type, elem_idx)"),
    ("ix_pev_ref", "(ref_type, ref_id)"),
]
RETIRED_SHRED_INDEXES = ["ix_pev_path"]

# The shred is written once at high volume and then scanned end to end by every
# audit pass, which makes it the worst possible tenant of a USB HDD and the best
# possible tenant of flash: UNLOGGED, fully rebuildable, nothing lost if the
# volume misbehaves. Both are overridable because these tablespace names are
# local to this machine.
DEFAULT_TABLESPACE = "bulk_ssd"
DEFAULT_INDEX_TABLESPACE = "hot_idx"
# The previous shred was 78,994,855 values at ~16 GB; parent_ordinal adds four
# bytes a row. Budget with headroom and refuse to start rather than fill the
# volume that also carries Aetna's 400 GB table.
SHRED_BUDGET_BYTES = 25 * 1024 ** 3


# --------------------------------------------------------------------------
# execution helpers
# --------------------------------------------------------------------------

def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _fetch(conn, sql: str, params=None):
    """Execute at driver level, NOT through sqlalchemy.text().

    text() reads `:word` as a bind parameter, and the generated SQL is full of
    literal colons that look exactly like one -- `urn:oid:2.16...` in the
    CLIA/NAIC slice discriminators, `urn:uuid:` in the uuid validity regex.
    exec_driver_sql passes the string through untouched and uses psycopg's own
    `%(name)s` placeholders for the few real parameters.
    """
    return conn.exec_driver_sql(sql, params or ()).all()


def _exec(conn, sql: str, params=None):
    return conn.exec_driver_sql(sql, params) if params is not None \
        else conn.exec_driver_sql(sql)


def _chunk(seq: list, n: int) -> list[list]:
    return [seq[i:i + n] for i in range(0, len(seq), n)]


def configure_session(conn) -> None:
    """Session settings every shred/audit connection needs.

    statement_timeout: the scans are unbounded over 1.86M-row tables on a
    spinning volume; a default timeout aborts one mid-sweep.
    jit: the shred is one plan repeated over millions of rows, so compiling it
    buys nothing against the jsonpath cost and costs a second per chunk.
    synchronous_commit: the shred is rebuildable derived data (see SHRED_DDL),
    so an fsync per chunk commit buys nothing.
    The maintenance settings are for the post-shred index builds.
    """
    for stmt in ("SET statement_timeout = 0",
                 "SET jit = off",
                 "SET synchronous_commit = off",
                 "SET max_parallel_maintenance_workers = 4",
                 "SET maintenance_work_mem = '4GB'"):
        _exec(conn, stmt)


def resolve_tablespace(conn, name: Optional[str]) -> Optional[str]:
    """`name` if the server has it, else None (the server default).

    These names are this machine's, not the schema's. A checkout on another box
    must not fail its shred because `bulk_ssd` happens not to exist there.
    """
    if not name:
        return None
    if not _fetch(conn, "SELECT 1 FROM pg_tablespace WHERE spcname = %(n)s", {"n": name}):
        _log(f"tablespace {name!r} does not exist here; using the server default")
        return None
    return name


def _ts_clause(name: Optional[str]) -> str:
    return f" TABLESPACE {safe_ident(name)}" if name else ""


def check_tablespace_capacity(conn, name: Optional[str],
                              need_bytes: int = SHRED_BUDGET_BYTES) -> None:
    """Refuse to start a shred that will not fit. Loudly, before it runs.

    pg_tablespace_size says what is already there; the volume's free space is
    what actually decides, and Postgres does not expose it -- so this stats the
    tablespace's own directory. That only works when the server is local, which
    it is here; if it is not, the check reports that it could not run rather
    than pretending it passed.
    """
    if not name:
        return
    rows = _fetch(conn, "SELECT pg_tablespace_location(oid), pg_tablespace_size(oid) "
                        "FROM pg_tablespace WHERE spcname = %(n)s", {"n": name})
    if not rows or not rows[0][0]:
        return
    loc, used = rows[0][0], int(rows[0][1])
    try:
        free = shutil.disk_usage(loc).free
    except OSError as exc:  # noqa: BLE001 - a remote data directory is not a failure
        _log(f"tablespace {name}: cannot stat {loc} ({exc}); capacity NOT verified")
        return
    _log(f"tablespace {name} at {loc}: {used / 2 ** 30:,.0f} GiB used, "
         f"{free / 2 ** 30:,.0f} GiB free, shred needs ~{need_bytes / 2 ** 30:,.0f} GiB")
    if free < need_bytes:
        raise SystemExit(
            f"tablespace {name} at {loc} has {free / 2 ** 30:,.1f} GiB free, below the "
            f"{need_bytes / 2 ** 30:,.0f} GiB this shred budgets. Refusing to start: "
            f"filling that volume takes the whole cluster down with it. Free space, "
            f"or pass --tablespace with somewhere that fits.")


def ensure_partitions(conn, schemas: list[str], tablespace: Optional[str] = None) -> None:
    """One leaf partition per (resource_type, schema), created up front so a
    shred never pays for on-the-fly DDL and an empty cell is visibly an empty
    partition rather than an absent one."""
    ts = _ts_clause(tablespace)
    for rtype, tbl in RESOURCE_TABLES.items():
        mid = f"pev_{tbl.name}"
        _exec(conn, f"CREATE TABLE IF NOT EXISTS public.{mid} "
                    f"PARTITION OF public.plannet_element_value "
                    f"FOR VALUES IN ({_lit(rtype)}) PARTITION BY LIST (schema_name)")
        for schema in schemas:
            _exec(conn, f"CREATE UNLOGGED TABLE IF NOT EXISTS "
                        f"public.{mid}_{safe_ident(schema)} "
                        f"PARTITION OF public.{mid} FOR VALUES IN ({_lit(schema)}){ts}")


def relocate_partitions(conn, schemas: list[str], tablespace: Optional[str],
                        tables: Optional[set[str]] = None) -> list[str]:
    """Move EMPTY leaf partitions onto `tablespace`. Returns what was moved.

    Partitions created before this option existed sit wherever the server
    default put them -- for this cluster, a USB HDD. SET TABLESPACE rewrites the
    table, which is free on an empty one and hours on a 13 GB one, so a
    partition that still holds rows is reported and left alone rather than
    silently rewritten under an operator who only asked for a shred.
    """
    if not tablespace:
        return []
    moved: list[str] = []
    for rtype, tbl in RESOURCE_TABLES.items():
        if tables and tbl.name not in tables:
            continue
        for schema in schemas:
            leaf = f"pev_{tbl.name}_{safe_ident(schema)}"
            rows = _fetch(conn, """
                SELECT coalesce(ts.spcname, '(server default)')
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
                WHERE n.nspname = 'public' AND c.relname = %(l)s""", {"l": leaf})
            if not rows or rows[0][0] == tablespace:
                continue
            n = int(_fetch(conn, f"SELECT count(*) FROM public.{leaf}")[0][0])
            if n:
                _log(f"  {leaf} is on {rows[0][0]} and holds {n:,} rows; leaving it "
                     f"there (re-run with --reset-shred to move it)")
                continue
            _exec(conn, f"ALTER TABLE public.{leaf} SET TABLESPACE {safe_ident(tablespace)}")
            conn.commit()
            moved.append(leaf)
    if moved:
        _log(f"moved {len(moved)} empty partition(s) to {tablespace}: {', '.join(moved)}")
    return moved


def build_indexes(conn, tablespace: Optional[str] = None) -> None:
    for name in RETIRED_SHRED_INDEXES:
        _exec(conn, f"DROP INDEX IF EXISTS public.{name}")
        conn.commit()
    ts = _ts_clause(tablespace)
    for name, cols in SHRED_INDEXES:
        t0 = time.time()
        _exec(conn, f"CREATE INDEX IF NOT EXISTS {name} "
                    f"ON public.plannet_element_value {cols}{ts}")
        conn.commit()
        _log(f"index {name} {cols} built in {time.time() - t0:.0f}s"
             + (f" on {tablespace}" if tablespace else ""))


# --------------------------------------------------------------------------
# shred driver
# --------------------------------------------------------------------------

def leaf_partition(schema: str, table: str) -> str:
    return f"public.pev_{safe_ident(table)}_{safe_ident(schema)}"


def assert_shred_shape(conn, schema: str, table: str) -> None:
    """Refuse to RESUME a cell shredded before parent_ordinal existed.

    The two shapes are indistinguishable by row count and differ only in what
    `ordinal` MEANS: flat across the resource in the old one, per parent
    instance in the new. A resume would leave one cell holding both, and
    measuring that reports fabricated cardinality breaches -- the exact failure
    0a exists to remove -- while looking like a clean run. One row with a NULL
    parent_ordinal is proof, so this stops rather than guessing.

    Only the resume path pays for this scan, and only when the partition is
    non-empty: after --reset-shred there is nothing to scan. The audit path gets
    the same guarantee for free out of the measurement pass (see audit_schema).
    """
    leaf = leaf_partition(schema, table)
    if not _fetch(conn, "SELECT 1 FROM pg_class c JOIN pg_namespace n "
                        "ON n.oid = c.relnamespace WHERE n.nspname = 'public' "
                        "AND c.relname = %(l)s", {"l": leaf.split(".", 1)[1]}):
        return
    if _fetch(conn, f"SELECT 1 FROM {leaf} WHERE parent_ordinal IS NULL LIMIT 1"):
        raise SystemExit(
            f"{leaf} holds rows with no parent_ordinal: it was shredded before the "
            f"per-parent cardinality fix, so its `ordinal` counts occurrences flat "
            f"across the whole resource. Measuring it now would fabricate max-"
            f"cardinality breaches. Re-run the shred for this cell with "
            f"--reset-shred.")


def reconcile_progress(conn, schema: str, tables: list[str]) -> list[str]:
    """Drop progress rows whose shred data is no longer there. Returns the
    cells that were invalidated.

    The shred partitions are UNLOGGED; ``ig_shred_progress`` is not. Postgres
    TRUNCATES every unlogged table during crash recovery, so an unclean
    shutdown leaves the progress table claiming a full shred over partitions
    that are empty. A resume would then skip every chunk and the audit would
    run against nothing -- reporting 0% completeness on all 737 elements and
    looking like a clean run. Silent-empty-success is the failure this repo
    has actually been bitten by, so the claim is checked against reality
    before it is trusted, rather than after.

    Two signals, both cheap enough to run every time:
      * pg_postmaster_start_time() newer than the row's -- the server has
        restarted since that chunk was written, which for an unlogged
        partition means the rows are gone. Exact, and it costs nothing.
        NULL means the row predates this column, so it proves nothing and the
        row count decides.
      * the leaf partition holding fewer rows than the progress claims. More
        general -- it also catches a manual TRUNCATE or a partition someone
        dropped and recreated -- and it is the authority when the timestamp
        is unavailable.
    """
    stale: list[str] = []
    for table in tables:
        rows = _fetch(conn, """
            SELECT coalesce(sum(n_values), 0)::bigint, count(*)::bigint,
                   count(*) FILTER (
                       WHERE postmaster_start IS NOT NULL
                         AND postmaster_start < pg_postmaster_start_time())::bigint,
                   count(*) FILTER (WHERE postmaster_start IS NULL)::bigint
            FROM public.ig_shred_progress
            WHERE schema_name = %(s)s AND table_name = %(t)s""",
            {"s": schema, "t": table})
        claimed, n_chunks, n_restarted, n_unknown = (int(x) for x in rows[0])
        if n_chunks == 0:
            continue
        actual = int(_fetch(conn, f"SELECT count(*) FROM {leaf_partition(schema, table)}")[0][0])
        reason = None
        if n_restarted:
            reason = (f"{n_restarted} of {n_chunks} chunk(s) were written before the "
                      f"current postmaster start, and the partition is UNLOGGED")
        elif actual < claimed:
            reason = f"partition holds {actual:,} rows, progress claims {claimed:,}"
        if reason is None:
            assert_shred_shape(conn, schema, table)
            _log(f"  progress OK {schema}.{table}: {n_chunks} chunk(s), "
                 f"{actual:,} rows present vs {claimed:,} claimed"
                 + (f" ({n_unknown} chunk(s) predate the postmaster stamp)"
                    if n_unknown else ""))
            continue
        _log(f"  STALE PROGRESS {schema}.{table}: {reason}. "
             f"Discarding {n_chunks} progress row(s) and re-shredding this cell.")
        _exec(conn, "DELETE FROM public.ig_shred_progress "
                    "WHERE schema_name = %(s)s AND table_name = %(t)s",
              {"s": schema, "t": table})
        if actual:
            _exec(conn, f"TRUNCATE {leaf_partition(schema, table)}")
        conn.commit()
        stale.append(f"{schema}.{table}")
    return stale


def plan_chunks(conn, schema: str, checks: list[ElementCheck], blocks_per_chunk: int,
                only_tables: Optional[set[str]] = None) -> list[tuple]:
    """The (table, rtype, checks, lo, hi) units still to shred for one schema.

    Chunking is by ctid block range, not by element batch: one pass per chunk
    covers ALL of that table's elements, so the JSONB is read once per chunk
    instead of once per element batch. Block ranges also keep each unit
    sequential on disk -- an id-range or hash-bucket split would turn a
    1.86M-row seq scan into random IO.
    """
    by_table: dict[str, list[ElementCheck]] = {}
    for c in checks:
        by_table.setdefault(c.table, []).append(c)

    reconcile_progress(conn, schema,
                       [t for t in by_table if not only_tables or t in only_tables])

    units: list[tuple] = []
    for table, tchecks in by_table.items():
        if only_tables and table not in only_tables:
            continue
        rtype = tchecks[0].resource_type
        nblocks = int(_fetch(conn, "SELECT (pg_relation_size(%(r)s) / 8192)::bigint",
                             {"r": f"{schema}.{table}"})[0][0])
        nrows = _fetch(conn, f"SELECT count(*) FROM "
                             f"{safe_ident(schema)}.{safe_ident(table)}")[0][0]
        if nrows == 0:
            _log(f"{schema}.{table}: 0 rows, nothing to shred")
            continue
        done = {r[0] for r in _fetch(
            conn, "SELECT chunk_lo FROM public.ig_shred_progress "
                  "WHERE schema_name = %(s)s AND table_name = %(t)s",
            {"s": schema, "t": table})}
        bounds = list(range(0, nblocks + 1, blocks_per_chunk)) + [nblocks + 1]
        todo = [(lo, hi) for lo, hi in zip(bounds, bounds[1:]) if lo not in done]
        _log(f"{schema}.{table}: {nrows:,} rows, {nblocks:,} blocks, "
             f"{len(bounds) - 1} chunk(s), {len(done)} already done")
        units.extend((schema, table, rtype, tchecks, lo, hi) for lo, hi in todo)
    return units


def shred_one(conn, unit: tuple) -> int:
    schema, table, rtype, tchecks, lo, hi = unit
    where = f"t.ctid >= '({lo},0)'::tid AND t.ctid < '({hi},0)'::tid"
    sql = build_shred_insert(schema, table, rtype, tchecks, where)
    t0 = time.time()
    n = _exec(conn, sql).rowcount
    secs = time.time() - t0
    _exec(conn, "INSERT INTO public.ig_shred_progress "
                "(schema_name, table_name, chunk_lo, chunk_hi, n_values, seconds, "
                " postmaster_start) "
                "VALUES (%(s)s, %(t)s, %(lo)s, %(hi)s, %(n)s, %(sec)s, "
                "        pg_postmaster_start_time()) "
                "ON CONFLICT (schema_name, table_name, chunk_lo) DO UPDATE "
                "SET chunk_hi = EXCLUDED.chunk_hi, n_values = EXCLUDED.n_values, "
                "seconds = EXCLUDED.seconds, done_at = now(), "
                "postmaster_start = EXCLUDED.postmaster_start",
          {"s": schema, "t": table, "lo": lo, "hi": hi, "n": n, "sec": secs})
    conn.commit()
    _log(f"  {schema}.{table} blocks {lo}-{hi}: {n:,} values in {secs:.0f}s")
    return n


def run_shred(conn, units: list[tuple], workers: int) -> None:
    """Shred `units`, optionally across several connections.

    The shred is CPU-bound on jsonb_path_query_array, not IO-bound -- a trial
    chunk read 637k buffers with 4.5k dirtied, essentially all cache hits. That
    would normally argue for intra-query parallelism, but Postgres will not
    parallelise an INSERT ... SELECT: the INSERT node is parallel-unsafe, so the
    whole plan stays serial no matter what the parallel GUCs say (verified: zero
    parallel workers appear during a chunk). Running independent ctid ranges on
    separate connections is therefore the only way to use more than one core,
    and it is safe because chunks are disjoint block ranges writing to the same
    UNLOGGED partition with no unique index to contend on.
    """
    if workers <= 1 or len(units) <= 1:
        for u in units:
            shred_one(conn, u)
        return

    import queue
    import threading

    q: "queue.Queue[tuple]" = queue.Queue()
    for u in units:
        q.put(u)
    errors: list[BaseException] = []
    engine = get_engine()

    def worker(n: int) -> None:
        try:
            with engine.connect() as wconn:
                configure_session(wconn)
                while True:
                    try:
                        u = q.get_nowait()
                    except queue.Empty:
                        return
                    shred_one(wconn, u)
        except BaseException as exc:  # noqa: BLE001 - surfaced after the join
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(min(workers, len(units)))]
    _log(f"shredding {len(units)} chunk(s) across {len(threads)} connection(s)")
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


def reset_shred(conn, schemas: list[str], tables: Optional[set[str]] = None) -> None:
    """Empty the shred for the given cells and forget their progress.

    TRUNCATE of a leaf partition, not DELETE: the partitions are UNLOGGED and
    hold ~100M rows apiece, so DELETE would generate more work than the shred
    it is undoing. Progress rows are cleared in the same transaction -- a
    truncated partition with surviving progress rows would read as "already
    shredded" and silently produce an empty audit.
    """
    for rtype, tbl in RESOURCE_TABLES.items():
        if tables and tbl.name not in tables:
            continue
        for schema in schemas:
            leaf = f"public.pev_{tbl.name}_{safe_ident(schema)}"
            _exec(conn, f"TRUNCATE {leaf}")
            _exec(conn, "DELETE FROM public.ig_shred_progress "
                        "WHERE schema_name = %(s)s AND table_name = %(t)s",
                  {"s": schema, "t": tbl.name})
    conn.commit()


def write_catalogue(conn, checks: list[ElementCheck], pkg: str,
                    table: str = CATALOGUE_TABLE) -> None:
    """Rewrite the element catalogue from the compiled surface.

    `table` defaults to the production catalogue and exists so a test can point
    the real writer at a fixture table: every downstream number depends on
    min_card / max_n / validity_kind / types landing in the right columns, and
    a test that hand-builds its own catalogue is testing its own transcription.
    """
    _exec(conn, f"DELETE FROM {table}")
    _exec(conn, f"""
        INSERT INTO {table}
          (elem_idx, ig_package, profile, resource_type, table_name, cell,
           element_path, jsonpath, min_card, max_card, max_n, types, primary_type,
           must_support, is_reference, ref_targets, binding_strength,
           binding_valueset, parent_path, parent_idx, ancestor_repeats,
           validity_kind, card_method, notes)
        VALUES (%(elem_idx)s, %(ig_package)s, %(profile)s, %(resource_type)s,
           %(table_name)s, %(cell)s, %(element_path)s, %(jsonpath)s, %(min_card)s,
           %(max_card)s, %(max_n)s, %(types)s, %(primary_type)s, %(must_support)s,
           %(is_reference)s, %(ref_targets)s, %(binding_strength)s,
           %(binding_valueset)s, %(parent_path)s, %(parent_idx)s,
           %(ancestor_repeats)s, %(validity_kind)s, %(card_method)s, %(notes)s)""",
          [{
              "elem_idx": c.idx, "ig_package": pkg, "profile": c.profile,
              "resource_type": c.resource_type, "table_name": c.table, "cell": c.cell,
              "element_path": c.path,
              "jsonpath": (" | ".join(c.jsonpaths) if not c.open_value_parent
                           else f"(any value* key on {c.open_value_parent})"),
              "min_card": c.min, "max_card": c.max_card, "max_n": c.max_n,
              "types": c.types, "primary_type": c.primary_type,
              "must_support": c.must_support, "is_reference": c.is_reference,
              "ref_targets": c.ref_targets, "binding_strength": c.binding_strength,
              "binding_valueset": c.binding_valueset, "parent_path": c.parent_path,
              "parent_idx": c.parent_idx, "ancestor_repeats": c.ancestor_repeats,
              "validity_kind": c.validity_kind, "card_method": c.card_method,
              "notes": c.notes,
          } for c in checks])


# --------------------------------------------------------------------------
# checks over the shred
# --------------------------------------------------------------------------

def denominators(conn, schema: str, table: str) -> dict[tuple[Optional[str], str], int]:
    """(cell, lob) -> row count, straight off the payer table.

    The shred cannot supply this: a resource with no shreddable element at all
    would contribute no shred rows and silently vanish from the denominator.
    """
    rows = _fetch(conn, f"""
        SELECT {LOB_SQL.format(t='t')} AS lob, count(*) AS n,
               count(*) FILTER (WHERE {NETWORK_PRED.format(t='t')}) AS n_net
        FROM {safe_ident(schema)}.{safe_ident(table)} t GROUP BY 1""")
    out: dict[tuple[Optional[str], str], int] = {}
    for lob_v, n, n_net in rows:
        key = lob_v or "(unknown)"
        out[(None, key)] = int(n)
        out[("network", key)] = int(n_net)
        out[("not_network", key)] = int(n) - int(n_net)
    return out


M_ELEM, M_PROFILE, M_LOB, M_PRESENT, M_OCC, M_MAXV, M_MINSAT, M_NOCODING, \
    M_TEXTONLY, M_INVOCC, M_INVROWS, M_PRE0A = range(12)


def min_violation(c: ElementCheck, n_parent_instances: int,
                  n_min_satisfied: int) -> Optional[int]:
    """Parent instances that fall short of `c.min`, or None when min = 0.

    This is 0b. The old rule was `if c.min > 0 and not c.ancestor_repeats:
    n_rows - n_present`, and `ancestor_repeats` asks whether an ancestor
    REPEATS -- never whether it is OPTIONAL. Four elements have min=1 under a
    `min=0, max=1` parent, so every row whose parent was simply absent was
    charged with a violation: ~3,525,000 fabricated "certain violations" across
    9 cells, every one of them legal FHIR.

    Conditioning on parent presence unifies the two cases instead of special-
    casing one. `n_parent_instances` is the row count when the parent IS the
    resource, and the parent element's own occurrence count otherwise; a parent
    that never occurs owes nothing, and the arithmetic says so without a branch.

    Deliberately not clamped at zero. A negative result means the shred holds
    more parent instances than the parent element does -- duplicated rows, which
    a `--blocks` grid mismatch produces on a partition with no unique constraint
    -- and that has to be visible, not rounded away into a clean 0.
    """
    if c.min <= 0:
        return None
    return n_parent_instances - n_min_satisfied


def measure_presence(conn, schema: str, rtype: str, shred: str = SHRED_TABLE,
                     catalogue: str = CATALOGUE_TABLE) -> list[tuple]:
    """Presence, occurrences, cardinality and validity for every element of one
    (schema, resource type), in ONE pass over the shred.

    * n_present -- count(DISTINCT resource_id), and it has to be. The old
      `count(*) FILTER (ordinal = 1 AND coding_ordinal = 1)` was only ever
      correct because `ordinal` was unique per resource; now that ordinals
      restart inside every parent it would count PARENT INSTANCES and call them
      resources. `parent_ordinal = 1` does not rescue it either: if parent
      occurrence 1 has no children and occurrence 2 does, no row carries
      parent_ordinal 1 and the resource vanishes from the numerator entirely.

    * n_occurrences -- deliberately still FLAT, `count(*) FILTER
      (coding_ordinal = 1)` over every row of the element regardless of parent.
      The hierarchical enumeration is a PARTITION of the flat one, so this total
      is preserved by construction -- which is exactly what makes an element's
      own occurrence count valid as its children's parent-instance denominator.

    * n_min_satisfied / n_max_violation -- both are per-parent-instance and
      both are exact, because `ordinal` is now the child's index within its
      parent. A parent instance holds >= min children iff a row exists at
      ordinal = min; it breaches max = N iff a row exists at ordinal N+1. One
      row per parent instance satisfies each, so these stay cheap FILTERs
      rather than a group-and-compare over 100M rows. Parent instances with NO
      children produce no row at all, which is why n_min_violation is computed
      against the parent's own occurrence count in audit_schema() rather than
      here.

    * n_concept_no_coding / n_concept_text_only -- CodeableConcept occurrences
      carrying no `coding`. They score as fully populated and never reach
      measure_codes (which filters code_code IS NOT NULL), so under a required
      binding they are a conformance failure rendered as a clean pass. The
      shred puts `text` in value_text for exactly this case, so the two are
      distinguishable: text-only is a real value nobody coded, no-text-either
      is an empty object.
    """
    return _fetch(conn, presence_sql(shred, catalogue), {"s": schema, "r": rtype})


def presence_sql(shred: str = SHRED_TABLE, catalogue: str = CATALOGUE_TABLE) -> str:
    """The one-pass measurement query. Its target list is positional -- the
    M_* constants index it -- so it is built here where a test can read the
    column order back off the SQL rather than trusting two lists to stay in
    step by eye.

    `shred` / `catalogue` default to the production tables and exist so a test
    can execute THIS query -- the one that writes every number in ig_audit --
    against a small fixture pair instead of hand-rolling a parallel SQL
    statement and comparing the code to its own restatement. A parallel-SQL
    test passes with this function reverted; an execution test does not.
    """
    cases = " ".join(f"WHEN d.validity_kind = {_lit(kind)} THEN ({sql})"
                     for kind, sql in VALIDITY_SQL.items())
    bad = f"(CASE {cases} ELSE false END)"
    concept = "'CodeableConcept' = ANY(d.types)"
    return f"""
        SELECT p.elem_idx AS m_elem, p.profile AS m_profile, p.lob AS m_lob,
               count(DISTINCT p.resource_id) AS n_present,
               count(*) FILTER (WHERE p.coding_ordinal = 1) AS n_occurrences,
               count(*) FILTER (WHERE p.coding_ordinal = 1 AND d.max_n IS NOT NULL
                                AND p.ordinal = d.max_n + 1) AS n_max_violation,
               count(*) FILTER (WHERE p.coding_ordinal = 1 AND d.min_card > 0
                                AND p.ordinal = d.min_card) AS n_min_satisfied,
               count(*) FILTER (WHERE p.coding_ordinal = 1 AND p.code_code IS NULL
                                AND {concept}) AS n_concept_no_coding,
               count(*) FILTER (WHERE p.coding_ordinal = 1 AND p.code_code IS NULL
                                AND p.value_text IS NOT NULL
                                AND {concept}) AS n_concept_text_only,
               count(*) FILTER (WHERE d.validity_kind IS NOT NULL AND {bad}) AS n_invalid_occ,
               count(DISTINCT p.resource_id) FILTER (WHERE d.validity_kind IS NOT NULL
                                AND {bad}) AS n_invalid_rows,
               count(*) FILTER (WHERE p.parent_ordinal IS NULL) AS n_pre0a
        FROM {shred} p
        JOIN {catalogue} d USING (elem_idx)
        WHERE p.schema_name = %(s)s AND p.resource_type = %(r)s
        GROUP BY 1, 2, 3"""


def measure_refs(conn, schema: str, rtype: str) -> list[tuple]:
    """Reference resolution for every Reference element of one resource type.

    Resolves against the SAME schema's table for the referenced Type, one
    target type at a time so each is a single hash join rather than a probe per
    occurrence. Nested-extension references are in here on exactly the same
    footing as top-level ones: they are just rows in the shred.
    """
    types = [r[0] for r in _fetch(conn, """
        SELECT DISTINCT ref_type FROM public.plannet_element_value
        WHERE schema_name = %(s)s AND resource_type = %(r)s AND ref_type IS NOT NULL""",
        {"s": schema, "r": rtype})]
    out: list[tuple] = []
    for target in types:
        tbl = RESOURCE_TABLES.get(target)
        if tbl is None:
            # a reference to a type this payer schema does not carry, or an
            # unparseable/contained reference: every occurrence dangles
            out.extend(_fetch(conn, """
                SELECT elem_idx, profile, lob, ref_type, count(*)::bigint,
                       count(DISTINCT ref_id)::bigint, 0::bigint, count(*)::bigint,
                       0::bigint, 0::bigint
                FROM public.plannet_element_value
                WHERE schema_name = %(s)s AND resource_type = %(r)s AND ref_type = %(tt)s
                GROUP BY 1, 2, 3, 4""", {"s": schema, "r": rtype, "tt": target}))
            continue
        # is_net / is_ods are evaluated in the join's derived table -- once per
        # TARGET row over a thin (id, bool, bool) projection -- never in a
        # FILTER on the joined row. As FILTER expressions on t.resource they
        # forced the entire resource column (whales included) through the hash
        # join and detoasted every matched document twice per occurrence:
        # Location's ref pass alone ran 2h45m before being killed. Same defect
        # class as the shred's cell predicate; same hoist.
        net = ("count(*) FILTER (WHERE t.is_net)::bigint"
               if target == "Organization" else "0::bigint")
        # `ods` is a source system in meta.source, not a line of business. The
        # question this answers is whether anything references the Organizations
        # carrying it -- an unreferenced 73.9% of a table is a provenance dump,
        # not a peer LOB, and the report must not present it as one.
        ods = ("count(*) FILTER (WHERE t.is_ods)::bigint"
               if target == "Organization" else "0::bigint")
        if target == "Organization":
            # OFFSET 0 is load-bearing: without the fence the planner pulls
            # the subquery up, pushes these expressions into a memoized
            # per-row index scan under a SERIAL nested loop, and the pass
            # runs one-core for hours re-detoasting organizations. Fenced,
            # it hash-joins a thin (id, bool, bool) projection built in one
            # parallel scan.
            target_sql = (f"SELECT o.id, {NETWORK_PRED.format(t='o')} AS is_net, "
                          f"{LOB_SQL.format(t='o')} = 'ods' AS is_ods "
                          f"FROM {safe_ident(schema)}.{safe_ident(tbl.name)} o "
                          f"OFFSET 0")
        else:
            target_sql = (f"SELECT o.id "
                          f"FROM {safe_ident(schema)}.{safe_ident(tbl.name)} o")
        out.extend(_fetch(conn, f"""
            SELECT p.elem_idx, p.profile, p.lob, p.ref_type, count(*)::bigint,
                   count(DISTINCT p.ref_id)::bigint,
                   count(*) FILTER (WHERE t.id IS NOT NULL)::bigint,
                   count(*) FILTER (WHERE t.id IS NULL)::bigint,
                   {net}, {ods}
            FROM public.plannet_element_value p
            LEFT JOIN ({target_sql}) t ON t.id = p.ref_id
            WHERE p.schema_name = %(s)s AND p.resource_type = %(r)s AND p.ref_type = %(tt)s
            GROUP BY 1, 2, 3, 4""", {"s": schema, "r": rtype, "tt": target}))
    return out


def measure_codes(conn, schema: str, rtype: str, bound_idx: list[int]) -> list[tuple]:
    if not bound_idx:
        return []
    return _fetch(conn, """
        SELECT elem_idx, profile, lob, coalesce(code_system, '(none)'), code_code,
               count(*)::bigint, count(DISTINCT resource_id)::bigint
        FROM public.plannet_element_value
        WHERE schema_name = %(s)s AND resource_type = %(r)s
          AND code_code IS NOT NULL AND elem_idx = ANY(%(idx)s)
        GROUP BY 1, 2, 3, 4, 5""", {"s": schema, "r": rtype, "idx": bound_idx})


# --------------------------------------------------------------------------
# verification: does the shred hold what the source says it should?
# --------------------------------------------------------------------------

# `@.url == "..."` inside a compiled jsonpath. Every one of these comes verbatim
# from a StructureDefinition, so a URL the data carries and this set does not is
# content the IG never declared -- and a URL this set carries that the data does
# not is a payer fact, not a compiler bug.
SLICE_URL_RE = re.compile(r'@\.url\s*==\s*"([^"]+)"')


# The occurrence recount is keyed by (elem_idx, lob), not elem_idx, because
# n_parent_instances is used PER LOB in audit_schema(): a miscount that moves
# occurrences from one LOB to another leaves the per-element total untouched
# and would pass a total-only comparison while every per-LOB denominator
# downstream of it is wrong. Normalising '' and NULL to '(unknown)' here is the
# same normalisation denominators() and audit_schema() apply, so the keys of
# all three line up.
OccKey = tuple[int, str]


def _occ_key(elem_idx, lob) -> OccKey:
    return (int(elem_idx), lob or "(unknown)")


def source_occurrences(conn, schema: str, table: str, checks: list[ElementCheck],
                       where: str = "true") -> dict[OccKey, int]:
    """(elem_idx, lob) -> occurrence count, counted directly off the source JSONB.

    Independent of the shred in the way that matters: it uses each element's
    ABSOLUTE jsonpath in one flat jsonb_path_query_array, the pre-0a reading.
    The hierarchical shred partitions that same set of occurrences across
    parents, so the totals must agree exactly -- and a `--blocks` grid mismatch
    that duplicates rows into a partition with no unique constraint shows up
    here as an inflated shred count. count(DISTINCT resource_id) structurally
    cannot see that duplication; this can.
    """
    vals = ", ".join(f"({c.idx}, ({arr_expr(c)}), {cell_pred(c.cell)})" for c in checks)
    rows = _fetch(conn, f"""
        SELECT e.elem_idx, {LOB_SQL.format(t='t')} AS lob,
               sum(jsonb_array_length(e.arr))::bigint
        FROM {safe_ident(schema)}.{safe_ident(table)} t
        CROSS JOIN LATERAL (VALUES {vals}) AS e(elem_idx, arr, in_cell)
        WHERE ({where}) AND e.in_cell
        GROUP BY 1, 2""")
    return {_occ_key(a, b): int(n or 0) for a, b, n in rows}


def shred_occurrences(conn, schema: str, rtype: str) -> dict[OccKey, int]:
    """(elem_idx, lob) -> occurrence count as stored in the shred."""
    rows = _fetch(conn, """
        SELECT elem_idx, lob, count(*)::bigint FROM public.plannet_element_value
        WHERE schema_name = %(s)s AND resource_type = %(r)s AND coding_ordinal = 1
        GROUP BY 1, 2""", {"s": schema, "r": rtype})
    return {_occ_key(a, b): int(n) for a, b, n in rows}


def select_occurrences(conn, schema: str, table: str, rtype: str,
                       checks: list[ElementCheck], where: str = "true"
                       ) -> dict[OccKey, int]:
    """(elem_idx, lob) -> occurrence count from the generated shred SELECT,
    unwritten.

    Runs the real hierarchical expression read-only, so the rewrite can be
    proved occurrence-preserving against source_occurrences() without a shred.
    """
    select = build_shred_select(schema, table, rtype, checks, where)
    rows = _fetch(conn, f"SELECT elem_idx, lob, count(*)::bigint FROM ({select}) q "
                        f"WHERE coding_ordinal = 1 GROUP BY 1, 2")
    return {_occ_key(a, b): int(n) for a, b, n in rows}


def compare_occurrences(source: dict[OccKey, int], shred: dict[OccKey, int]
                        ) -> list[tuple[OccKey, int, int]]:
    """((elem_idx, lob), source, shred) for every cell where the two disagree."""
    return [(k, source.get(k, 0), shred.get(k, 0))
            for k in sorted(set(source) | set(shred))
            if source.get(k, 0) != shred.get(k, 0)]


def key_space_inventory(conn, schema: str, table: str, checks: list[ElementCheck],
                        where: str = "true") -> tuple[list[tuple], list[tuple]]:
    """(top-level key census, extension-URL census) for one payer table.

    This is NOT a check on our own paths -- a URL in a compiled jsonpath comes
    verbatim from the StructureDefinition, so a zero on our side is a payer
    fact, not a bug. It is a search for the opposite: content the IG does not
    declare. Vendor extensions and undeclared keys are invisible to an
    IG-driven audit by construction, and "we tested all 737 declared elements"
    says nothing about what else is on the wire.

    The URL census is a RECURSIVE walk (`strict $.**.url`), not a fixed pair of
    depths. It used to be `$.extension[*].url` UNION
    `$.extension[*].extension[*].url` -- two of the six shapes these resources
    actually use -- and a census that only looks where it already expects to
    find something cannot do the one job this function exists for. Measured on
    `bcbs_az_new` (first 3,000 ctid blocks per table), those two paths saw 19
    of the 32 extension URLs present: they missed every URL under
    `meta.extension` (`ex:createdAt`, in ALL EIGHT tables), everything under
    `qualification[*].extension[*]` (so Practitioner -- whose entire extension
    surface is `practitioner-qualification` -- reported zero extension URLs of
    any kind), `address[*].extension` (`geolocation`/`latitude`/`longitude`),
    and `data-absent-reason`.

    Keying on the `url` member rather than on a container path is deliberate:
    it finds an extension wherever a payer hangs it, including places FHIR
    itself does not define one, which is exactly the content an IG-driven audit
    cannot see.

    `checks` must be the checks for THIS table only -- run_verification passes
    its per-table group. Scoping matters: a URL declared on Location and
    appearing on Organization would otherwise score as declared, biasing the
    undeclared count low.

    `where` is the same ctid slice the occurrence recount uses, so
    `--verify-blocks N` means one thing across the whole verification rather
    than bounding one half of it and letting the other walk the table.
    """
    keys = _fetch(conn, f"""
        SELECT k, count(*)::bigint
        FROM {safe_ident(schema)}.{safe_ident(table)} t,
             LATERAL jsonb_object_keys(t.resource) k
        WHERE ({where})
        GROUP BY 1 ORDER BY 2 DESC""")
    urls = _fetch(conn, f"""
        SELECT u #>> '{{}}', count(*)::bigint
        FROM {safe_ident(schema)}.{safe_ident(table)} t,
             LATERAL jsonb_path_query(t.resource, 'strict $.**.url') u
        WHERE ({where}) AND jsonb_typeof(u) = 'string'
        GROUP BY 1""")
    tables = {c.table for c in checks}
    if len(tables) > 1:
        raise ValueError(
            f"key_space_inventory censuses one table at a time, but was given "
            f"checks for {sorted(tables)}; a URL declared on one table and "
            f"appearing on another would score as declared")
    declared = set()
    for c in checks:
        for jp in c.jsonpaths + ([c.parent_jsonpath] if c.parent_jsonpath else []):
            declared.update(SLICE_URL_RE.findall(jp))
    return ([(k, int(n)) for k, n in keys],
            sorted(((u, int(n), u in declared) for u, n in urls),
                   key=lambda r: -r[1]))


def catalogue_coverage(conn, checks: list[ElementCheck]) -> tuple[list, list]:
    """(elements the shred never produced a row for, elements not in the
    catalogue table at all).

    All 737 elem_idx must be reachable. An element that yields zero rows is a
    finding to state explicitly -- it is either genuinely absent from the payer
    data or a path that silently matches nothing, and the report must not let
    the two look the same.
    """
    have = {int(r[0]) for r in _fetch(
        conn, "SELECT DISTINCT elem_idx FROM public.plannet_element_value")}
    catalogued = {int(r[0]) for r in _fetch(
        conn, "SELECT elem_idx FROM public.plannet_element")}
    return ([(c.idx, c.profile, c.path) for c in checks if c.idx not in have],
            [(c.idx, c.profile, c.path) for c in checks if c.idx not in catalogued])


def run_verification(conn, schemas: list[str], checks: list[ElementCheck],
                     blocks: int = 0, tables: Optional[set[str]] = None) -> int:
    """Every post-shred invariant, in one read-only pass. Returns a exit code."""
    problems = 0
    by_table: dict[str, list[ElementCheck]] = {}
    for c in checks:
        by_table.setdefault(c.table, []).append(c)

    missing, uncatalogued = catalogue_coverage(conn, checks)
    _log(f"catalogue coverage: {len(checks) - len(missing)}/{len(checks)} elem_idx "
         f"produced at least one shred row")
    if uncatalogued:
        problems += 1
        _log(f"  {len(uncatalogued)} element(s) MISSING from public.plannet_element:")
        for idx, prof, path in uncatalogued:
            _log(f"    {idx} {prof} {path}")
    for idx, prof, path in missing:
        _log(f"  zero rows: {idx} {prof} {path}")

    for schema in schemas:
        for table, tchecks in by_table.items():
            if tables and table not in tables:
                continue
            rtype = tchecks[0].resource_type
            where = "true"
            if blocks:
                where = f"t.ctid < '({blocks},0)'::tid"
            t0 = time.time()
            src = source_occurrences(conn, schema, table, tchecks, where)
            shred = (shred_occurrences(conn, schema, rtype) if not blocks
                     else select_occurrences(conn, schema, table, rtype, tchecks, where))
            diff = compare_occurrences(src, shred)
            _log(f"{schema}.{table}: {sum(src.values()):,} source occurrences vs "
                 f"{sum(shred.values()):,} shred, {len(diff)} (element, lob) "
                 f"cell(s) disagree ({time.time() - t0:.0f}s)")
            problems += len(diff)
            for (idx, lob), a, b in diff[:40]:
                _log(f"    elem_idx {idx} lob {lob}: source {a:,}, shred {b:,}")

            keys, urls = key_space_inventory(conn, schema, table, tchecks, where)
            undeclared = [(u, n) for u, n, known in urls if not known]
            _log(f"  {len(keys)} distinct top-level keys; {len(urls)} extension URL(s), "
                 f"{len(undeclared)} NOT declared by the IG")
            for u, n in undeclared[:20]:
                _log(f"    undeclared extension {u}: {n:,}")
    return 1 if problems else 0


# --------------------------------------------------------------------------
# audit driver
# --------------------------------------------------------------------------

def audit_rows(run_id: int, schema: str, rtype: str, rchecks: list[ElementCheck],
               lobs: list[str], denom: dict, measured: dict) -> list[dict]:
    """The ig_audit rows for one (schema, resource type), assembled from the
    measurement pass and the payer-table denominators.

    Split out of audit_schema() so it can be exercised directly: this is where
    a per-cell measurement becomes a stored number, where the Network /
    non-Network cell split picks its denominator out of `denom`, and where
    min_violation() gets its `n_parent_instances`. audit_schema() around it is
    all IO.
    """
    rows = []
    for c in rchecks:
        for lob in lobs:
            m = measured.get((c.idx, lob))
            n_rows = denom.get((c.cell, lob), 0)
            # The denominator FHIR actually defines cardinality against.
            # For a root-parented element the parent instance is the
            # resource, so it is the row count; otherwise it is the parent
            # element's own occurrence count, which the same pass measured
            # and which the hierarchical shred guarantees is the number of
            # parent occurrences the children were enumerated under.
            if c.is_root_parented:
                n_parent = n_rows
            else:
                pm = measured.get((c.parent_idx, lob))
                n_parent = int(pm[M_OCC]) if pm else 0
            min_viol = min_violation(c, n_parent, int(m[M_MINSAT]) if m else 0)
            rows.append({
                "run_id": run_id, "schema_name": schema, "profile": c.profile,
                "elem_idx": c.idx, "element_path": c.path, "lob": lob,
                "resource_type": rtype, "n_rows": n_rows,
                "n_present": int(m[M_PRESENT]) if m else 0,
                "n_occurrences": int(m[M_OCC]) if m else 0,
                "n_parent_instances": n_parent,
                "n_min_violation": min_viol,
                "n_max_violation": int(m[M_MAXV]) if m else 0,
                "n_invalid_occ": int(m[M_INVOCC]) if m else 0,
                "n_invalid_rows": int(m[M_INVROWS]) if m else 0,
                "n_concept_no_coding": int(m[M_NOCODING]) if m else 0,
                "n_concept_text_only": int(m[M_TEXTONLY]) if m else 0,
            })
    return rows


def audit_schema(conn, run_id: int, schema: str, checks: list[ElementCheck],
                 vs_codes, vs_systems) -> None:
    by_idx = {c.idx: c for c in checks}
    by_rtype: dict[str, list[ElementCheck]] = {}
    for c in checks:
        by_rtype.setdefault(c.resource_type, []).append(c)

    for rtype, rchecks in by_rtype.items():
        table = rchecks[0].table
        t0 = time.time()
        denom = denominators(conn, schema, table)
        lobs = sorted({k[1] for k in denom}) or ["(unknown)"]

        rows_measured = measure_presence(conn, schema, rtype)
        # The measurement pass already scans the whole partition, so the
        # pre-0a shape check rides along in it for free rather than costing a
        # second full scan of the largest object in the system. Nothing is
        # written until it passes.
        pre0a = sum(int(r[M_PRE0A]) for r in rows_measured)
        if pre0a:
            raise SystemExit(
                f"{leaf_partition(schema, table)} holds {pre0a:,} rows with no "
                f"parent_ordinal: it was shredded before the per-parent cardinality "
                f"fix, so its `ordinal` counts occurrences flat across the whole "
                f"resource and measuring it now would fabricate max-cardinality "
                f"breaches. Re-shred this cell with --reset-shred.")
        measured = {(r[M_ELEM], r[M_LOB] or "(unknown)"): r for r in rows_measured}
        _log(f"{schema}.{rtype}: presence pass {time.time() - t0:.0f}s")

        rows = audit_rows(run_id, schema, rtype, rchecks, lobs, denom, measured)

        _exec(conn, """
            INSERT INTO public.ig_audit (run_id, schema_name, profile, elem_idx,
              element_path, lob, resource_type, n_rows, n_present, n_occurrences,
              n_parent_instances, n_min_violation, n_max_violation, n_invalid_occ,
              n_invalid_rows, n_concept_no_coding, n_concept_text_only)
            VALUES (%(run_id)s, %(schema_name)s, %(profile)s, %(elem_idx)s,
              %(element_path)s, %(lob)s, %(resource_type)s, %(n_rows)s, %(n_present)s,
              %(n_occurrences)s, %(n_parent_instances)s, %(n_min_violation)s,
              %(n_max_violation)s, %(n_invalid_occ)s, %(n_invalid_rows)s,
              %(n_concept_no_coding)s, %(n_concept_text_only)s)
            ON CONFLICT (run_id, schema_name, profile, elem_idx, lob) DO UPDATE SET
              n_rows = EXCLUDED.n_rows, n_present = EXCLUDED.n_present,
              n_occurrences = EXCLUDED.n_occurrences,
              n_parent_instances = EXCLUDED.n_parent_instances,
              n_min_violation = EXCLUDED.n_min_violation,
              n_max_violation = EXCLUDED.n_max_violation,
              n_invalid_occ = EXCLUDED.n_invalid_occ,
              n_invalid_rows = EXCLUDED.n_invalid_rows,
              n_concept_no_coding = EXCLUDED.n_concept_no_coding,
              n_concept_text_only = EXCLUDED.n_concept_text_only,
              measured_at = now()""", rows)

        rrows = []
        for idx, profile, lob, ttype, n_refs, n_dist, n_res, n_dang, n_net, n_ods in \
                measure_refs(conn, schema, rtype):
            c = by_idx[idx]
            declared = {t.replace("plannet-", "") for t in c.ref_targets}
            rrows.append({
                "run_id": run_id, "schema_name": schema, "profile": profile,
                "elem_idx": idx, "element_path": c.path, "lob": lob or "(unknown)",
                "target_type": ttype,
                # plannet-Network IS an Organization on the wire, so a declared
                # Network target is satisfied by an Organization/<id> reference
                "declared_ok": ttype in declared
                               or (ttype == "Organization" and "Network" in declared),
                "n_refs": int(n_refs), "n_distinct": int(n_dist),
                "n_resolved": int(n_res), "n_dangling": int(n_dang),
                "n_to_network": int(n_net), "n_to_ods": int(n_ods),
            })
        if rrows:
            _exec(conn, """
                INSERT INTO public.ig_audit_ref (run_id, schema_name, profile, elem_idx,
                  element_path, lob, target_type, declared_ok, n_refs, n_distinct,
                  n_resolved, n_dangling, n_to_network, n_to_ods)
                VALUES (%(run_id)s, %(schema_name)s, %(profile)s, %(elem_idx)s,
                  %(element_path)s, %(lob)s, %(target_type)s, %(declared_ok)s,
                  %(n_refs)s, %(n_distinct)s, %(n_resolved)s, %(n_dangling)s,
                  %(n_to_network)s, %(n_to_ods)s)
                ON CONFLICT (run_id, schema_name, profile, elem_idx, lob, target_type)
                DO UPDATE SET n_refs = EXCLUDED.n_refs, n_distinct = EXCLUDED.n_distinct,
                  n_resolved = EXCLUDED.n_resolved, n_dangling = EXCLUDED.n_dangling,
                  n_to_network = EXCLUDED.n_to_network,
                  n_to_ods = EXCLUDED.n_to_ods""", rrows)

        bound = [c for c in rchecks if c.binding_valueset
                 and c.binding_strength in ("required", "extensible")]
        crows = []
        for idx, profile, lob, system, code, n_occ, n_r in \
                measure_codes(conn, schema, rtype, [c.idx for c in bound]):
            c = by_idx[idx]
            codes = vs_codes.get(c.binding_valueset)
            systems = vs_systems.get(c.binding_valueset)
            crows.append({
                "run_id": run_id, "schema_name": schema, "profile": profile,
                "elem_idx": idx, "element_path": c.path, "lob": lob or "(unknown)",
                "code": code, "system": system, "strength": c.binding_strength,
                "n_occurrences": int(n_occ), "n_rows": int(n_r),
                "code_member": (code in codes) if codes is not None else None,
                "system_member": ((system in systems) if systems is not None
                                  and system != "(none)" else None),
            })
        if crows:
            _exec(conn, """
                INSERT INTO public.ig_audit_code (run_id, schema_name, profile, elem_idx,
                  element_path, lob, code, system, strength, n_occurrences, n_rows,
                  code_member, system_member)
                VALUES (%(run_id)s, %(schema_name)s, %(profile)s, %(elem_idx)s,
                  %(element_path)s, %(lob)s, %(code)s, %(system)s, %(strength)s,
                  %(n_occurrences)s, %(n_rows)s, %(code_member)s, %(system_member)s)
                ON CONFLICT (run_id, schema_name, profile, elem_idx, lob, code, system)
                DO UPDATE SET n_occurrences = EXCLUDED.n_occurrences,
                  n_rows = EXCLUDED.n_rows, code_member = EXCLUDED.code_member,
                  system_member = EXCLUDED.system_member""", crows)

        conn.commit()
        _log(f"{schema}.{rtype}: audited in {time.time() - t0:.0f}s "
             f"({len(rows)} element rows, {len(rrows)} ref rows, {len(crows)} code rows)")


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def load_surface(pkg: str = PKG_DIR, cache=DEFAULT_CACHE):
    """(checks, closure canonical index, closure package list) for the IG."""
    elements = build(pkg)
    packages = resolve_closure(cache, PLAN_NET)
    index = LazyIndex(cache, packages)
    sds = {d["name"]: d for d in load_structure_definitions(pkg) if d.get("name")}
    return compile_checks(elements, index, sds), index, packages


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Plan-Net IG audit: shred, then query")
    ap.add_argument("--schema", action="append", default=None)
    ap.add_argument("--pkg", default=PKG_DIR)
    ap.add_argument("--stage", default="all",
                    choices=["all", "closure", "catalogue", "shred", "index",
                             "audit", "verify"])
    ap.add_argument("--table", action="append", default=None,
                    help="restrict the shred to these payer tables")
    ap.add_argument("--workers", type=int, default=4,
                    help="concurrent shred connections (Postgres cannot "
                         "parallelise INSERT ... SELECT internally)")
    ap.add_argument("--blocks", type=int, default=20_000,
                    help="ctid blocks per resumable shred chunk (~1GB)")
    ap.add_argument("--resume-run", type=int, default=None)
    ap.add_argument("--reset-shred", action="store_true",
                    help="TRUNCATE the shred for the selected schemas/tables first")
    ap.add_argument("--tablespace", default=DEFAULT_TABLESPACE,
                    help="tablespace for the shred leaf partitions; falls back to "
                         "the server default when it does not exist here")
    ap.add_argument("--index-tablespace", default=DEFAULT_INDEX_TABLESPACE,
                    help="tablespace for the shred indexes")
    ap.add_argument("--verify-blocks", type=int, default=0,
                    help="verification: recount only the first N ctid blocks, "
                         "against the generated SELECT rather than the stored "
                         "shred. Applies to --stage verify and to the "
                         "verification --stage all runs before the audit; 0 = "
                         "the whole table")
    args = ap.parse_args(argv)
    schemas = args.schema or ["bcbs_az", "bcbs_az_new"]

    checks, index, packages = load_surface(args.pkg)
    _log(f"compiled {len(checks)} checks across {len({c.profile for c in checks})} "
         f"profiles; closure = {len(packages)} packages, {len(index)} canonicals")

    if args.stage == "closure":
        from ig_closure import format_report, verify
        print(format_report(verify(element_paths={(c.profile, c.path) for c in checks})))
        return 0

    vs_codes: dict[str, Optional[set[str]]] = {}
    vs_systems: dict[str, Optional[set[str]]] = {}
    for c in checks:
        vs = c.binding_valueset
        if vs and c.binding_strength in ("required", "extensible") and vs not in vs_codes:
            vs_codes[vs] = expand(index, vs)
            vs_systems[vs] = valueset_systems(index, vs)
    _log(f"expanded {sum(1 for v in vs_codes.values() if v is not None)}/{len(vs_codes)}"
         f" bound value sets")

    engine = get_engine()
    only = set(args.table) if args.table else None
    with engine.connect() as conn:
        configure_session(conn)
        _exec(conn, SHRED_DDL)
        _exec(conn, AUDIT_DDL)
        heap_ts = resolve_tablespace(conn, args.tablespace)
        idx_ts = resolve_tablespace(conn, args.index_tablespace)
        ensure_partitions(conn, schemas, heap_ts)
        conn.commit()

        if args.stage == "verify":
            return run_verification(conn, schemas, checks, args.verify_blocks, only)

        if args.stage in ("all", "catalogue"):
            write_catalogue(conn, checks, args.pkg)
            conn.commit()
            _log(f"catalogue: {len(checks)} elements in public.plannet_element")

        if args.reset_shred:
            reset_shred(conn, schemas, only)
            _log("shred reset for the selected cells")

        if args.stage in ("all", "shred"):
            check_tablespace_capacity(conn, heap_ts)
            relocate_partitions(conn, schemas, heap_ts, only)
            units: list[tuple] = []
            for schema in schemas:
                units.extend(plan_chunks(conn, schema, checks, args.blocks, only))
            t0 = time.time()
            run_shred(conn, units, args.workers)
            _log(f"shred finished in {(time.time() - t0) / 60:.1f} min")

        if args.stage in ("all", "index"):
            build_indexes(conn, idx_ts)
            _exec(conn, "ANALYZE public.plannet_element_value")
            conn.commit()

        if args.stage == "all":
            # The audit's denominators are only as good as the shred beneath
            # them, and until now nothing consumed that verdict: `verify` was
            # a mutually-exclusive branch whose exit code went nowhere, so a
            # full `all` run could shred wrong, audit the wrong shred, and
            # report the result with no gate anywhere in the path. Refuse to
            # write an audit over a shred that does not reconcile.
            if run_verification(conn, schemas, checks, args.verify_blocks, only):
                raise SystemExit(
                    "verification FAILED (see the lines above): the shred does "
                    "not reconcile with the source JSONB, so every number the "
                    "audit would write is measured against a denominator that "
                    "is already known to be wrong. No audit was recorded.\n"
                    "Fix: re-shred the disagreeing cells, e.g.\n"
                    "  python scripts/audit_plannet.py --stage shred --schema "
                    f"{schemas[0]} --blocks {args.blocks} --reset-shred\n"
                    "then re-run. To inspect without re-shredding: --stage verify.")

        if args.stage in ("all", "audit"):
            if args.resume_run:
                run_id = args.resume_run
            else:
                run_id = _fetch(conn,
                    "INSERT INTO public.ig_audit_run (ig_package, schemas, n_elements, "
                    "notes) VALUES (%(p)s, %(s)s, %(n)s, %(notes)s) RETURNING run_id",
                    {"p": args.pkg, "s": schemas, "n": len(checks),
                     "notes": json.dumps({"lob_labels": LOB_LABELS,
                                          "packages": [f"{n}#{v}" for n, v in packages]})}
                )[0][0]
                conn.commit()
            _log(f"audit run {run_id}")
            for schema in schemas:
                audit_schema(conn, run_id, schema, checks, vs_codes, vs_systems)
            _exec(conn, "UPDATE public.ig_audit_run SET finished_at = now() "
                        "WHERE run_id = %(r)s", {"r": run_id})
            conn.commit()
            _log(f"run {run_id} complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

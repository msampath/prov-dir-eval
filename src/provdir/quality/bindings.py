"""DB-side exhaustive terminology-binding membership checker.

The HL7 validator (``scripts/validate_conformance.py``) checks binding
membership too, but only on a few-thousand-instance sample per cell. This
module expands the same Plan-Net value sets OFFLINE (no terminology server,
no validator, no Docker) and checks membership against every row of the
extracted code columns -- 100% of the data, including the 500M-row slim
``aetna_cvs.practitioner_role`` table, whose extracted columns are populated
even though its ``resource`` JSONB is a stub.

KEY CONSTRAINT -- code-only membership: ``etl/transform.py``'s ``_codes()``
extracts CODE VALUES ONLY into the array/text columns this module scans; the
coding ``system`` is discarded. So this is a code-level check: a right code
under the WRONG system passes here (e.g. a NUCC code accidentally coded under
a different CodeSystem). The validator's sampled pass is what catches
system-correctness; this module's numbers should always be read next to it,
never as a full replacement. Documented again on :func:`expand` below since
that is where the consequence (systems dropped from the returned set) is
concrete.

Package loading: value sets, code systems, and profile StructureDefinitions
are read from the local npm-style FHIR package cache
(``F:/fhir_cache/packages/<name>#<version>/package/*.json``), the same layout
``scripts/validate_conformance.py`` populates via its ``-ig``/``--pkg-cache``
Docker mount. :func:`ensure_packages` downloads any package missing from that
cache straight from packages.fhir.org, so this module works standalone even
before/without a validator run.
"""

from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path
from typing import NamedTuple, Optional

import httpx
import psycopg
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import ARRAY

from ..models import (
    endpoint_resource,
    healthcare_service,
    insurance_plan,
    location,
    organization,
    organization_affiliation,
    practitioner,
    practitioner_role,
)

DEFAULT_CACHE = Path("F:/fhir_cache/packages")

# Packages, in resolution-precedence order. Order matters for load_index():
# plan-net's own canonicals shadow anything a dependency (us.core, r4.core)
# also happens to define at the same URL, mirroring the validator's own -ig
# load order (plan-net is the profile source of truth; the others fill in
# base FHIR / US Core value sets and code systems Plan-Net's bindings point
# at, e.g. v2-0360 for Practitioner.qualification.code).
PLAN_NET = ("hl7.fhir.us.davinci-pdex-plan-net", "1.2.0")
R4_CORE = ("hl7.fhir.r4.core", "4.0.1")
THO = ("hl7.terminology.r4", "6.5.0")
# us.core's version is NOT hardcoded here -- it is read from plan-net's own
# package.json "dependencies" in ensure_packages() (currently resolves to
# 3.1.1), so a future plan-net release that bumps its us.core dependency is
# picked up automatically instead of silently pinning a stale version.
US_CORE_NAME = "hl7.fhir.us.core"

PROFILE_BASE = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"


def _pkg_dir(cache: Path, name: str, version: str) -> Path:
    return cache / f"{name}#{version}"


def _download_package(cache: Path, name: str, version: str) -> None:
    """Fetch and extract one package tarball into `cache`/<name>#<version>/package/."""
    url = f"https://packages.fhir.org/{name}/{version}"
    resp = httpx.get(url, follow_redirects=True, timeout=120.0)
    resp.raise_for_status()
    pkg_dir = _pkg_dir(cache, name, version)
    pkg_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tf:
        tf.extractall(pkg_dir)  # tarball's own top-level entry is "package/"


def _ensure_one(cache: Path, name: str, version: str) -> None:
    if (_pkg_dir(cache, name, version) / "package").is_dir():
        return
    _download_package(cache, name, version)


def _read_us_core_version(cache: Path) -> str:
    pkg_json = _pkg_dir(cache, *PLAN_NET) / "package" / "package.json"
    deps = json.loads(pkg_json.read_text(encoding="utf-8")).get("dependencies", {})
    version = deps.get(US_CORE_NAME)
    if not version:
        raise RuntimeError(
            f"{US_CORE_NAME} not found in plan-net's package.json dependencies; "
            "cannot resolve which us.core version to load"
        )
    return version


def ensure_packages(cache: Path = DEFAULT_CACHE) -> list[tuple[str, str]]:
    """Ensure all 4 FHIR packages exist under `cache`, downloading any that are
    missing. Returns [(name, version), ...] in load-precedence order.

    Safe to call every run: each package is checked (not re-downloaded) once
    its `package/` directory exists, so a warm cache costs one stat per
    package and this is cheap to call unconditionally rather than plumbing a
    "did I already do this" flag through every caller.
    """
    cache.mkdir(parents=True, exist_ok=True)
    _ensure_one(cache, *PLAN_NET)
    us_core_version = _read_us_core_version(cache)
    _ensure_one(cache, US_CORE_NAME, us_core_version)
    _ensure_one(cache, *R4_CORE)
    _ensure_one(cache, *THO)
    return [PLAN_NET, (US_CORE_NAME, us_core_version), R4_CORE, THO]


def load_index(cache: Path = DEFAULT_CACHE,
               packages: Optional[list[tuple[str, str]]] = None) -> dict[str, dict]:
    """canonical url -> resource, merged across `packages` (default: the 4
    packages via :func:`ensure_packages`). Every ValueSet, CodeSystem, and
    StructureDefinition resource in each package is indexed by its `.url`.

    The FIRST package in `packages` order that defines a given url wins on a
    collision -- passing plan-net first (the default order) means its own
    canonicals shadow anything a downstream dependency also happens to
    define at the same url.
    """
    if packages is None:
        packages = ensure_packages(cache)
    index: dict[str, dict] = {}
    for name, version in packages:
        pkg_dir = _pkg_dir(cache, name, version) / "package"
        if not pkg_dir.is_dir():
            continue
        for fn in sorted(pkg_dir.glob("*.json")):
            if fn.name == "package.json":
                continue
            try:
                res = json.loads(fn.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(res, dict):
                continue
            if res.get("resourceType") not in ("ValueSet", "CodeSystem", "StructureDefinition"):
                continue
            url = res.get("url")
            if not url or url in index:
                continue  # first package wins
            index[url] = res
    return index


def binding_for(profile_sd: dict, element_path: str) -> Optional[tuple[str, str]]:
    """(valueSet_url, strength) for the binding on `element_path` in
    `profile_sd`'s snapshot, or None if the element does not exist or carries
    no binding. Strengths are read from the StructureDefinition at call time
    -- never hardcoded -- so a profile revision that changes a binding's
    strength is picked up automatically without touching this module.

    Matches the FIRST snapshot element whose `.path` equals `element_path`
    and which itself carries a `.binding.valueSet` (a differential slice can
    repeat a path without repeating the binding; this skips past those).
    """
    for ed in ((profile_sd.get("snapshot") or {}).get("element") or []):
        if ed.get("path") != element_path:
            continue
        binding = ed.get("binding") or {}
        vs = binding.get("valueSet")
        if vs:
            return vs.split("|", 1)[0], binding.get("strength", "example")
    return None


_SUPPORTED_FILTER_OPS = {"is-a", "descendent-of"}


def _all_codes(concepts: list[dict]) -> set[str]:
    """Every `.code` in a CodeSystem's concept list, walking nested `.concept`
    children (hierarchical code systems nest; flat ones, like THO's v3 code
    systems, do not -- this handles both)."""
    out: set[str] = set()
    for c in concepts:
        if "code" in c:
            out.add(c["code"])
        if "concept" in c:
            out |= _all_codes(c["concept"])
    return out


def _filter_codes(cs: dict, filt: dict) -> Optional[set[str]]:
    """Codes satisfying one `is-a`/`descendent-of` compose.include.filter
    against CodeSystem `cs`, or None if unsupported / undecidable locally.

    THO's v3 code systems (e.g. v3-RoleCode, whose
    v3-ServiceDeliveryLocationRoleType subset drives Location.type) express
    their hierarchy as a FLAT concept list annotated with a `subsumedBy`
    concept property (code -> parent code), not nested `.concept` children.
    Genuinely hierarchical code systems instead (or additionally) nest
    `.concept` directly. Both are walked here so either encoding resolves the
    same descendant set.
    """
    op = filt.get("op")
    value = filt.get("value")
    if op not in _SUPPORTED_FILTER_OPS or filt.get("property") != "concept" or not value:
        return None
    if cs.get("content") != "complete":
        # A partial/fragment CodeSystem cannot be trusted to enumerate every
        # descendant edge -- treating it as complete would silently
        # UNDER-report nonmembers. Same "give up cleanly" posture as any
        # other unsupported shape.
        return None

    parent_to_children: dict[str, list[str]] = {}
    all_codes: set[str] = set()

    def index_tree(concepts: list[dict], parent_code: Optional[str]) -> None:
        for c in concepts:
            code = c.get("code")
            if code is None:
                continue
            all_codes.add(code)
            if parent_code is not None:
                parent_to_children.setdefault(parent_code, []).append(code)
            for p in c.get("property", []) or []:
                if p.get("code") == "subsumedBy" and p.get("valueCode"):
                    parent_to_children.setdefault(p["valueCode"], []).append(code)
            if "concept" in c:
                index_tree(c["concept"], code)

    index_tree(cs.get("concept", []) or [], None)
    if value not in all_codes:
        return None  # the filter's anchor code is not IN this codesystem at all

    descendants: set[str] = set()
    stack = [value]
    while stack:
        cur = stack.pop()
        for child in parent_to_children.get(cur, []):
            if child not in descendants:
                descendants.add(child)
                stack.append(child)
    if op == "is-a":
        descendants.add(value)
    return descendants


def _expand_include(index: dict[str, dict], inc: dict,
                    seen: frozenset[str]) -> Optional[set[str]]:
    if "valueSet" in inc:
        # A single include entry naming >=1 valueSets contributes their
        # INTERSECTION (FHIR compose.include semantics); in every case seen
        # in the Plan-Net/THO packages this list has exactly one entry, so it
        # degenerates to that value set's own expansion, but intersection is
        # implemented generally rather than assuming length 1.
        parts = [expand(index, url, seen) for url in inc["valueSet"]]
        if not parts or any(p is None for p in parts):
            return None
        result = parts[0]
        for p in parts[1:]:
            result = result & p
        return result
    if "concept" in inc:
        return {c["code"] for c in inc["concept"] if "code" in c}
    if "system" in inc:
        cs = index.get(inc["system"])
        if "filter" in inc:
            if cs is None or cs.get("resourceType") != "CodeSystem":
                return None
            codes: Optional[set[str]] = None
            for f in inc["filter"]:
                part = _filter_codes(cs, f)
                if part is None:
                    return None
                codes = part if codes is None else (codes & part)
            return codes if codes is not None else set()
        # No concept, no filter: the WHOLE code system. Only decidable
        # locally when the package enumerates it completely -- Plan-Net's own
        # 14 CodeSystems all do (content=complete), which is exactly why
        # -tx n/a still caught 12k real binding errors in the validator run.
        if cs is None or cs.get("resourceType") != "CodeSystem" or cs.get("content") != "complete":
            return None
        return _all_codes(cs.get("concept", []) or [])
    return None  # a compose.include shape this offline expander does not support


def expand(index: dict[str, dict], vs_url: str,
          _seen: Optional[frozenset[str]] = None) -> Optional[set[str]]:
    """Expand a FHIR ValueSet to its member CODES ONLY -- coding `system`s are
    dropped, matching the extracted DB columns this checker scans (see the
    module docstring's code-only-membership caveat). Returns None when the
    compose uses a construct this offline expander cannot resolve locally;
    callers MUST record status='unexpanded' for that element rather than
    treating None as an empty set -- an empty set would silently flag every
    coded row as a binding failure.

    Supported compose.include/.exclude shapes:
      * inline `.concept` lists
      * whole-CodeSystem includes (bare `.system`) when that CodeSystem's
        `content` is 'complete' -- its nested `.concept` tree is walked fully
      * `.valueSet` references, expanded recursively (intersected within one
        include entry, unioned across entries, per FHIR compose semantics)
      * `.exclude` entries (any of the above shapes), subtracted from the
        union of `.include`
      * `is-a`/`descendent-of` `.filter`s over a 'complete' CodeSystem's
        hierarchy (nested `.concept` children AND `subsumedBy` properties --
        see :func:`_filter_codes`)
    Any other filter operator, or a `.compose` this module does not
    recognize, returns None rather than guessing.
    """
    vs_url = vs_url.split("|", 1)[0]
    seen = _seen or frozenset()
    if vs_url in seen:
        return set()  # cycle guard: a self-referential compose adds nothing new
    seen = seen | {vs_url}

    vs = index.get(vs_url)
    if vs is None or vs.get("resourceType") != "ValueSet":
        return None
    compose = vs.get("compose")
    if not compose:
        return None

    codes: set[str] = set()
    for inc in compose.get("include", []) or []:
        part = _expand_include(index, inc, seen)
        if part is None:
            return None
        codes |= part
    for exc in compose.get("exclude", []) or []:
        part = _expand_include(index, exc, seen)
        if part is None:
            return None
        codes -= part
    return codes


class Check(NamedTuple):
    """One (extracted column, profile element) binding to verify.

    `table`/`column` are SQLAlchemy objects from models.py, not fresh
    strings, so the identifiers scripts/check_bindings.py interpolates into
    SQL are exactly the ones models.py already asserted safe at import
    (_SAFE_ID) -- see the module-level assertion loop below, which further
    confirms `column` actually exists on `table`.
    """
    table: Table
    column: str
    cell_where: Optional[str]  # fixed SQL fragment restricting rows within the table, or None
    resource_type: str         # FHIR resource type this cell represents on the wire
    element_path: str          # ElementDefinition.path the binding is read from
    profile_id: str            # plan-net StructureDefinition id; PROFILE_BASE + this = canonical url


# The ~15 extracted-column x profile-element bindings covered by this
# checker. Organization/Network is one table split by is_network, same as
# sample_for_validation.py's CELL_SPECS -- Plan-Net profiles a network as its
# own plannet-Network (an Organization with type=ntwk), so it needs its own
# row rather than being folded into the Organization check.
CHECKS: list[Check] = [
    Check(organization, "type_codes", "coalesce(is_network,0)=0",
         "Organization", "Organization.type", "plannet-Organization"),
    Check(organization, "type_codes", "is_network=1",
         "Network", "Organization.type", "plannet-Network"),
    Check(practitioner, "qualification_codes", None,
         "Practitioner", "Practitioner.qualification.code", "plannet-Practitioner"),
    Check(practitioner_role, "role_codes", None,
         "PractitionerRole", "PractitionerRole.code", "plannet-PractitionerRole"),
    Check(practitioner_role, "specialty_codes", None,
         "PractitionerRole", "PractitionerRole.specialty", "plannet-PractitionerRole"),
    Check(location, "type_codes", None,
         "Location", "Location.type", "plannet-Location"),
    Check(healthcare_service, "category_codes", None,
         "HealthcareService", "HealthcareService.category", "plannet-HealthcareService"),
    Check(healthcare_service, "type_codes", None,
         "HealthcareService", "HealthcareService.type", "plannet-HealthcareService"),
    Check(healthcare_service, "specialty_codes", None,
         "HealthcareService", "HealthcareService.specialty", "plannet-HealthcareService"),
    Check(organization_affiliation, "role_codes", None,
         "OrganizationAffiliation", "OrganizationAffiliation.code", "plannet-OrganizationAffiliation"),
    Check(organization_affiliation, "specialty_codes", None,
         "OrganizationAffiliation", "OrganizationAffiliation.specialty", "plannet-OrganizationAffiliation"),
    Check(insurance_plan, "type_codes", None,
         "InsurancePlan", "InsurancePlan.type", "plannet-InsurancePlan"),
    Check(insurance_plan, "plan_type_codes", None,
         "InsurancePlan", "InsurancePlan.plan.type", "plannet-InsurancePlan"),
    Check(endpoint_resource, "connection_type", None,
         "Endpoint", "Endpoint.connectionType", "plannet-Endpoint"),
    Check(endpoint_resource, "payload_types", None,
         "Endpoint", "Endpoint.payloadType", "plannet-Endpoint"),
]

for _chk in CHECKS:
    if _chk.column not in _chk.table.c:
        raise ValueError(f"binding check references unknown column "
                         f"{_chk.table.name}.{_chk.column}")


def is_array_column(chk: Check) -> bool:
    """True for the extracted array columns (specialty_codes, ...); False
    for the one scalar check (endpoint_resource.connection_type is Text, not
    ARRAY -- see models.py). Derived from the column's actual SQLAlchemy
    type rather than hardcoded per-check, so it can never drift from the
    schema."""
    return isinstance(chk.table.c[chk.column].type, ARRAY)


def resolve_binding(index: dict[str, dict], chk: Check) -> tuple[str, str]:
    """(value_set_url, strength) for `chk`, resolved from its profile's
    snapshot. Raises if the profile or the element/binding cannot be found --
    unlike expand()'s None-on-unsupported-construct, a missing binding here
    means CHECKS itself is out of sync with the loaded package (a code bug,
    not a data condition), so this fails loudly rather than writing a
    placeholder row.
    """
    profile_sd = index.get(PROFILE_BASE + chk.profile_id)
    if profile_sd is None:
        raise KeyError(f"profile not found in loaded packages: {chk.profile_id}")
    resolved = binding_for(profile_sd, chk.element_path)
    if resolved is None:
        raise KeyError(f"no binding found for {chk.element_path} on {chk.profile_id}")
    return resolved


def payer_binding_rates(conn: psycopg.Connection, run_id: int) -> list[dict]:
    """Per-payer row-weighted binding rate from a check_bindings.py run.

    Only `status='ok'` elements contribute -- 'unexpanded'/'skipped-example'
    rows are written with all-zero counts (see scripts/check_bindings.py) and
    must never enter this sum, or an unexpandable value set would silently
    read as a 100%-clean element. `required` is the headline (Plan-Net
    conformance failures); `extensible` is reported as a separate, advisory
    column, matching the binding strength's own meaning.

    Row-weighted, not check-averaged: a payer's rate reflects the share of
    its ACTUAL rows with a nonmember code, not an unweighted average across
    checks whose row counts can differ by orders of magnitude.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payer_id,
                   sum(rows_with_codes) FILTER (WHERE strength = 'required')::float8 AS req_n,
                   sum(rows_with_nonmember) FILTER (WHERE strength = 'required')::float8 AS req_bad,
                   sum(rows_with_codes) FILTER (WHERE strength = 'extensible')::float8 AS ext_n,
                   sum(rows_with_nonmember) FILTER (WHERE strength = 'extensible')::float8 AS ext_bad
            FROM public.binding_check_element
            WHERE run_id = %s AND status = 'ok'
            GROUP BY payer_id ORDER BY payer_id
            """,
            (run_id,),
        )
        out = []
        for payer, req_n, req_bad, ext_n, ext_bad in cur.fetchall():
            out.append({
                "payer_id": payer,
                "rows_with_codes_required": req_n,
                "binding_rate_required":
                    round(100.0 * (1 - req_bad / req_n), 1) if req_n else None,
                "rows_with_codes_extensible": ext_n,
                "binding_rate_extensible":
                    round(100.0 * (1 - ext_bad / ext_n), 1) if ext_n else None,
            })
        return out


def system_code_inventory(conn: psycopg.Connection, run_id: int,
                          limit: int = 50) -> list[tuple]:
    """Top nonmember (resource_type, element, code) triples by row count,
    with payer breadth -- the "what codes are actually failing" inventory
    for the report, mirroring conformance.py's unknown_systems()."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT resource_type, element, code,
                   sum(n_rows)::bigint AS rows, count(DISTINCT payer_id) AS payers
            FROM public.binding_check_code
            WHERE run_id = %s AND NOT in_valueset
            GROUP BY 1, 2, 3 ORDER BY rows DESC LIMIT %s
            """,
            (run_id, limit),
        )
        return cur.fetchall()

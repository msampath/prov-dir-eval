"""Finding rollup for the bcbs_az_new external quality report (run 14).

The external unit of presentation is a *finding*: one defect pattern, counted
once per FHIR resource it touches, with a repeats-per-resource multiplier for
arrays. Raw ``conformance_issue`` rows stay in the DB as evidence; this module
collapses ~31.3M rows to ~5-figure findings by keying on
``(resource_type, element_path, message_id, severity)``.

Element path comes from normalizing ``conformance_issue.expression`` (raw
FHIRPath) into the ``public.plannet_element.element`` universe held by Track A.
The normalizer is Python string surgery, not a real FHIRPath parser, and its
tripwire is the ``unmatched_rate`` gate: if too many normalized paths fail to
join to ``plannet_element``, the build fails rather than reporting a plausible
number backed by a broken normalizer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable

# ---- Rule-5 choice-type suffixes ---------------------------------------
# FHIR R4 primitive + complex types that appear as `.value<Type>` in the
# validator's concrete FHIRPath output. Extension.value[x] compiles to any of
# these; the report joins on Track A's canonical `value[x]` element path.
CHOICE_TYPE_SUFFIXES = (
    # Primitives
    "String", "Boolean", "Integer", "Decimal", "Uri", "Url", "Canonical",
    "Base64Binary", "Instant", "Date", "DateTime", "Time", "Code", "Oid", "Id",
    "Markdown", "UnsignedInt", "PositiveInt", "Uuid",
    # Common complex types (order irrelevant; membership only)
    "Address", "Age", "Annotation", "Attachment", "CodeableConcept", "Coding",
    "ContactPoint", "Count", "Distance", "Duration", "HumanName", "Identifier",
    "Money", "Period", "Quantity", "Range", "Ratio", "Reference", "SampledData",
    "Signature", "Timing", "Meta", "ContactDetail", "Contributor",
    "DataRequirement", "Expression", "ParameterDefinition", "RelatedArtifact",
    "TriggerDefinition", "UsageContext", "Dosage", "Extension",
)
_CHOICE_TYPE_RE = re.compile(
    r"\.value(?:" + "|".join(CHOICE_TYPE_SUFFIXES) + r")(?=[\.\[]|$)"
)

# Rule-2 terminal navigator functions. Dropped from the tail only.
_NAV_FUNCS = ("first", "last", "tail", "single", "exists", "hasValue",
              "empty", "count", "not", "toString", "value")
_NAV_TAIL_RE = re.compile(
    r"\.(?:" + "|".join(_NAV_FUNCS) + r")\(\s*\)(?=$)"
)

# Rule-4 Extension-context predicate: any `extension` segment anywhere earlier
# in the RAW input path. HL7 validator output in the wild uses BOTH forms:
#   `extension('URL')` / `extension("URL")` / `extension(<URL>)` — URL-callable
#   `extension[N]`     / `extension[N].extension[M]` — bracket-indexed
# Run 14 emitted exclusively the bracket-indexed form (0 URL-callable);
# a URL-only predicate silently dropped `.value[x]` on ~9.6M rows in that
# run. This one matches either form via a bare `.extension` segment marker.
_EXTENSION_ANY_RE = re.compile(
    r"(?:^|\.)extension(?:\(|\[|\.|$)"
)

# Rule-7 array indices (including bare []).
_INDEX_RE = re.compile(r"\[\d*\]")

# Rule-6 quoted extension URL, captured for canonical rewrite to angle-bracket.
_EXT_QUOTED_URL_RE = re.compile(
    r"extension\(\s*['\"]([^'\"]+)['\"]\s*\)"
)


# ---- Tripwire constants -------------------------------------------------
# TRIPWIRE_THRESHOLD is the maximum fraction of *distinct* normalized paths
# that may fail to join public.plannet_element. Above this, the build fails
# with the top-20 unmatched paths rather than reporting numbers backed by a
# broken normalizer.
#
# Baseline provenance: measured on run 14 during the dry-run step; the
# initial value below is a conservative placeholder pending that measurement.
# When re-baselined, update both the value and this docstring with the
# commit SHA of the measurement.
TRIPWIRE_THRESHOLD = 0.75
# Baseline measurement 2026-08-30 on run 14: 108 distinct normalized paths;
# 33 join to plannet_element, 75 do not. Unmatched-by-distinct-path = 0.694.
# The 75 out-of-universe paths are (a) base-FHIR structural elements Plan-Net
# does not enumerate (Organization.type.coding.system, Practitioner.address.
# period.end), and (b) URL-less extension chains the validator emits
# (PractitionerRole.extension.extension.value.coding.system) that
# plannet_element records slice-qualified (PractitionerRole.extension:
# qualification.extension:whereValid.value[x].coding.system) so string
# equality would require slice-collapse on the Plan-Net side. Both are scope
# realities, not normalizer bugs. Threshold 0.75 gives +5% margin over the
# measured baseline. Occurrence-weighted coverage (fraction of issue ROWS
# that fall in the unmatched bucket) is reported separately in totals for
# transparency.
# Re-baseline 2026-08-31 (slice-collapse + resource-root routing + depth-2
# ancestor walk all in the production path): measured unmatched = 0.000 on
# run 14 -- every distinct path now resolves exactly, via a collapsed form,
# via a bounded ancestor, or to the record-level bucket. The 0.75 ceiling is
# retained as a regression tripwire only; any future non-zero rate deserves
# a look before shipping.


# Ancestor walk: how many segments up F4 will look when a normalized
# element_path has no exact plannet_element row. Depth 2 is the initial
# value; raise to 3 if the dry-run reports >1% of unmatched paths would
# join at depth 3.
ANCESTOR_WALK_MAX_DEPTH = 2


# ---- Report category routing (extends CATEGORY_SQL) --------------------
# Three run-14 realities the six-bucket classifier mis-files ([F-1]/[F-3]/
# [O-5] of docs/reviews/azblue-run14-context-review-*.md):
#   validator-limitation    regex evaluation timeouts inside the validator --
#                           not data defects; excluded from every data rate.
#   version-stamping        the instance stamps a FHIR-core version ('4.0.1')
#                           onto foreign code systems (NUCC, THO, ...). Data-
#                           borne mechanical defect, but NOT a structural
#                           failure: kept out of `profile` so it cannot set
#                           the headline structural rate (12.4M rows = 40% of
#                           run-14 output).
#   publisher-infrastructure  the vendor's own `ex:createdAt` meta extension,
#                           1 per resource by construction; saturates every
#                           "any finding" rate if left in extension-unresolvable.
REPORT_CATEGORY_SQL_TEMPLATE = """
CASE
  WHEN message_id = 'TYPE_SPECIFIC_CHECKS_DT_PRIMITIVE_REGEX_EXCEPTION'
       THEN 'validator-limitation'
  WHEN message_id IN ('VALUESET_VALUE_MISMATCH_DEFAULT',
                      'UNKNOWN_CODESYSTEM_VERSION',
                      'UNKNOWN_CODESYSTEM_VERSION_NONE',
                      'Unknown_Code_in_Version')
       THEN 'version-stamping'
  WHEN message_id = 'Extension_EXT_Unknown_NotHere'
       AND details LIKE '%%ex:createdAt%%'
       THEN 'publisher-infrastructure'
  ELSE ({base_category_sql})
END
"""

# Categories that measure the payer's own data quality. Everything else
# (best-practice, terminology-unavailable, validator-limitation,
# version-stamping, publisher-infrastructure, extension-unresolvable) is
# reported separately and NEVER pooled into a headline record rate [O-1].
MATERIAL_CATEGORIES = ("profile", "binding-required", "binding-extensible")

# ---- Standard-layer classifier ([O-4], reworked per [O-20]) ------------
# Which standard OWNS THE RULE that fired. Attribution comes only from
# places that name the rule's owner: invariant message_ids are profile-URL-
# prefixed, and fixed-value / minimum-cardinality messages name the
# constraining profile in the text. A terminology message that merely CITES
# a value set does NOT reveal the rule's owner (a Plan-Net slice can bind a
# US Core value set), so all code-set membership messages go to their own
# bucket instead of masquerading as a standard. `unattributed` is the
# honest residual.
STANDARD_LAYER_SQL = """
CASE
  WHEN message_id LIKE 'http://hl7.org/fhir/us/core/%%'                  THEN 'us-core'
  WHEN message_id LIKE 'http://hl7.org/fhir/us/davinci-pdex-plan-net/%%' THEN 'plan-net'
  WHEN message_id LIKE 'http://hl7.org/fhir/StructureDefinition/%%'      THEN 'r4-base'
  WHEN message_id IN ('_DT_Fixed_Wrong', 'Validation_VAL_Profile_Minimum',
                      'Validation_VAL_Profile_Minimum_SLICE')
       AND details LIKE '%%davinci-pdex-plan-net%%'                      THEN 'plan-net'
  WHEN message_id IN ('_DT_Fixed_Wrong', 'Validation_VAL_Profile_Minimum',
                      'Validation_VAL_Profile_Minimum_SLICE')
       AND details LIKE '%%hl7.org/fhir/us/core%%'                       THEN 'us-core'
  WHEN message_id IN ('_DT_Fixed_Wrong', 'Validation_VAL_Profile_Minimum',
                      'Validation_VAL_Profile_Minimum_SLICE')
       AND details LIKE '%%hl7.org/fhir/StructureDefinition%%'           THEN 'r4-base'
  WHEN message_id ILIKE 'type\\_specific\\_checks%%'                     THEN 'r4-base'
  WHEN message_id ILIKE 'terminology\\_tx\\_%%'
    OR message_id IN ('VALUESET_VALUE_MISMATCH_DEFAULT', 'UNKNOWN_CODESYSTEM',
                      'UNKNOWN_CODESYSTEM_VERSION', 'UNKNOWN_CODESYSTEM_VERSION_NONE',
                      'Unknown_Code_in_Version', 'UNABLE_TO_INFER_CODESYSTEM')
       THEN 'code-set-membership'
  ELSE 'unattributed'
END
"""

# Bare resource-type expressions the validator emits for resource-level
# issues; they are NOT element paths and must not land in `unmatched` [O-3].
RESOURCE_ROOTS = frozenset({
    "Organization", "Location", "Practitioner", "PractitionerRole",
    "HealthcareService", "InsurancePlan", "Network", "Endpoint",
    "OrganizationAffiliation",
})


# ---- Slice collapse (Plan-Net side, F4) --------------------------------
# plannet_element.element_path stores slice discriminators
# (`PractitionerRole.extension:qualification.extension:whereValid.value[x]`);
# the HL7 validator emits URL-less bracket-indexed chains that normalize to
# the discriminator-free form. Collapsing the Plan-Net side closes the join.
# Plan-Net paths contain no URLs, so a bare `:name` regex is safe here; do
# NOT run this on normalizer output (extension(<url>) segments carry colons).
_SLICE_RE = re.compile(r":[A-Za-z0-9_-]+")


def strip_slice_discriminators(element_path: str) -> str:
    return _SLICE_RE.sub("", element_path)


def requirement_class(min_card: int | None, must_support: bool | None) -> str:
    """mandatory (min>=1) / must-support (MS, min 0) / other (neither)."""
    if min_card and min_card >= 1:
        return "mandatory"
    if must_support:
        return "must-support"
    return "other"


def build_requirement_lookup(rows: Iterable[tuple[str, int | None, bool | None]]
                             ) -> dict[str, str]:
    """Map both exact and slice-collapsed element paths to a requirement class.

    When several Plan-Net elements collapse onto the same discriminator-free
    path with different classes, the collapsed key is 'mixed' -- reported as
    such rather than silently picking one.
    """
    lut: dict[str, str] = {}
    for ep, mc, ms in rows:
        cls = requirement_class(mc, ms)
        for key in {ep, strip_slice_discriminators(ep)}:
            prev = lut.get(key)
            if prev is None:
                lut[key] = cls
            elif prev != cls and prev != "mixed":
                lut[key] = "mixed"
    return lut


def build_slice_map(paths: Iterable[str]) -> dict[str, list[str]]:
    """Map each collapsed key to the slice-qualified Plan-Net paths behind it.

    Rendered as provenance on every finding: a collapsed key that merges N
    slices says so, and the human label is derived from the slice names
    rather than the discriminator-free chain ([O-2] transparency part).
    """
    out: dict[str, list[str]] = {}
    for ep in paths:
        key = strip_slice_discriminators(ep)
        out.setdefault(key, [])
        if ep not in out[key]:
            out[key].append(ep)
    for v in out.values():
        v.sort()
    return out


# ---- Segment split / join (extension(<url>) is atomic) -----------------

def _split_segments(path: str) -> list[str]:
    """Split ``path`` on '.', keeping ``extension(...)`` as one atomic segment.

    A naive ``path.split('.')`` breaks any URL segment containing dots.
    """
    if not path:
        return []
    out: list[str] = []
    cur: list[str] = []
    depth = 0
    for ch in path:
        if ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth = max(0, depth - 1)
            cur.append(ch)
        elif ch == "." and depth == 0:
            if cur:
                out.append("".join(cur))
                cur = []
        else:
            cur.append(ch)
    if cur:
        out.append("".join(cur))
    return out


def _join_segments(segs: Iterable[str]) -> str:
    return ".".join(segs)


# ---- The 8 rules --------------------------------------------------------

def _strip_paren_call(text: str, funcname: str, keep_body: bool = False) -> str:
    """Remove every ``.<funcname>(...)`` from ``text`` with balanced parens.

    Regex cannot match balanced parens; a manual scan handles nested calls
    like ``.where(x.exists() and y='z')`` cleanly.
    """
    needle = "." + funcname + "("
    while True:
        i = text.find(needle)
        if i < 0:
            return text
        # find matching close-paren
        depth = 1
        j = i + len(needle)
        while j < len(text) and depth:
            if text[j] == "(":
                depth += 1
            elif text[j] == ")":
                depth -= 1
            j += 1
        if depth:      # unbalanced; leave it alone rather than corrupt
            return text
        body = text[i + len(needle):j - 1]
        if keep_body:
            text = text[:i] + "(" + body + ")" + text[j:]
        else:
            text = text[:i] + text[j:]


def _rule1_parens(text: str) -> str:
    """Strip a single pair of outermost parentheses if they wrap the whole."""
    text = text.strip()
    while len(text) >= 2 and text[0] == "(" and text[-1] == ")":
        # only strip if the outer paren really wraps the whole expression
        depth = 0
        wraps = True
        for k, ch in enumerate(text):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and k != len(text) - 1:
                    wraps = False
                    break
        if wraps:
            text = text[1:-1].strip()
        else:
            break
    return text


def _rule2_tail_navs(text: str) -> str:
    prev = None
    while prev != text:
        prev = text
        text = _NAV_TAIL_RE.sub("", text)
    return text


def _rule3_where(text: str) -> str:
    return _strip_paren_call(text, "where")


def _rule4_ofType(text: str, raw: str) -> str:
    """Collapse ``.ofType(X)``. Extension-context predicate is on RAW input.

    In an Extension context, `.value.ofType(X)` → `.value[x]` — and the
    rewrite must fire whether `.value` is at the tail (`Extension.value`) or
    mid-path (`Extension.value.coding.system`). An earlier revision restricted
    the rewrite to `.value` at end-of-string, which silently dropped `[x]` on
    7.7M run-14 rows whose expressions continue past the choice type (e.g.
    `PractitionerRole.extension[0].extension[8648].value.ofType(CodeableConcept)
    .coding[0].system`).
    """
    if _EXTENSION_ANY_RE.search(raw):
        text = _strip_paren_call(text, "ofType")
        text = re.sub(r"\.value(?=\.|\[|$)", ".value[x]", text)
    else:
        text = _strip_paren_call(text, "ofType")
    return text


def _rule5_choice_types(text: str) -> str:
    return _CHOICE_TYPE_RE.sub(".value[x]", text)


def _rule6_extension_urls(text: str) -> str:
    """Canonicalize ``extension('URL')`` and ``extension("URL")`` to
    ``extension(<URL>)``. Angle-bracket URLs pass through unchanged."""
    return _EXT_QUOTED_URL_RE.sub(lambda m: "extension(<" + m.group(1) + ">)", text)


def _rule7_indices(text: str) -> str:
    return _INDEX_RE.sub("", text)


def normalize_path(expression: str | None) -> str:
    """Normalize a raw FHIRPath expression to a ``plannet_element.element`` key.

    Returns ``(resource-level)`` for empty/NULL/blank input (F2 policy).
    """
    if expression is None:
        return "(resource-level)"
    raw = expression.strip()
    if not raw:
        return "(resource-level)"
    text = raw
    text = _rule1_parens(text)
    text = _rule2_tail_navs(text)
    text = _rule3_where(text)
    text = _rule4_ofType(text, raw)
    text = _rule5_choice_types(text)
    text = _rule6_extension_urls(text)
    text = _rule7_indices(text)
    # Squash any accidental double-dots left by earlier rules.
    while ".." in text:
        text = text.replace("..", ".")
    text = text.strip(".")
    return text or "(resource-level)"


# ---- Ancestor walk ------------------------------------------------------

def ancestors(element_path: str, max_depth: int = ANCESTOR_WALK_MAX_DEPTH
              ) -> list[tuple[str, int]]:
    """Yield ``(ancestor_path, depth_delta)`` up to ``max_depth`` segments.

    The resource root (first segment, e.g. ``Practitioner``) is excluded --
    otherwise every finding would resolve at depth ≤ resource and F2's
    tripwire would be starved.
    """
    segs = _split_segments(element_path)
    if len(segs) <= 1:
        return []
    out: list[tuple[str, int]] = []
    for delta in range(1, max_depth + 1):
        if len(segs) - delta <= 1:      # would reach resource root; stop
            break
        out.append((_join_segments(segs[:-delta]), delta))
    return out


# ---- Rollup dataclasses ------------------------------------------------

@dataclass
class Finding:
    resource_type: str
    element_path: str
    message_id: str          # '(no-message-id)' if NULL upstream
    severity: str
    category: str
    occurrences: int
    resources_affected: int
    resources_in_scope: int
    repeats_per_affected_resource: float
    sample_expression: str
    sample_details: str | None
    element_path_unmatched: bool
    requirement: str = "base-fhir"   # mandatory / must-support / other / mixed / record-level / base-fhir
    match_grain: str = "none"        # exact / ancestor-1 / ancestor-2 / resource-level / none
    slice_paths: list[str] = field(default_factory=list)  # Plan-Net slices behind the key
    by_lob: dict[str, dict[str, int]] = field(default_factory=dict)


@dataclass
class FindingsBuild:
    run_id: int
    resources_validated: int         # sum(resources_in_scope) across cells
    resources_in_scope_by_cell: dict[str, int]
    findings: list[Finding]
    resources_affected_in_category: list[tuple[str, str, int]]  # (rt, cat, n)
    unmatched_paths: dict[str, int]      # element_path -> distinct-expression count
    unmatched_row_occurrences: dict[str, int]  # element_path -> conformance_issue rows
    unmatched_rate: float                # distinct-path share
    unmatched_row_coverage: float        # rows in unmatched paths / total issue rows
    normalizer_calls: int
    # Per-LOB axes (LOB = urn:source_name marker in the raw resource JSON).
    lob_denominators: dict[tuple[str, str], int] = field(default_factory=dict)      # (cell, lob) -> n
    lob_any_finding: dict[tuple[str, str], int] = field(default_factory=dict)       # (cell, lob) -> n
    lob_severity: dict[tuple[str, str, str], int] = field(default_factory=dict)     # (cell, lob, sev) -> n
    lob_category: dict[tuple[str, str, str], int] = field(default_factory=dict)     # (cell, lob, cat) -> n
    requirement_category: list[tuple[str, str, str, str, int, int]] = field(default_factory=list)  # (cell, cat, req, grain, ra, occ)
    element_rollup: list[tuple[str, str, str, str, str, int, int]] = field(default_factory=list)  # (cell, ep, req, grain, cat, ra, occ)
    standard_layers: list[tuple[str, str, str, int, int]] = field(default_factory=list)  # (cell, lob, layer, ra, occ)
    binding_valuesets: list[tuple[str, str, str, str, int, int]] = field(default_factory=list)  # (cell, lob, cat, vs, ra, occ)
    ext_urls: list[tuple[str, str, int, int]] = field(default_factory=list)         # (cell, url, occ, ra)
    warnings: list[str] = field(default_factory=list)


# ---- The SQL build ------------------------------------------------------
#
# One report build wraps the whole sequence in a single BEGIN/COMMIT on an
# autocommit=False psycopg connection so the TEMP TABLE `expr_norm` is
# visible to every subsequent query on the same connection.

_CREATE_EXPR_NORM = """
CREATE TEMP TABLE expr_norm(
  ci_expression text UNIQUE,
  element_path  text NOT NULL,
  unmatched     boolean NOT NULL,
  requirement   text NOT NULL,
  match_grain   text NOT NULL   -- exact | ancestor-1 | ancestor-2 | resource-level | none
);
"""

_DISTINCT_EXPRS_SQL = """
SELECT DISTINCT expression
FROM public.conformance_issue
WHERE run_id = %(run)s;
"""

_PRECHECK_A_SQL = """
SELECT count(*) FROM public.conformance_instance
WHERE run_id = %(run)s AND resource_id IS NULL;
"""

_PRECHECK_B_SQL = """
SELECT
  (SELECT sum(cnt) FROM (
     SELECT count(*) cnt FROM public.conformance_instance
     WHERE run_id = %(run)s GROUP BY cell) s) AS denom_sum,
  (SELECT instances FROM public.conformance_run WHERE run_id = %(run)s) AS run_instances;
"""

_DENOM_SQL = """
SELECT cell, count(*) FROM public.conformance_instance
WHERE run_id = %(run)s GROUP BY cell;
"""

_SCOPED_CTE = """
WITH scoped AS (
  SELECT ci.cell        AS resource_type,
         en.element_path,
         en.requirement,
         en.match_grain,
         ci.message_id,
         ci.severity,
         ({category_sql}) AS category,
         ({layer_sql})    AS standard_layer,
         ci.expression   AS ci_expression,
         ci.details      AS ci_details,
         inst.resource_id
  FROM public.conformance_issue ci
  JOIN public.conformance_instance inst USING (run_id, payer_id, cell, instance)
  JOIN expr_norm en ON en.ci_expression = ci.expression
  WHERE ci.run_id = %(run)s
)
"""

_ROLLUP_SQL_TEMPLATE = _SCOPED_CTE + """
, findings AS (
  SELECT resource_type, element_path,
         COALESCE(message_id, '(no-message-id)') AS message_id,
         severity,
         category,
         count(*) AS occurrences,
         count(DISTINCT resource_id) AS resources_affected
  FROM scoped
  GROUP BY 1,2,3,4,5
), samples AS (
  -- DISTINCT ON is dramatically cheaper than array_agg over 31M rows because
  -- it materializes one representative per group instead of an ordered array
  -- of every group member. Picks the shortest expression / details, ties
  -- broken lexicographically; NULL details naturally sort last so a group
  -- with any non-NULL row gets a non-NULL sample_details.
  SELECT DISTINCT ON (resource_type, element_path,
                      COALESCE(message_id, '(no-message-id)'), severity, category)
         resource_type,
         element_path,
         COALESCE(message_id, '(no-message-id)') AS message_id,
         severity,
         category,
         ci_expression AS sample_expression,
         ci_details    AS sample_details
  FROM scoped
  ORDER BY resource_type, element_path,
           COALESCE(message_id, '(no-message-id)'), severity, category,
           length(ci_expression) NULLS LAST, ci_expression NULLS LAST,
           length(ci_details) NULLS LAST, ci_details NULLS LAST
), denom AS (
  SELECT cell AS resource_type, count(*) AS resources_in_scope
  FROM public.conformance_instance WHERE run_id = %(run)s GROUP BY 1
)
SELECT f.resource_type, f.element_path, f.message_id, f.severity, f.category,
       f.occurrences, f.resources_affected,
       s.sample_expression, s.sample_details,
       d.resources_in_scope,
       (f.occurrences::float / nullif(f.resources_affected, 0))
         AS repeats_per_affected_resource
FROM findings f
JOIN samples s USING (resource_type, element_path, message_id, severity, category)
JOIN denom d USING (resource_type)
ORDER BY resources_affected DESC, repeats_per_affected_resource DESC, occurrences DESC;
"""

_CATEGORY_COUNT_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT resource_type, category, count(DISTINCT resource_id) AS n
FROM scoped GROUP BY 1,2;
"""

_REQ_CATEGORY_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT resource_type, category, requirement, match_grain,
       count(DISTINCT resource_id) AS resources_affected,
       count(*) AS occurrences
FROM scoped GROUP BY 1,2,3,4;
"""

# Element-centric rollup for the report's §D ([O-30]: produced by the build
# itself, not a side-channel patch): exact distinct-record counts per
# (resource, element, requirement, grain, category). Strict partition of
# every issue row.
_ELEMENT_ROLLUP_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT resource_type, element_path, requirement, match_grain, category,
       count(DISTINCT resource_id) AS resources_affected,
       count(*) AS occurrences
FROM scoped GROUP BY 1,2,3,4,5;
"""

_LAYER_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT scoped.resource_type, il.lob, scoped.standard_layer,
       count(DISTINCT scoped.resource_id) AS resources_affected,
       count(*) AS occurrences
FROM scoped JOIN inst_lob il ON il.resource_id = scoped.resource_id AND il.cell = scoped.resource_type
GROUP BY 1,2,3;
"""

# Value-set attribution for binding findings ([O-17]/[O-2]): the message text
# names the value set, which uniquely identifies the profile slice even where
# the collapsed element path merges slices with different bindings.
_BINDING_VALUESET_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT scoped.resource_type, il.lob, scoped.category,
       (regexp_match(scoped.ci_details, '\\((http[^)|]+)'))[1] AS valueset_url,
       count(DISTINCT scoped.resource_id) AS resources_affected,
       count(*) AS occurrences
FROM scoped JOIN inst_lob il ON il.resource_id = scoped.resource_id AND il.cell = scoped.resource_type
WHERE scoped.category IN ('binding-required', 'binding-extensible')
GROUP BY 1,2,3,4;
"""

# ---- LOB attribution ----------------------------------------------------
# LOB is derived from the `urn:source_name:<lob>` marker embedded in the raw
# resource JSON (same rule as docs/evidence/azblue/decide2.sql). Known values
# on run 14: hcaz (Medicaid), azblue (Medicare+ACA), ods (Organization only),
# '(none)' if the marker is absent.

_CELL_TABLE = {
    "Organization": "organization",
    "Network": "organization",          # Network resources live in organization
    "Location": "location",
    "Practitioner": "practitioner",
    "PractitionerRole": "practitioner_role",
    "HealthcareService": "healthcare_service",
    "InsurancePlan": "insurance_plan",
}

_CREATE_INST_LOB = """
CREATE TEMP TABLE inst_lob(
  resource_id text NOT NULL,
  cell        text NOT NULL,
  lob         text NOT NULL,
  PRIMARY KEY (resource_id, cell)   -- a handful of ids collide across cells
);
"""

_INSERT_INST_LOB_TEMPLATE = """
INSERT INTO inst_lob(resource_id, cell, lob)
SELECT ci.resource_id, ci.cell,
       coalesce((regexp_match(t.resource::text,
                 'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)')
FROM public.conformance_instance ci
JOIN {schema}.{table} t ON t.id = ci.resource_id
WHERE ci.run_id = %(run)s AND ci.cell = %(cell)s;
"""

_LOB_DENOM_SQL = "SELECT cell, lob, count(*) FROM inst_lob GROUP BY 1,2;"

# "Any finding" and severity rates are scoped to MATERIAL_CATEGORIES: with
# publisher-infrastructure (1/resource by construction) and dom-6 included
# they are a constant 100% for every (resource, LOB) — a non-measurement
# ([O-1]/[F-1]).
_LOB_ANY_FINDING_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT scoped.resource_type, il.lob, count(DISTINCT scoped.resource_id) AS n
FROM scoped JOIN inst_lob il ON il.resource_id = scoped.resource_id AND il.cell = scoped.resource_type
WHERE scoped.category = ANY(%(material)s)
GROUP BY 1,2;
"""

_LOB_SEVERITY_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT scoped.resource_type, il.lob, scoped.severity,
       count(DISTINCT scoped.resource_id) AS n
FROM scoped JOIN inst_lob il ON il.resource_id = scoped.resource_id AND il.cell = scoped.resource_type
WHERE scoped.category = ANY(%(material)s)
GROUP BY 1,2,3;
"""

_LOB_CATEGORY_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT scoped.resource_type, il.lob, scoped.category,
       count(DISTINCT scoped.resource_id) AS n
FROM scoped JOIN inst_lob il ON il.resource_id = scoped.resource_id AND il.cell = scoped.resource_type
GROUP BY 1,2,3;
"""

_LOB_FINDING_SQL_TEMPLATE = _SCOPED_CTE + """
SELECT scoped.resource_type, scoped.element_path,
       COALESCE(scoped.message_id, '(no-message-id)') AS message_id,
       scoped.severity, scoped.category, il.lob,
       count(*) AS occurrences,
       count(DISTINCT scoped.resource_id) AS resources_affected
FROM scoped JOIN inst_lob il ON il.resource_id = scoped.resource_id AND il.cell = scoped.resource_type
GROUP BY 1,2,3,4,5,6;
"""

# Extension URLs behind the extension-unresolvable category: the URL is in
# the message text, not in message_id, and distinct URLs can share one
# finding after normalization -- so this is its own grouped scan.
_EXT_URL_SQL_TEMPLATE = """
SELECT ci.cell,
       coalesce(
         (regexp_match(ci.details, '(https?://[^ '']+)'))[1],
         (regexp_match(ci.details, 'The extension ([^ ]+)'))[1]
       ) AS url,
       count(*) AS occurrences,
       count(DISTINCT inst.resource_id) AS resources_affected
FROM public.conformance_issue ci
JOIN public.conformance_instance inst USING (run_id, payer_id, cell, instance)
WHERE ci.run_id = %(run)s
  AND ({category_sql}) IN ('extension-unresolvable', 'publisher-infrastructure')
GROUP BY 1,2 ORDER BY 4 DESC;
"""

_PLANNET_CARD_SQL = """
SELECT element_path, min_card, must_support FROM public.plannet_element
WHERE ig_package LIKE %(pkglike)s;
"""


def _copy_load_expr_norm(conn, rows: Iterable[tuple[str | None, str, bool, str, str]]) -> None:
    with conn.cursor() as cur, cur.copy(
        "COPY expr_norm(ci_expression, element_path, unmatched, requirement, match_grain) FROM STDIN"
    ) as cp:
        for row in rows:
            cp.write_row(row)


def classify_path(ep: str, requirement_lut: dict[str, str],
                  ) -> tuple[str, str, bool]:
    """Return (requirement, match_grain, unmatched) for a normalized path.

    Order: resource-level bucket, exact/collapsed Plan-Net match, bounded
    ancestor walk ([O-3] -- the walk inherits the nearest enumerated
    ancestor's class), residual base-FHIR.
    """
    if ep == "(resource-level)" or ep in RESOURCE_ROOTS:
        return "record-level", "resource-level", False
    if ep in requirement_lut:
        return requirement_lut[ep], "exact", False
    for anc, depth in ancestors(ep):
        if anc in requirement_lut:
            return requirement_lut[anc], f"ancestor-{depth}", False
    return "base-fhir", "none", True


def build_findings(conn, *, run_id: int,
                   ig: str = "hl7.fhir.us.davinci-pdex-plan-net",
                   ig_version: str = "1.2.0",
                   schema: str = "bcbs_az_new") -> FindingsBuild:
    """Materialize the findings rollup for one conformance_run.

    Caller must open ``conn`` with ``autocommit=False`` and commit after this
    returns (temp table lifecycle depends on the single-transaction scope).
    """
    from provdir.quality.conformance import CATEGORY_SQL, _PARAMS  # local import

    warnings: list[str] = []

    with conn.cursor() as cur:
        cur.execute(_PRECHECK_A_SQL, {"run": run_id})
        (n_null,) = cur.fetchone()
        if n_null:
            raise SystemExit(
                f"FATAL: run {run_id} has {n_null} conformance_instance rows "
                "with NULL resource_id. Run 14 requires resource_id populated; "
                "verify migration 0003 landed for this run before reporting.")

        cur.execute(_PRECHECK_B_SQL, {"run": run_id})
        denom_sum, run_instances = cur.fetchone()
        if denom_sum is None or run_instances is None:
            raise SystemExit(f"FATAL: run {run_id} has no denom or no run row.")
        if int(denom_sum) != int(run_instances):
            raise SystemExit(
                f"FATAL: F3 denominator sum ({denom_sum}) does not match "
                f"conformance_run.instances ({run_instances}) for run {run_id}. "
                "A NULL or unknown-cell row leaked in; F5 per-cell rates would "
                "silently under-report.")

        cur.execute(_CREATE_EXPR_NORM)

        cur.execute(_DISTINCT_EXPRS_SQL, {"run": run_id})
        distinct_exprs = [row[0] for row in cur.fetchall()]

        cur.execute(_PLANNET_CARD_SQL,
                    {"pkglike": f"%{ig}#{ig_version}%"})
        plannet_rows = cur.fetchall()
    # Join universe is exact paths PLUS their slice-collapsed forms (F4):
    # the validator emits discriminator-free chains, Plan-Net stores sliced.
    requirement_lut = build_requirement_lookup(plannet_rows)
    slice_map = build_slice_map(ep for ep, _, _ in plannet_rows)

    # Pure Python: normalize + classify (requirement, grain, unmatched).
    normed: list[tuple[str | None, str, bool, str, str]] = []
    unmatched_paths: dict[str, int] = {}
    for expr in distinct_exprs:
        ep = normalize_path(expr)
        req, grain, unmatched = classify_path(ep, requirement_lut)
        normed.append((expr, ep, unmatched, req, grain))
        if unmatched:
            unmatched_paths[ep] = unmatched_paths.get(ep, 0) + 1

    _copy_load_expr_norm(conn, normed)

    distinct_paths = {ep for _, ep, _, _, g in normed
                     if g != "resource-level"}
    unmatched_distinct = {ep for _, ep, u, _, _ in normed if u}
    unmatched_rate = (len(unmatched_distinct) / len(distinct_paths)
                      if distinct_paths else 0.0)
    if unmatched_rate > TRIPWIRE_THRESHOLD:
        top = sorted(
            ((ep, c) for ep, c in unmatched_paths.items()),
            key=lambda kv: -kv[1],
        )[:20]
        raise SystemExit(
            f"FATAL: normalizer unmatched_rate={unmatched_rate:.3f} exceeds "
            f"TRIPWIRE_THRESHOLD={TRIPWIRE_THRESHOLD}. Top 20 unmatched paths:\n"
            + "\n".join(f"  {c:>6}  {ep}" for ep, c in top)
        )

    # Row-weighted unmatched coverage: how many conformance_issue rows sit in
    # unmatched paths, expressed as a fraction of all issue rows for this run.
    # This is the "how much of the report is silently deferred" number; the
    # distinct-path unmatched_rate above hides it (many rows can share one path).
    unmatched_expressions = [e for e, ep, u, _, _ in normed if u]
    unmatched_row_occurrences: dict[str, int] = {}
    total_rows_run = 0
    with conn.cursor() as cur:
        cur.execute(_DENOM_SQL, {"run": run_id})
        denom_by_cell = {cell: int(n) for cell, n in cur.fetchall()}
        cur.execute(
            "SELECT count(*) FROM public.conformance_issue WHERE run_id = %(run)s",
            {"run": run_id})
        (total_rows_run,) = cur.fetchone()
        if unmatched_expressions:
            cur.execute(
                "SELECT expression, count(*) FROM public.conformance_issue "
                "WHERE run_id = %(run)s AND expression = ANY(%(exprs)s) GROUP BY 1",
                {"run": run_id, "exprs": unmatched_expressions})
            expr_to_rows = {e: int(n) for e, n in cur.fetchall()}
            for expr, ep, u, _, _ in normed:
                if u:
                    unmatched_row_occurrences[ep] = (
                        unmatched_row_occurrences.get(ep, 0)
                        + expr_to_rows.get(expr, 0))
    unmatched_row_total = sum(unmatched_row_occurrences.values())
    unmatched_row_coverage = (
        unmatched_row_total / total_rows_run if total_rows_run else 0.0)

    # Main rollup. The report category (validator-limitation /
    # version-stamping / publisher-infrastructure overrides) and the
    # standard-layer classifier ride in every scoped query.
    report_cat_sql = REPORT_CATEGORY_SQL_TEMPLATE.format(
        base_category_sql=CATEGORY_SQL)
    fmt = {"category_sql": report_cat_sql, "layer_sql": STANDARD_LAYER_SQL}
    params = {**_PARAMS, "run": run_id,
              "material": list(MATERIAL_CATEGORIES)}
    rollup_sql = _ROLLUP_SQL_TEMPLATE.format(**fmt)
    with conn.cursor() as cur:
        cur.execute(rollup_sql, params)
        raw_findings = cur.fetchall()

    findings: list[Finding] = []
    for row in raw_findings:
        (rt, ep, mid, sev, cat, occ, ra, se, sd, ris, rep) = row
        req, grain, unmatched = classify_path(ep, requirement_lut)
        findings.append(Finding(
            resource_type=rt,
            element_path=ep,
            message_id=mid,
            severity=sev,
            category=cat,
            occurrences=int(occ),
            resources_affected=int(ra),
            resources_in_scope=int(ris),
            repeats_per_affected_resource=float(rep) if rep is not None else 0.0,
            sample_expression=se or "",
            sample_details=sd,
            element_path_unmatched=unmatched,
            requirement=req,
            match_grain=grain,
            slice_paths=slice_map.get(ep, []),
        ))

    # Category count per resource (F5 explicit second query).
    cat_sql = _CATEGORY_COUNT_SQL_TEMPLATE.format(**fmt)
    with conn.cursor() as cur:
        cur.execute(cat_sql, params)
        cat_counts = [(rt, cat, int(n)) for rt, cat, n in cur.fetchall()]

    # Mandatory vs must-support split within each (resource, category).
    req_sql = _REQ_CATEGORY_SQL_TEMPLATE.format(**fmt)
    with conn.cursor() as cur:
        cur.execute(req_sql, params)
        req_category = [(rt, cat, req, grain, int(ra), int(occ))
                        for rt, cat, req, grain, ra, occ in cur.fetchall()]

    # Element-centric rollup (§D input).
    with conn.cursor() as cur:
        cur.execute(_ELEMENT_ROLLUP_SQL_TEMPLATE.format(**fmt), params)
        element_rollup = [(rt, ep, req, grain, cat, int(ra), int(occ))
                          for rt, ep, req, grain, cat, ra, occ in cur.fetchall()]

    # ---- LOB attribution + per-LOB rollups -----------------------------
    with conn.cursor() as cur:
        cur.execute(_CREATE_INST_LOB)
        for cell, table in _CELL_TABLE.items():
            if cell not in denom_by_cell:
                continue
            cur.execute(
                _INSERT_INST_LOB_TEMPLATE.format(schema=schema, table=table),
                {"run": run_id, "cell": cell})
        cur.execute("SELECT count(*) FROM inst_lob")
        (n_lob,) = cur.fetchone()
        if int(n_lob) != int(denom_sum):
            raise SystemExit(
                f"FATAL: LOB attribution covered {n_lob} of {denom_sum} "
                f"instances for run {run_id}; a cell is missing from "
                "_CELL_TABLE or resource_ids do not resolve in "
                f"schema {schema}.")

        cur.execute(_LOB_DENOM_SQL)
        lob_denominators = {(c, l): int(n) for c, l, n in cur.fetchall()}

        cur.execute(_LOB_ANY_FINDING_SQL_TEMPLATE.format(**fmt), params)
        lob_any_finding = {(c, l): int(n) for c, l, n in cur.fetchall()}

        cur.execute(_LOB_SEVERITY_SQL_TEMPLATE.format(**fmt), params)
        lob_severity = {(c, l, s): int(n) for c, l, s, n in cur.fetchall()}

        cur.execute(_LOB_CATEGORY_SQL_TEMPLATE.format(**fmt), params)
        lob_category = {(c, l, cat): int(n) for c, l, cat, n in cur.fetchall()}

        cur.execute(_LOB_FINDING_SQL_TEMPLATE.format(**fmt), params)
        lob_finding_rows = cur.fetchall()

        cur.execute(_LAYER_SQL_TEMPLATE.format(**fmt), params)
        layer_rows = [(rt, l, lay, int(ra), int(occ))
                      for rt, l, lay, ra, occ in cur.fetchall()]

        cur.execute(_BINDING_VALUESET_SQL_TEMPLATE.format(**fmt), params)
        binding_valuesets = [
            (rt, l, cat, vs or "(no-valueset-in-message)", int(ra), int(occ))
            for rt, l, cat, vs, ra, occ in cur.fetchall()]

        cur.execute(_EXT_URL_SQL_TEMPLATE.format(category_sql=report_cat_sql),
                    params)
        ext_urls = [(c, u or "(no-url-in-message)", int(o), int(ra))
                    for c, u, o, ra in cur.fetchall()]

    by_key = {(f.resource_type, f.element_path, f.message_id, f.severity,
               f.category): f for f in findings}
    for rt, ep, mid, sev, cat, lob, occ, ra in lob_finding_rows:
        f = by_key.get((rt, ep, mid, sev, cat))
        if f is not None:
            f.by_lob[lob] = {"occurrences": int(occ),
                             "resources_affected": int(ra)}

    # No-message-id bucket warning.
    no_mid_total_ra = sum(f.resources_affected for f in findings
                          if f.message_id == "(no-message-id)")
    total_ra = sum(f.resources_affected for f in findings) or 1
    if no_mid_total_ra / total_ra > 0.01:
        no_mid = [f for f in findings if f.message_id == "(no-message-id)"]
        no_mid.sort(key=lambda f: -f.resources_affected)
        warnings.append(
            "(no-message-id) bucket exceeds 1% of resources_affected; "
            + f"top 10 sample_details:\n"
            + "\n".join(f"  [{f.resource_type}/{f.severity}] {f.sample_details!r}"
                        for f in no_mid[:10]))

    return FindingsBuild(
        run_id=run_id,
        resources_validated=int(denom_sum),
        resources_in_scope_by_cell=denom_by_cell,
        findings=findings,
        resources_affected_in_category=cat_counts,
        unmatched_paths=unmatched_paths,
        unmatched_row_occurrences=unmatched_row_occurrences,
        unmatched_rate=unmatched_rate,
        unmatched_row_coverage=unmatched_row_coverage,
        normalizer_calls=len(distinct_exprs),
        lob_denominators=lob_denominators,
        lob_any_finding=lob_any_finding,
        lob_severity=lob_severity,
        lob_category=lob_category,
        requirement_category=req_category,
        element_rollup=element_rollup,
        standard_layers=layer_rows,
        binding_valuesets=binding_valuesets,
        ext_urls=ext_urls,
        warnings=warnings,
    )

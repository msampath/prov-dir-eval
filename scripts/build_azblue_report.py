#!/usr/bin/env python
"""Build the external bcbs_az_new quality report from conformance_run 14.

Emits four files under ``--out``:

  findings.json          F3 rollup + F4 element rollup + run identity
  data-dictionary.md     Every findings.json field, unit, SQL, denominator
  azblue-run14.html      Self-contained HTML per docs/evaluation-framework.md §9
  evidence.md            SQL to reproduce any finding against the DB

Usage:
    # dry-run to seed TRIPWIRE_THRESHOLD baseline
    python scripts/build_azblue_report.py --conformance-run 14 --dry-run

    # full build
    python scripts/build_azblue_report.py --conformance-run 14 \
        --audit-run <A> --out output/quality/azblue-run14
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.etl.loader import pg_connection  # noqa: E402
from provdir.quality import findings as F  # noqa: E402
from provdir.quality.conformance import (  # noqa: E402
    describe_run,
    parse_expect_payers,
    select_run,
)


# ---- Dry-run: just the normalizer + tripwire measurements ------------

_DRY_DISTINCT_EXPRS_SQL = """
SELECT expression, count(*) FROM public.conformance_issue
WHERE run_id = %(run)s GROUP BY 1;
"""

_DRY_PLANNET_ELEMENTS_SQL = """
SELECT DISTINCT element_path FROM public.plannet_element
WHERE ig_package LIKE %(pkglike)s;
"""


def run_dry_measurement(conn, *, run_id: int,
                        ig: str = "hl7.fhir.us.davinci-pdex-plan-net",
                        ig_version: str = "1.2.0") -> dict:
    """Measure unmatched_rate + ancestor-walk depth-3 hits without loading temp table.

    Purpose: seed TRIPWIRE_THRESHOLD and validate ANCESTOR_WALK_MAX_DEPTH before
    the constants are locked into the module. Reports the numbers on stdout so
    a human commits them.
    """
    with conn.cursor() as cur:
        cur.execute(_DRY_PLANNET_ELEMENTS_SQL,
                    {"pkglike": f"%{ig}#{ig_version}%"})
        plannet_elements = {row[0] for row in cur.fetchall()}
        cur.execute(_DRY_DISTINCT_EXPRS_SQL, {"run": run_id})
        exprs = cur.fetchall()

    if not plannet_elements:
        raise SystemExit(
            f"FATAL: no rows in public.plannet_element for ig={ig} version={ig_version}. "
            "Run scripts/ig_elements.py first.")

    matched = 0
    unmatched: dict[str, int] = {}
    ancestor_matches_by_depth: dict[int, int] = {}
    n_exprs = len(exprs)
    n_null = 0
    for expr, cnt in exprs:
        if expr is None:
            n_null += 1
            continue
        ep = F.normalize_path(expr)
        if ep == "(resource-level)":
            continue
        if ep in plannet_elements:
            matched += 1
        else:
            unmatched[ep] = unmatched.get(ep, 0) + int(cnt)
            # Try ancestor walk up to depth 5 to know how deep to set the cap.
            for anc, delta in F.ancestors(ep, max_depth=5):
                if anc in plannet_elements:
                    ancestor_matches_by_depth[delta] = (
                        ancestor_matches_by_depth.get(delta, 0) + 1)
                    break

    distinct_paths = matched + len(unmatched)
    unmatched_rate = (len(unmatched) / distinct_paths) if distinct_paths else 0.0

    print(f"dry-run baseline for run {run_id}:")
    print(f"  distinct expressions: {n_exprs:,}  (NULL: {n_null})")
    print(f"  plannet_element universe: {len(plannet_elements):,}")
    print(f"  distinct normalized paths: {distinct_paths:,}")
    print(f"  matched exact:              {matched:,}")
    print(f"  unmatched:                  {len(unmatched):,}  "
          f"({unmatched_rate:.3%})")
    print(f"  current TRIPWIRE_THRESHOLD: {F.TRIPWIRE_THRESHOLD:.3f}")
    print(f"  ---")
    print(f"  ancestor-walk hits by depth (of unmatched):")
    for d in sorted(ancestor_matches_by_depth):
        n = ancestor_matches_by_depth[d]
        pct = n / len(unmatched) if unmatched else 0.0
        print(f"    depth {d}: {n:,}  ({pct:.1%} of unmatched)")
    depth3_hits = ancestor_matches_by_depth.get(3, 0)
    depth3_pct = depth3_hits / len(unmatched) if unmatched else 0.0
    print(f"  depth-3 hits: {depth3_pct:.2%} of unmatched  "
          f"(gate: >1% raises ANCESTOR_WALK_MAX_DEPTH to 3)")
    print(f"  current ANCESTOR_WALK_MAX_DEPTH: {F.ANCESTOR_WALK_MAX_DEPTH}")
    print(f"  ---")
    print(f"top 20 unmatched paths (by occurrences):")
    for ep, c in sorted(unmatched.items(), key=lambda kv: -kv[1])[:20]:
        print(f"  {c:>10,}  {ep}")

    return {
        "run_id": run_id,
        "unmatched_rate": unmatched_rate,
        "distinct_exprs": n_exprs,
        "distinct_paths": distinct_paths,
        "matched_exact": matched,
        "unmatched_distinct": len(unmatched),
        "depth3_hit_pct": depth3_pct,
        "ancestor_hits_by_depth": ancestor_matches_by_depth,
    }


# ---- Full build ------------------------------------------------------

# External-hygiene scrub for validator message text ([O-28]): whitespace
# checks quote the raw field value verbatim — which for a directory means
# real names and street addresses — and identifier-system URNs can carry the
# vendor's name. Both are masked in every external artifact.
_WS_VALUE_RE = re.compile(r"whitespace '[^']*'")

# Network names quotable in external artifacts: placeholders only.
_QUOTABLE_NET_NAMES = frozenset(
    {"INVALID", "NOT APPLICABLE", "UNKNOWN", "UNKNOWN-ERROR", "NONE", "TEST"})


def _scrub_details(message_id: str | None, details: str | None) -> str | None:
    if details is None:
        return None
    out = details.replace("innovaccer", "<vendor>")
    if message_id == "Type_Specific_Checks_DT_String_WS":
        out = _WS_VALUE_RE.sub("whitespace '<value withheld>'", out)
    return out


def _finding_to_dict(f: F.Finding) -> dict:
    return {
        "resource_type": f.resource_type,
        "element_path": f.element_path,
        "message_id": f.message_id,
        "severity": f.severity,
        "category": f.category,
        "occurrences": f.occurrences,
        "resources_affected": f.resources_affected,
        "resources_in_scope": f.resources_in_scope,
        "repeats_per_affected_resource": round(f.repeats_per_affected_resource, 3),
        "sample_expression": f.sample_expression,
        "sample_details": _scrub_details(f.message_id, f.sample_details),
        "element_path_unmatched": f.element_path_unmatched,
        "requirement": f.requirement,
        "match_grain": f.match_grain,
        "slice_paths": f.slice_paths,
        "by_lob": f.by_lob,
    }


# ---- Size / shape analysis (run-14 population, bcbs_az_new schema) ----
#
# Every query is LOB-grouped where a LOB exists on the source row, and every
# reference is parsed with an explicit `Type/id` pattern -- an unparseable
# reference lands in its own counted bucket instead of inflating `dangling`
# ([O-12]). `_assert_population_matches_run` gates the whole block: these
# queries read the live payer schema, and the report may not mix a drifted
# population with frozen run denominators ([O-11]).

_REF_ID = "(regexp_match({ref}, '^([A-Za-z]+)/([A-Za-z0-9._-]{{1,64}})$'))[2]"

# (edge name, source table, ref column, target table, needs_unnest)
_REF_EDGES = [
    ("PractitionerRole->Practitioner", "practitioner_role",
     "practitioner_ref", "practitioner", False),
    ("PractitionerRole->Organization", "practitioner_role",
     "organization_ref", "organization", False),
    ("PractitionerRole->Location", "practitioner_role",
     "location_refs", "location", True),
    ("HealthcareService->Location", "healthcare_service",
     "location_refs", "location", True),
    ("HealthcareService->Organization", "healthcare_service",
     "provided_by_ref", "organization", False),
    ("Location->Organization(managing)", "location",
     "managing_organization_ref", "organization", False),
    ("InsurancePlan->Organization(ownedBy)", "insurance_plan",
     "owned_by_ref", "organization", False),
]


# Per-table (id -> lob) maps, materialized once per build so edge queries
# hash-join instead of re-running the LOB regex per joined row (the naive
# form regexes 1.86M-org JSON twice per Location row and takes ~an hour).
_CREATE_LOB_MAP = """
CREATE TEMP TABLE lob_of_{t} AS
SELECT id, {lob_expr} AS lob FROM {s}.{t};
CREATE INDEX ON lob_of_{t}(id);
"""


def _edge_sql(schema: str, src_table: str, ref_col: str, tgt_table: str,
              unnest: bool) -> str:
    src_lob = f"sl.lob"
    if unnest:
        base = (f"SELECT {src_lob} AS lob, "
                f"{_REF_ID.format(ref='x')} AS tid "
                f"FROM {schema}.{src_table} t "
                f"JOIN lob_of_{src_table} sl ON sl.id = t.id, "
                f"unnest(t.{ref_col}) x")
    else:
        base = (f"SELECT {src_lob} AS lob, "
                f"{_REF_ID.format(ref='t.' + ref_col)} AS tid "
                f"FROM {schema}.{src_table} t "
                f"JOIN lob_of_{src_table} sl ON sl.id = t.id "
                f"WHERE t.{ref_col} IS NOT NULL")
    return f"""
WITH src AS ({base})
SELECT src.lob,
       count(*)                                            AS refs,
       count(*) FILTER (WHERE src.tid IS NULL)             AS unparseable,
       count(*) FILTER (WHERE src.tid IS NOT NULL
                          AND tl.id IS NULL)               AS dangling,
       count(*) FILTER (WHERE tl.id IS NOT NULL
                          AND tl.lob = src.lob)            AS same_lob,
       count(*) FILTER (WHERE tl.id IS NOT NULL
                          AND tl.lob <> src.lob)           AS cross_lob
FROM src LEFT JOIN lob_of_{tgt_table} tl ON tl.id = src.tid
GROUP BY 1;
"""


_SIZE_SQL = {
    "networks": """
        SELECT o.resource->>'name',
               coalesce((regexp_match(o.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)')
        FROM public.conformance_instance ci
        JOIN {s}.organization o ON o.id = ci.resource_id
        WHERE ci.run_id = %(run)s AND ci.cell = 'Network'
        ORDER BY 1;""",
    "insurance_plans": """
        SELECT resource->>'name', resource ? 'network', resource ? 'coverageArea',
               coalesce((regexp_match(resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)')
        FROM {s}.insurance_plan;""",
    "pr_network_refs_populated": """
        SELECT count(network_refs) FROM {s}.practitioner_role;""",
    "org_affiliation_rows": """
        SELECT count(*) FROM {s}.organization_affiliation;""",
    # ---- graph reachability, per LOB of the orphaned record ----
    "practitioners_by_lob": """
        WITH refd AS (
          SELECT DISTINCT (regexp_match(practitioner_ref, '^Practitioner/([A-Za-z0-9._-]{{1,64}})$'))[1] AS pid
          FROM {s}.practitioner_role
        )
        SELECT coalesce((regexp_match(p.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE refd.pid IS NULL) AS role_less,
               count(*) FILTER (WHERE p.resource->>'active' = 'false') AS inactive,
               count(*) FILTER (WHERE p.resource->>'gender' = 'unknown') AS gender_unknown
        FROM {s}.practitioner p LEFT JOIN refd ON refd.pid = p.id
        GROUP BY 1;""",
    "locations_by_lob": """
        WITH refd AS (
          SELECT DISTINCT (regexp_match(x, '^Location/([A-Za-z0-9._-]{{1,64}})$'))[1] AS lid
          FROM {s}.practitioner_role r, unnest(r.location_refs) x
          UNION
          SELECT DISTINCT (regexp_match(x, '^Location/([A-Za-z0-9._-]{{1,64}})$'))[1]
          FROM {s}.healthcare_service h, unnest(h.location_refs) x
        )
        SELECT coalesce((regexp_match(l.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE refd.lid IS NULL) AS unreferenced,
               count(*) FILTER (WHERE l.latitude IS NULL) AS no_geo,
               count(*) FILTER (WHERE coalesce(l.address_line,'') = '') AS no_address_line
        FROM {s}.location l LEFT JOIN refd ON refd.lid = l.id
        GROUP BY 1;""",
    "orgs_referenced_by_roles_by_lob": """
        SELECT coalesce((regexp_match(o.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*)
        FROM (SELECT DISTINCT (regexp_match(organization_ref, '^Organization/([A-Za-z0-9._-]{{1,64}})$'))[1] AS oid
              FROM {s}.practitioner_role) x
        JOIN {s}.organization o ON o.id = x.oid GROUP BY 1;""",
    "organizations_by_lob": """
        SELECT coalesce((regexp_match(o.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE o.resource->>'active' = 'false') AS inactive
        FROM {s}.organization o GROUP BY 1;""",
    # ---- dimension 7: Arizona locality ----
    "location_state_by_lob": """
        SELECT coalesce((regexp_match(l.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE l.address_state = 'AZ') AS az,
               count(*) FILTER (WHERE coalesce(trim(l.address_state),'') = '') AS no_state
        FROM {s}.location l GROUP BY 1;""",
    "location_top_nonaz_states": """
        SELECT l.address_state, count(*)
        FROM {s}.location l
        WHERE coalesce(trim(l.address_state),'') NOT IN ('', 'AZ')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10;""",
    "org_state_by_lob": """
        SELECT coalesce((regexp_match(o.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE o.resource->'address'->0->>'state' = 'AZ') AS az,
               count(*) FILTER (WHERE coalesce(trim(
                   o.resource->'address'->0->>'state'),'') = '') AS no_state
        FROM {s}.organization o GROUP BY 1;""",
    "role_referenced_location_state_by_lob": """
        SELECT coalesce((regexp_match(l.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE l.address_state = 'AZ') AS az
        FROM (SELECT DISTINCT (regexp_match(x, '^Location/([A-Za-z0-9._-]{{1,64}})$'))[1] AS lid
              FROM {s}.practitioner_role r, unnest(r.location_refs) x) rl
        JOIN {s}.location l ON l.id = rl.lid GROUP BY 1;""",
    # ---- dimension 5: placeholder / default values ----
    "dar_extension_count": """
        SELECT count(*) FROM (
          SELECT 1 FROM {s}.organization WHERE resource::text LIKE '%%data-absent-reason%%'
          UNION ALL SELECT 1 FROM {s}.location WHERE resource::text LIKE '%%data-absent-reason%%'
          UNION ALL SELECT 1 FROM {s}.practitioner WHERE resource::text LIKE '%%data-absent-reason%%'
          UNION ALL SELECT 1 FROM {s}.practitioner_role WHERE resource::text LIKE '%%data-absent-reason%%'
          UNION ALL SELECT 1 FROM {s}.healthcare_service WHERE resource::text LIKE '%%data-absent-reason%%'
          UNION ALL SELECT 1 FROM {s}.insurance_plan WHERE resource::text LIKE '%%data-absent-reason%%'
        ) u;""",
    "practitioner_sentinel_dates_by_lob": """
        SELECT coalesce((regexp_match(p.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE p.resource::text ~ '(2999|9999)-12-31') AS sentinel
        FROM {s}.practitioner p GROUP BY 1;""",
    # WHICH fields carry the sentinel dates (user revision): almost entirely
    # the address validity period end, plus qualification/licence periods.
    "practitioner_sentinel_fields_by_lob": """
        SELECT coalesce((regexp_match(p.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE jsonb_path_exists(p.resource,
                 '$."address"[*]."period"."end" ? (@ like_regex "^(2999|9999)")'))
                 AS addr_period_end,
               count(*) FILTER (WHERE jsonb_path_exists(p.resource,
                 '$."address"[*]."period"."start" ? (@ like_regex "^1753")'))
                 AS addr_period_start_1753,
               count(*) FILTER (WHERE jsonb_path_exists(p.resource,
                 '$."qualification"[*].** ? (@.type() == "string" && @ like_regex "^(2999|9999)")'))
                 AS qualification_sentinel
        FROM {s}.practitioner p GROUP BY 1;""",
    "hs_name_unknown_by_lob": """
        SELECT coalesce((regexp_match(h.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(*) FILTER (WHERE upper(coalesce(h.name,'')) IN
                 ('UNKNOWN','INVALID','N/A','NOT APPLICABLE','NONE','TEST')) AS junk_name
        FROM {s}.healthcare_service h GROUP BY 1;""",
    "location_junk_by_lob": """
        SELECT coalesce((regexp_match(l.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*) FILTER (WHERE l.address_postalcode IN ('00000','99999','0')) AS junk_postal,
               count(*) FILTER (WHERE upper(coalesce(l.name,'')) IN
                 ('UNKNOWN','INVALID','N/A','NOT APPLICABLE','NONE','TEST')) AS junk_name
        FROM {s}.location l GROUP BY 1;""",
    # ---- address duplication (tested, not asserted [F-6]) ----
    "location_address_dup_by_lob": """
        SELECT coalesce((regexp_match(l.resource::text,
                         'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)'),
               count(*),
               count(DISTINCT (coalesce(l.address_line,''), coalesce(l.address_city,''),
                               coalesce(l.address_postalcode,''))) AS distinct_addresses
        FROM {s}.location l GROUP BY 1;""",
}

_POP_MATCH_SQL = """
SELECT ci.cell, count(*) FROM public.conformance_instance ci
WHERE ci.run_id = %(run)s GROUP BY 1;
"""

_LIVE_COUNT_SQL = "SELECT count(*) FROM {s}.{t};"

_CELL_LIVE_TABLE = {
    "Location": "location", "Practitioner": "practitioner",
    "PractitionerRole": "practitioner_role",
    "HealthcareService": "healthcare_service", "InsurancePlan": "insurance_plan",
}


def _assert_population_matches_run(conn, run_id: int, schema: str) -> None:
    """Precheck C ([O-11]): the live payer tables must hold exactly the
    validated population, else structural numerators and run denominators
    describe different data."""
    with conn.cursor() as cur:
        cur.execute(_POP_MATCH_SQL, {"run": run_id})
        run_counts = dict(cur.fetchall())
        for cell, table in _CELL_LIVE_TABLE.items():
            cur.execute(_LIVE_COUNT_SQL.format(s=schema, t=table))
            (live,) = cur.fetchone()
            if int(live) != int(run_counts.get(cell, 0)):
                raise SystemExit(
                    f"FATAL: live {schema}.{table} holds {live} rows but the "
                    f"validated run recorded {run_counts.get(cell, 0)} {cell} "
                    "instances. The population has drifted since validation; "
                    "structural analysis would mix two different datasets.")
        cur.execute(_LIVE_COUNT_SQL.format(s=schema, t="organization"))
        (live_org,) = cur.fetchone()
        expect = int(run_counts.get("Organization", 0)) + int(run_counts.get("Network", 0))
        if int(live_org) != expect:
            raise SystemExit(
                f"FATAL: live {schema}.organization holds {live_org} rows but "
                f"the validated run recorded {expect} Organization+Network "
                "instances. Population drift.")


def _size_analysis(conn, run_id: int, schema: str = "bcbs_az_new") -> dict:
    _assert_population_matches_run(conn, run_id, schema)
    out: dict = {}
    with conn.cursor() as cur:
        for key, sql in _SIZE_SQL.items():
            cur.execute(sql.format(s=schema), {"run": run_id})
            rows = cur.fetchall()
            if key == "networks":
                # [R2-1]: network names are the payer's own product/geography
                # branding — a de-anonymization vector in the shipped
                # dataset, not just the HTML. Only placeholder names ship;
                # the rest become neutral tokens.
                out[key] = [
                    {"name": (n if (n or "").upper() in _QUOTABLE_NET_NAMES
                              else f"(name withheld {i+1})"),
                     "lob": l}
                    for i, (n, l) in enumerate(rows)]
            elif key == "insurance_plans":
                out[key] = [{"name": n, "has_network_key": hn,
                             "has_coverage_area": hc, "lob": l}
                            for n, hn, hc, l in rows]
            elif key == "location_top_nonaz_states":
                out[key] = [{"state": st, "n": int(n)} for st, n in rows]
            elif key == "orgs_referenced_by_roles_by_lob":
                out[key] = {l: int(n) for l, n in rows}
            elif key in ("pr_network_refs_populated", "org_affiliation_rows",
                         "dar_extension_count"):
                out[key] = int(rows[0][0])
            else:
                # per-LOB rows: first column is the LOB, rest are counts.
                out[key] = [
                    {"lob": r[0], "counts": [int(x) if x is not None else 0
                                             for x in r[1:]]}
                    for r in rows]
        # Referential integrity, per edge, per source LOB.
        lob_expr = ("coalesce((regexp_match(resource::text,"
                    "'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)')")
        for t in ("practitioner", "organization", "location",
                  "practitioner_role", "healthcare_service", "insurance_plan"):
            cur.execute(_CREATE_LOB_MAP.format(t=t, s=schema,
                                               lob_expr=lob_expr))
        edges = []
        for name, src, ref, tgt, unnest in _REF_EDGES:
            cur.execute(_edge_sql(schema, src, ref, tgt, unnest))
            for lob, refs, unparseable, dangling, same_lob, cross_lob in cur.fetchall():
                edges.append({
                    "edge": name, "lob": lob, "refs": int(refs),
                    "unparseable": int(unparseable), "dangling": int(dangling),
                    "resolves_same_lob": int(same_lob),
                    "resolves_cross_lob": int(cross_lob)})
        out["reference_edges"] = edges
    return out


# ---- Track A completeness + reference audit (ig_audit, per LOB) --------

_LATEST_AUDIT_RUN_SQL = """
SELECT run_id FROM public.ig_audit_run
WHERE %(schema)s = ANY(schemas) ORDER BY run_id DESC LIMIT 1;
"""

_COMPLETENESS_SQL = """
SELECT a.element_path, a.resource_type, a.lob, a.n_rows, a.n_present,
       pe.min_card, pe.must_support
FROM public.ig_audit a
LEFT JOIN public.plannet_element pe
  ON pe.element_path = a.element_path AND pe.profile = a.profile
WHERE a.run_id = %(audit)s AND a.schema_name = %(schema)s;
"""


def _track_a(conn, schema: str, audit_run_id: int | None) -> dict:
    from provdir.quality.findings import requirement_class
    with conn.cursor() as cur:
        if audit_run_id is None:
            cur.execute(_LATEST_AUDIT_RUN_SQL, {"schema": schema})
            row = cur.fetchone()
            if row is None:
                return {"audit_run_id": None, "completeness": [],
                        "reference_audit": []}
            audit_run_id = int(row[0])
        cur.execute(_COMPLETENESS_SQL,
                    {"audit": audit_run_id, "schema": schema})
        completeness = [
            {"element_path": ep, "resource_type": rt, "lob": lob,
             "n_rows": int(nr), "n_present": int(np_) if np_ is not None else 0,
             "requirement": requirement_class(mc, ms)}
            for ep, rt, lob, nr, np_, mc, ms in cur.fetchall()]
        cur.execute(
            "SELECT element_path, split_part(element_path,'.',1), lob, "
            "target_type, n_refs, n_resolved, n_dangling, n_to_network, "
            "n_to_ods FROM public.ig_audit_ref "
            "WHERE run_id = %(audit)s AND schema_name = %(schema)s",
            {"audit": audit_run_id, "schema": schema})
        ref_audit = [
            {"element_path": ep, "resource_type": rt, "lob": lob,
             "target_type": tt, "n_refs": int(nr), "n_resolved": int(nres),
             "n_dangling": int(nd), "n_to_network": int(nn), "n_to_ods": int(no)}
            for ep, rt, lob, tt, nr, nres, nd, nn, no in cur.fetchall()]
    return {"audit_run_id": audit_run_id, "completeness": completeness,
            "reference_audit": ref_audit}


_SLICE_BINDINGS_SQL = """
SELECT element_path, binding_valueset, binding_strength
FROM public.plannet_element
WHERE ig_package LIKE %(pkglike)s AND element_path LIKE '%%:%%'
  AND binding_valueset IS NOT NULL;
"""


def _slice_bindings(conn, ig: str = "hl7.fhir.us.davinci-pdex-plan-net",
                    ig_version: str = "1.2.0") -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_SLICE_BINDINGS_SQL, {"pkglike": f"%{ig}#{ig_version}%"})
        return [{"element_path": ep, "binding_valueset": vs,
                 "binding_strength": st}
                for ep, vs, st in cur.fetchall()]


_PLANNET_META_SQL = """
SELECT element_path, min_card, max_card, primary_type, must_support,
       binding_strength, binding_valueset
FROM public.plannet_element WHERE ig_package LIKE %(pkglike)s;
"""


def _plannet_meta(conn, ig: str = "hl7.fhir.us.davinci-pdex-plan-net",
                  ig_version: str = "1.2.0") -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_PLANNET_META_SQL, {"pkglike": f"%{ig}#{ig_version}%"})
        return [{"element_path": ep, "min_card": mc, "max_card": mx,
                 "primary_type": pt, "must_support": ms,
                 "binding_strength": bs, "binding_valueset": bv}
                for ep, mc, mx, pt, ms, bs, bv in cur.fetchall()]


# ---- USCDI + Medicare Plan Finder coverage lenses ([O-30]: produced by
# the build itself). Pooled rows come from the newest element_coverage run
# holding this payer; the mpf checks are additionally re-measured grouped
# by source LOB so the ODS feed can never pool into a readiness figure.

_MPF_ELIGIBILITY = (
    "Public-record determination, recorded in "
    "provdir.quality.mpf.MA_OVERRIDE_INCLUDE: the plan publicly sells "
    "Medicare Advantage products and the captured directory carries "
    "MA-designated network records; the automated signal set does not trip "
    "on this capture's naming and the endpoint's manifest status keeps it "
    "outside the classifier's roster, so the mpf lens is measured directly "
    "for this payer with the same check set as every MA payer.")

_LOB_JSON_EXPR = ("coalesce((regexp_match(resource::text,"
                  "'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)')")

_MPF_TABLE = {
    "PractitionerRole": "practitioner_role", "Practitioner": "practitioner",
    "Location": "location", "Organization": "organization",
    "OrganizationAffiliation": "organization_affiliation",
    "InsurancePlan": "insurance_plan",
}


def _coverage_lenses(conn, schema: str = "bcbs_az_new") -> dict:
    from provdir.quality.mpf import MPF_CHECKS, MPF_SOURCE_PROVENANCE

    with conn.cursor() as cur:
        cur.execute(
            "SELECT run_id, notes FROM public.element_coverage_run "
            "WHERE run_id IN (SELECT DISTINCT run_id FROM "
            "  public.element_coverage WHERE payer_id = %(p)s) "
            "ORDER BY run_id DESC LIMIT 1", {"p": schema})
        row = cur.fetchone()
        if row is None:
            return {}
        cov_run, cov_notes = int(row[0]), row[1] or {}
        asof = (cov_notes.get("asof") or {}).get(schema)
        cur.execute(
            "SELECT assessment, resource_type, element, denominator, "
            "populated, required, mapping, max(measured_at) OVER () "
            "FROM public.element_coverage "
            "WHERE run_id = %(r)s AND payer_id = %(p)s "
            "  AND assessment IN ('uscdi', 'mpf') "
            "ORDER BY assessment, resource_type, element",
            {"r": cov_run, "p": schema})
        fetched = cur.fetchall()
        rows = [{"assessment": a, "resource_type": rt, "element": el,
                 "denominator": int(d), "populated": int(p), "required": rq,
                 "mapping": mp}
                for a, rt, el, d, p, rq, mp, _ in fetched]
        mpf_measured = max((r[7] for r in fetched if r[0] == "mpf"),
                           default=None)

        # per-LOB mpf re-measurement over the live schema (Precheck C has
        # already asserted the live population equals the validated one).
        mpf_by_lob = []
        for c in MPF_CHECKS:
            t = f"{schema}.{_MPF_TABLE[c.resource_type]}"
            where = f"WHERE {c.cell_where}" if c.cell_where else ""
            cur.execute(
                f"SELECT {_LOB_JSON_EXPR} AS lob, count(*), "
                f"count(*) FILTER (WHERE {c.condition_sql}) "
                f"FROM {t} {where} GROUP BY 1",
                {"asof": asof})
            got = cur.fetchall()
            if not got:
                mpf_by_lob.append({"resource_type": c.resource_type,
                                   "element": c.element, "lob": "(none)",
                                   "denominator": 0, "populated": 0,
                                   "required": c.required})
            for lob, denom, pop in got:
                mpf_by_lob.append({"resource_type": c.resource_type,
                                   "element": c.element, "lob": lob,
                                   "denominator": int(denom),
                                   "populated": int(pop),
                                   "required": c.required})

    prov = MPF_SOURCE_PROVENANCE
    return {
        "run": cov_run,
        "rows": rows,
        "mpf_by_lob": mpf_by_lob,
        "mpf_measured_at": (str(mpf_measured)[:10] if mpf_measured
                            else "(measurement date not recorded)"),
        "mpf_eligibility": _MPF_ELIGIBILITY,
        "mpf_provenance": (
            f"{prov['document']} ({prov['dated']}), under "
            f"{prov['regulation']}, naming {prov['fhir_format']} as the "
            f"required format. {prov['access_note']}"),
    }


def build_full(conn, *, run_id: int, audit_run_id: int | None, out_dir: Path) -> dict:
    build = F.build_findings(conn, run_id=run_id)

    # Assemble payload.
    identity = _run_identity(conn, run_id)
    payload = {
        "identity": identity,
        "totals": {
            "resources_validated": build.resources_validated,
            "resources_in_scope_by_cell": build.resources_in_scope_by_cell,
            "occurrences_total": sum(f.occurrences for f in build.findings),
            "findings_total": len(build.findings),
            "unmatched_rate_by_distinct_paths": round(build.unmatched_rate, 4),
            "unmatched_row_coverage": round(build.unmatched_row_coverage, 4),
            "normalizer_distinct_expressions": build.normalizer_calls,
        },
        "findings": [_finding_to_dict(f) for f in build.findings],
        "resources_affected_in_category": [
            {"resource_type": rt, "category": cat, "resources_affected": n}
            for rt, cat, n in build.resources_affected_in_category
        ],
        "lob": {
            "denominators": [
                {"resource_type": c, "lob": l, "n": n}
                for (c, l), n in sorted(build.lob_denominators.items())
            ],
            "any_finding": [
                {"resource_type": c, "lob": l, "n": n}
                for (c, l), n in sorted(build.lob_any_finding.items())
            ],
            "severity": [
                {"resource_type": c, "lob": l, "severity": s, "n": n}
                for (c, l, s), n in sorted(build.lob_severity.items())
            ],
            "category": [
                {"resource_type": c, "lob": l, "category": cat, "n": n}
                for (c, l, cat), n in sorted(build.lob_category.items())
            ],
        },
        "requirement_category": [
            {"resource_type": rt, "category": cat, "requirement": req,
             "match_grain": grain, "resources_affected": ra, "occurrences": occ}
            for rt, cat, req, grain, ra, occ in build.requirement_category
        ],
        "standard_layers": [
            {"resource_type": rt, "lob": l, "layer": lay,
             "resources_affected": ra, "occurrences": occ}
            for rt, l, lay, ra, occ in build.standard_layers
        ],
        "binding_valuesets": [
            {"resource_type": rt, "lob": l, "category": cat,
             "valueset_url": vs, "resources_affected": ra, "occurrences": occ}
            for rt, l, cat, vs, ra, occ in build.binding_valuesets
        ],
        "extension_urls": [
            {"resource_type": c, "url": u, "occurrences": o,
             "resources_affected": ra}
            for c, u, o, ra in build.ext_urls
        ],
        "size_analysis": _size_analysis(conn, run_id),
        "track_a": _track_a(conn, "bcbs_az_new", audit_run_id),
        "element_rollup": [
            {"resource_type": rt, "element_path": ep, "requirement": req,
             "match_grain": g, "category": cat,
             "resources_affected": ra, "occurrences": occ}
            for rt, ep, req, g, cat, ra, occ in build.element_rollup
        ],
        "plannet_element_meta": _plannet_meta(conn),
        "coverage_lenses": _coverage_lenses(conn),
        # Slice bindings: lets the renderer resolve WHICH profile slice a
        # binding finding belongs to via its failing value set ([O-18]).
        "plannet_slice_bindings": _slice_bindings(conn),
        "unmatched_paths_top20": [
            {
                "element_path": ep,
                "distinct_expressions": build.unmatched_paths.get(ep, 0),
                "row_occurrences": build.unmatched_row_occurrences.get(ep, 0),
            }
            for ep, _ in sorted(build.unmatched_row_occurrences.items(),
                                key=lambda kv: -kv[1])[:20]
        ],
        "warnings": build.warnings,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "findings.json").write_text(json.dumps(payload, indent=2, default=str),
                                           encoding="utf-8")
    _write_data_dictionary(out_dir / "data-dictionary.md", identity)
    _write_evidence_md(out_dir / "evidence.md", run_id, identity)

    # Render HTML in the same process — one build produces all four artifacts.
    try:
        from build_azblue_report_html import render
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "build_azblue_report_html",
            Path(__file__).parent / "build_azblue_report_html.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        render = mod.render
    render(payload, out_dir / f"azblue-run{run_id}.html")

    return payload


# Capture window = the observation window of the snapshot that was
# validated: min/max of last_seen_at (when each record was last confirmed at
# the source), NOT min(ingested_at) — ingest history spans every historical
# pull and misreads as continuous observation ([F-20]).
_CAPTURE_WINDOW_SQL = """
SELECT min(lo)::date, max(hi)::date FROM (
  SELECT min(coalesce(last_seen_at, ingested_at)) lo,
         max(coalesce(last_seen_at, ingested_at)) hi FROM {s}.organization
  UNION ALL SELECT min(coalesce(last_seen_at, ingested_at)),
         max(coalesce(last_seen_at, ingested_at)) FROM {s}.location
  UNION ALL SELECT min(coalesce(last_seen_at, ingested_at)),
         max(coalesce(last_seen_at, ingested_at)) FROM {s}.practitioner
  UNION ALL SELECT min(coalesce(last_seen_at, ingested_at)),
         max(coalesce(last_seen_at, ingested_at)) FROM {s}.practitioner_role
  UNION ALL SELECT min(coalesce(last_seen_at, ingested_at)),
         max(coalesce(last_seen_at, ingested_at)) FROM {s}.healthcare_service
  UNION ALL SELECT min(coalesce(last_seen_at, ingested_at)),
         max(coalesce(last_seen_at, ingested_at)) FROM {s}.insurance_plan
) u;
"""

# Only these run-note keys reach the payload; the raw notes carry internal
# orchestration detail (schema names, worker counts, chunk plans) that the
# external artifact must not ship ([F-14]).
_NOTE_WHITELIST = ("tx", "tho")


def _scrub_notes(notes) -> dict:
    if not isinstance(notes, dict):
        return {}
    out = {k: v for k, v in notes.items() if k in _NOTE_WHITELIST}
    sample = notes.get("sample") or {}
    for k in ("carin_bb_stripped", "instances_stripped"):
        if k in sample:
            out[k] = sample[k]
    return out


def _run_identity(conn, run_id: int, schema: str = "bcbs_az_new") -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ig, ig_version, instances, started_at, finished_at, notes "
            "FROM public.conformance_run WHERE run_id = %s", (run_id,))
        ig, igv, n, t0, t1, notes = cur.fetchone()
        cur.execute(_CAPTURE_WINDOW_SQL.format(s=schema))
        cap_lo, cap_hi = cur.fetchone()
    return {
        "run_id": run_id,
        "ig": ig, "ig_version": igv,
        "instances": int(n) if n is not None else None,
        "started_at": str(t0), "finished_at": str(t1) if t1 else None,
        "validator": "org.hl7.fhir.validator_cli 6.10.1",  # pinned in run 14 bat
        # External identity: the rendered report carries only these. The
        # payer is not named publicly ("a mid-size Blues plan" is permitted).
        "source_name": "a mid-size Blues plan",
        "lobs_represented": "Medicaid-LOB · Medicare-LOB",
        "data_captured_from": str(cap_lo), "data_captured_to": str(cap_hi),
        "analysis_date": str(t0)[:10],
        "notes": _scrub_notes(notes),
    }


def _write_data_dictionary(path: Path, identity: dict) -> None:
    md = f"""# findings.json — data dictionary

Report source: conformance_run {identity['run_id']}, IG {identity['ig']}#{identity['ig_version']},
HL7 validator {identity['validator']}.

## `findings[]` fields

| field | unit | source | denominator | notes |
| --- | --- | --- | --- | --- |
| `resource_type` | text | `conformance_issue.cell` | — | Plan-Net resource type; `Network` is a synthetic split of `Organization`. |
| `element_path` | text | normalized `conformance_issue.expression` | — | Output of `provdir.quality.findings.normalize_path`. `(resource-level)` for empty/NULL. |
| `message_id` | text | `conformance_issue.message_id` | — | `(no-message-id)` if NULL upstream (explicit bucket, never coalesced with unrelated messages). |
| `severity` | text | `conformance_issue.severity` | — | `error` / `warning` / `information`. |
| `category` | text | `REPORT_CATEGORY_SQL_TEMPLATE` over `CATEGORY_SQL` | — | One of nine: the six base buckets (`profile`, `binding-required`, `binding-extensible`, `extension-unresolvable`, `terminology-unavailable`, `best-practice`) plus three report-level reroutes: `validator-limitation` (regex timeouts), `version-stamping` (instance stamps '4.0.1' onto foreign code systems — 12.4M rows on run 14), `publisher-infrastructure` (the vendor `ex:createdAt` meta extension, 1/resource). Only `MATERIAL_CATEGORIES` (`profile`, `binding-required`, `binding-extensible`) enter grades and headline rates. |
| `match_grain` | text | `classify_path` | — | `exact` (Plan-Net element, direct or slice-collapsed), `ancestor-1`/`ancestor-2` (nearest enumerated parent), `resource-level` (whole-record message), `none` (base-FHIR residual). |
| `slice_paths` | list | `build_slice_map` | — | The slice-qualified Plan-Net paths behind a collapsed key; length > 1 means the row spans several profile slices (binding attribution then comes from `binding_valuesets`). |
| `occurrences` | int | `COUNT(*)` on the finding group | — | Raw issue-row count. **Do not report alone**; always paired with `resources_affected`. |
| `resources_affected` | int | `COUNT(DISTINCT conformance_instance.resource_id)` on the finding group | — | The number of distinct FHIR resources this finding touches. |
| `resources_in_scope` | int | `COUNT(*) FROM conformance_instance WHERE run_id=:run AND cell=<resource_type>` | — | Denominator: every validated resource of that type. For run 14 (100% validated) this equals population count. Resources that produced zero issues **are** in the denominator. |
| `repeats_per_affected_resource` | float, ≥1.0 | `occurrences / resources_affected` | — | Intra-resource array repetition multiplier. High values (e.g. ~3163 for `whereValid` qualification codings) surface pathological arrays. |
| `sample_expression` | text | `array_agg(expression ORDER BY length, expression)[1]` | — | Shortest representative expression in the group. Ties broken lexicographically. |
| `sample_details` | text or NULL | `array_agg(details FILTER (WHERE details IS NOT NULL) ORDER BY length, details)[1]` | — | Shortest non-NULL details; NULL if every row in the group has NULL details. |
| `element_path_unmatched` | bool | comparison to `plannet_element.element` | — | `true` when the normalized path did not join to Plan-Net's element universe — exact OR slice-collapsed form (surfaced in `unmatched_paths_top20`). |
| `requirement` | text | `classify_path` over `plannet_element.min_card` / `must_support` | — | `mandatory` (min ≥ 1), `must-support` (MS, min 0), `other` (enumerated, neither flag), `mixed` (slices collapse with disagreeing classes), `record-level` (whole-record message), `base-fhir` (core-FHIR sub-element the profile does not enumerate — counted, not dropped). Ancestor-grain rows inherit the nearest enumerated parent's class; see `match_grain`. |
| `by_lob` | object | finding rollup re-grouped with `inst_lob` joined | per-LOB denominators in `lob.denominators` | `{{lob: {{occurrences, resources_affected}}}}`. LOB from the `urn:source_name` marker in the raw resource JSON. |

## `totals`

| field | notes |
| --- | --- |
| `resources_validated` | Reconciled to `conformance_run.instances` at build-time (Precheck B). |
| `resources_in_scope_by_cell` | Denominator per `resource_type`. Sum equals `resources_validated`. |
| `occurrences_total` | Sum of `occurrences` across all findings; must equal `SELECT COUNT(*) FROM conformance_issue WHERE run_id=:run`. |
| `findings_total` | Number of rows in `findings[]`. |
| `unmatched_rate_by_distinct_paths` | Fraction of distinct normalized paths that failed to join `plannet_element`. Build fails if above `TRIPWIRE_THRESHOLD` (see `provdir.quality.findings`). |
| `unmatched_row_coverage` | Fraction of `conformance_issue` rows whose expression normalizes to an unmatched path. Row-weighted counterpart to `unmatched_rate_by_distinct_paths`; these two can diverge sharply. Reported for transparency. |
| `normalizer_distinct_expressions` | Number of unique `conformance_issue.expression` values fed to the normalizer. |

## `resources_affected_in_category[]`

`resources_affected_in_category` is **not derivable** from `findings[]` alone
(the same `resource_id` can appear in multiple same-category findings, so
summing `resources_affected` overcounts). It is a separate SQL:
`SELECT resource_type, category, COUNT(DISTINCT resource_id) FROM <scoped> GROUP BY 1,2`.
Its rate is `resources_affected_in_category / resources_in_scope`, bounded ≤ 1.0.

## `lob` block

LOB is **derived**, not stored on the validation row: each validated record is
joined back to its raw JSON in the payer schema and the
`urn:source_name:<lob>` marker is read (same rule as
`docs/evidence/azblue/decide2.sql`). Known values on run 14: `hcaz`
(rendered as Medicaid-LOB), `azblue` (rendered as Medicare-LOB), `ods`
(rendered as OtherDataSource; Organization only — a third source marker
whose records span the whole country, are NPI-complete, and are virtually
never referenced by any provider role), `(none)` if absent. The build fails if attribution does
not cover every validated record.

| field | grain | notes |
| --- | --- | --- |
| `lob.denominators` | (resource_type, lob) | record counts; per-LOB denominator for every LOB rate. |
| `lob.any_finding` | (resource_type, lob) | distinct records with ≥1 issue. |
| `lob.severity` | (resource_type, lob, severity) | distinct records with ≥1 issue at that severity. |
| `lob.category` | (resource_type, lob, category) | distinct records with ≥1 issue in that category. |

## `requirement_category[]`

Grain (resource_type, category, requirement, match_grain): distinct records
affected + raw occurrences, where `requirement` is the Plan-Net class of
the element the issue sits on (see `findings[].requirement`).
Distinct-record counts at this grain are NOT derivable from `findings[]`
(same record in several findings), hence the dedicated query. Summing
`occurrences` over this table accounts for 100% of issue rows.

## `standard_layers[]`

Grain (resource_type, lob, layer): which standard's rule fired — `r4-base`,
`us-core`, `plan-net`, or `unattributed` — decided by `STANDARD_LAYER_SQL`
(message_id prefix first, then the profile / value-set URL in the message
text). Caveat rendered in-frame: the run validates Plan-Net only, so the
us-core rows are the US Core rules Plan-Net imports, not a USCDI v3
conformance measurement.

## `binding_valuesets[]`

Grain (resource_type, lob, category, valueset_url) over the two binding
categories: the failing value set extracted from the message text. This is
the slice-exact attribution wherever a collapsed `element_path` spans
profile slices with different bindings.

## `track_a`

Element-population audit (`ig_audit`) joined per element per LOB:
`completeness[]` carries n_rows / n_present / requirement per
(element_path, resource_type, lob) — the validator cannot see absent
elements, so §C of the HTML comes exclusively from here.
`reference_audit[]` carries the audit's per-element reference resolution
(n_refs / n_resolved / n_dangling) per LOB.

## `extension_urls[]`

For the `extension-unresolvable` AND `publisher-infrastructure` categories:
the defining URL (or non-URL extension token, e.g. `ex:createdAt`)
extracted from the message text, with occurrences and distinct records per
(resource_type, url). On this run every row is the publisher's
`ex:createdAt` meta extension (publisher-infrastructure); the
extension-unresolvable category is empty. Distinct URLs can share one
finding after normalization, so this is its own grouped scan.

## `element_rollup[]`, `plannet_element_meta[]`, `coverage_lenses`

All produced by the build itself: `element_rollup` is the §D input (exact
distinct records per (resource_type, element_path, requirement,
match_grain, category); a strict partition of every issue row);
`plannet_element_meta` carries each Plan-Net element's cardinality, type,
must-support flag and binding for the §D "what the profile expects"
column; `coverage_lenses` carries the USCDI v3 and Medicare Plan Finder
element-coverage rows from the newest element_coverage run holding this
payer, plus a per-source re-measurement of every MPF check
(`mpf_by_lob`), the measurement date, the MA-eligibility basis, and the
CMS rule-set provenance.

## `size_analysis`

Structural population observations (graph reachability, network references,
plan shape) computed from the payer schema — descriptive context for §E of
the HTML, not conformance findings. Includes forward referential-integrity
checks (`dangling_*` keys): a record counts as dangling if at least one of
its references of that kind names a target id absent from the captured
population. The validator cannot see these — resources were validated
standalone — so the anti-joins here are the only place reference resolution
is measured.

## Render states (§9.1a + §9.1b of the evaluation framework)

A rendered cell shows a banded rate with `n` (never `100.0%` / `0.0%`), or
exactly one absent-state word from §9.1a's closed list: **not measured** ·
**wrong element** · **too few rows** · **no signal**. On this full-census
run, §9.1b replaces *too few rows* for sub-floor cells with the exact count
("1 of 1 records") — a census cell has no sampling error to guard, and
suppression would hide the sole InsurancePlan's failed network constraint.

## Grades (badges)

Good/OK/Bad grades per (resource_type, material category) test — the
explicitly requested reading aid, recorded as a bounded exception in
framework §9.1b. Rules (constants in `scripts/build_azblue_report_html.py`):
fixed fleet-wide cuts (`BADGE_GOOD_MAX=0.10`, `BADGE_BAD_MIN=0.40`); Good
requires no error-severity finding in the cell; Bad requires error-severity
presence (warning-only cells cap at OK); cells below `ROW_FLOOR` are
ungraded and excluded from the tally; grades never render without the
banded rate and `n`; only material categories are graded.

## Numbers we do NOT report

- `occurrences` alone.
- Per-payer comparison (single-payer run).
- `pass` / `fail` labels or claims of regulatory compliance.
- "D3" as a label — the report renders "unweighted mean of per-resource rates" instead.
"""
    path.write_text(md, encoding="utf-8")


def _write_evidence_md(path: Path, run_id: int, identity: dict) -> None:
    md = f"""# findings.json — evidence & reproduction SQL

Reproducibility target: every number in `findings.json` for
conformance_run {run_id} re-derives from `public.conformance_issue` and
`public.conformance_instance` given (a) the SQL below and (b) the
normalizer + classifier code at the pinned commit — the `element_path` /
`requirement` / `category` axes are functions of that code, so SQL alone
reproduces message-id-grain counts, and the exact finding grain reproduces
by running:

```
python -c "import sys; sys.path.insert(0,'src');
from provdir.quality.findings import normalize_path; print(normalize_path(sys.argv[1]))" "<expression>"
```

**Pinned versions:**
- IG: {identity['ig']} @ {identity['ig_version']}
- HL7 validator: {identity['validator']}
- Normalizer: `provdir.quality.findings.normalize_path` (commit SHA recorded
  in the constant's docstring on re-baseline).

## Reproduce one finding

Given a `(resource_type, element_path, message_id, severity)` tuple from
`findings.json`:

```sql
WITH scoped AS (
  SELECT ci.cell AS resource_type,
         ci.expression, ci.details, ci.message_id, ci.severity,
         inst.resource_id
  FROM public.conformance_issue ci
  JOIN public.conformance_instance inst USING (run_id, payer_id, cell, instance)
  WHERE ci.run_id = {run_id}
    AND ci.cell = :resource_type
    AND (ci.message_id = :message_id OR (ci.message_id IS NULL AND :message_id = '(no-message-id)'))
    AND ci.severity = :severity
)
SELECT count(*)                          AS occurrences,
       count(DISTINCT resource_id)       AS resources_affected,
       array_agg(DISTINCT resource_id)   FILTER (WHERE resource_id IS NOT NULL)
                                         AS sample_resource_ids
FROM scoped;
```

(The `element_path` filter is applied client-side against the normalizer
output; the SQL above returns a superset if multiple element paths share a
`message_id` on the same resource type.)

## Reproduce a per-LOB count

LOB is derived from the raw resource JSON. For a (resource_type, lob) pair:

```sql
SELECT coalesce((regexp_match(t.resource::text,
                 'urn:source_name:([a-zA-Z0-9_.-]+)'))[1], '(none)') AS lob,
       count(*)
FROM public.conformance_instance ci
JOIN <payer_schema>.<table_for_cell> t ON t.id = ci.resource_id
WHERE ci.run_id = {run_id} AND ci.cell = :resource_type
GROUP BY 1;
```

Cell→table map: Organization and Network → `organization`; the other five
cells map to their snake_case table. Add the `conformance_issue` join from
the previous block to reproduce per-LOB affected counts.

## Reconciliation gates

Every build asserts these before writing artifacts:

- `sum(occurrences) across findings[] == COUNT(*) FROM conformance_issue WHERE run_id={run_id}`
- `sum(resources_in_scope over resource_types) == conformance_run.instances == 3,133,014`
- `COUNT(*) FROM conformance_instance WHERE run_id={run_id} AND resource_id IS NULL == 0`
"""
    path.write_text(md, encoding="utf-8")


# ---- CLI ----------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conformance-run", dest="run_id", type=int,
                    help="conformance_run to report on. Without it, "
                         "provdir.quality.conformance.choose_run picks it.")
    ap.add_argument("--audit-run", dest="audit_run_id", type=int, default=None,
                    help="Track A audit run to join for the F4 element matrix.")
    ap.add_argument("--expect-payers", default=None, metavar="N|a,b,c")
    ap.add_argument("--out", type=Path, default=None,
                    help="output directory; created if missing.")
    ap.add_argument("--dry-run", action="store_true",
                    help="only run the normalizer over distinct expressions and "
                         "report unmatched_rate + depth-3 hits. Seeds "
                         "TRIPWIRE_THRESHOLD baseline.")
    ap.add_argument("--allow-unverified", action="store_true")
    args = ap.parse_args()

    conn = pg_connection()
    # dry-run does not need a transaction; full build does.
    conn.autocommit = args.dry_run
    try:
        run_id, note = select_run(
            conn, run_id=args.run_id,
            expect_payers=parse_expect_payers(args.expect_payers),
            allow_unverified=args.allow_unverified)
        print(f"run selection: {note}", file=sys.stderr)

        if args.dry_run:
            run_dry_measurement(conn, run_id=run_id)
            return 0

        if args.out is None:
            print("--out is required for a full build", file=sys.stderr)
            return 2

        payload = build_full(conn, run_id=run_id,
                             audit_run_id=args.audit_run_id,
                             out_dir=args.out)
        conn.commit()
        print(f"wrote {args.out}/findings.json ({payload['totals']['findings_total']:,} findings, "
              f"{payload['totals']['occurrences_total']:,} occurrences)")
        print(f"wrote {args.out}/data-dictionary.md")
        print(f"wrote {args.out}/evidence.md")
        print(f"wrote {args.out}/azblue-run{run_id}.html")
        for w in payload["warnings"]:
            print(f"WARNING: {w}", file=sys.stderr)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

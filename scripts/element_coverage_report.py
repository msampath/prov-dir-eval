#!/usr/bin/env python
"""USCDI v3 / MPF / Plan-Net-extensions element coverage report.

Reads straight from public.element_coverage (like conformance_report.py reads
from public.conformance_issue) rather than from the JSON artifacts
run_element_coverage() writes -- so `--run-id` can point at any past run, not
just the most recent one, and the MA roster/disclaimers are always the CURRENT
classifier/rule-set logic rather than whatever was frozen into an old
artifact.

    python scripts/element_coverage_report.py                # latest run
    python scripts/element_coverage_report.py --run-id 4 --json out.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import load_manifest  # noqa: E402
from provdir.etl.loader import pg_connection, set_search_path  # noqa: E402
from provdir.quality.element_coverage import payer_asof  # noqa: E402
from provdir.quality.mpf import (  # noqa: E402
    MPF_CALLOUTS, MPF_SOURCE_PROVENANCE, PLANNET_EXT_ATTRIBUTION, classify_ma,
)
from provdir.quality.uscdi import USCDI_ELEMENT_META, uscdi_headline  # noqa: E402

# Freshness percentiles over aetna_cvs.practitioner_role's ~500M rows would sort
# the whole table; TABLESAMPLE keeps the cost bounded while still estimating
# the distribution -- same tradeoff sample_for_validation.py makes.
FRESHNESS_SAMPLE_ABOVE_ROWS = 2_000_000
FRESHNESS_SAMPLE_TARGET_ROWS = 200_000


def latest_run(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(run_id) FROM public.element_coverage_run WHERE finished_at IS NOT NULL"
        )
        r = cur.fetchone()
        return r[0] if r else None


def fetch_rows(conn, run_id: int, assessment: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT payer_id, resource_type, element, denominator, populated, "
            "method, required, mapping, detail "
            "FROM public.element_coverage WHERE run_id = %s AND assessment = %s "
            "ORDER BY payer_id, resource_type, element",
            (run_id, assessment),
        )
        cols = ["payer_id", "resource_type", "element", "denominator", "populated",
                "method", "required", "mapping", "detail"]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def pct(r: dict) -> float | None:
    return round(100.0 * r["populated"] / r["denominator"], 1) if r["denominator"] else None


def by_payer(rows: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r["payer_id"], []).append(r)
    return out


# --- USCDI ---------------------------------------------------------------

def print_uscdi(rows: list[dict]) -> None:
    print("=== USCDI v3 element coverage ===")
    print("No official USCDI v3 -> Plan-Net element mapping exists (US Core maps")
    print("Care Team Member to CareTeam only, a resource Plan-Net does not define).")
    print("Every row below is an inference labeled by `mapping`; see uscdi.py.")
    print("USCDI v3 has NO Facility class (that arrives in USCDI v4+).")
    print()
    grouped = by_payer(rows)
    print(f"{'payer':26s} {'headline%':>10}  (unweighted mean, measurable presence-tier rows)")
    for payer in sorted(grouped):
        h = uscdi_headline(grouped[payer])
        print(f"{payer:26s} {h if h is not None else '(n/a)':>10}")
    print()

    print("--- per-element (all payers) ---")
    print(f"{'payer':22s} {'resource':20s} {'element':38s} {'req':>4} {'mapping':13s} "
          f"{'method':9s} {'pct':>7}")
    for r in rows:
        p = pct(r)
        print(f"{r['payer_id']:22s} {r['resource_type']:20s} {r['element']:38s} "
              f"{'Y' if r['required'] else 'n':>4} {r['mapping']:13s} {r['method']:9s} "
              f"{'n/a' if p is None else f'{p:.1f}':>7}")
    print()


# --- MPF -------------------------------------------------------------------

def _freshness(conn, payer: str, asof) -> dict:
    if asof is None:
        return {"median_days": None, "p90_days": None, "method": "no-asof"}
    with conn.cursor() as cur:
        # schemaname MUST be pinned to this payer: pg_stat_user_tables is
        # cluster-wide, so an unqualified `relname = 'practitioner_role'`
        # matches one row per payer schema (63+ of them) and fetchone() would
        # silently grab an arbitrary one -- e.g. aetna_cvs's 500M-row count
        # for a payer with 200 rows, wrongly forcing the TABLESAMPLE branch
        # (or the reverse: a small schema's count for a genuinely huge payer,
        # wrongly skipping it and sorting the whole table).
        cur.execute("SELECT n_live_tup FROM pg_stat_user_tables "
                    "WHERE relname = 'practitioner_role' AND schemaname = %s", (payer,))
        r = cur.fetchone()
        live = int(r[0]) if r else 0
    method = "exact"
    sample_clause = ""
    if live > FRESHNESS_SAMPLE_ABOVE_ROWS:
        method = "tablesample"
        pct_sample = min(100.0, 100.0 * FRESHNESS_SAMPLE_TARGET_ROWS / live)
        sample_clause = f" TABLESAMPLE SYSTEM ({pct_sample})"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY days), "
            f"percentile_cont(0.9) WITHIN GROUP (ORDER BY days) "
            f"FROM (SELECT extract(epoch FROM (%(asof)s::timestamptz - meta_last_updated)) "
            f"/86400.0 AS days FROM practitioner_role{sample_clause} "
            f"WHERE payer_id = %(payer)s AND meta_last_updated IS NOT NULL) s",
            {"asof": asof, "payer": payer},
        )
        row = cur.fetchone()
    med, p90 = row if row else (None, None)
    return {
        "median_days": round(med, 1) if med is not None else None,
        "p90_days": round(p90, 1) if p90 is not None else None,
        "method": method,
    }


def print_mpf(conn, manifest, rows: list[dict]) -> tuple[list[dict], dict]:
    print("=== MPF (Medicare Advantage Plan Finder) element coverage ===")
    print(f"source: {MPF_SOURCE_PROVENANCE['document']} ({MPF_SOURCE_PROVENANCE['dated']})")
    print(f"  regulation: {MPF_SOURCE_PROVENANCE['regulation']}, "
          f"fhir format: {MPF_SOURCE_PROVENANCE['fhir_format']}")
    print(f"  ACCESS NOTE: {MPF_SOURCE_PROVENANCE['access_note']}")
    print()

    roster = classify_ma(conn, manifest)
    print("--- MA roster (all known payers; classified via 5 signals + overrides) ---")
    for r in roster:
        flag = "MA" if r["classified"] else "--"
        detail = r["override"] or ", ".join(e["signal"] for e in r["evidence"]) or "(no signal)"
        print(f"  [{flag}] {r['payer']:24s} {detail}")
    ma_keys = {r["payer"] for r in roster if r["classified"]}
    print()

    grouped = by_payer(rows)
    present = sorted(k for k in grouped if k in ma_keys)
    missing = sorted(ma_keys - set(grouped))
    if missing:
        print(f"(MA-classified but not in this run's MPF rows: {', '.join(missing)})")
    print("--- per-element (classified-MA payers only) ---")
    print(f"{'payer':22s} {'resource':20s} {'element':34s} {'req':>4} {'method':9s} {'pct':>7}")
    for payer in present:
        for r in grouped[payer]:
            p = pct(r)
            print(f"{r['payer_id']:22s} {r['resource_type']:20s} {r['element']:34s} "
                  f"{'Y' if r['required'] else 'n':>4} {r['method']:9s} "
                  f"{'n/a' if p is None else f'{p:.1f}':>7}")
    print()

    print("--- freshness (staleness AT CAPTURE -- not the regulatory MPF metric) ---")
    freshness = {}
    for payer in present:
        set_search_path(conn, payer)
        asof = payer_asof(conn, payer)
        fr = _freshness(conn, payer, asof)
        within30 = next(
            (r for r in grouped[payer]
             if r["resource_type"] == "PractitionerRole"
             and r["element"] == "last_updated_within_30d_of_capture"),
            None,
        )
        fr["pct_within_30d"] = pct(within30) if within30 else None
        freshness[payer] = fr
        print(f"  {payer:24s} median={fr['median_days']!s:>7}d p90={fr['p90_days']!s:>7}d "
              f"within30d={fr['pct_within_30d']!s:>6}%  ({fr['method']})")
    print()

    print("--- callouts ---")
    for c in MPF_CALLOUTS:
        print(f"  - {c}")
    print()
    return roster, freshness


# --- Plan-Net extensions -----------------------------------------------

def print_plannet_ext(rows: list[dict]) -> None:
    print("=== Plan-Net directory extensions ===")
    print(f"attribution: {PLANNET_EXT_ATTRIBUTION}")
    print()
    grouped = by_payer(rows)
    print(f"{'payer':22s} {'resource':20s} {'element':34s} {'method':9s} {'pct':>7}")
    for payer in sorted(grouped):
        for r in grouped[payer]:
            p = pct(r)
            print(f"{r['payer_id']:22s} {r['resource_type']:20s} {r['element']:34s} "
                  f"{r['method']:9s} {'n/a' if p is None else f'{p:.1f}':>7}")
    print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", type=int)
    ap.add_argument("--json", help="also write the full report as JSON here")
    args = ap.parse_args()

    manifest = load_manifest()
    conn = pg_connection()
    conn.autocommit = True
    try:
        run_id = args.run_id or latest_run(conn)
        if not run_id:
            raise SystemExit("no finished element_coverage_run found")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT assessments, started_at, finished_at FROM public.element_coverage_run "
                "WHERE run_id = %s", (run_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise SystemExit(f"no element_coverage_run with run_id={run_id}")
        assessments, t0, t1 = row
        mins = (t1 - t0).total_seconds() / 60 if t1 else None
        print(f"element_coverage run {run_id}: {assessments}"
              + (f", {mins:.1f}m" if mins else " (UNFINISHED)"))
        print()

        payload = {"run_id": run_id, "assessments": assessments}

        if "uscdi" in assessments:
            rows = fetch_rows(conn, run_id, "uscdi")
            print_uscdi(rows)
            payload["uscdi"] = {
                "element_meta": USCDI_ELEMENT_META,
                "rows": rows,
                "headline_by_payer": {p: uscdi_headline(rs) for p, rs in by_payer(rows).items()},
            }

        if "mpf" in assessments:
            rows = fetch_rows(conn, run_id, "mpf")
            roster, freshness = print_mpf(conn, manifest, rows)
            payload["mpf"] = {
                "source_provenance": MPF_SOURCE_PROVENANCE, "callouts": MPF_CALLOUTS,
                "ma_roster": roster, "freshness": freshness, "rows": rows,
            }

        if "plannet_ext" in assessments:
            rows = fetch_rows(conn, run_id, "plannet_ext")
            print_plannet_ext(rows)
            payload["plannet_ext"] = {"attribution": PLANNET_EXT_ATTRIBUTION, "rows": rows}

        if args.json:
            Path(args.json).write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
            print(f"wrote {args.json}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

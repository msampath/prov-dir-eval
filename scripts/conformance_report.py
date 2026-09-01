#!/usr/bin/env python
"""Per-payer conformance + gap report from a conformance_run.

Combines the two halves of the assessment: what the HL7 validator found in the
sampled instances (conformance), and what is simply absent (gap) -- a payer
that serves nothing scores no violations, so conformance alone would rank it
alongside a perfect one.

    python scripts/conformance_report.py            # latest finished run
    python scripts/conformance_report.py --run-id 2 --json out.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.etl.loader import pg_connection  # noqa: E402
from provdir.models import RESOURCE_TABLES  # noqa: E402
from provdir.quality.conformance import (  # noqa: E402
    D3_UNIT, D3_UNIT_CAVEAT, by_category, cell_mean_rate, cell_scores,
    parse_expect_payers, payer_scores, select_run, top_violations,
)

CELL_ORDER = ["Organization", "Network", "Practitioner", "PractitionerRole",
              "Location", "HealthcareService", "OrganizationAffiliation",
              "InsurancePlan", "Endpoint"]


def loaded_rows(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT schemaname, sum(n_live_tup) FROM pg_stat_all_tables "
            "WHERE relname = ANY(%s) "
            "AND schemaname NOT IN ('public','pg_catalog','information_schema') "
            "GROUP BY 1",
            ([t.name for t in RESOURCE_TABLES.values()],),
        )
        return {r[0]: int(r[1] or 0) for r in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", "--conformance-run", dest="run_id", type=int,
                    help="report on this run explicitly. Without it the run is "
                         "chosen by the shared rule in provdir.quality."
                         "conformance.choose_run, which REFUSES rather than "
                         "guess when the widest payer set and the largest "
                         "instance count are different runs.")
    ap.add_argument("--expect-payers", default=None, metavar="N|a,b,c",
                    help="required payer count, or the exact comma-separated "
                         "payer set. Auto-selection is refused unless a run "
                         "matches it. Same flag, same meaning, in "
                         "build_map_data.py and build_framework_payload.py.")
    ap.add_argument("--allow-unverified", action="store_true",
                    help="report on a run that did not account for every "
                         "instance it attempted. Say so in the write-up.")
    ap.add_argument("--json", help="also write the full report as JSON here")
    args = ap.parse_args()

    conn = pg_connection()
    conn.autocommit = True
    try:
        run_id, run_note = select_run(
            conn, run_id=args.run_id,
            expect_payers=parse_expect_payers(args.expect_payers),
            allow_unverified=args.allow_unverified)
        print(f"run selection: {run_note}")
        with conn.cursor() as cur:
            cur.execute("SELECT ig, ig_version, instances, started_at, finished_at "
                        "FROM public.conformance_run WHERE run_id = %s", (run_id,))
            ig, ver, n, t0, t1 = cur.fetchone()
        mins = (t1 - t0).total_seconds() / 60 if t1 else None
        print(f"conformance run {run_id}: {ig}#{ver}, {n:,} instances"
              + (f", {mins:.1f}m" if mins else " (UNFINISHED)"))
        print()

        print("=== issues by category ===")
        print(f"{'category':26s} {'severity':10s} {'issues':>9} {'instances':>10}")
        for cat, sev, cnt, inst in by_category(conn, run_id):
            print(f"{cat:26s} {sev:10s} {cnt:>9,} {inst:>10,}")
        print("\nOnly 'profile' errors count toward conformance below.")
        print("'terminology-unavailable' is an artifact of running without a")
        print("terminology server and says nothing about the payer.")
        print()

        rows = payer_scores(conn, run_id)
        live = loaded_rows(conn)
        print("=== per-payer conformance (profile violations only) ===")
        print(f"{'payer':26s} {'rows':>13} {'cells':>5} {'sampled':>8} "
              f"{'conform%':>9} {'badext%':>8}")
        for r in rows:
            ext = (100.0 * r["with_bad_extension"] / r["validated"]
                   if r["validated"] else 0)
            print(f"{r['payer_id']:26s} {live.get(r['payer_id'], 0):>13,} "
                  f"{r['cells']:>5} {r['validated']:>8,} "
                  f"{r['conformance']:>8.1f}% {ext:>7.1f}%")
        print()

        print("=== most widespread profile violations (by payer breadth) ===")
        print(f"{'payers':>6} {'hits':>8}  message")
        for mid, hits, payers, sample in top_violations(conn, run_id, 20):
            print(f"{payers:>6} {hits:>8,}  {sample}")
        print()

        grid = cell_scores(conn, run_id)
        bad_cells = [(p, c, n, b) for p, c, n, b in grid if b]
        print(f"=== cells with violations: {len(bad_cells)} of {len(grid)} sampled ===")
        for p, c, n, b in sorted(bad_cells, key=lambda x: -x[3] / x[2])[:25]:
            print(f"  {p:24s} {c:24s} {b:>5}/{n:<5} {100.0 * b / n:>5.1f}%")
        print()

        # The declared aggregation unit, computed the same way here, in
        # build_map_data.py and in build_framework_payload.py.
        d3_rate, d3_used, d3_sup = cell_mean_rate(
            [(v, v - b) for _p, _c, v, b in grid])
        pooled_v = sum(v for _p, _c, v, _b in grid)
        pooled_c = sum(v - b for _p, _c, v, b in grid)
        print("=== structural conformance ===")
        print(f"unit: {D3_UNIT}")
        print(f"      {D3_UNIT_CAVEAT}")
        print("  declared unit : "
              + (f"{100.0 * d3_rate:.1f}% clean over {d3_used} cell(s) "
                 f"({d3_sup} suppressed)" if d3_rate is not None
                 else f"not computable (all {d3_sup} cell(s) suppressed)"))
        print("  pooled        : "
              + (f"{100.0 * pooled_c / pooled_v:.1f}% clean over "
                 f"{pooled_v:,} instance(s) -- NOT the declared unit; it is the "
                 "largest cell's rate diluted by row count"
                 if pooled_v else "no instances"))

        if args.json:
            payload = {
                "run_id": run_id, "run_selection": run_note,
                "ig": ig, "ig_version": ver, "instances": n,
                "conformance_unit": D3_UNIT,
                "conformance_unit_caveat": D3_UNIT_CAVEAT,
                "conformance_rate": d3_rate,
                "conformance_cells_used": d3_used,
                "conformance_cells_suppressed": d3_sup,
                "pooled_validated": pooled_v, "pooled_clean": pooled_c,
                "categories": [{"category": a, "severity": b, "issues": c,
                                "instances": d} for a, b, c, d in by_category(conn, run_id)],
                "payers": rows,
                "cells": [{"payer": p, "cell": c, "validated": v, "with_violation": b}
                          for p, c, v, b in grid],
                "top_violations": [{"message_id": m, "hits": h, "payers": pc,
                                    "sample": s}
                                   for m, h, pc, s in top_violations(conn, run_id, 40)],
            }
            Path(args.json).write_text(json.dumps(payload, indent=1), encoding="utf-8")
            print(f"\nwrote {args.json}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

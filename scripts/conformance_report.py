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
    by_category, cell_scores, latest_run, payer_scores, top_violations,
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
    ap.add_argument("--run-id", type=int)
    ap.add_argument("--json", help="also write the full report as JSON here")
    args = ap.parse_args()

    conn = pg_connection()
    conn.autocommit = True
    try:
        run_id = args.run_id or latest_run(conn)
        if not run_id:
            raise SystemExit("no finished conformance_run found")
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

        if args.json:
            payload = {
                "run_id": run_id, "ig": ig, "ig_version": ver, "instances": n,
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

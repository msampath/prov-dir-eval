#!/usr/bin/env python
"""Build the JSON backing the interactive per-payer directory map.

One file, three layers:
  geography  -- Location rows per payer per USPS state, from address_state
  scale      -- row counts per payer per resource type
  quality    -- conformance / binding / USCDI / MPF / Plan-Net-extension rates

The geography layer is the expensive one: ~44.7M Location rows across 64 payer
schemas. It is a grouped seq scan per payer, so it runs once and is cached in
the output file; nothing downstream re-queries the database.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import load_manifest          # noqa: E402
from provdir.db import get_engine                 # noqa: E402
from sqlalchemy import text                       # noqa: E402

RESOURCES = ["Practitioner", "PractitionerRole", "Organization", "Location",
             "HealthcareService", "InsurancePlan", "OrganizationAffiliation"]
TABLE = {r: "".join("_" + ch.lower() if ch.isupper() else ch for ch in r).lstrip("_")
         for r in RESOURCES}

# USPS codes we plot. Territories are collected but drawn separately.
STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
    "PR", "VI", "GU", "AS", "MP",
}


def payer_schemas(conn) -> list[str]:
    rows = conn.execute(text(
        "select distinct table_schema from information_schema.tables "
        "where table_name='location' and table_schema not in "
        "('public','information_schema','pg_catalog') order by 1"))
    return [r[0] for r in rows]


def resource_counts(conn, payer: str) -> dict[str, int]:
    """Live row counts. n_live_tup, not reltuples: reltuples goes stale by ~7x
    on a freshly bulk-loaded table and would misreport the biggest payers."""
    out = {}
    for res, tbl in TABLE.items():
        n = conn.execute(text(
            "select coalesce(n_live_tup,0) from pg_stat_all_tables "
            "where schemaname=:s and relname=:t"), {"s": payer, "t": tbl}).scalar()
        if n is not None:
            out[res] = int(n)
    return out


def state_counts(conn, payer: str) -> dict[str, int]:
    rows = conn.execute(text(
        f"select upper(trim(address_state)) st, count(*) "
        f"from {payer}.location where address_state is not null "
        f"and trim(address_state) <> '' group by 1"))
    out: dict[str, int] = {}
    unknown = 0
    for st, n in rows:
        if st in STATES:
            out[st] = out.get(st, 0) + int(n)
        else:
            unknown += int(n)
    if unknown:
        out["__other__"] = unknown
    return out


def geocoded(conn, payer: str) -> tuple[int, int]:
    r = conn.execute(text(
        f"select count(*) filter (where latitude is not null and longitude is not null), "
        f"count(*) from {payer}.location")).one()
    return int(r[0]), int(r[1])


def pick_conformance_run(conn) -> tuple[int, str]:
    """Finished run holding the most validated instances.

    Neither "newest" nor "complete" is the right rule on its own. `finished_at`
    is stamped even when most chunks crashed (run 3: 50,974 of 118,718), and a
    strict planned==recorded test picks the 90-instance pilot over run 2's
    117,387, because oversize chunks time out in every full run. Most instances
    wins, and the shortfall is reported rather than hidden.
    """
    rows = list(conn.execute(text(
        "select r.run_id, r.instances, count(i.*) recorded "
        "from conformance_run r left join conformance_instance i on i.run_id=r.run_id "
        "where r.finished_at is not null group by 1,2")))
    if not rows:
        raise SystemExit("no finished conformance run to report on")
    run_id, planned, recorded = max(rows, key=lambda r: r[2])
    if planned and recorded < planned:
        pct = 100.0 * recorded / planned
        return int(run_id), f"{recorded:,} of {planned:,} instances ({pct:.1f}%)"
    return int(run_id), f"complete ({recorded:,} instances)"


def conformance(conn, run_id: int) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for payer, cell, tot, clean in conn.execute(text(
            "select payer_id, split_part(cell,'__',1) c, count(*), "
            "count(*) filter (where errors=0) from conformance_instance "
            "where run_id=:r group by 1,2"), {"r": run_id}):
        p = out.setdefault(payer, {"validated": 0, "clean": 0, "by_resource": {}})
        p["validated"] += int(tot)
        p["clean"] += int(clean)
        b = p["by_resource"].setdefault(cell, {"validated": 0, "clean": 0})
        b["validated"] += int(tot)
        b["clean"] += int(clean)
    return out


def bindings(conn, run_id: int) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for payer, strength, rows_c, rows_bad in conn.execute(text(
            "select payer_id, strength, sum(rows_with_codes), sum(rows_with_nonmember) "
            "from binding_check_element where run_id=:r and status='ok' "
            "group by 1,2"), {"r": run_id}):
        p = out.setdefault(payer, {})
        p[strength or "unknown"] = {
            "rows_with_codes": int(rows_c or 0),
            "rows_with_nonmember": int(rows_bad or 0),
        }
    return out


def coverage(conn, run_id: int) -> dict[str, dict]:
    """Mean of per-element populated/denominator over measurable required
    elements. Unmeasured elements are reported separately, never as zero:
    folding them in would score a payer down for a gap in our mapping."""
    out: dict[str, dict] = {}
    for payer, assess, elem, den, pop, method, req in conn.execute(text(
            "select payer_id, assessment, element, denominator, populated, method, required "
            "from element_coverage where run_id=:r"), {"r": run_id}):
        a = out.setdefault(payer, {}).setdefault(assess, {
            "elements": [], "unmeasured": 0, "sample_based": 0})
        if method == "unmeasured" or not den:
            a["unmeasured"] += 1
            continue
        if method == "sample":
            a["sample_based"] += 1
        if req:
            a["elements"].append({"element": elem, "rate": float(pop) / float(den)})
    for payer, per in out.items():
        for assess, a in per.items():
            rates = [e["rate"] for e in a["elements"]]
            a["mean_rate"] = sum(rates) / len(rates) if rates else None
            a["n_elements"] = len(rates)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--conformance-run", type=int, default=None,
                    help="defaults to the newest finished run")
    ap.add_argument("--binding-run", type=int, default=None)
    ap.add_argument("--coverage-run", type=int, default=None)
    ap.add_argument("--out", default=str(REPO / "output" / "quality" / "map_data.json"))
    a = ap.parse_args()

    eng = get_engine()
    with eng.connect() as c:
        if a.conformance_run:
            conf_run, conf_note = a.conformance_run, "pinned by --conformance-run"
        else:
            conf_run, conf_note = pick_conformance_run(c)
        bind_run = a.binding_run or c.execute(text(
            "select max(run_id) from binding_check_run")).scalar()
        cov_run = a.coverage_run or c.execute(text(
            "select max(run_id) from element_coverage_run")).scalar()
        print(f"conformance run {conf_run} [{conf_note}], "
              f"binding run {bind_run}, coverage run {cov_run}", flush=True)

        conf = conformance(c, conf_run) if conf_run else {}
        bind = bindings(c, bind_run) if bind_run else {}
        cov = coverage(c, cov_run) if cov_run else {}
        ma = {r[0] for r in c.execute(text(
            "select distinct payer_id from element_coverage "
            "where run_id=:r and assessment='mpf'"), {"r": cov_run})} if cov_run else set()

        try:
            labels = {e.key: (e.display_name or e.key) for e in load_manifest().endpoints}
        except Exception:
            labels = {}

        payers = []
        for p in payer_schemas(c):
            print(f"  {p}", flush=True)
            geo_n, loc_n = geocoded(c, p)
            payers.append({
                "id": p,
                "name": labels.get(p, p),
                "is_ma": p in ma,
                "resources": resource_counts(c, p),
                "states": state_counts(c, p),
                "geocoded": {"with_latlon": geo_n, "locations": loc_n},
                "conformance": conf.get(p),
                "bindings": bind.get(p),
                "coverage": cov.get(p),
            })

    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs": {"conformance": conf_run, "conformance_note": conf_note,
                 "binding": bind_run, "coverage": cov_run},
        "payers": payers,
    }
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=1), encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size:,} bytes, {len(payers)} payers)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

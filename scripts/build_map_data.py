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
from provdir.quality.conformance import (         # noqa: E402
    D3_UNIT, D3_UNIT_CAVEAT, cell_mean_rate, parse_expect_payers, select_run,
)
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
    """Estimated row counts, from whichever estimator is actually populated.

    Neither estimator is reliable alone here. n_live_tup reads 0 for every
    payer except the one written most recently, and reltuples goes stale
    during a bulk load. Taking the larger of the two is right because both
    fail low, never high: a reset stat collector reports 0 and a mid-load
    reltuples reports a fraction of the final count.

    These are estimates. Location is overwritten with an exact count in main(),
    since state_counts already scans that table, and the two counts are
    reported separately so the page can say which is which. An exact count of
    every resource is not affordable: practitioner_role alone is ~520M rows
    corpus-wide, ~499.5M of it Aetna.
    """
    out = {}
    for res, tbl in TABLE.items():
        row = conn.execute(text(
            "select coalesce(st.n_live_tup,0), coalesce(cl.reltuples,0)::bigint "
            "from pg_class cl join pg_namespace n on n.oid=cl.relnamespace "
            "left join pg_stat_all_tables st on st.relid=cl.oid "
            "where n.nspname=:s and cl.relname=:t"), {"s": payer, "t": tbl}).fetchone()
        if row:
            out[res] = max(int(row[0]), int(row[1]), 0)
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


def conformance(conn, run_id: int) -> dict[str, dict]:
    """Per-payer conformance at the DECLARED unit, with the pooled figure beside it.

    This used to emit ONLY the pooled counts (`p["validated"] += tot` across
    every cell), which is the defect the framework named as "the sampling
    design leaking into the headline". With Organization at 59% of rows and
    PractitionerRole at 143.8 issues/instance against Location's 2.1, a pooled
    rate is the biggest cell's rate diluted by row count: it moves when the
    sample shape moves even if no payer changed.

    `rate` is the declared unit -- the unweighted mean over this payer's
    (payer, resource) cells, from the same helper conformance_report.py and
    build_framework_payload.py use, so all three artifacts compute one number
    one way. `validated`/`clean` remain, unchanged in meaning, because
    build_map_artifact.py still divides them; they are the POOLED counts and
    are labelled as such in the document's `conformance_unit` note.
    """
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
    for p in out.values():
        rate, used, suppressed = cell_mean_rate(
            [(b["validated"], b["clean"]) for b in p["by_resource"].values()])
        p["rate"] = rate
        p["unit"] = D3_UNIT
        p["cells_used"] = used
        p["cells_suppressed"] = suppressed
        p["pooled_rate"] = (p["clean"] / p["validated"]) if p["validated"] else None
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
                    help="report on this run explicitly. Without it the run is "
                         "chosen by the shared rule in provdir.quality."
                         "conformance.choose_run, which REFUSES rather than "
                         "guess when the widest payer set and the largest "
                         "instance count are different runs -- the case that "
                         "would otherwise swap this 61-payer map for a "
                         "single-payer full-population run.")
    ap.add_argument("--expect-payers", default=None, metavar="N|a,b,c",
                    help="required payer count, or the exact comma-separated "
                         "payer set. Same flag, same meaning, in "
                         "conformance_report.py and build_framework_payload.py.")
    ap.add_argument("--allow-unverified", action="store_true",
                    help="accept a run that did not account for every instance "
                         "it attempted. Say so in the write-up.")
    ap.add_argument("--binding-run", type=int, default=None)
    ap.add_argument("--coverage-run", type=int, default=None)
    ap.add_argument("--out", default=str(REPO / "output" / "quality" / "map_data.json"))
    a = ap.parse_args()

    eng = get_engine()
    with eng.connect() as c:
        conf_run, conf_note = select_run(
            c, run_id=a.conformance_run,
            expect_payers=parse_expect_payers(a.expect_payers),
            allow_unverified=a.allow_unverified)
        bind_run = a.binding_run or c.execute(text(
            "select max(run_id) from binding_check_run")).scalar()
        cov_run = a.coverage_run or c.execute(text(
            "select max(run_id) from element_coverage_run")).scalar()
        # Whether THO was loaded changes how v2-0360 qualification codes resolve,
        # so the run's terminology basis travels with its numbers.
        notes = c.execute(text("select notes from conformance_run where run_id=:r"),
                          {"r": conf_run}).scalar()
        if isinstance(notes, str):
            try:
                notes = json.loads(notes)
            except ValueError:
                notes = {}
        conf_tho = (notes or {}).get("tho")
        print(f"conformance run {conf_run} [{conf_note}], tho={conf_tho or 'none'}, "
              f"binding run {bind_run}, coverage run {cov_run}", flush=True)

        conf = conformance(c, conf_run) if conf_run else {}
        bind = bindings(c, bind_run) if bind_run else {}
        cov = coverage(c, cov_run) if cov_run else {}
        ma = {r[0] for r in c.execute(text(
            "select distinct payer_id from element_coverage "
            "where run_id=:r and assessment='mpf'"), {"r": cov_run})} if cov_run else set()

        # brands[0] is the payer's own name for itself; the schema key is a slug.
        # No silent except here: a lookup that quietly falls back leaves every
        # row labelled with its slug and looks like a data problem, not a bug.
        labels = {e.key: (e.brands[0] if e.brands else e.key)
                  for e in load_manifest().endpoints}
        unlabelled = [p for p in payer_schemas(c) if p not in labels]
        if unlabelled:
            print(f"  note: no manifest brand for {len(unlabelled)} schema(s): "
                  f"{', '.join(sorted(unlabelled))}", flush=True)

        payers = []
        for p in payer_schemas(c):
            print(f"  {p}", flush=True)
            geo_n, loc_n = geocoded(c, p)
            res = resource_counts(c, p)
            res["Location"] = loc_n          # exact; geocoded() already scanned it
            payers.append({
                "id": p,
                "name": labels.get(p, p),
                "is_ma": p in ma,
                "resources": res,
                "counts_exact": ["Location"],
                "states": state_counts(c, p),
                "geocoded": {"with_latlon": geo_n, "locations": loc_n},
                "conformance": conf.get(p),
                "bindings": bind.get(p),
                "coverage": cov.get(p),
            })

    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs": {"conformance": conf_run, "conformance_note": conf_note,
                 "conformance_tho": conf_tho,
                 "conformance_sample": (notes or {}).get("sample"),
                 "conformance_unit": D3_UNIT,
                 "conformance_unit_caveat": D3_UNIT_CAVEAT,
                 "conformance_pooled_note":
                     "payer.conformance.validated/clean are POOLED instance "
                     "counts, kept for build_map_artifact.py; the declared "
                     "unit is payer.conformance.rate",
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

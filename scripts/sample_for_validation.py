#!/usr/bin/env python
"""Sample resources per (payer, resource type) for HL7 instance validation.

Validating all ~670M loaded resources is infeasible (the HL7 validator runs on
the order of 100 instances/sec/core), and unnecessary: we want each payer's
*error rate* per resource type, which a few thousand instances estimates to
within a couple of percent. This script draws that sample and writes it as
individual JSON files laid out for batch validation.

Layout: one directory per resource type, files named
``<payer>__<resource>__<n>.json``. Grouping by resource type -- not by payer --
is deliberate: the validator reloads the IG context on every invocation (tens of
seconds), so one invocation per resource type with a single forced ``-profile``
amortizes that, and the payer is still recoverable from each result's filename.

Sampling method per cell, recorded in the manifest so the report can state it:
  * ``all``        - the cell has <= --per-cell rows; take everything.
  * ``tablesample``- big cells: TABLESAMPLE SYSTEM picks random PAGES, which is
    the only sampling that stays cheap on a 500M-row table. Caveat worth
    reporting: rows sharing a page are correlated (loaded together), so this is
    a cluster sample, not a simple random one.
  * ``file-head``  - slim tables have no full `resource` in the DB at all (the
    column holds a stub), so the sample must come from the raw ndjson archive.
    Taken from the head of several files spread across the capture rather than
    one, to avoid characterizing a payer by whatever landed in one export chunk.

    python scripts/sample_for_validation.py --out F:\\fhir_val --per-cell 1000
    python scripts/sample_for_validation.py --out ... --payers humana,elevance
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.etl.loader import pg_connection  # noqa: E402
from provdir.models import RESOURCE_TABLES, RESOURCE_TYPE_BY_TABLE  # noqa: E402

# Aim the TABLESAMPLE at this multiple of the target so page-level clustering
# and rounding still leave enough rows to hit --per-cell after the LIMIT.
OVERSAMPLE = 4
SLIM_FILES_PER_CELL = 3   # spread a slim sample across this many capture files

PROFILE_BASE = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"

# What to sample, and which profile each cell is judged against. Mostly one cell
# per resource type, with one exception: Plan-Net profiles a network as its own
# `plannet-Network` (an Organization with type=ntwk), so validating networks
# against plannet-Organization -- or plain organizations against
# plannet-Network -- would report differences that belong to the profile split,
# not to the payer. Networks are only ~0.1% of organizations, so this changes
# little in aggregate, but it would otherwise put a small permanent error floor
# under exactly the 28 payers that bother to publish networks.
CELL_SPECS: list[tuple[str, str, str | None, str]] = [
    ("Organization", "organization", "coalesce(is_network, 0) = 0", "plannet-Organization"),
    ("Network", "organization", "is_network = 1", "plannet-Network"),
    ("Practitioner", "practitioner", None, "plannet-Practitioner"),
    ("PractitionerRole", "practitioner_role", None, "plannet-PractitionerRole"),
    ("Location", "location", None, "plannet-Location"),
    ("HealthcareService", "healthcare_service", None, "plannet-HealthcareService"),
    ("OrganizationAffiliation", "organization_affiliation", None,
     "plannet-OrganizationAffiliation"),
    ("InsurancePlan", "insurance_plan", None, "plannet-InsurancePlan"),
    ("Endpoint", "endpoint_resource", None, "plannet-Endpoint"),
]
# cell name -> the FHIR resourceType its rows actually carry (Network is an
# Organization on the wire; the validator must be pointed at the right profile,
# not a made-up resource type).
CELL_RESOURCE_TYPE = {c[0]: RESOURCE_TYPE_BY_TABLE[c[1]] for c in CELL_SPECS}
CELL_PROFILE = {c[0]: PROFILE_BASE + c[3] for c in CELL_SPECS}


def cells(conn, payers: set[str] | None, resources: set[str] | None) -> list[tuple]:
    """(payer, cell, table, where, live_rows) for every non-empty cell.

    `live_rows` is n_live_tup for the whole table, used only to choose a
    TABLESAMPLE percentage; for a split cell (Network) it overstates the cell,
    which just means a wider net and is harmless.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT schemaname, relname, n_live_tup FROM pg_stat_all_tables "
            "WHERE relname = ANY(%s) "
            "AND schemaname NOT IN ('public','pg_catalog','information_schema') "
            "AND n_live_tup > 0 ORDER BY schemaname, relname",
            ([t.name for t in RESOURCE_TABLES.values()],),
        )
        live = {(s, r): int(n) for s, r, n in cur.fetchall()}
    out = []
    for payer_tbl, n in sorted(live.items()):
        payer, tbl = payer_tbl
        if payers and payer not in payers:
            continue
        for cell, spec_tbl, where, _prof in CELL_SPECS:
            if spec_tbl != tbl:
                continue
            if resources and cell not in resources:
                continue
            out.append((payer, cell, tbl, where, n))
    return out


def slim_pairs(conn) -> dict[tuple[str, str], str]:
    """(payer, resource) -> raw ndjson location, for tables holding stubs."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='slim_table'"
        )
        if cur.fetchone() is None:
            return {}
        cur.execute("SELECT payer_id, resource_type, raw_location FROM public.slim_table")
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}


def sample_from_db(conn, schema: str, table: str, payer: str, where: str | None,
                   live: int, want: int, seed: int) -> tuple[list[dict], str]:
    # `where` comes from CELL_SPECS (a fixed internal constant), never user input.
    filt = f" AND ({where})" if where else ""
    with conn.cursor() as cur:
        if live <= want:
            cur.execute(f'SELECT resource FROM "{schema}"."{table}" '
                        f"WHERE payer_id = %s{filt}", (payer,))
            return [r[0] for r in cur.fetchall()][:want], "all"
        pct = min(100.0, 100.0 * want * OVERSAMPLE / live)
        # REPEATABLE makes the page draw a pure function of (seed, table state)
        # -- without it --seed silently promised a reproducibility TABLESAMPLE
        # does not provide. Still only same-draw against an UNCHANGED table.
        cur.execute(
            f'SELECT resource FROM "{schema}"."{table}" TABLESAMPLE SYSTEM ({pct}) '
            f"REPEATABLE ({int(seed)}) "
            f"WHERE payer_id = %s{filt} LIMIT %s", (payer, want),
        )
        rows = [r[0] for r in cur.fetchall()]
        if rows:
            return rows, "tablesample"
        # A tiny percentage can miss every page on a small-but-not-small-enough
        # table, and a split cell (Network) can be far rarer than the table's
        # row count suggests, so fall back rather than silently emit nothing.
        cur.execute(f'SELECT resource FROM "{schema}"."{table}" '
                    f"WHERE payer_id = %s{filt} LIMIT %s", (payer, want))
        return [r[0] for r in cur.fetchall()], "limit-fallback"


def sample_from_files(raw_location: str, rtype: str, want: int) -> tuple[list[dict], str]:
    d = Path(raw_location)
    files = sorted(d.glob("*.ndjson")) if d.is_dir() else ([d] if d.is_file() else [])
    if not files:
        return [], "file-missing"
    picks = files
    if len(files) > SLIM_FILES_PER_CELL:
        step = len(files) // SLIM_FILES_PER_CELL
        picks = [files[i * step] for i in range(SLIM_FILES_PER_CELL)]
    per = max(1, want // len(picks))
    out: list[dict] = []
    for f in picks:
        got = 0
        with open(f, "rb", buffering=1 << 20) as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if isinstance(obj, dict) and obj.get("resourceType") == rtype:
                    out.append(obj)
                    got += 1
                    if got >= per:
                        break
    return out[:want], "file-head"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="output root directory")
    ap.add_argument("--per-cell", type=int, default=1000)
    ap.add_argument("--payers", help="comma-separated payer keys (default: all)")
    ap.add_argument("--resources", help="comma-separated resource types (default: all)")
    ap.add_argument("--seed", type=int, default=20260810)
    args = ap.parse_args()

    payers = set(args.payers.split(",")) if args.payers else None
    resources = set(args.resources.split(",")) if args.resources else None

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    conn = pg_connection()
    conn.autocommit = True
    try:
        slim = slim_pairs(conn)
        todo = cells(conn, payers, resources)
        print(f"{len(todo)} cell(s) to sample, target {args.per_cell} each", flush=True)
        manifest = []
        for payer, cell, tbl, where, live in todo:
            rtype = CELL_RESOURCE_TYPE[cell]
            raw = slim.get((payer, rtype))
            if raw:
                rows, method = sample_from_files(raw, rtype, args.per_cell)
            else:
                rows, method = sample_from_db(conn, payer, tbl, payer, where,
                                              live, args.per_cell, args.seed)
            if not rows:
                continue
            dest = out / cell
            dest.mkdir(exist_ok=True)
            for i, r in enumerate(rows):
                (dest / f"{payer}__{cell}__{i}.json").write_text(
                    json.dumps(r), encoding="utf-8")
            manifest.append({"payer": payer, "cell": cell, "resource_type": rtype,
                             "profile": CELL_PROFILE[cell], "live_rows": live,
                             "sampled": len(rows), "method": method})
            print(f"  {payer:24s} {cell:24s} {len(rows):>5} / {live:>12,}  ({method})",
                  flush=True)
        (out / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
        tot = sum(m["sampled"] for m in manifest)
        print(f"\nwrote {tot:,} instance(s) across {len(manifest)} cell(s) -> {out}",
              flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

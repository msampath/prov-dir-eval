#!/usr/bin/env python
"""Sample resources per (payer, resource type) for HL7 instance validation.

Two modes:

  * **Sampled** (``--per-cell N``, the default). Validating all ~670M loaded
    resources is infeasible, and unnecessary when what is wanted is each
    payer's *error rate* per resource type -- a few thousand instances
    estimates that to within a couple of percent.
  * **Full population** (``--per-cell 0``). One schema, every row. Used for the
    azblue audit, where a rate estimate is not enough and the headline has to
    survive "which rows did you actually look at?". 3.13M instances stream
    through a server-side cursor and land on disk a file at a time -- an
    unbounded ``fetchall()`` with psycopg's jsonb->dict loader would need
    25-40 GB of Python heap for the 1.86M-row Organization cell alone.

Layout: one directory per resource type, files named
``<payer>__<resource>__<n>.json``. Grouping by resource type -- not by payer --
is deliberate: the validator reloads the IG context on every invocation (tens of
seconds), so one invocation per resource type with a single forced ``-profile``
amortizes that, and the payer is still recoverable from each result's filename.

Sampling method per cell, recorded in the manifest so the report can state it:
  * ``full``       - --per-cell 0: every row, streamed.
  * ``all``        - the cell has <= --per-cell rows; take everything.
  * ``tablesample``- big cells: TABLESAMPLE SYSTEM picks random PAGES, which is
    the only sampling that stays cheap on a 500M-row table. Caveat worth
    reporting: rows sharing a page are correlated (loaded together), so this is
    a cluster sample, not a simple random one.
  * ``file-head``  - slim tables have no full `resource` in the DB at all (the
    column holds a stub), so the sample must come from the raw ndjson archive.
    Taken from the head of several files spread across the capture rather than
    one, to avoid characterizing a payer by whatever landed in one export chunk.

Every branch is ``ORDER BY id`` so ``<payer>__<cell>__<n>.json`` names the same
resource on every run; without it a finding recorded against instance 41,207
pointed at a different row each time the sampler was re-run. The id itself is
written to ``_ids/<cell>__<payer>.tsv`` so a finding traces back to a DB row.

    python scripts/sample_for_validation.py --out E:/fhir_val --per-cell 1000
    python scripts/sample_for_validation.py --out ... --payers humana,elevance
    python scripts/sample_for_validation.py --out E:/fhir_val_azblue \
        --payers bcbs_az_new --per-cell 0
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
# Rows pulled from the server per round trip on the full-population path. Big
# enough that 1.86M rows is ~370 round trips, small enough that one batch of
# Organization JSON (~1.7KB each) is a few MB, not a few GB.
STREAM_BATCH = 5000
PROGRESS_EVERY = 100_000

PROFILE_BASE = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"

# CARIN Blue Button profiles declared in `meta.profile`. The validator honours
# declared profiles IN ADDITION to the one forced with -profile, and offers no
# flag to ignore them (the full 6.10.1 option set was enumerated). azblue's
# Organizations and Practitioners declare C4BB-Organization both unversioned and
# pinned to |1.0.0; we hold carin-bb 2.1.0, so every one of them is validated
# against a package version the payer never published against. That is an
# artifact of our package set, not a payer defect -- ~3.7M issue rows of it --
# and it costs 2.58s vs 1.29s per Organization to produce.
#
# NOT a Plan-Net concern: no Plan-Net element or constraint touches
# `meta.profile`. `organization-meta-profile-version` is a CARIN BB constraint
# on C4BB-Organization and disappears with the profile either way. The
# justification here is cost plus removing false findings, nothing else.
CARIN_BB_MARKER = "/us/carin-bb/"

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


def strip_carin_bb_profiles(text: str) -> tuple[str, list[str]]:
    """Drop CARIN BB canonicals from a resource's ``meta.profile``.

    Returns (possibly rewritten JSON text, the canonicals removed). Surgical:
    only entries carrying CARIN_BB_MARKER are popped, so the payer's Plan-Net
    and US Core declarations survive and are still validated. The substring
    pre-check keeps this to one ``in`` test for the resources that carry no
    CARIN BB profile at all, instead of a parse/serialise round trip.
    """
    if CARIN_BB_MARKER not in text:
        return text, []
    obj = json.loads(text)
    meta = obj.get("meta")
    if not isinstance(meta, dict):
        return text, []
    profiles = meta.get("profile")
    if not isinstance(profiles, list):
        return text, []
    dropped = [p for p in profiles if isinstance(p, str) and CARIN_BB_MARKER in p]
    if not dropped:
        return text, []
    keep = [p for p in profiles if p not in dropped]
    if keep:
        meta["profile"] = keep
    else:
        meta.pop("profile", None)
    return json.dumps(obj), dropped


def cells(conn, payers: set[str] | None, resources: set[str] | None,
          verify_empty: bool | None = None) -> list[tuple]:
    """(payer, cell, table, where, est_rows) for every non-empty cell.

    ``est_rows`` is a row estimate for the whole table, used only to choose a
    TABLESAMPLE percentage; for a split cell (Network) it overstates the cell,
    which just means a wider net and is harmless.

    The estimate is GREATEST(n_live_tup, reltuples), not n_live_tup alone. A
    stats reset zeroes n_live_tup while leaving reltuples intact, and the
    previous ``AND n_live_tup > 0`` filter then dropped the whole payer: `hcsc`
    reads 0 live tuples against 18.46M actual rows right now, and a run
    targeting it would have discovered zero cells and exited 0.

    A table both estimates call empty is confirmed with count(*) -- but only
    when the payer was named explicitly (``verify_empty``), because count(*)
    over every zero-estimate table in a 60-payer database means scanning
    500M-row tables to learn they are empty.
    """
    if verify_empty is None:
        verify_empty = payers is not None
    names = [t.name for t in RESOURCE_TABLES.values()]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT t.schemaname, t.relname, "
            "       GREATEST(t.n_live_tup, k.reltuples::bigint, 0) AS est "
            "FROM pg_stat_all_tables t JOIN pg_class k ON k.oid = t.relid "
            "WHERE t.relname = ANY(%s) "
            "AND t.schemaname NOT IN ('public','pg_catalog','information_schema') "
            "ORDER BY t.schemaname, t.relname",
            (names,),
        )
        est = {(s, r): int(n) for s, r, n in cur.fetchall()}

        unconfirmed = sorted(k for k, n in est.items()
                             if n <= 0 and (not payers or k[0] in payers))
        if unconfirmed and verify_empty:
            for schema, tbl in unconfirmed:
                cur.execute(f'SELECT count(*) FROM "{schema}"."{tbl}"')
                real = int(cur.fetchone()[0])
                if real:
                    print(f"  NOTE {schema}.{tbl}: planner estimate 0 but "
                          f"{real:,} actual rows (stats reset); using the count",
                          flush=True)
                est[(schema, tbl)] = real
        elif unconfirmed:
            print(f"  WARN {len(unconfirmed)} table(s) estimate 0 rows and were "
                  "not confirmed with count(*) (pass --verify-empty, or name "
                  "the payer with --payers, to check them)", flush=True)

    out = []
    for (payer, tbl), n in sorted(est.items()):
        if n <= 0:
            continue
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


def stream_full_population(schema: str, table: str, payer: str, filt: str):
    """Yield (resource_id, resource_json_text) for EVERY row in a cell.

    Three things make this survivable where ``cur.fetchall()`` is not:
      * a server-side named cursor, so Postgres holds the result set and hands
        it over STREAM_BATCH rows at a time;
      * ``resource::text``, which skips psycopg's jsonb->dict loader entirely --
        the caller wants bytes to write to a file, and materialising 1.86M
        Python dicts only to re-serialise them is where the 25-40 GB went;
      * its own connection, because a server-side cursor needs a transaction
        and the caller's connection is deliberately in autocommit.
    """
    conn = pg_connection()          # autocommit False: DECLARE CURSOR needs a tx
    try:
        with conn.cursor(name=f"provdir_full_{schema}_{table}") as sc:
            sc.itersize = STREAM_BATCH
            sc.execute(
                f'SELECT id, resource::text FROM "{schema}"."{table}" '
                f"WHERE payer_id = %s{filt} ORDER BY id", (payer,))
            yield from sc
    finally:
        conn.rollback()
        conn.close()


def sample_from_db(conn, schema: str, table: str, payer: str, where: str | None,
                   live: int, want: int, seed: int):
    """(iterator of (resource_id, json_text), method name).

    ``want <= 0`` means the full population. Every branch is ORDER BY id, so
    the n-th file of a cell names the same resource on every run.
    """
    # `where` comes from CELL_SPECS (a fixed internal constant), never user input.
    filt = f" AND ({where})" if where else ""
    if want <= 0:
        return stream_full_population(schema, table, payer, filt), "full"
    with conn.cursor() as cur:
        if live <= want:
            cur.execute(f'SELECT id, resource::text FROM "{schema}"."{table}" '
                        f"WHERE payer_id = %s{filt} ORDER BY id LIMIT %s",
                        (payer, want))
            return iter(cur.fetchall()), "all"
        pct = min(100.0, 100.0 * want * OVERSAMPLE / live)
        # REPEATABLE makes the page draw a pure function of (seed, table state)
        # -- without it --seed silently promised a reproducibility TABLESAMPLE
        # does not provide. Still only same-draw against an UNCHANGED table.
        cur.execute(
            f'SELECT id, resource::text FROM "{schema}"."{table}" '
            f"TABLESAMPLE SYSTEM ({pct}) REPEATABLE ({int(seed)}) "
            f"WHERE payer_id = %s{filt} ORDER BY id LIMIT %s", (payer, want),
        )
        rows = cur.fetchall()
        if rows:
            return iter(rows), "tablesample"
        # A tiny percentage can miss every page on a small-but-not-small-enough
        # table, and a split cell (Network) can be far rarer than the table's
        # row count suggests, so fall back rather than silently emit nothing.
        cur.execute(f'SELECT id, resource::text FROM "{schema}"."{table}" '
                    f"WHERE payer_id = %s{filt} ORDER BY id LIMIT %s", (payer, want))
        return iter(cur.fetchall()), "limit-fallback"


def sample_from_files(raw_location: str, rtype: str, want: int):
    """(iterator of (resource_id, json_text), method) from the raw ndjson."""
    d = Path(raw_location)
    files = sorted(d.glob("*.ndjson")) if d.is_dir() else ([d] if d.is_file() else [])
    if not files:
        return iter(()), "file-missing"
    picks = files
    if len(files) > SLIM_FILES_PER_CELL:
        step = len(files) // SLIM_FILES_PER_CELL
        picks = [files[i * step] for i in range(SLIM_FILES_PER_CELL)]
    per = max(1, want // len(picks)) if want > 0 else None

    def gen():
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
                        yield obj.get("id"), line.decode("utf-8", "replace")
                        got += 1
                        if per is not None and got >= per:
                            break

    return gen(), "file-head"


def write_cell(dest: Path, ids_path: Path, payer: str, cell: str, rows,
               want: int, strip_carin: bool, inventory: dict) -> int:
    """Write one cell's instances to disk. Returns how many were written.

    Streams: nothing is accumulated, so the full-population path's memory is
    one resource at a time whether the cell holds 1 row or 1.86M. The id
    sidecar is appended as it goes, which doubles as a progress record if the
    sampler is killed mid-cell.
    """
    dest.mkdir(parents=True, exist_ok=True)
    ids_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(ids_path, "w", encoding="utf-8", newline="\n") as ids:
        for rid, text in rows:
            if strip_carin:
                text, dropped = strip_carin_bb_profiles(text)
                if dropped:
                    bucket = inventory.setdefault(f"{payer}|{cell}", {})
                    for p in dropped:
                        bucket[p] = bucket.get(p, 0) + 1
                    bucket["_instances"] = bucket.get("_instances", 0) + 1
            name = f"{payer}__{cell}__{n}.json"
            (dest / name).write_text(text, encoding="utf-8")
            # Tab-separated: instance names are <payer>__<cell>__<int> and FHIR
            # ids are [A-Za-z0-9.-]{1,64}, so neither can contain a tab.
            ids.write(f"{name}\t{rid or ''}\n")
            n += 1
            if want > 0 and n >= want:
                break
            if n % PROGRESS_EVERY == 0:
                ids.flush()
                print(f"    ... {n:,} written", flush=True)
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="output root directory")
    ap.add_argument("--per-cell", type=int, default=1000,
                    help="instances per (payer, resource) cell. 0 means the "
                         "FULL population -- every row, streamed through a "
                         "server-side cursor. Default: %(default)s")
    ap.add_argument("--payers", help="comma-separated payer keys (default: all)")
    ap.add_argument("--resources", help="comma-separated resource types (default: all)")
    ap.add_argument("--seed", type=int, default=20260810)
    ap.add_argument("--strip-carin-bb", dest="strip_carin_bb",
                    action="store_true", default=True,
                    help="pop CARIN BB canonicals from meta.profile as each "
                         "instance is written, recording what was removed to "
                         "_carin_bb_inventory.json. ON by default: the "
                         "validator enforces declared profiles in ADDITION to "
                         "-profile and has no flag to ignore them, so leaving "
                         "them in measures our carin-bb 2.1.0 package against "
                         "resources pinned to 1.0.0. Default: on")
    ap.add_argument("--no-strip-carin-bb", dest="strip_carin_bb",
                    action="store_false")
    ap.add_argument("--verify-empty", action="store_true", default=None,
                    help="confirm zero-estimate tables with count(*) instead "
                         "of trusting planner stats. Implied by --payers.")
    args = ap.parse_args()

    payers = set(args.payers.split(",")) if args.payers else None
    resources = set(args.resources.split(",")) if args.resources else None

    if args.per_cell <= 0 and not payers:
        # A full-population dump of ALL payers is ~670M rows streamed to disk.
        # Nobody has ever meant that; every real full-population run names its
        # schema. Refuse rather than fill the volume.
        raise SystemExit("FATAL: --per-cell 0 requires --payers (a full dump "
                         "of every payer is almost certainly not what you "
                         "want, and would fill the disk).")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "manifest.json"
    inventory_path = out / "_carin_bb_inventory.json"
    conn = pg_connection()
    conn.autocommit = True
    try:
        slim = slim_pairs(conn)
        todo = cells(conn, payers, resources, args.verify_empty)
        if not todo:
            # Never exit 0 here. The previous gate (`n_live_tup > 0`) turned a
            # stats reset into "no cells, success", and everything downstream
            # then validated nothing while reporting no error at all.
            raise SystemExit(
                "FATAL: discovered 0 cells to sample"
                + (f" for payer(s) {sorted(payers)}" if payers else "")
                + ".\nEither the schema really is empty, or planner statistics "
                  "are stale/reset. Re-run with --verify-empty to confirm with "
                  "count(*), and check that ANALYZE has run on the schema."
            )
        target = "the FULL population" if args.per_cell <= 0 else f"{args.per_cell} each"
        print(f"{len(todo)} cell(s) to sample, target {target}"
              f"{'' if args.strip_carin_bb else ' (CARIN BB profiles KEPT)'}",
              flush=True)
        manifest: list[dict] = []
        inventory: dict = {}
        for payer, cell, tbl, where, live in todo:
            rtype = CELL_RESOURCE_TYPE[cell]
            raw = slim.get((payer, rtype))
            if raw:
                rows, method = sample_from_files(raw, rtype, args.per_cell)
            else:
                rows, method = sample_from_db(conn, payer, tbl, payer, where,
                                              live, args.per_cell, args.seed)
            dest = out / cell
            ids_path = out / "_ids" / f"{cell}__{payer}.tsv"
            n = write_cell(dest, ids_path, payer, cell, rows, args.per_cell,
                           args.strip_carin_bb, inventory)
            if not n:
                ids_path.unlink(missing_ok=True)
                continue
            manifest.append({"payer": payer, "cell": cell, "resource_type": rtype,
                             "profile": CELL_PROFILE[cell], "live_rows": live,
                             "sampled": n, "method": method,
                             "carin_bb_stripped": args.strip_carin_bb,
                             "ids": ids_path.name})
            # Checkpoint after EVERY cell, not once at the end: a crash 30
            # hours into a full-population run used to leave no record at all
            # of which cells had finished.
            manifest_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
            inventory_path.write_text(json.dumps(inventory, indent=1), encoding="utf-8")
            print(f"  {payer:24s} {cell:24s} {n:>9,} / {live:>12,}  ({method})",
                  flush=True)
        tot = sum(m["sampled"] for m in manifest)
        print(f"\nwrote {tot:,} instance(s) across {len(manifest)} cell(s) -> {out}",
              flush=True)
        if inventory:
            n_str = sum(v.get("_instances", 0) for v in inventory.values())
            print(f"stripped CARIN BB meta.profile from {n_str:,} instance(s); "
                  f"inventory -> {inventory_path}", flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

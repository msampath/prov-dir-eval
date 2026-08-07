#!/usr/bin/env python
"""REPLACE-load Aetna PractitionerRole from a local bulk-export ndjson capture.

Truncates the target table, drops its indexes, streams every captured line
through a plain (non-upserting) COPY straight into the now index-less table,
then rebuilds the indexes on the ``hot_idx`` tablespace and ANALYZEs.

    python scripts/bulk_replace_load.py --dry-run
    python scripts/bulk_replace_load.py
    python scripts/bulk_replace_load.py --skip-truncate   # see NOT RESUMABLE below
    python scripts/bulk_replace_load.py --rebuild-only    # load OK, rebuild failed

This is the disk-safe/fast counterpart to ``bulk_ingest.py --ingest-file``:
that script upserts row-by-row via a staged ``ON CONFLICT`` merge, which is
correct for incremental loads but pays index-maintenance cost on every one of
~500M rows. Here the capture is the authoritative full snapshot (a prior,
crashed export left ~59.9M stale rows in the table), so it is cheaper and
simpler to drop the indexes, bulk-COPY everything, and rebuild the indexes
once at the end -- as long as the table is empty and index-less for the
duration (this script's whole job is to make that safe).

NOT RESUMABLE MID-LOAD. If this script dies partway through the load loop,
the table is truncated and index-less with a partial row set. The correct
recovery is to re-run the whole script from the top -- it re-truncates (a
no-op on an already-empty table) and re-drops indexes (a no-op via
``IF EXISTS``/``DROP INDEX IF EXISTS``, so a from-scratch re-run is always
safe without needing any flag). ``--skip-truncate`` exists only for the case
where you know TRUNCATE/DROP already succeeded and want to skip re-issuing
those four statements before restarting the load from file 0; it does NOT
skip re-loading already-loaded files or dedupe them -- any duplicate
(payer_id, id) pairs that results are caught (loudly) when the unique index
is rebuilt at the end, per FHIR ids being expected to be unique.

If the LOAD completed but the INDEX REBUILD (or a later step) failed, the
table holds the full, correct row set -- do NOT re-run bare (re-truncates and
re-loads 1.45 TB) and do NOT use --skip-truncate (re-loads on top, duplicating
every row). Use ``--rebuild-only``: it skips truncate and load entirely, drops
whatever leftover indexes exist (safe on any partial state), rebuilds all
three on hot_idx, ANALYZEs, and records provenance.

Disk layout this script assumes:
  * source ndjson capture: I:\\aetna_export\\*.ndjson (30 files, ~1.45 TB)
  * table heap:            tablespace bulk_heap (E:)
  * table indexes:         tablespace hot_idx   (M:) -- kept off F: on purpose
  * WAL:                   F: -- batched commits (not one mega-transaction)
    keep it recycling instead of growing unbounded across a multi-TB load
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import get_settings, load_manifest              # noqa: E402
from provdir.etl.loader import (                                    # noqa: E402
    bulk_load, insert_provenance, pg_connection, schema_for, truncate,
)
from provdir.etl.transform import TransformError, transform_resource  # noqa: E402
from provdir.models import RESOURCE_TABLES                          # noqa: E402

PAYER = "aetna_cvs"
RTYPE = "PractitionerRole"
DEFAULT_SOURCE_DIR = r"I:\aetna_export"
INDEX_TABLESPACE = "hot_idx"
BATCH = 50_000
PROGRESS_EVERY = 1_000_000

PK_NAME = "practitioner_role_pkey"
IX_PRACTITIONER = "ix_practitioner_role_practitioner_ref"
IX_ORGANIZATION = "ix_practitioner_role_organization_ref"
EXPECTED_INDEXES = {PK_NAME, IX_PRACTITIONER, IX_ORGANIZATION}


def autocommit_connection(schema: str) -> psycopg.Connection:
    """A dedicated AUTOCOMMIT connection for DDL (TRUNCATE/DROP/CREATE INDEX/
    ALTER) so each statement lands immediately instead of piling up in one
    long-held transaction alongside the load loop's WAL.
    """
    s = get_settings()
    conn = psycopg.connect(
        host=s.postgres_host, port=s.postgres_port, dbname=s.postgres_db,
        user=s.postgres_user, password=s.postgres_password, autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema}", public')
    return conn


def rebuild_connection(schema: str) -> psycopg.Connection:
    """Fresh AUTOCOMMIT session for the index rebuild, opened right before it.

    Two reasons this must be a NEW connection at rebuild time:
      * the original DDL connection sits idle for the entire multi-hour load
        and may have been dropped by the server/network by the time the
        rebuild starts -- failing then would strand a fully loaded but
        unindexed table;
      * ``temp_tablespaces`` (and the other sort GUCs) only affect the session
        they are SET on, so they must be applied to the SAME session that runs
        CREATE INDEX for the sort spill to land on M: instead of F:.
    """
    conn = autocommit_connection(schema)
    with conn.cursor() as cur:
        cur.execute("SET maintenance_work_mem = '16GB'")
        cur.execute("SET max_parallel_maintenance_workers = 4")
        cur.execute(f"SET temp_tablespaces = '{INDEX_TABLESPACE}'")
    return conn


def live_indexes(conn: psycopg.Connection, schema: str, tname: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            (schema, tname),
        )
        return {r[0] for r in cur.fetchall()}


def est_rows(conn: psycopg.Connection, schema: str, tname: str) -> int:
    """Fast row-count ESTIMATE from pg_class.reltuples (accurate right after
    ANALYZE). A real count(*) on this table would scan ~1.3 TB on the E: HDD --
    hours -- and is unnecessary: `after` is derived exactly from the load
    counters (see main), and `before` is only informational.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reltuples::bigint FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (schema, tname),
        )
        r = cur.fetchone()
        return max(0, int(r[0])) if r and r[0] is not None else 0


def table_exists(conn: psycopg.Connection, schema: str, tname: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, tname),
        )
        return cur.fetchone() is not None


def tablespace_exists(conn: psycopg.Connection, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_tablespace WHERE spcname = %s", (name,))
        return cur.fetchone() is not None


def drop_statements(schema: str, tname: str) -> list[str]:
    return [
        f'TRUNCATE TABLE "{schema}"."{tname}"',
        f'ALTER TABLE "{schema}"."{tname}" DROP CONSTRAINT IF EXISTS {PK_NAME}',
        f'DROP INDEX IF EXISTS "{schema}"."{IX_PRACTITIONER}"',
        f'DROP INDEX IF EXISTS "{schema}"."{IX_ORGANIZATION}"',
    ]


def rebuild_statements(schema: str, tname: str) -> list[str]:
    return [
        f'CREATE UNIQUE INDEX {PK_NAME} ON "{schema}"."{tname}" '
        f'(payer_id, id) TABLESPACE {INDEX_TABLESPACE}',
        f'ALTER TABLE "{schema}"."{tname}" ADD CONSTRAINT {PK_NAME} '
        f'PRIMARY KEY USING INDEX {PK_NAME}',
        f'CREATE INDEX {IX_PRACTITIONER} ON "{schema}"."{tname}" '
        f'(practitioner_ref) TABLESPACE {INDEX_TABLESPACE}',
        f'CREATE INDEX {IX_ORGANIZATION} ON "{schema}"."{tname}" '
        f'(organization_ref) TABLESPACE {INDEX_TABLESPACE}',
    ]


def preflight(conn: psycopg.Connection, schema: str, tname: str,
              files: list[Path], skip_truncate: bool, rebuild_only: bool) -> set[str]:
    """Verify the table + hot_idx tablespace exist, and that the live index
    set is in one of the two known-good states (all 3 present, or none --
    the latter meaning a prior run already dropped them). Anything else is
    an ambiguous partial state: abort rather than guess -- except under
    --rebuild-only, whose whole purpose is recovering from a partial rebuild
    (it re-drops whatever exists before re-creating all 3).
    """
    if not table_exists(conn, schema, tname):
        raise SystemExit(f"table {schema}.{tname} does not exist; aborting")
    if not tablespace_exists(conn, INDEX_TABLESPACE):
        raise SystemExit(
            f"tablespace {INDEX_TABLESPACE!r} does not exist; it must be created "
            "(e.g. pointing at M:/pg_ts/hot_idx) before running this script"
        )
    if not files and not rebuild_only:
        raise SystemExit("no *.ndjson source files found; aborting")

    live = live_indexes(conn, schema, tname)
    if rebuild_only:
        print(f"--rebuild-only: current indexes on {schema}.{tname} = "
              f"{sorted(live) or '(none)'}; leftovers will be dropped and all "
              f"{len(EXPECTED_INDEXES)} rebuilt on {INDEX_TABLESPACE}", flush=True)
    elif skip_truncate:
        if live:
            raise SystemExit(
                f"--skip-truncate assumes TRUNCATE + index drops already succeeded, "
                f"but {schema}.{tname} still has indexes: {sorted(live)}. Loading "
                "into an indexed table defeats the point of this script and (if the "
                "PK survives) can abort mid-COPY. Re-run WITHOUT the flag -- the "
                "TRUNCATE/DROP statements are safe to re-issue."
            )
    elif live == EXPECTED_INDEXES:
        pass  # fresh state: TRUNCATE/DROP will do real work
    elif live == set():
        print(f"NOTE: {schema}.{tname} already has no indexes (a prior run likely "
              "already dropped them); TRUNCATE/DROP steps will be no-ops", flush=True)
    else:
        raise SystemExit(
            f"unexpected index state on {schema}.{tname}: found {sorted(live)}, "
            f"expected exactly {sorted(EXPECTED_INDEXES)} or none. Aborting rather "
            "than guessing -- investigate manually before re-running."
        )
    return live


def run_load(load_conn: psycopg.Connection, table, payer: str, base_url: str,
             files: list[Path]) -> tuple[int, int]:
    """Stream every file's lines through transform_resource -> bulk COPY into the
    (truncated, index-less) target table. Commits every ~BATCH rows so WAL
    recycles instead of one mega-transaction. Returns (total_lines, errors).
    """
    batch: list[dict] = []
    total_lines = 0
    errors = 0
    for fi, path in enumerate(files):
        with open(path, "rb") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                total_lines += 1
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        raise TransformError("line is valid JSON but not an object")
                    batch.append(transform_resource(obj, payer, base_url))
                except (TransformError, ValueError, TypeError):
                    errors += 1
                    continue
                if len(batch) >= BATCH:
                    bulk_load(load_conn, table, batch)
                    load_conn.commit()
                    batch = []
                    if total_lines % PROGRESS_EVERY < BATCH:
                        print(f"  loaded ~{total_lines:,} lines "
                              f"(file {fi + 1}/{len(files)}: {path.name})", flush=True)
        if batch:
            bulk_load(load_conn, table, batch)
            load_conn.commit()
            batch = []
        print(f"  file {fi + 1}/{len(files)} done ({path.name}); "
              f"cumulative lines={total_lines:,} errors={errors:,}", flush=True)
    return total_lines, errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-dir", default=DEFAULT_SOURCE_DIR,
                     help="directory of captured *.ndjson files (default: %(default)s)")
    ap.add_argument("--dry-run", action="store_true",
                     help="print the plan and exit; make no changes")
    ap.add_argument("--skip-truncate", action="store_true",
                     help="skip TRUNCATE + index drop (assumes already done); "
                          "the load loop still restarts from file 0 -- see docstring")
    ap.add_argument("--rebuild-only", action="store_true",
                     help="skip truncate AND load; only drop leftover indexes, "
                          "rebuild all 3 on hot_idx, ANALYZE, record provenance. "
                          "Recovery path for a completed load whose rebuild failed.")
    args = ap.parse_args()
    if args.rebuild_only and args.skip_truncate:
        ap.error("--rebuild-only already skips truncate and load; "
                 "do not combine it with --skip-truncate")

    ep = load_manifest().by_key(PAYER)
    base_url = ep.base_url
    table = RESOURCE_TABLES[RTYPE]
    schema = schema_for(PAYER)

    source_dir = Path(args.source_dir)
    files = sorted(source_dir.glob("*.ndjson")) if source_dir.is_dir() else []
    total_bytes = sum(f.stat().st_size for f in files)

    ddl_conn = autocommit_connection(schema)
    load_conn: psycopg.Connection | None = None
    try:
        before = est_rows(ddl_conn, schema, table.name)  # estimate; avoids a ~1.3TB scan
        live = preflight(ddl_conn, schema, table.name, files,
                         args.skip_truncate, args.rebuild_only)

        if args.rebuild_only:
            print(f"plan (--rebuild-only): {schema}.{table.name} -- no truncate, "
                  "no load; drop leftover indexes, rebuild, ANALYZE", flush=True)
        else:
            print(f"plan: {schema}.{table.name} <- {len(files)} file(s), "
                  f"{total_bytes:,} bytes, from {source_dir}", flush=True)
        print(f"  current row count: {before:,}", flush=True)
        print(f"  current indexes:   {sorted(live) or '(none)'}", flush=True)
        if args.rebuild_only:
            print("  will run:", flush=True)
            for stmt in (drop_statements(schema, table.name)[1:]
                         + rebuild_statements(schema, table.name)):
                print(f"    {stmt}", flush=True)
        else:
            if not args.skip_truncate:
                print("  will run:", flush=True)
                for stmt in drop_statements(schema, table.name):
                    print(f"    {stmt}", flush=True)
            print("  will then load, then run:", flush=True)
            for stmt in rebuild_statements(schema, table.name):
                print(f"    {stmt}", flush=True)

        if args.dry_run:
            print("DRY RUN: no changes made.", flush=True)
            return 0

        started = datetime.now(tz=timezone.utc)

        total_lines = 0
        errors = 0
        if not args.rebuild_only:
            if not args.skip_truncate:
                print(f"WARNING: TRUNCATING {schema}.{table.name} ({before:,} rows) "
                      f"and DROPPING indexes {sorted(EXPECTED_INDEXES)} -- the ndjson "
                      "capture at --source-dir becomes the sole source of truth from "
                      "here on.", flush=True)
                truncate(ddl_conn, table)
                for stmt in drop_statements(schema, table.name)[1:]:
                    with ddl_conn.cursor() as cur:
                        cur.execute(stmt)

            print(f"loading {len(files)} file(s)...", flush=True)
            load_conn = pg_connection(schema)
            total_lines, errors = run_load(load_conn, table, PAYER, base_url, files)

        # Fresh session for the rebuild: the original DDL connection idled
        # through the whole load and may be dead, and the sort GUCs (notably
        # temp_tablespaces -> M:) must live on the session running CREATE INDEX.
        ddl_conn.close()
        ddl_conn = rebuild_connection(schema)

        print(f"rebuilding indexes on {INDEX_TABLESPACE}...", flush=True)
        try:
            if args.rebuild_only:
                for stmt in drop_statements(schema, table.name)[1:]:
                    with ddl_conn.cursor() as cur:
                        cur.execute(stmt)
            for stmt in rebuild_statements(schema, table.name):
                with ddl_conn.cursor() as cur:
                    cur.execute(stmt)
        except psycopg.errors.UniqueViolation as exc:
            print(f"FATAL: duplicate (payer_id, id) pairs found while rebuilding "
                  f"{PK_NAME} -- the loaded data is not unique on that key: {exc}. "
                  "--rebuild-only would hit the same duplicates; fix the source "
                  "data, then re-run the FULL script (truncate + reload).",
                  flush=True)
            raise
        except Exception:
            print("FATAL: index rebuild failed AFTER the table data was fully "
                  "loaded. The rows are intact but unindexed. Do NOT re-run bare "
                  "(re-truncates and re-loads everything) and do NOT use "
                  "--skip-truncate (re-loads on top, duplicating every row). Fix "
                  "the cause and re-run with --rebuild-only.", flush=True)
            raise

        print("ANALYZE...", flush=True)
        with ddl_conn.cursor() as cur:
            cur.execute(f'ANALYZE "{schema}"."{table.name}"')

        # `after` exactly = rows COPYed: table was truncated to 0, and the unique
        # PK rebuild above would have failed on any duplicate id, so distinct rows
        # == successfully-transformed lines. Avoids an hours-long count(*) on the
        # ~500M-row / ~1.3 TB heap. rebuild-only has no load counters -> estimate.
        after = est_rows(ddl_conn, schema, table.name) if args.rebuild_only else (total_lines - errors)
        finished = datetime.now(tz=timezone.utc)
        if args.rebuild_only:
            error_count = None
            notes = {
                "method": "bulk-export-local-replace", "rebuild_only": True,
                "before": before, "after": after,
            }
        else:
            error_count = errors
            notes = {
                "method": "bulk-export-local-replace", "files": [str(f) for f in files],
                "total_lines": total_lines, "loaded": after,
                "transform_errors": errors, "before": before, "after": after,
            }
        insert_provenance(ddl_conn, {
            "payer_id": PAYER, "source_base_url": base_url, "resource_type": RTYPE,
            "started_at": started, "finished_at": finished,
            "status": "ok", "page_count": 0, "resource_count": after,
            "error_count": error_count, "prior_count": before,
            "pct_change": round(100.0 * (after - before) / before, 1) if before else None,
            "notes": notes,
        })

        elapsed = time.time() - started.timestamp()
        print(f"DONE {PAYER}/{RTYPE}: files={len(files)} total_lines={total_lines:,} "
              f"loaded={after:,} transform_errors={errors:,} "
              f"before={before:,} after={after:,} elapsed={elapsed / 3600:.2f}h", flush=True)
    finally:
        if load_conn is not None:
            load_conn.close()
        ddl_conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

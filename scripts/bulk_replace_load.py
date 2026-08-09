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

--slim mode (structured-columns-only, fast, resumable):
Full fidelity above writes the toasted ``resource`` JSON (~1.3 TB) to the E:
SMR USB HDD and crawls at ~1,667 rows/s with no mid-load resume. ``--slim``
drops the raw resource blob (replaced with a small stub -- extracted columns
are unaffected) and targets the ``bulk_ssd`` tablespace (M: SATA SSD)
instead, which is far smaller and far faster. Progress is tracked per-file in
``public.slim_load_done``, written in the SAME transaction as that file's
COPY, so ``--slim --resume`` picks up after a crash without risking
duplicate/missing rows. See the approved plan (indexed-riding-gadget) for the
full rationale, including the matching ``quality/rules.py`` change needed so
completeness/conformance scoring reads the extracted columns instead of the
now-stubbed ``resource`` JSON.

    python scripts/bulk_replace_load.py --slim --dry-run
    python scripts/bulk_replace_load.py --slim
    python scripts/bulk_replace_load.py --slim --resume   # after a crash/kill
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

# --slim-only constants.
SLIM_TABLESPACE = "bulk_ssd"
DONE_TABLE = "public.slim_load_done"
# Capacity probe budget (see capacity_probe): projected heap + ~3 indexes at
# ~0.3x heap each + sort headroom, measured against bulk_ssd's 774 GB free.
# Decimal GB (10**9), matching both the probe's printed figures (which divide
# by 1e9) and the plan's ~650-vs-774 arithmetic.
CAPACITY_CAP_BYTES = 650 * 10 ** 9
# 3 indexes (PK on payer_id,id + practitioner_ref + organization_ref) vs heap.
# Grounded in a real measurement: the PK was 7.27 GB at 59.8M rows (~61 GB at
# 500M); the two ref indexes are similar, so ~160 GB total ~= 0.43x heap. Use
# 0.5 for a small safety margin (0.9 was a ~2x-too-high guess that false-aborted).
INDEX_OVERHEAD_FACTOR = 0.5
SORT_HEADROOM_BYTES = 50 * 10 ** 9


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


# --- --slim helpers ----------------------------------------------------------

def ensure_done_table(conn: psycopg.Connection) -> None:
    """Create the per-file resume manifest, if absent. Never call this before
    the --dry-run check -- dry-run must make zero changes.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {DONE_TABLE} ("
            "payer_id text, resource_type text, file text, "
            "loaded_at timestamptz NOT NULL DEFAULT now(), "
            "PRIMARY KEY (payer_id, resource_type, file))"
        )


def done_files(conn: psycopg.Connection, payer: str) -> set[str]:
    """Files already recorded loaded for (payer, RTYPE). Safe to call even if
    the done table has never been created yet (returns empty set) -- so it
    can be used for --dry-run planning without side effects.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'slim_load_done'"
        )
        if cur.fetchone() is None:
            return set()
        cur.execute(
            f"SELECT file FROM {DONE_TABLE} WHERE payer_id = %s AND resource_type = %s",
            (payer, RTYPE),
        )
        return {r[0] for r in cur.fetchall()}


def delete_done_files(conn: psycopg.Connection, payer: str) -> None:
    """Wipe stale done-rows from a prior (discarded) attempt before a fresh
    truncate, so their PKs cannot collide with this run's ON CONFLICT DO
    UPDATE and falsely mark a not-yet-loaded file as done.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {DONE_TABLE} WHERE payer_id = %s AND resource_type = %s",
            (payer, RTYPE),
        )


def mark_file_done(load_conn: psycopg.Connection, payer: str, filename: str) -> None:
    """Record `filename` as loaded, on the SAME connection/transaction as the
    COPY batches for that file -- the caller's single commit() right after
    this call makes the file's rows and its done-marker atomic. MUST run on
    load_conn, never the autocommit DDL connection (see module docstring).
    """
    with load_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {DONE_TABLE} (payer_id, resource_type, file) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (payer_id, resource_type, file) DO UPDATE SET loaded_at = now()",
            (payer, RTYPE, filename),
        )


def set_autovacuum(conn: psycopg.Connection, schema: str, tname: str, enabled: bool) -> None:
    val = "true" if enabled else "false"
    with conn.cursor() as cur:
        cur.execute(
            f'ALTER TABLE "{schema}"."{tname}" SET '
            f"(autovacuum_enabled = {val}, toast.autovacuum_enabled = {val})"
        )


def reenable_autovacuum(schema: str, tname: str) -> None:
    """Always re-enable autovacuum on the slim table, success or failure.

    Opens its OWN fresh autocommit connection -- the ddl/load connections used
    during the run may already be closed or dead by the time a failure
    reaches this point, so re-enabling on them is not reliable. Called from a
    `finally` in run_slim() so it runs no matter how the run ends.
    """
    hint = (f'ALTER TABLE {schema}.{tname} SET (autovacuum_enabled=true); '
            f'ALTER TABLE {schema}.{tname} SET (toast.autovacuum_enabled=true)')
    try:
        conn = autocommit_connection(schema)
    except Exception as exc:
        print(f"FATAL: could not open a connection to re-enable autovacuum on "
              f"{schema}.{tname}: {exc}. Run manually: {hint}", flush=True)
        return
    try:
        set_autovacuum(conn, schema, tname, True)
        print(f"autovacuum re-enabled on {schema}.{tname}", flush=True)
    except Exception as exc:
        print(f"FATAL: failed to re-enable autovacuum on {schema}.{tname}: {exc}. "
              f"Run manually: {hint}", flush=True)
    finally:
        conn.close()


def preflight_slim(conn: psycopg.Connection, schema: str, tname: str, files: list[Path]) -> None:
    if not table_exists(conn, schema, tname):
        raise SystemExit(f"table {schema}.{tname} does not exist; aborting")
    if not tablespace_exists(conn, INDEX_TABLESPACE):
        raise SystemExit(
            f"tablespace {INDEX_TABLESPACE!r} does not exist; it must be created "
            "(e.g. pointing at M:/pg_ts/hot_idx) before running this script"
        )
    if not tablespace_exists(conn, SLIM_TABLESPACE):
        raise SystemExit(
            f"tablespace {SLIM_TABLESPACE!r} does not exist; it must be created "
            f"first: CREATE TABLESPACE {SLIM_TABLESPACE} LOCATION 'M:/pg_ts/{SLIM_TABLESPACE}'"
        )
    if not files:
        raise SystemExit("no *.ndjson source files found; aborting")


def relation_size(conn: psycopg.Connection, schema: str, tname: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size(%s)", (f'"{schema}"."{tname}"',))
        return cur.fetchone()[0]


def capacity_probe(ddl_conn: psycopg.Connection, schema: str, tname: str,
                    bytes_loaded_so_far: int, total_bytes: int) -> None:
    """Abort BEFORE loading further if projected final bulk_ssd (M:) usage
    exceeds the ~650 GB budget (774 GB free). Extrapolates from the ratio of
    current heap size to source bytes read so far, scaled by the FULL source
    set's total bytes -- not a flat per-file multiplier, since the 30 files
    are not equal-sized.
    """
    if bytes_loaded_so_far <= 0:
        return
    heap_size = relation_size(ddl_conn, schema, tname)
    ratio = heap_size / bytes_loaded_so_far
    projected_heap = ratio * total_bytes
    projected_indexes = projected_heap * INDEX_OVERHEAD_FACTOR
    projected_total = projected_heap + projected_indexes + SORT_HEADROOM_BYTES
    print(f"  capacity probe: heap={heap_size / 1e9:.1f}GB from "
          f"{bytes_loaded_so_far / 1e9:.1f}GB read (ratio={ratio:.3f}) -> "
          f"projected heap={projected_heap / 1e9:.1f}GB + indexes~"
          f"{projected_indexes / 1e9:.1f}GB + {SORT_HEADROOM_BYTES / 1e9:.0f}GB "
          f"sort headroom = {projected_total / 1e9:.1f}GB "
          f"(cap {CAPACITY_CAP_BYTES / 1e9:.0f}GB)", flush=True)
    if projected_total > CAPACITY_CAP_BYTES:
        raise SystemExit(
            f"FATAL: projected {SLIM_TABLESPACE} usage {projected_total / 1e9:.1f} GB "
            f"exceeds the ~{CAPACITY_CAP_BYTES / 1e9:.0f} GB cap -- aborting before "
            "loading further. Re-evaluate storage before re-running --slim."
        )


def run_slim_load(load_conn: psycopg.Connection, ddl_conn: psycopg.Connection, table,
                   schema: str, payer: str, base_url: str, files: list[Path],
                   done: set[str], total_bytes: int) -> tuple[int, int]:
    """Stream every not-yet-done file through transform_resource, stub the
    `resource` column, and COPY into the target. Batches within a file share
    one transaction (no intermediate commit); at file end the done-marker is
    inserted on load_conn and a single commit() makes the file's rows and its
    done-marker atomic (see mark_file_done). Runs the capacity probe once,
    right after the first newly-loaded file. Returns (total_lines, errors)
    for files processed THIS invocation (already-done/skipped files are not
    re-read, so their line/error counts are not included).
    """
    total_lines = 0
    errors = 0
    bytes_loaded_so_far = sum(f.stat().st_size for f in files if f.name in done)
    probed = False
    for fi, path in enumerate(files):
        if path.name in done:
            print(f"  skip file {fi + 1}/{len(files)} ({path.name}) -- already "
                  f"recorded in {DONE_TABLE}", flush=True)
            continue
        batch: list[dict] = []
        file_lines = 0
        file_errors = 0
        with open(path, "rb") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                total_lines += 1
                file_lines += 1
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        raise TransformError("line is valid JSON but not an object")
                    row = transform_resource(obj, payer, base_url)
                    # Stub the toasted blob; every extracted/common column
                    # (raw_hash, source_base_url, meta_last_updated,
                    # last_seen_at, all extracted columns) stays as transform
                    # produced it. resource is NOT NULL, so a stub -- never
                    # None -- is required.
                    row["resource"] = {"resourceType": RTYPE, "id": row["id"]}
                    batch.append(row)
                except (TransformError, ValueError, TypeError):
                    errors += 1
                    file_errors += 1
                    continue
                if len(batch) >= BATCH:
                    bulk_load(load_conn, table, batch)
                    batch = []
                    if total_lines % PROGRESS_EVERY < BATCH:
                        print(f"  loaded ~{total_lines:,} lines "
                              f"(file {fi + 1}/{len(files)}: {path.name})", flush=True)
        if batch:
            bulk_load(load_conn, table, batch)
            batch = []
        mark_file_done(load_conn, payer, path.name)
        load_conn.commit()  # atomic: this file's rows + its done-marker together
        bytes_loaded_so_far += path.stat().st_size
        print(f"  file {fi + 1}/{len(files)} done ({path.name}); "
              f"file_lines={file_lines:,} file_errors={file_errors:,}; "
              f"cumulative lines={total_lines:,} errors={errors:,}", flush=True)
        if not probed:
            capacity_probe(ddl_conn, schema, table.name, bytes_loaded_so_far, total_bytes)
            probed = True
    return total_lines, errors


def run_slim(args: argparse.Namespace, table, schema: str, base_url: str,
             files: list[Path], total_bytes: int) -> int:
    ddl_conn = autocommit_connection(schema)
    load_conn: psycopg.Connection | None = None
    slim_started = False
    try:
        preflight_slim(ddl_conn, schema, table.name, files)

        before = est_rows(ddl_conn, schema, table.name)
        live = live_indexes(ddl_conn, schema, table.name)
        done = done_files(ddl_conn, PAYER) if args.resume else set()
        remaining = [f for f in files if f.name not in done]

        if args.resume:
            print(f"plan (--slim --resume): {schema}.{table.name} <- "
                  f"{len(remaining)}/{len(files)} file(s) not yet in {DONE_TABLE}; "
                  "no truncate/tablespace move/index drop/done-wipe", flush=True)
        else:
            print(f"plan (--slim): {schema}.{table.name} <- {len(files)} file(s), "
                  f"{total_bytes:,} bytes; TRUNCATE, move heap to {SLIM_TABLESPACE}, "
                  f"drop indexes {sorted(EXPECTED_INDEXES)}, disable autovacuum, wipe "
                  f"{DONE_TABLE} for {PAYER}/{RTYPE}, load with stubbed resource, "
                  f"then rebuild indexes on {INDEX_TABLESPACE}.", flush=True)
        print(f"  current row count: {before:,}", flush=True)
        print(f"  current indexes:   {sorted(live) or '(none)'}", flush=True)
        print(f"  files already done: {len(done)}; remaining: {len(remaining)}", flush=True)

        if args.dry_run:
            print("DRY RUN: no changes made.", flush=True)
            return 0

        if args.resume and not done:
            raise SystemExit(
                f"--resume found NO files recorded in {DONE_TABLE} for "
                f"{PAYER}/{RTYPE}: there is nothing to resume (either no --slim "
                "run ever committed a file, or the table state is not a slim "
                "load's). Resuming here would skip the TRUNCATE/tablespace-move "
                "and load every file ON TOP of whatever rows exist -- duplicates "
                "surfacing only at the PK rebuild, hours later. Re-run a fresh "
                "--slim instead (safe: it re-truncates)."
            )

        started = datetime.now(tz=timezone.utc)
        slim_started = True
        ensure_done_table(ddl_conn)

        if not args.resume:
            print(f"WARNING: TRUNCATING {schema}.{table.name} ({before:,} rows), "
                  f"moving heap to {SLIM_TABLESPACE}, DROPPING indexes "
                  f"{sorted(EXPECTED_INDEXES)}, and wiping {DONE_TABLE} for "
                  f"{PAYER}/{RTYPE} -- the ndjson capture becomes the sole "
                  "source of truth from here on.", flush=True)
            # ONE transaction for the whole destructive prelude (all of these
            # are transactional DDL in Postgres). Done piecemeal on the
            # autocommit conn, a crash between the committed TRUNCATE and the
            # done-table wipe would leave an empty table with stale done-rows,
            # and the documented recovery (--slim --resume) would then
            # silently SKIP those files -- a permanent gap the PK rebuild
            # cannot detect. Atomic: either the old state survives intact or
            # the table is empty with zero done-rows; both are safe to re-run.
            prep_conn = pg_connection(schema)
            try:
                truncate(prep_conn, table)
                with prep_conn.cursor() as cur:
                    cur.execute(f'ALTER TABLE "{schema}"."{table.name}" '
                                f'SET TABLESPACE {SLIM_TABLESPACE}')
                for stmt in drop_statements(schema, table.name)[1:]:
                    with prep_conn.cursor() as cur:
                        cur.execute(stmt)
                set_autovacuum(prep_conn, schema, table.name, False)
                delete_done_files(prep_conn, PAYER)
                prep_conn.commit()
            finally:
                prep_conn.close()
        elif remaining:
            # Keep disabled during the resumed load (idempotent even if it was
            # already off); VACUUM first to reclaim aborted-transaction bloat
            # from the prior crash.
            set_autovacuum(ddl_conn, schema, table.name, False)
            print(f"VACUUM {schema}.{table.name} before resuming "
                  "(reclaim aborted-transaction bloat)...", flush=True)
            with ddl_conn.cursor() as cur:
                cur.execute(f'VACUUM "{schema}"."{table.name}"')

        total_lines = errors = 0
        if remaining:
            print(f"loading {len(remaining)} file(s)...", flush=True)
            load_conn = pg_connection(schema)
            total_lines, errors = run_slim_load(
                load_conn, ddl_conn, table, schema, PAYER, base_url,
                files, done, total_bytes,
            )
        else:
            print("all files already recorded done; skipping straight to "
                  "index rebuild.", flush=True)

        # Fresh session for the rebuild, same reasoning as the full path.
        ddl_conn.close()
        ddl_conn = rebuild_connection(schema)

        print(f"rebuilding indexes on {INDEX_TABLESPACE}...", flush=True)
        try:
            for stmt in drop_statements(schema, table.name)[1:]:
                with ddl_conn.cursor() as cur:
                    cur.execute(stmt)
            for stmt in rebuild_statements(schema, table.name):
                with ddl_conn.cursor() as cur:
                    cur.execute(stmt)
        except psycopg.errors.UniqueViolation as exc:
            print(f"FATAL: duplicate (payer_id, id) pairs found while rebuilding "
                  f"{PK_NAME} -- the loaded data is not unique on that key: {exc}. "
                  f"Investigate {DONE_TABLE} against the source files before "
                  "re-running -- do not blindly re-run bare (re-truncates and "
                  "reloads the full capture).", flush=True)
            raise
        except Exception:
            print("FATAL: index rebuild failed AFTER the slim load completed. "
                  "The rows are intact but unindexed. Re-run with "
                  "--slim --resume: every file will show done in "
                  f"{DONE_TABLE} and it will go straight to the rebuild.", flush=True)
            raise

        print("ANALYZE...", flush=True)
        with ddl_conn.cursor() as cur:
            cur.execute(f'ANALYZE "{schema}"."{table.name}"')

        # Exact only when every file was processed THIS invocation (fresh run,
        # or a resume where nothing was actually skipped): the unique PK
        # rebuild guarantees no duplicates, so total_lines - errors == rows.
        # A genuinely-resumed partial run can't know prior invocations'
        # line/error counts (slim_load_done intentionally has no such
        # columns -- see plan), so fall back to the post-ANALYZE estimate,
        # same as --rebuild-only does on the full path.
        skipped = len(files) - len(remaining)
        after = (total_lines - errors) if skipped == 0 else est_rows(ddl_conn, schema, table.name)
        finished = datetime.now(tz=timezone.utc)
        notes = {
            "method": "bulk-export-local-slim",
            "files": [str(f) for f in files],
            "total_lines": total_lines, "loaded": after, "transform_errors": errors,
            "before": before, "after": after,
            "resource_stub": True,
            "raw_resource_location": str(files[0].parent) if files else None,
            "raw_hash_invariant": ("raw_hash reflects the archived ndjson file's "
                                    "full resource, not the stub resource column"),
            "partial_resume": skipped > 0,
        }
        insert_provenance(ddl_conn, {
            "payer_id": PAYER, "source_base_url": base_url, "resource_type": RTYPE,
            "started_at": started, "finished_at": finished,
            "status": "ok", "page_count": 0, "resource_count": after,
            "error_count": errors, "prior_count": before,
            "pct_change": round(100.0 * (after - before) / before, 1) if before else None,
            "notes": notes,
        })

        elapsed = time.time() - started.timestamp()
        print(f"DONE (slim) {PAYER}/{RTYPE}: files={len(files)} "
              f"total_lines={total_lines:,} loaded={after:,} "
              f"transform_errors={errors:,} before={before:,} after={after:,} "
              f"elapsed={elapsed / 3600:.2f}h", flush=True)
    finally:
        # Nested so the autovacuum re-enable runs even if a close() raises on
        # a dead/broken connection -- the very scenario it exists for.
        try:
            try:
                if load_conn is not None:
                    load_conn.close()
            finally:
                ddl_conn.close()
        finally:
            if slim_started:
                reenable_autovacuum(schema, table.name)
    return 0


def run_full(args: argparse.Namespace, table, schema: str, base_url: str,
             files: list[Path], total_bytes: int) -> int:
    source_dir = Path(args.source_dir)

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
    ap.add_argument("--slim", action="store_true",
                     help="structured-columns-only load to the bulk_ssd (M:) "
                          "tablespace: the raw `resource` JSON is replaced with a "
                          "small stub, extracted columns are untouched. Faster and "
                          "far smaller than the full-fidelity path; resumable via "
                          "--resume. Compatible with --dry-run and --resume; NOT "
                          "with --skip-truncate/--rebuild-only (use --slim --resume "
                          "instead). See module docstring.")
    ap.add_argument("--resume", action="store_true",
                     help="(--slim only) resume a prior --slim run after a crash: "
                          "skip TRUNCATE/tablespace-move/index-drop/done-table wipe, "
                          "VACUUM the table, and load only files not yet recorded in "
                          f"{DONE_TABLE}. If every file is already recorded, skip "
                          "straight to the index rebuild.")
    args = ap.parse_args()
    if args.rebuild_only and args.skip_truncate:
        ap.error("--rebuild-only already skips truncate and load; "
                 "do not combine it with --skip-truncate")
    if args.resume and not args.slim:
        ap.error("--resume is only meaningful with --slim")
    if args.slim and (args.skip_truncate or args.rebuild_only):
        ap.error("--slim is not compatible with --skip-truncate/--rebuild-only; "
                 "use --slim --resume to recover a partial --slim run instead")

    ep = load_manifest().by_key(PAYER)
    base_url = ep.base_url
    table = RESOURCE_TABLES[RTYPE]
    schema = schema_for(PAYER)

    source_dir = Path(args.source_dir)
    files = sorted(source_dir.glob("*.ndjson")) if source_dir.is_dir() else []
    total_bytes = sum(f.stat().st_size for f in files)

    if args.slim:
        return run_slim(args, table, schema, base_url, files, total_bytes)
    return run_full(args, table, schema, base_url, files, total_bytes)


if __name__ == "__main__":
    raise SystemExit(main())

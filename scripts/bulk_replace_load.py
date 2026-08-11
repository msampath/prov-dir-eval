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
  * source ndjson capture: I:\\aetna_export\\*.ndjson (30 files, ~1.59 TB)
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

--slim --workers N (parallel):
A single --slim process is CPU-bound in the transform, not I/O-bound: measured
per row, json.loads costs ~138us, sha256_hash's canonical re-serialization
~98us, and the field extraction only ~15us -- ~4,000 rows/s on one core, while
this box has 36. ``--workers N`` shards the file list N ways and runs N
``--slim --load-only --shard i/N`` child processes concurrently, each COPYing
into the same index-less heap on its own connection. --slim also swaps the
canonical-form hash for a sha256 of the raw ndjson line (~14us), which is the
same bytes the archive holds and ~7x cheaper. Together these take the load from
CPU-bound to bounded by the I: read ceiling (~200-250 MB/s).

The parent owns everything except the loading: preflight, the destructive
prelude, VACUUM, the autovacuum toggle, the capacity probe (re-run periodically
during the load, so a runaway is caught early rather than at the end), the
single index rebuild after ALL shards finish, and provenance. Children only
load. Shards are ``files[i::N]`` of the sorted file list, so they are disjoint
and stable across restarts; on top of that each child takes a transaction-scoped
advisory lock per file and re-checks the done table inside that lock, so even a
stray orphan from a killed run cannot double-load a file.

    python scripts/bulk_replace_load.py --slim --resume --workers 12
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import get_settings, load_manifest              # noqa: E402
from provdir.etl.loader import (                                    # noqa: E402
    bulk_load, insert_provenance, pg_connection, register_slim_table, schema_for,
    truncate, unregister_slim_table,
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
# Capacity probe budget (see capacity_probe). Decimal GB (10**9), matching the
# probe's printed figures. Preferred source is the LIVE volume (current
# tablespace usage + free bytes - VOLUME_RESERVE_BYTES), because a constant
# silently goes stale: this cap was written as 650 against "774 GB free" when
# the table was empty, but ~91 GB of that has since been consumed by the seven
# committed files, so a fixed 650 now measures a moved target and leaves only a
# ~2% margin on an otherwise-healthy projection. CAPACITY_CAP_BYTES is only the
# fallback for when the volume cannot be stat'd.
CAPACITY_CAP_BYTES = 650 * 10 ** 9
# Left unallocated on the tablespace's volume for the OS and anything else
# sharing it, when deriving the cap from live free space.
VOLUME_RESERVE_BYTES = 60 * 10 ** 9
# The index rebuild runs AFTER all loading, and needs room for ~3 indexes plus
# external-sort spill (temp_tablespaces points at M: too). The supervisor
# aborts the load if free space on the volume falls below this while children
# are still writing, so the rebuild cannot be starved by the heap.
INDEX_RESERVE_BYTES = 250 * 10 ** 9
# 3 indexes (PK on payer_id,id + practitioner_ref + organization_ref) vs heap.
# Grounded in a real measurement: the PK was 7.27 GB at 59.8M rows (~61 GB at
# 500M); the two ref indexes are similar, so ~160 GB total ~= 0.43x heap. Use
# 0.5 for a small safety margin (0.9 was a ~2x-too-high guess that false-aborted).
INDEX_OVERHEAD_FACTOR = 0.5
SORT_HEADROOM_BYTES = 50 * 10 ** 9
# Big read buffer: with N shards streaming N different 53 GB files off ONE USB
# HDD, the default 8 KB buffer turns a sequential read into a seek storm. 16 MB
# per reader amortizes the ~12 ms seek over ~65 ms of transfer.
READ_BUFFER = 16 * 1024 * 1024
# How often the parent re-runs the capacity probe while children load. The
# single-process path probes once after its first file; with N children the
# parent has no such hook, so it polls instead.
PROBE_INTERVAL_SEC = 300


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


def live_rows(conn: psycopg.Connection, schema: str, tname: str) -> int:
    """Row-count estimate from pg_stat_all_tables.n_live_tup, falling back to
    est_rows (pg_class.reltuples).

    reltuples alone is badly wrong during this load: it is only refreshed by
    VACUUM/ANALYZE, both of which are suppressed for the duration (autovacuum
    is disabled on the table on purpose), so it stays frozen at whatever the
    last resume-VACUUM saw -- measured at 16.6M while the table actually held
    116.5M rows, a 7x understatement that would land in provenance as
    `prior_count` and blow up `pct_change`. n_live_tup is maintained
    incrementally per commit, so it tracks the load.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT n_live_tup FROM pg_stat_all_tables "
            "WHERE schemaname = %s AND relname = %s",
            (schema, tname),
        )
        r = cur.fetchone()
    if r and r[0] is not None:
        return max(0, int(r[0]))
    return est_rows(conn, schema, tname)


def foreign_table_lockers(conn: psycopg.Connection, schema: str, tname: str) -> list[tuple]:
    """Other backends currently holding a lock on the target table.

    The per-file advisory claim protects against a rogue loader duplicating a
    file, but nothing protected against the destructive prelude: TRUNCATE takes
    ACCESS EXCLUSIVE and will happily wait for, then obliterate, a live COPY
    from an older run. Checking for foreign lockers up front turns that into a
    refusal instead of silent data loss.

    Autovacuum workers are deliberately NOT counted. They auto-cancel the
    moment a conflicting lock is requested (so they can neither destroy this
    run's work nor be destroyed by it), and they are routinely present exactly
    when this check runs: a crashed run's `finally` re-enables autovacuum on a
    table that just gained millions of dead tuples, so by the time the operator
    re-runs --resume a worker is often mid-scan on it. Counting it would
    false-refuse the documented recovery path.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT l.pid, a.state, left(coalesce(a.query, ''), 80) "
            "FROM pg_locks l "
            "JOIN pg_class c ON c.oid = l.relation "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE n.nspname = %s AND c.relname = %s AND l.pid <> pg_backend_pid() "
            "AND coalesce(a.backend_type, '') <> 'autovacuum worker'",
            (schema, tname),
        )
        return cur.fetchall()


def acquire_run_lock(conn: psycopg.Connection, payer: str) -> None:
    """Session-scoped advisory lock making the PARENT run singular.

    Per-file claims stop two loaders from writing the same file, but two
    parents are still catastrophic in ways per-file locks cannot see: their
    destructive preludes race (one TRUNCATE wipes the other's committed rows
    and `delete_done_files` erases its markers), and their index rebuilds race
    (CREATE UNIQUE INDEX has no IF NOT EXISTS, so the loser dies after hours).
    Held for the whole run on a dedicated connection -- ddl_conn is closed and
    replaced before the rebuild, which would release it at the worst moment.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s))",
            ("provdir_slim_load", f"{payer}/{RTYPE}"),
        )
        if not cur.fetchone()[0]:
            raise SystemExit(
                f"FATAL: another {PAYER}/{RTYPE} --slim run already holds the run "
                "lock. Two parents would race each other's TRUNCATE and index "
                "rebuild. Wait for it to finish, or stop it, then re-run."
            )


def kill_on_parent_death() -> object | None:
    """A Windows Job Object configured to kill its members when the handle
    closes, i.e. when this process dies for any reason.

    Without it, `terminate()`-ing or crashing the parent leaves N children
    running unattended for hours: each child owns its whole shard and never
    checks whether the parent is still alive. The operator believes the load is
    stopped, starts something else, and collides with the orphans. Returns None
    (with a warning) if the Job Object cannot be created -- degraded, not fatal.
    """
    if sys.platform != "win32":
        return None
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JobObjectExtendedLimitInformation = 9
    try:
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [(n, ctypes.c_ulonglong) for n in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", ctypes.c_uint32),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", ctypes.c_uint32),
                ("Affinity", ctypes.POINTER(ctypes.c_ulong)),
                ("PriorityClass", ctypes.c_uint32),
                ("SchedulingClass", ctypes.c_uint32),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        k32.CreateJobObjectW.restype = ctypes.c_void_p
        job = k32.CreateJobObjectW(None, None)
        if not job:
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        ok = k32.SetInformationJobObject(
            ctypes.c_void_p(job), JobObjectExtendedLimitInformation,
            ctypes.byref(info), ctypes.sizeof(info))
        if not ok:
            # Without the kill-on-close flag the job is useless; close it so a
            # flag-less handle doesn't linger for the (multi-hour) run.
            err = ctypes.get_last_error()
            k32.CloseHandle(ctypes.c_void_p(job))
            raise OSError(err, "SetInformationJobObject failed")
        return job
    except Exception as exc:  # noqa: BLE001 -- degrade, never block the load
        print(f"WARNING: could not create a kill-on-close Job Object ({exc}); "
              "child loaders will NOT be killed automatically if this parent "
              "dies. If you kill the parent, check for stray python.exe loaders "
              "before starting another run.", flush=True)
        return None


def assign_to_job(job: object | None, proc: subprocess.Popen) -> None:
    if job is None:
        return
    try:
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        if not k32.AssignProcessToJobObject(ctypes.c_void_p(job),
                                            ctypes.c_void_p(int(proc._handle))):
            raise OSError(ctypes.get_last_error(), "AssignProcessToJobObject failed")
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: child pid {proc.pid} not assigned to the Job Object "
              f"({exc}); it may outlive this parent.", flush=True)


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

    ``lines``/``errors`` were added for the parallel path: children each load a
    disjoint shard in their own process, so the parent cannot observe their
    counters except through this table. They are nullable because rows written
    by earlier (pre-parallel) runs predate the columns -- see done_stats.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {DONE_TABLE} ("
            "payer_id text, resource_type text, file text, "
            "loaded_at timestamptz NOT NULL DEFAULT now(), "
            "PRIMARY KEY (payer_id, resource_type, file))"
        )
        cur.execute(f"ALTER TABLE {DONE_TABLE} ADD COLUMN IF NOT EXISTS lines bigint")
        cur.execute(f"ALTER TABLE {DONE_TABLE} ADD COLUMN IF NOT EXISTS errors bigint")


def done_stats(conn: psycopg.Connection, payer: str) -> tuple[int, int, int]:
    """(summed lines, summed errors, count of done rows MISSING those counts).

    A non-zero third element means the sums cover only part of the load, so the
    caller must fall back to an estimate rather than report them as exact.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT coalesce(sum(lines), 0), coalesce(sum(errors), 0), "
            f"count(*) FILTER (WHERE lines IS NULL) FROM {DONE_TABLE} "
            "WHERE payer_id = %s AND resource_type = %s",
            (payer, RTYPE),
        )
        r = cur.fetchone()
        return int(r[0]), int(r[1]), int(r[2])


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


def mark_file_done(load_conn: psycopg.Connection, payer: str, filename: str,
                   lines: int, errors: int) -> None:
    """Record `filename` as loaded, on the SAME connection/transaction as the
    COPY batches for that file -- the caller's single commit() right after
    this call makes the file's rows and its done-marker atomic. MUST run on
    load_conn, never the autocommit DDL connection (see module docstring).
    """
    with load_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {DONE_TABLE} (payer_id, resource_type, file, lines, errors) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (payer_id, resource_type, file) DO UPDATE SET "
            "loaded_at = now(), lines = EXCLUDED.lines, errors = EXCLUDED.errors",
            (payer, RTYPE, filename, lines, errors),
        )


def claim_file(load_conn: psycopg.Connection, payer: str, filename: str) -> bool:
    """Take an exclusive, transaction-scoped claim on loading `filename`.

    Two independent guards, both inside the file's own transaction (so both are
    released automatically by the commit/rollback that ends it):

    * a ``pg_try_advisory_xact_lock`` -- shards are disjoint by construction, so
      this only matters when something unexpected is running: an orphaned child
      from a killed parent, or a hand-started second run. Without it, two
      processes could COPY the same 16.6M-row file and the duplicates would only
      surface hours later at the unique-PK rebuild;
    * a re-read of the done table *under* that lock -- the caller's `done` set
      was snapshotted before the loop, so a file finished by another process
      since then would otherwise be reloaded.

    Returns True if this process may load the file.
    """
    with load_conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s), hashtext(%s))",
            (f"{payer}/{RTYPE}", filename),
        )
        if not cur.fetchone()[0]:
            return False
        cur.execute(
            f"SELECT 1 FROM {DONE_TABLE} WHERE payer_id = %s AND resource_type = %s "
            "AND file = %s",
            (payer, RTYPE, filename),
        )
        return cur.fetchone() is None


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


def tablespace_volume(conn: psycopg.Connection, tablespace: str) -> Path | None:
    """Filesystem directory backing `tablespace`, or None for a built-in one."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname = %s",
            (tablespace,),
        )
        row = cur.fetchone()
    loc = (row[0] if row else "") or ""
    return Path(loc) if loc else None


def volume_free_bytes(conn: psycopg.Connection, tablespace: str) -> int | None:
    """Free bytes on the volume holding `tablespace`, or None if unknowable."""
    loc = tablespace_volume(conn, tablespace)
    if loc is None:
        return None
    try:
        return shutil.disk_usage(str(loc)).free
    except OSError:
        return None


def capacity_cap(conn: psycopg.Connection, tablespace: str) -> tuple[int, str]:
    """(byte budget for this table+indexes, how it was derived).

    Derived from live state where possible -- what the tablespace already holds
    plus what the volume still has free, minus a reserve -- so it tracks reality
    instead of a constant that ages out as the load consumes space.
    """
    free = volume_free_bytes(conn, tablespace)
    if free is None:
        return CAPACITY_CAP_BYTES, f"static fallback {CAPACITY_CAP_BYTES / 1e9:.0f}GB"
    with conn.cursor() as cur:
        cur.execute("SELECT pg_tablespace_size(%s)", (tablespace,))
        used = int(cur.fetchone()[0])
    cap = used + free - VOLUME_RESERVE_BYTES
    return cap, (f"live volume: {used / 1e9:.0f}GB used + {free / 1e9:.0f}GB free "
                 f"- {VOLUME_RESERVE_BYTES / 1e9:.0f}GB reserve")


def capacity_probe(ddl_conn: psycopg.Connection, schema: str, tname: str,
                    bytes_loaded_so_far: int, total_bytes: int,
                    fatal: bool = True) -> None:
    """Abort BEFORE loading further if the projected final size of the table
    plus its indexes exceeds what the bulk_ssd (M:) volume can hold.

    Extrapolates from the ratio of current heap size to source bytes read so
    far, scaled by the FULL source set's total bytes. Bytes rather than a flat
    "x30 files" multiplier because bytes stay correct no matter how the capture
    is chunked, and because the ratio is what actually varies.

    ONLY VALID WHEN NOTHING IS LOADING CONCURRENTLY. `heap_size` counts every
    page physically written, including rows from transactions still in flight,
    while `bytes_loaded_so_far` counts only files that have COMMITTED. Call this
    from the single-process path (which probes just after a per-file commit) or
    from the parent before it launches children -- never while children are
    mid-file, or the numerator races ahead of the denominator and the inflated
    ratio false-aborts a healthy run. The supervisor's mid-flight guard is
    free-space based instead, which has no such skew.

    ``fatal=False`` demotes an over-cap projection to a loud warning. Required
    on every --resume: the crash being resumed aborted up to one in-flight file
    PER WORKER, and those transactions' heap pages stay in the relation --
    VACUUM makes them reusable but never returns them to the OS -- so
    `heap_size` over-counts committed data while `bytes_loaded_so_far` counts
    only committed files. One aborted file (~13 GB) already puts a healthy
    projection near the cap; a killed 12-worker run (~150 GB of aborted heap)
    projects ~2x over it and would false-refuse the documented recovery. The
    over-count is also self-correcting in reality (new COPYs fill the reclaimed
    space first), which the formula cannot see. Enforcement on resume falls to
    free_space_guard, which measures the volume directly and has no such skew.
    """
    if bytes_loaded_so_far <= 0:
        return
    cap, how = capacity_cap(ddl_conn, SLIM_TABLESPACE)
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
          f"(cap {cap / 1e9:.0f}GB; {how})", flush=True)
    if projected_total > cap:
        if not fatal:
            print(f"  WARNING: projected {SLIM_TABLESPACE} usage "
                  f"{projected_total / 1e9:.1f} GB exceeds the {cap / 1e9:.0f} GB "
                  f"budget ({how}), but NOT aborting: this is a resume, and the "
                  "heap figure includes reusable aborted-transaction bloat from "
                  "the crash being resumed (see docstring). free_space_guard "
                  "remains the enforcing check.", flush=True)
            return
        raise SystemExit(
            f"FATAL: projected {SLIM_TABLESPACE} usage {projected_total / 1e9:.1f} GB "
            f"exceeds the {cap / 1e9:.0f} GB budget ({how}) -- aborting before "
            "loading further. Re-evaluate storage before re-running --slim."
        )


def free_space_guard(conn: psycopg.Connection) -> None:
    """Mid-flight capacity guard for the supervisor: abort while children are
    still writing if the volume no longer has room for the index rebuild.

    Unlike capacity_probe this measures the volume directly, so it is immune to
    the committed-vs-in-flight skew that makes a ratio projection meaningless
    while N children hold open transactions. It is also the property actually
    worth protecting: the rebuild runs last and needs INDEX_RESERVE_BYTES of
    room, so letting the heap eat into that reserve strands a fully loaded but
    unindexed table.
    """
    free = volume_free_bytes(conn, SLIM_TABLESPACE)
    if free is None:
        return
    print(f"  free space on {SLIM_TABLESPACE} volume: {free / 1e9:.1f}GB "
          f"(index-rebuild reserve {INDEX_RESERVE_BYTES / 1e9:.0f}GB)", flush=True)
    if free < INDEX_RESERVE_BYTES:
        raise SystemExit(
            f"FATAL: only {free / 1e9:.1f} GB free on the {SLIM_TABLESPACE} volume, "
            f"below the {INDEX_RESERVE_BYTES / 1e9:.0f} GB reserved for the index "
            "rebuild. Stopping the load now so the already-committed files stay "
            "usable and rebuildable."
        )


def run_slim_load(load_conn: psycopg.Connection, ddl_conn: psycopg.Connection | None, table,
                   schema: str, payer: str, base_url: str, files: list[Path],
                   done: set[str], total_bytes: int, probe: bool = True,
                   probe_fatal: bool = True) -> tuple[int, int]:
    """Stream every not-yet-done file through transform_resource, stub the
    `resource` column, and COPY into the target. Batches within a file share
    one transaction (no intermediate commit); at file end the done-marker is
    inserted on load_conn and a single commit() makes the file's rows and its
    done-marker atomic (see mark_file_done). Returns (total_lines, errors)
    for files processed THIS invocation (already-done/skipped files are not
    re-read, so their line/error counts are not included).

    ``probe`` runs the capacity probe once, right after the first newly-loaded
    file. Children (--load-only) pass False: each sees only its own shard's
    bytes, so its extrapolation would be meaningless. The parent probes
    globally on a timer instead (see run_parallel_workers).
    """
    total_lines = 0
    errors = 0
    bytes_loaded_so_far = sum(f.stat().st_size for f in files if f.name in done)
    probed = not probe
    for fi, path in enumerate(files):
        if path.name in done:
            print(f"  skip file {fi + 1}/{len(files)} ({path.name}) -- already "
                  f"recorded in {DONE_TABLE}", flush=True)
            continue
        if not claim_file(load_conn, payer, path.name):
            # Another process owns (or already finished) this file. Ending the
            # transaction here keeps the next file's claim in a clean one.
            load_conn.rollback()
            print(f"  SKIP file {fi + 1}/{len(files)} ({path.name}) -- claimed by "
                  "another process, or completed since this run started. NOT "
                  "loading it here; the parent verifies full coverage before the "
                  "index rebuild.", flush=True)
            continue
        batch: list[dict] = []
        file_lines = 0
        file_errors = 0
        with open(path, "rb", buffering=READ_BUFFER) as fh:
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
                    # Hash the source line (bytes as stored, minus surrounding
                    # whitespace) rather than let transform_resource
                    # re-serialize the dict in canonical form (~98us/row, ~40%
                    # of this loop). These ARE the bytes the I: archive holds,
                    # which is what raw_hash points at for a slim load anyway.
                    # NOT comparable with any other loader's raw_hash.
                    row = transform_resource(
                        obj, payer, base_url, raw_hash=hashlib.sha256(line).hexdigest())
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
        mark_file_done(load_conn, payer, path.name, file_lines, file_errors)
        load_conn.commit()  # atomic: this file's rows + its done-marker together
        bytes_loaded_so_far += path.stat().st_size
        print(f"  file {fi + 1}/{len(files)} done ({path.name}); "
              f"file_lines={file_lines:,} file_errors={file_errors:,}; "
              f"cumulative lines={total_lines:,} errors={errors:,}", flush=True)
        if not probed:
            capacity_probe(ddl_conn, schema, table.name, bytes_loaded_so_far,
                           total_bytes, fatal=probe_fatal)
            probed = True
    return total_lines, errors


def parse_shard(spec: str) -> tuple[int, int]:
    try:
        i_s, k_s = spec.split("/", 1)
        i, k = int(i_s), int(k_s)
    except ValueError:
        raise SystemExit(f"--shard must look like I/K (got {spec!r})")
    if k < 1 or not (0 <= i < k):
        raise SystemExit(f"--shard {spec!r}: need K >= 1 and 0 <= I < K")
    return i, k


def shard_files(files: list[Path], spec: str | None) -> list[Path]:
    """``files[i::k]`` of the SORTED full list -- disjoint across shards, and
    stable across restarts regardless of which files are already done (so a
    resumed child re-derives exactly the same slice).
    """
    if not spec:
        return files
    i, k = parse_shard(spec)
    return files[i::k]


def run_slim_child(args: argparse.Namespace, table, schema: str, base_url: str,
                   files: list[Path]) -> int:
    """--load-only: COPY this shard's not-yet-done files and nothing else.

    No preflight, no truncate/tablespace move, no index drop or rebuild, no
    autovacuum toggle, no VACUUM, no capacity probe, no provenance -- the parent
    did or will do all of it. A child that touched any of those would race its
    siblings.
    """
    # Shards are files[i::k] of each process's OWN glob of --source-dir. That is
    # only disjoint if every process sees the same list, so the parent passes
    # the count it sharded against and a mismatch is fatal here -- otherwise a
    # child that globbed a different set would quietly own a different slice,
    # and the gap would surface only at the parent's coverage gate (or, if two
    # slices overlapped, at the unique-PK rebuild hours later).
    if args.expect_files is not None and len(files) != args.expect_files:
        raise SystemExit(
            f"FATAL: child sees {len(files)} source file(s) but the parent "
            f"sharded against {args.expect_files}. Shard assignment would not "
            f"match. Check {args.source_dir} for files added/removed/unreadable "
            "mid-run."
        )
    mine = shard_files(files, args.shard)
    print(f"child shard {args.shard or '0/1'}: {len(mine)}/{len(files)} file(s) "
          "assigned", flush=True)
    if not mine:
        print("child: shard is empty (more workers than files); exiting 0", flush=True)
        return 0
    ddl_conn = autocommit_connection(schema)
    load_conn: psycopg.Connection | None = None
    try:
        done = done_files(ddl_conn, PAYER)
        remaining = [f for f in mine if f.name not in done]
        if not remaining:
            print("child: every assigned file already recorded done; exiting 0", flush=True)
            return 0
        print(f"child: loading {len(remaining)} file(s)...", flush=True)
        load_conn = pg_connection(schema)
        total_lines, errors = run_slim_load(
            load_conn, None, table, schema, PAYER, base_url, mine, done, 0, probe=False,
        )
        print(f"child shard {args.shard or '0/1'} DONE: total_lines={total_lines:,} "
              f"errors={errors:,}", flush=True)
    finally:
        if load_conn is not None:
            load_conn.close()
        ddl_conn.close()
    return 0


def _pump(tag: str, proc: subprocess.Popen) -> None:
    for line in proc.stdout:
        print(f"[{tag}] {line.rstrip()}", flush=True)


def run_parallel_workers(args: argparse.Namespace, schema: str, files: list[Path],
                         workers: int) -> None:
    """Launch `workers` --load-only children over disjoint shards and supervise.

    Supervision does two things a child cannot do for itself. It watches free
    space on the target volume every PROBE_INTERVAL_SEC (see free_space_guard --
    deliberately NOT capacity_probe, whose committed-bytes denominator is
    meaningless while N transactions are in flight), and it fails the whole
    batch fast when any single child dies, instead of letting eleven siblings
    grind for hours on a load that already cannot complete.

    Raises on any non-zero child exit -- the caller must NOT rebuild indexes
    then, since coverage would be incomplete. Everything already committed is
    preserved in the done table, so --resume picks up cleanly.

    Opens its OWN monitoring connection rather than borrowing the caller's: the
    supervisor sits idle for PROBE_INTERVAL_SEC at a stretch across a multi-hour
    run, and a dropped connection must degrade to "skip this check" rather than
    take down twelve healthy children.
    """
    cmd_base = [sys.executable, str(Path(__file__).resolve()), "--slim", "--load-only",
                "--source-dir", args.source_dir, "--expect-files", str(len(files))]
    job = kill_on_parent_death()
    procs: list[subprocess.Popen] = []
    threads: list[threading.Thread] = []
    mon: psycopg.Connection | None = None
    aborting = ""
    try:
        for i in range(workers):
            proc = subprocess.Popen(
                cmd_base + ["--shard", f"{i}/{workers}"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, encoding="utf-8", errors="replace",
            )
            assign_to_job(job, proc)
            procs.append(proc)
            t = threading.Thread(target=_pump, args=(f"w{i}", proc), daemon=True)
            t.start()
            threads.append(t)
        print(f"launched {workers} loader child process(es) (pids "
              f"{[p.pid for p in procs]}); supervising, free-space check every "
              f"{PROBE_INTERVAL_SEC}s", flush=True)

        def poll_all() -> list:
            """Poll EVERY child, not just up to the first live one.

            ``any(p.poll() is None for p in procs)`` short-circuits, so with w0
            still running none of w1..wN were ever polled and their
            ``returncode`` stayed None -- the dead-child check below could
            therefore never fire while the longest-running shard was alive,
            which is the whole load.
            """
            return [p.poll() for p in procs]

        next_probe = time.time() + PROBE_INTERVAL_SEC
        while any(code is None for code in poll_all()):
            time.sleep(5)
            dead = [(i, c) for i, c in enumerate(poll_all()) if c not in (None, 0)]
            if dead:
                aborting = ("child failure: " +
                            ", ".join(f"w{i} exited {c}" for i, c in dead))
                print(f"supervisor: {aborting} -- stopping the remaining children "
                      "now rather than letting them run for hours on a batch that "
                      "cannot complete. Committed files are preserved.", flush=True)
                break
            if time.time() < next_probe:
                continue
            next_probe = time.time() + PROBE_INTERVAL_SEC
            try:
                if mon is None or mon.closed:
                    mon = autocommit_connection(schema)
                done_now = done_files(mon, PAYER)
                print(f"  supervisor: {len(done_now)}/{len(files)} files done",
                      flush=True)
                free_space_guard(mon)
            except SystemExit:
                aborting = "capacity"
                print("supervisor: free-space guard TRIPPED -- terminating children "
                      "before they eat the index-rebuild reserve. Committed files "
                      "are preserved; re-run --slim --resume after freeing space.",
                      flush=True)
                raise
            except Exception as exc:  # noqa: BLE001
                # A blip on the monitoring connection must not kill the load.
                print(f"  supervisor: check skipped ({exc}); will reconnect next "
                      "interval", flush=True)
                try:
                    if mon is not None:
                        mon.close()
                except Exception:  # noqa: BLE001
                    pass
                mon = None
    finally:
        alive = [p for p in procs if p.poll() is None]
        if alive:
            for p in alive:
                p.terminate()
            for p in alive:
                try:
                    p.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()
        # Join AFTER the children are reaped so each pump drains its pipe to EOF
        # and no child's final FATAL line is lost.
        for t in threads:
            t.join(timeout=30)
        if mon is not None:
            try:
                mon.close()
            except Exception:  # noqa: BLE001
                pass
        if job is not None:
            try:
                ctypes.WinDLL("kernel32").CloseHandle(ctypes.c_void_p(job))
            except Exception:  # noqa: BLE001
                pass

    codes = [p.returncode for p in procs]
    bad = [(i, c) for i, c in enumerate(codes) if c != 0]
    if bad:
        raise SystemExit(
            "FATAL: " + ", ".join(f"child {i} exited {c}" for i, c in bad) +
            (f" (triggered by {aborting})" if aborting else "") +
            ". Not rebuilding indexes -- coverage may be incomplete. Every "
            f"committed file is recorded in {DONE_TABLE}; fix the cause and "
            "re-run --slim --resume (optionally with --workers) to finish."
        )
    print(f"all {workers} child process(es) exited 0", flush=True)


def run_slim(args: argparse.Namespace, table, schema: str, base_url: str,
             files: list[Path], total_bytes: int) -> int:
    if args.load_only:
        return run_slim_child(args, table, schema, base_url, files)
    ddl_conn = autocommit_connection(schema)
    lock_conn: psycopg.Connection | None = None
    load_conn: psycopg.Connection | None = None
    slim_started = False
    try:
        preflight_slim(ddl_conn, schema, table.name, files)

        before = live_rows(ddl_conn, schema, table.name)
        live = live_indexes(ddl_conn, schema, table.name)
        done = done_files(ddl_conn, PAYER) if args.resume else set()
        remaining = [f for f in files if f.name not in done]

        par = f" across {args.workers} worker(s)" if args.workers > 1 else ""
        if args.resume:
            print(f"plan (--slim --resume): {schema}.{table.name} <- "
                  f"{len(remaining)}/{len(files)} file(s) not yet in {DONE_TABLE}{par}; "
                  "no truncate/tablespace move/index drop/done-wipe", flush=True)
        else:
            print(f"plan (--slim): {schema}.{table.name} <- {len(files)} file(s), "
                  f"{total_bytes:,} bytes; TRUNCATE, move heap to {SLIM_TABLESPACE}, "
                  f"drop indexes {sorted(EXPECTED_INDEXES)}, disable autovacuum, wipe "
                  f"{DONE_TABLE} for {PAYER}/{RTYPE}, load with stubbed resource{par}, "
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

        # Singular-parent lock, on its own connection so it survives ddl_conn
        # being swapped for the rebuild session later.
        lock_conn = autocommit_connection(schema)
        acquire_run_lock(lock_conn, PAYER)

        # Nothing else may be touching the table. Critically this covers the
        # non-resume path, where TRUNCATE would otherwise queue behind a live
        # COPY from an older run and then destroy its work.
        others = foreign_table_lockers(ddl_conn, schema, table.name)
        if others:
            raise SystemExit(
                f"FATAL: {len(others)} other backend(s) hold locks on "
                f"{schema}.{table.name}: {others}. A loader running here would "
                "race them (and a non-resume run's TRUNCATE would destroy their "
                "work). Stop the other loader, and cancel any stray count(*) "
                "readers (pg_terminate_backend), then re-run."
            )

        started = datetime.now(tz=timezone.utc)
        slim_started = True
        ensure_done_table(ddl_conn)

        # Register BEFORE loading, not after. The hazard is live the moment the
        # table stops holding full resources, and a run that dies mid-load still
        # leaves a table full of stubs that an --upsert would re-inflate. Late
        # registration would leave exactly that window unguarded.
        register_slim_table(
            ddl_conn, PAYER, RTYPE,
            reason=("loaded structured-columns-only by bulk_replace_load.py --slim; "
                    "`resource` holds a stub, not the full FHIR JSON"),
            raw_location=str(Path(args.source_dir)),
        )

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
        if remaining and args.workers > 1:
            # Probe up front: unlike a cold start, a resume already has real
            # heap-vs-bytes data from the files done so far, so the projection
            # is available before committing another TB of writes. Advisory
            # only (fatal=False): a crash aborts up to one in-flight file per
            # worker, and that bloat stays in relation_size after VACUUM, so a
            # fatal probe would false-refuse the recovery it exists to serve.
            loaded_bytes = sum(f.stat().st_size for f in files if f.name in done)
            if loaded_bytes:
                capacity_probe(ddl_conn, schema, table.name, loaded_bytes,
                               total_bytes, fatal=False)
            run_parallel_workers(args, schema, files, args.workers)
        elif remaining:
            print(f"loading {len(remaining)} file(s)...", flush=True)
            load_conn = pg_connection(schema)
            total_lines, errors = run_slim_load(
                load_conn, ddl_conn, table, schema, PAYER, base_url,
                files, done, total_bytes,
                # Same bloat skew as above: only a FRESH run (post-truncate,
                # zero-bloat heap) has a numerator clean enough to abort on.
                probe_fatal=not args.resume,
            )
        else:
            print("all files already recorded done; skipping straight to "
                  "index rebuild.", flush=True)

        # Fresh session for the rebuild, same reasoning as the full path. Swap
        # BEFORE the coverage gate: ddl_conn idled through the entire multi-hour
        # load, so querying it here could fail on a dropped connection and
        # strand a fully loaded but unindexed table -- exactly what the swap
        # exists to prevent.
        ddl_conn.close()
        ddl_conn = rebuild_connection(schema)

        # Coverage gate. A child can legitimately decline a file (claim_file
        # loses the advisory lock to a stray process) and still exit 0, and a
        # single-process run can hit the same case. Rebuilding now would stamp
        # provenance on a silently short load, so verify every source file is
        # recorded before going near the indexes.
        final_done = done_files(ddl_conn, PAYER)
        missing = [f.name for f in files if f.name not in final_done]
        if missing:
            raise SystemExit(
                f"FATAL: {len(missing)} of {len(files)} source file(s) are still "
                f"not recorded in {DONE_TABLE} after the load: "
                f"{missing[:5]}{' ...' if len(missing) > 5 else ''}. Refusing to "
                "rebuild indexes or write provenance on an incomplete load. "
                "Check for a stray loader process, then re-run --slim --resume."
            )

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

        # Prefer the per-file counters recorded in the done table: they cover
        # every file of the load regardless of which invocation (or which child
        # process) actually read it, and the unique PK rebuild above guarantees
        # no duplicates, so lines - errors == rows exactly. Rows written before
        # those columns existed have NULL counters; if any remain, the sums
        # cover only part of the load, so fall back to the post-ANALYZE
        # estimate rather than report a number that is silently short.
        skipped = len(files) - len(remaining)
        sum_lines, sum_errors, uncounted = done_stats(ddl_conn, PAYER)
        exact = uncounted == 0
        # Report the sums either way. They are the best figures available even
        # when incomplete, and in the parallel path they are the ONLY ones: the
        # children hold the counters, so the parent's own total_lines/errors are
        # still 0 here and publishing those would record a load of hundreds of
        # millions of rows as having processed nothing.
        total_lines, errors = sum_lines, sum_errors
        after = (sum_lines - sum_errors) if exact else est_rows(ddl_conn, schema, table.name)
        finished = datetime.now(tz=timezone.utc)
        notes = {
            "method": "bulk-export-local-slim",
            "files": [str(f) for f in files],
            "total_lines": total_lines, "loaded": after, "transform_errors": errors,
            "before": before, "after": after,
            # total_lines/transform_errors are summed from slim_load_done and
            # therefore cover only the files_without_counters shortfall noted
            # here; counts_exact says whether they cover the whole load.
            "counts_exact": exact,
            "files_without_counters": uncounted,
            "workers": args.workers,
            "resource_stub": True,
            "raw_resource_location": str(files[0].parent) if files else None,
            "raw_hash_invariant": ("raw_hash is sha256 of the archived ndjson line's "
                                    "bytes with surrounding whitespace stripped -- NOT "
                                    "sha256_hash()'s canonical re-serialization used by "
                                    "every other loader, and not a hash of the stub "
                                    "resource column"),
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
              f"exact={exact} workers={args.workers} "
              f"elapsed={elapsed / 3600:.2f}h", flush=True)
    finally:
        # Nested so the autovacuum re-enable runs even if a close() raises on
        # a dead/broken connection -- the very scenario it exists for.
        try:
            try:
                if load_conn is not None:
                    load_conn.close()
            finally:
                try:
                    ddl_conn.close()
                finally:
                    # Closing releases the session-scoped run lock.
                    if lock_conn is not None:
                        lock_conn.close()
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

        # This run put FULL resources back (truncate + full-fidelity reload), so
        # a lingering slim registration would wrongly freeze the table out of
        # orchestrated pulls and upserts forever. NOT under --rebuild-only: that
        # path never loaded rows, so it cannot know the heap is full-fidelity
        # (it could follow a --slim run whose index rebuild failed).
        if not args.rebuild_only and unregister_slim_table(ddl_conn, PAYER, RTYPE):
            print(f"removed slim registration for {PAYER}/{RTYPE} "
                  "(table holds full resources again)", flush=True)

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
    ap.add_argument("--workers", type=int, default=1, metavar="N",
                     help="(--slim only) run the load in N concurrent child "
                          "processes over disjoint file shards. The transform is "
                          "CPU-bound at ~6k rows/s on one core; N children lift "
                          "that until the source disk's read ceiling binds. The "
                          "parent still does all DDL and rebuilds indexes ONCE "
                          "after every child finishes. Default 1 (in-process).")
    ap.add_argument("--load-only", action="store_true",
                     help="(--slim, internal) child mode: load this --shard's "
                          "not-yet-done files and exit. Skips all DDL, VACUUM, "
                          "index rebuild and provenance -- the parent owns those. "
                          "Spawned by --workers; rarely run by hand.")
    ap.add_argument("--shard", metavar="I/K",
                     help="(--slim --load-only) load only files[I::K] of the "
                          "sorted source list. Disjoint and restart-stable.")
    ap.add_argument("--expect-files", type=int, metavar="N",
                     help="(--slim --load-only) abort unless exactly N source "
                          "files are visible -- proves the child shards against "
                          "the same list the parent did. Set by --workers.")
    args = ap.parse_args()
    if args.rebuild_only and args.skip_truncate:
        ap.error("--rebuild-only already skips truncate and load; "
                 "do not combine it with --skip-truncate")
    if args.resume and not args.slim:
        ap.error("--resume is only meaningful with --slim")
    if args.slim and (args.skip_truncate or args.rebuild_only):
        ap.error("--slim is not compatible with --skip-truncate/--rebuild-only; "
                 "use --slim --resume to recover a partial --slim run instead")
    if (args.load_only or args.shard or args.expect_files is not None
            or args.workers != 1) and not args.slim:
        ap.error("--workers/--load-only/--shard/--expect-files are only "
                 "meaningful with --slim")
    if args.expect_files is not None and not args.load_only:
        ap.error("--expect-files is only meaningful with --load-only")
    if args.shard and not args.load_only:
        ap.error("--shard is only meaningful with --load-only (the parent shards "
                 "for you: use --workers N)")
    if args.load_only and args.workers != 1:
        ap.error("--load-only is a child process; it cannot spawn its own workers")
    if args.load_only and args.dry_run:
        ap.error("--dry-run is a parent-level plan; it is meaningless for a child")
    if args.workers < 1:
        ap.error("--workers must be >= 1")
    if args.shard:
        parse_shard(args.shard)  # fail fast on a malformed spec

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

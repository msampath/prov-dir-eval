#!/usr/bin/env python
"""Run the HL7 FHIR validator over sampled instances and record the findings.

Uses the official validator (``tools/validator_cli.jar``) inside a JRE container,
because this box has Docker but no JVM. Each cell's sample is validated against
the Plan-Net profile the cell is *supposed* to meet, forced with ``-profile``
rather than taken from the instance's own ``meta.profile``: 2 of 60 payers
declare no profile at all and 7 declare bare base-FHIR StructureDefinitions, so
trusting the declaration would quietly excuse exactly the payers most likely to
be non-conformant. What each payer *claims* is measured separately.

Cost shape: the validator spends ~55s loading the IG context and ~0.14s per
instance, so work is batched into large chunks (one container per chunk, several
containers at once) to amortize that load across thousands of instances instead
of paying it per cell.

    python scripts/validate_conformance.py --sample-dir F:/fhir_val --workers 12
    python scripts/validate_conformance.py --sample-dir ... --ig-version 1.2.0
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.etl.loader import pg_connection  # noqa: E402

sys.path.insert(0, str(REPO / "scripts"))
from sample_for_validation import CELL_PROFILE  # noqa: E402

IMAGE = "eclipse-temurin:21-jre"
JAR_HOST = REPO / "tools" / "validator_cli.jar"
IG = "hl7.fhir.us.davinci-pdex-plan-net"
# THO (terminology.hl7.org) supplies CodeSystem content r4.core only carries
# under version-suffixed canonicals -- notably v2-0360 (qualification codes),
# whose absence produced ~11k UNKNOWN_CODESYSTEM_VERSION noise warnings in the
# first full run.
THO_IG = "hl7.terminology.r4"
FHIR_VERSION = "4.0.1"
DEFAULT_CHUNK = 4000

# --- per-cell chunk sizing -------------------------------------------------
# One global --chunk is wrong once a run spans cells whose per-instance cost
# differs by 8x. At the measured 12.8 s/instance an 8,000-file PractitionerRole
# chunk needs 28.4 HOURS -- longer than any sane --chunk-timeout, so every one
# of them would be killed and discarded -- while an Organization chunk of the
# same size finishes in 5.7 h with room to spare.
#
# So: pick a target WALL TIME per chunk and let each cell's chunk size fall out
# of its measured rate, then derive the timeout from the same rate. Chunks are
# then interchangeable units of work, which is also what makes the tail behave
# (see chunk_cost / cost-descending dispatch below).
#
# Rates are seconds per instance, measured on azblue at 23 cores. They
# reproduce the plan's CPU-second budget exactly: 1,862,241 Organizations at
# 2.58 + 96,027 PractitionerRoles at 12.8 + the remaining 1,175,934 rows at
# 1.66 = 7.99M CPU-seconds = the "no levers" 96 h at 23 cores; swapping
# Organization to its post-CARIN-BB-strip 1.29 gives 4.49M = 54 h.
#
# Deliberately the PRE-lever rates. Over-estimating a rate costs a few extra
# chunk starts (~55 s of IG load each, ~0.3% of the run); under-estimating it
# builds chunks that cannot finish inside their own timeout, which is the exact
# failure this table exists to prevent. Override per cell with --cell-rate.
DEFAULT_RATE_S = 1.66
CELL_RATE_S: dict[str, float] = {
    "Organization": 2.58,
    "Network": 2.58,
    "PractitionerRole": 12.8,
}
# 2 h per chunk. Long enough that the ~55 s IG load is 0.8% overhead, short
# enough that a reboot inside the 45-96 h window loses at most 2 h x workers of
# work, and short enough that the last wave is a 2 h tail rather than a 28 h one.
TARGET_CHUNK_SECONDS = 7200
MIN_CHUNK = 25
MAX_CHUNK = 8000
# Timeout = fixed startup allowance + margin x expected validation time. The
# margin is wide because the rate is an average over a cell whose instances
# vary a lot in size; the timeout is a hang detector, not a scheduler.
TIMEOUT_MARGIN = 3.0
CHUNK_LOAD_SECONDS = 1800
# Oversize isolation: run 2's Network__000 chunk hung the validator for 6h
# because 23 of its 1,331 files are 1-25MB "network" Organizations (bcbs_az
# publishes networks carrying up to 66k address entries EACH), and one chunk
# timeout then throws away the WHOLE cell. Files above OVERSIZE_BYTES go into
# their own small chunks so a timeout costs at most OVERSIZE_PER_CHUNK
# pathological files, never the 1,300 normal ones sharing their cell.
OVERSIZE_BYTES = 1_000_000
# One oversize file per chunk (was 2). Measured: 13 Network chunks of two
# bcbs_az "network" Organizations -- resources carrying up to 66,235 address
# entries each -- still blew a 1h timeout, so pairing them just doubled what
# each timeout threw away. One per chunk plus a separate, longer budget
# (--oversize-timeout) means a whale costs only itself.
OVERSIZE_PER_CHUNK = 1
DEFAULT_PKG_CACHE = "F:/fhir_cache"
FILE_EXT = "http://hl7.org/fhir/StructureDefinition/operationoutcome-file"
MSGID_EXT = "http://hl7.org/fhir/StructureDefinition/operationoutcome-message-id"

DDL = """
CREATE TABLE IF NOT EXISTS public.conformance_run (
    run_id       bigserial PRIMARY KEY,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    ig           text NOT NULL,
    ig_version   text NOT NULL,
    per_cell     integer,
    instances    bigint,
    notes        jsonb);
CREATE TABLE IF NOT EXISTS public.conformance_issue (
    run_id       bigint NOT NULL REFERENCES public.conformance_run(run_id),
    payer_id     text NOT NULL,
    cell         text NOT NULL,
    instance     text NOT NULL,
    severity     text NOT NULL,
    code         text,
    message_id   text,
    expression   text,
    details      text);
CREATE INDEX IF NOT EXISTS ix_conformance_issue_run
    ON public.conformance_issue (run_id, payer_id, cell);
-- Which instances were validated, so a payer with zero issues is distinguishable
-- from a payer that was never sampled. Without this a clean cell and a missing
-- cell look identical in the aggregate.
CREATE TABLE IF NOT EXISTS public.conformance_instance (
    run_id       bigint NOT NULL REFERENCES public.conformance_run(run_id),
    payer_id     text NOT NULL,
    cell         text NOT NULL,
    instance     text NOT NULL,
    errors       integer NOT NULL DEFAULT 0,
    warnings     integer NOT NULL DEFAULT 0,
    resource_id  text,
    PRIMARY KEY (run_id, payer_id, cell, instance));
-- The PK is a FILENAME. Without the FHIR resource id alongside it, a finding
-- cannot be traced back to the row it came from -- and the filename only means
-- anything at all because the sampler now orders by id. Self-healing ALTER so
-- an existing database picks the column up without a migration run first (the
-- migration under migrations/versions/ owns it for a fresh build).
ALTER TABLE public.conformance_instance
    ADD COLUMN IF NOT EXISTS resource_id text;
"""


# Chunk-dir names holding an oversize instance, populated by build_chunks and
# read by run_chunk to pick the longer timeout. A module-level set rather than
# a field on the chunk tuple so the (cell, dir) shape stays what every caller
# and test already expects.
OVERSIZE_CHUNKS: set[str] = set()

# cell -> hard timeout in seconds for one of its chunks, derived by chunk_plan()
# and read by run_chunk(). Same module-level-dict shape as OVERSIZE_CHUNKS, for
# the same reason: the (cell, dir) chunk tuple is what every caller and test
# already expects, and widening it would churn all of them.
CHUNK_TIMEOUTS: dict[str, int] = {}


class UnreadableOutput(Exception):
    """A validator output file exists but does not parse.

    This is a chunk FAILURE, not a warning. A chunk killed mid-write leaves a
    non-empty but truncated JSON document; swallowing the JSONDecodeError makes
    it contribute zero rows with nothing recorded as failed, --reuse-outputs
    then accepts it forever, and the run reports success while short by a whole
    chunk. That is the mechanism behind the 1,463,451-of-1,862,183-recorded-ok
    incident.
    """


def parse_kv_overrides(raw: str | None, cast) -> dict[str, float | int]:
    """Parse ``Cell=value,Cell=value`` into a dict. Empty/None -> {}."""
    out: dict[str, float | int] = {}
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise SystemExit(f"expected Cell=value, got {part!r}")
        k, v = part.split("=", 1)
        out[k.strip()] = cast(v.strip())
    return out


def chunk_plan(cell_names, args) -> dict[str, tuple[int, int]]:
    """cell -> (chunk size, per-chunk timeout in seconds).

    Chunk size is whatever fits TARGET_CHUNK_SECONDS at the cell's measured
    rate; the timeout is derived from the same rate so a chunk can never be
    built larger than the budget that will kill it. --chunk / --chunk-timeout
    still force a single global value for every cell (that is what they mean),
    and --cell-chunk / --cell-rate override one cell at a time.
    """
    rates = dict(CELL_RATE_S)
    rates.update(parse_kv_overrides(getattr(args, "cell_rate", None), float))
    sizes = parse_kv_overrides(getattr(args, "cell_chunk", None), int)
    plan: dict[str, tuple[int, int]] = {}
    for cell in sorted(set(cell_names)):
        rate = float(rates.get(cell, DEFAULT_RATE_S))
        if rate <= 0:
            raise SystemExit(f"--cell-rate for {cell} must be > 0")
        if cell in sizes:
            size = sizes[cell]          # explicit: honoured as written
        elif args.chunk:
            size = args.chunk           # explicit: honoured as written
        else:
            # Derived, so clamp: a rate that is wrong by an order of magnitude
            # must not produce a 3-file or a 400,000-file chunk.
            size = max(MIN_CHUNK,
                       min(MAX_CHUNK, int(round(TARGET_CHUNK_SECONDS / rate))))
        if size < 1:
            raise SystemExit(f"chunk size for {cell} must be >= 1, got {size}")
        timeout = (args.chunk_timeout if args.chunk_timeout
                   else int(CHUNK_LOAD_SECONDS + TIMEOUT_MARGIN * rate * size))
        plan[cell] = (size, timeout)
    return plan


def chunk_cost(cell: str, n_files: int, rates: dict[str, float],
               oversize_cost: float, chunk_name: str = "") -> float:
    """Expected seconds for one chunk, used only to order dispatch.

    Alphabetical dispatch put every Organization chunk ahead of every
    PractitionerRole chunk, so the run ended with workers grinding the most
    expensive cell one chunk at a time with no parallelism left to hide it.
    Longest-processing-time-first is the standard fix and it is worth real
    hours here. Oversize chunks (single multi-MB whales, historically 4-6 h
    each) get the oversize budget as their nominal cost so they start first.
    """
    if chunk_name and chunk_name in OVERSIZE_CHUNKS:
        return oversize_cost
    return rates.get(cell, DEFAULT_RATE_S) * n_files


def worker_cache(pkg_cache: Path, worker: int) -> Path:
    """Per-worker copy of the warmed FHIR package cache.

    A SHARED /root/.fhir mount cannot survive 14 concurrent validators: the
    cache manager does housekeeping writes (it deletes and re-extracts package
    directories) with no cross-process locking, so one container's cleanup
    walks a tree another is reading. Measured: 17 of 46 chunks died together
    in `FileUtils.forceDelete`/`walkFileTree` after warm-up had already proven
    the cache complete -- warming fixes the download storm, not the race.

    Each worker therefore validates against its own copy, seeded once from the
    warmed original. Costs disk and a one-time copy; buys the only thing that
    actually makes concurrent validators safe.
    """
    return pkg_cache.parent / f"{pkg_cache.name}_w{worker}"


def _count_files(root: Path) -> int:
    return sum(1 for p in root.rglob("*") if p.is_file())


def clone_sentinel(dest: Path) -> Path:
    """Completeness marker for a worker cache clone.

    Kept OUTSIDE the clone, as a sibling file, so it never lands inside the
    tree that gets mounted at /root/.fhir and never perturbs the file count it
    records.
    """
    return dest.parent / f"{dest.name}.complete"


def seed_worker_caches(pkg_cache: Path, workers: int) -> list[Path]:
    """Clone the warmed cache once per worker. Returns the per-worker paths.

    Existence of the directory is NOT proof of a complete clone. 14
    `F:/fhir_cache_w*` clones survive from a prior era, and a clone
    interrupted by a reboot or a full disk leaves a directory that looks
    identical to a good one; validating against a partial IG cache produces
    resolution errors that read as payer findings. So a clone counts as usable
    only when a sentinel records the source file count and that count still
    matches -- the same paranoia the shared cache's own warm-up sentinel
    already applies, for the same reason.

    An UNMARKED clone whose file count already matches is stamped rather than
    re-copied: the 14 legacy clones are probably fine, and re-copying 3.4 GB
    fourteen times to prove it is not free.
    """
    src_n = _count_files(pkg_cache)
    out = []
    for w in range(workers):
        dest = worker_cache(pkg_cache, w)
        sent = clone_sentinel(dest)
        if dest.exists():
            recorded = None
            if sent.exists():
                try:
                    recorded = int(sent.read_text(encoding="utf-8").split()[0])
                except (OSError, ValueError, IndexError):
                    recorded = None
            actual = recorded if recorded is not None else _count_files(dest)
            if actual == src_n:
                sent.write_text(str(src_n), encoding="utf-8")
                out.append(dest)
                continue
            print(f"  re-cloning {dest.name}: holds {actual:,} file(s), source "
                  f"has {src_n:,} -- an incomplete cache produces IG-resolution "
                  "errors that look like payer findings", flush=True)
            sent.unlink(missing_ok=True)
            shutil.rmtree(dest)
        shutil.copytree(pkg_cache, dest)
        sent.write_text(str(src_n), encoding="utf-8")
        out.append(dest)
    return out


def restore_from_backup(sample_dir: Path, backup_dir: Path) -> int:
    """COPY sample files back from a backup taken of the old `_batches` tree.

    Exists because the pre-fix revision of this script deleted `_batches` --
    the only copy of the sample -- at end of run; a manual robocopy of it to
    `F:\\fhir_val_keep` is the sole surviving copy. The backup keeps the
    chunk-dir layout (`<Cell>__NNN/`), so files route to cell dirs by the same
    name-prefix rule restore_cells uses. COPIES (never moves): the backup must
    stay intact for anything else that reads it (element_coverage's sample
    fallback does).
    """
    if not backup_dir.is_dir():
        return 0
    n = 0
    for chunk_dir in backup_dir.iterdir():
        if not chunk_dir.is_dir():
            continue
        cell = chunk_dir.name.split("__", 1)[0]
        dest = sample_dir / cell
        dest.mkdir(parents=True, exist_ok=True)
        for f in chunk_dir.glob("*.json"):
            target = dest / f.name
            if not target.exists():
                shutil.copyfile(f, target)
                n += 1
    return n


def restore_cells(sample_dir: Path) -> int:
    """Move any files a previous run left in `_batches/` back to their cell
    directories. Returns the number of files restored.

    build_chunks MOVES the sample into `_batches/` (rename, not copy), so
    `_batches` can be the ONLY copy of the sample. An early revision of this
    script deleted it wholesale at the end of a run -- which destroys the
    sample and forces a full (and not exactly reproducible: TABLESAMPLE) re-
    sampling. Restoring instead makes chunking a round-trip: the sample always
    ends a run back in its canonical layout.
    """
    batch_root = sample_dir / "_batches"
    if not batch_root.exists():
        return 0
    n = 0
    for chunk_dir in batch_root.iterdir():
        if not chunk_dir.is_dir():
            continue
        cell = chunk_dir.name.split("__", 1)[0]
        dest = sample_dir / cell
        dest.mkdir(exist_ok=True)
        for f in chunk_dir.glob("*.json"):
            # replace(), not rename(): on Windows rename() raises if the
            # destination exists, and the destination CAN exist -- an
            # interrupted run followed by a backup restore puts the same file
            # in both the cell dir and _batches. Content is identical (same
            # sampled instance by name), so overwriting is always safe.
            f.replace(dest / f.name)
            n += 1
    shutil.rmtree(batch_root)
    return n


_SAMPLE_LOCK = None  # held open for the process lifetime; OS frees it on exit


def lock_sample_dir(sample_dir: Path) -> None:
    """Take an exclusive OS lock on the sample dir, or refuse to start.

    Two runs cannot share a sample dir. build_chunks() moves every file into
    `_batches/` and restore_cells() moves them all back, so a second run pulls
    files out from under the first one's containers while they are reading.

    This is not hypothetical. The whale retry was created with a TimeTrigger
    and started nine minutes before the run it was meant to follow had
    finished. It re-chunked the sample under the running validator and then
    crashed that run's cleanup with a PermissionError, leaving 10,986 files
    sitting in both a cell dir and a chunk dir.

    An OS byte-range lock, not a PID file: the kernel releases it when the
    process dies, so a crashed run leaves nothing stale to reap. Checking
    liveness by hand would be worse than useless here, since os.kill(pid, 0)
    on Windows terminates the target instead of probing it.
    """
    global _SAMPLE_LOCK
    # r+b, not a+b: append mode ignores seek() on writes, so the holder line
    # would land at offset 0 and overlap the locked byte.
    lock_path = sample_dir / ".run.lock"
    lock_path.touch(exist_ok=True)
    f = open(lock_path, "r+b")
    f.seek(0)
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.seek(1)
        who = f.read(120).decode("utf-8", "replace").strip()
        f.close()
        raise SystemExit(
            f"FATAL: another validate_conformance run already holds {sample_dir}"
            + (f"\n  holder: {who}" if who else "")
            + "\nTwo runs cannot share a sample dir: each re-chunks the whole "
              "sample and would move files out from under the other's "
              "containers. Wait for it to finish, or stop it first.")
    f.seek(1)
    f.write(f"pid {os.getpid()} started "
            f"{datetime.now().isoformat(timespec='seconds')}".ljust(120).encode())
    f.flush()
    _SAMPLE_LOCK = f          # closing (or exiting) releases the lock


def build_chunks(sample_dir: Path,
                 chunk_size: int | dict[str, int]) -> list[tuple[str, Path]]:
    """Move each cell's files into per-chunk directories. Returns [(cell, dir)].

    The validator takes a directory, not a file list, so chunking has to be
    physical. Files are moved (same volume, so it is a rename) rather than
    copied to avoid a second full copy of the sample on disk.

    `chunk_size` is either one size for every cell (the old behaviour, and what
    an explicit --chunk still means) or a per-cell mapping from chunk_plan().
    """
    restored = restore_cells(sample_dir)
    if restored:
        print(f"restored {restored:,} file(s) from a previous run's _batches",
              flush=True)
    batch_root = sample_dir / "_batches"
    batch_root.mkdir()
    chunks: list[tuple[str, Path]] = []
    for cell_dir in sorted(p for p in sample_dir.iterdir()
                           if p.is_dir() and not p.name.startswith("_")):
        cell = cell_dir.name
        per_cell = (chunk_size.get(cell, DEFAULT_CHUNK)
                    if isinstance(chunk_size, dict) else chunk_size)
        files = sorted(cell_dir.glob("*.json"))
        normal = [f for f in files if f.stat().st_size <= OVERSIZE_BYTES]
        oversize = [f for f in files if f.stat().st_size > OVERSIZE_BYTES]
        idx = 0
        # Oversize files ride in their own small chunks (see OVERSIZE_BYTES
        # above) so a --chunk-timeout kill loses only them, not their cell.
        for group, per, big in ((normal, per_cell, False),
                                (oversize, OVERSIZE_PER_CHUNK, True)):
            for i in range(0, len(group), per):
                dest = batch_root / f"{cell}__{idx:03d}"
                idx += 1
                dest.mkdir()
                for f in group[i:i + per]:
                    f.rename(dest / f.name)
                chunks.append((cell, dest))
                if big:
                    OVERSIZE_CHUNKS.add(dest.name)
    return chunks


def output_looks_complete(out_file: Path) -> bool:
    """Cheap structural check that a validator output was not truncated.

    `exists() and size > 0` is not enough: a chunk killed mid-write leaves a
    non-empty file, --reuse-outputs accepts it, and it contributes zero rows.
    A complete document -- Bundle or bare OperationOutcome -- always ends in
    `}`, and a JSON write cut off at an arbitrary byte essentially never does.
    One seek+read, so it stays affordable across ~1,100 chunks at resume time;
    the authoritative check is the full parse at ingest, which quarantines
    anything that gets past this one.
    """
    try:
        size = out_file.stat().st_size
        if size <= 0:
            return False
        with open(out_file, "rb") as fh:
            fh.seek(max(0, size - 4096))
            return fh.read().rstrip().endswith(b"}")
    except OSError:
        return False


def pick_warmup_chunk(pending: list[tuple[str, Path]]) -> int:
    """Index into a cost-DESCENDING `pending` of the chunk to warm up on.

    The warm-up runs alone and synchronously before any fan-out, and the run
    dies if it fails -- so it is the only gate a resolved heap ever passes
    through. It used to be `pending.pop()`: the LAST element of a
    cost-descending list, i.e. the CHEAPEST chunk in the run. An --xmx that
    fits a 25-file tail chunk but not a 2,790-file Organization one clears that
    gate and then kills 44 workers at hour 3.

    The most expensive ORDINARY chunk is the representative case: it is the
    heap and wall-time worst case the fan-out actually meets, and it costs at
    most the TARGET_CHUNK_SECONDS every chunk is sized for. Oversize chunks are
    skipped deliberately -- they run under --oversize-xmx with their own longer
    budget, so one proves nothing about the ordinary tier while holding the
    whole run behind a multi-hour whale.
    """
    return next((i for i, (_cell, d) in enumerate(pending)
                 if d.name not in OVERSIZE_CHUNKS), 0)


def read_sample_provenance(sample_dir: Path) -> dict:
    """What the sampler did to the data, read back so it can travel with run_id.

    ``manifest.json`` and ``_carin_bb_inventory.json`` are written by
    sample_for_validation.py and were, until now, read by NOTHING -- a
    repo-wide grep found only the writer. The consequence is that a 1,000-per-
    cell, 61-payer, unstripped sample and a full-population, single-payer
    sample whose ``meta.profile`` was rewritten before validation are
    indistinguishable in the database: same ig, same ig_version, ``per_cell``
    NULL in both. Every published conformance number then depends on which run
    a report happened to pick, with nothing recorded anywhere to reveal that
    the two are not comparable.

    A missing manifest is FATAL. A run whose provenance cannot be recorded is
    a run whose numbers cannot be interpreted later, and this is the cheapest
    possible moment to say so.

    ``per_cell`` is INFERRED, because the sampler does not record its own
    ``--per-cell`` argument: a cell is "capped" when it wrote fewer instances
    than the table's estimated rows, and if every capped cell wrote the SAME
    number that number is the cap. Anything else (no capped cells, or capped
    cells of differing sizes, which is what a full-population run against
    imperfect row estimates looks like) is reported as 0 = "no uniform cap".
    0 and NULL are different states on purpose: NULL is the nine legacy rows
    that recorded nothing at all.
    """
    manifest_path = sample_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(
            f"FATAL: no manifest.json under {sample_dir}.\n"
            "  The sampler writes it per cell; without it the run cannot "
            "record which payers it covers, whether the sample is capped, or "
            "whether CARIN BB profiles were stripped before validation -- and "
            "a run whose provenance is unrecorded is indistinguishable from "
            "every other run in the database.\n"
            "  Re-run scripts/sample_for_validation.py against this sample "
            "dir, or point --sample-dir at the directory the sampler wrote.")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"FATAL: {manifest_path} is unreadable: {exc}") from exc
    if not isinstance(manifest, list) or not manifest:
        raise SystemExit(f"FATAL: {manifest_path} records no sampled cells.")

    payers = sorted({m.get("payer") for m in manifest if m.get("payer")})
    sampled_total = sum(int(m.get("sampled") or 0) for m in manifest)
    capped = {int(m["sampled"]) for m in manifest
              if m.get("sampled") and m.get("live_rows")
              and int(m["sampled"]) < int(m["live_rows"])}
    per_cell = capped.pop() if len(capped) == 1 else 0

    inventory: dict = {}
    inv_path = sample_dir / "_carin_bb_inventory.json"
    if inv_path.exists():
        try:
            loaded = json.loads(inv_path.read_text(encoding="utf-8"))
            inventory = loaded if isinstance(loaded, dict) else {}
        except (OSError, ValueError):
            inventory = {}
    instances_stripped = 0
    canonicals: dict[str, int] = {}
    for bucket in inventory.values():
        if not isinstance(bucket, dict):
            continue
        instances_stripped += int(bucket.get("_instances") or 0)
        for canonical, n in bucket.items():
            if canonical != "_instances":
                canonicals[canonical] = canonicals.get(canonical, 0) + int(n or 0)

    return {
        "payers": payers,
        "payer_count": len(payers),
        "cells": len(manifest),
        "per_cell": per_cell,
        "sampled": sampled_total,
        # any(), not all(): one stripped cell is enough to make the run's
        # numbers non-comparable with an unstripped run's.
        "carin_bb_stripped": any(bool(m.get("carin_bb_stripped"))
                                 for m in manifest),
        "instances_stripped": instances_stripped,
        "canonicals_removed": canonicals,
        "methods": sorted({m.get("method") for m in manifest if m.get("method")}),
    }


def run_chunk(cell: str, chunk_dir: Path, sample_dir: Path, args,
              out_dir: Path, worker: int = 0,
              shared_cache: bool = False) -> tuple[str, Path, int, str]:
    out_file = out_dir / f"{chunk_dir.name}.json"
    if args.reuse_outputs and output_looks_complete(out_file):
        # Resume: this chunk already produced output in an earlier attempt.
        # Chunk names are deterministic (sorted cell dirs, fixed chunk size),
        # so `<cell>__NNN.json` identifies the same file set across runs. The
        # DB load at the end reads every output present, so reusing here yields
        # exactly the same rows as re-validating -- for ~10 min of GPU-less
        # JVM time per chunk saved.
        return cell, out_file, 0, "(reused existing output)"
    rel_chunk = f"/data/_batches/{chunk_dir.name}"
    rel_out = f"/data/_out/{out_file.name}"
    mounts = [
        "-v", f"{REPO.as_posix()}/tools:/tools",
        "-v", f"{sample_dir.as_posix()}:/data",
    ]
    if args.pkg_cache:
        # The validator (running as root in the container) keeps its FHIR
        # package cache under /root/.fhir. Without this mount every container
        # re-downloads the IGs (~55s each). The mount is the WORKER'S OWN copy,
        # never the shared original -- see worker_cache() for why sharing it
        # across concurrent containers corrupts them.
        # shared_cache is the WARM-UP's mount. Without it the warm-up chunk
        # ran with worker=0 and --per-worker-cache on (the default), so it
        # populated `<cache>_w0` -- while the "cache is warm" sentinel was
        # written to the SHARED tree and seed_worker_caches then cloned that
        # shared tree, untouched by the warm-up, 44 times. Every worker started
        # against a cache missing the validator's transitive packages and
        # fetched them concurrently: precisely the download storm the warm-up
        # exists to prevent, and the one seed_worker_caches' own comment says
        # "produces IG-resolution errors that look like payer findings".
        cache = Path(args.pkg_cache)
        if not shared_cache and (worker > 0 or args.per_worker_cache):
            cache = worker_cache(Path(args.pkg_cache), worker)
        mounts += ["-v", f"{cache.as_posix()}:/root/.fhir"]
    if args.tx_cache:
        mounts += ["-v", f"{Path(args.tx_cache).as_posix()}:/txcache"]
    # -Xmx bounds the JVM HEAP; it does not bound container RSS (metaspace,
    # code cache, direct buffers and the JVM's own footprint sit outside it).
    # --docker-memory is the only thing that caps what a chunk can take from
    # the Docker VM, which matters once 44 workers each claim 8 GB of heap
    # against a 101 GB VM.
    #
    # Two tiers, for the same reason --oversize-timeout exists: a single 25 MB
    # Organization carrying 66,000 addresses needs a heap that 44 concurrent
    # containers cannot each be given. Sizing the ordinary chunks to fit the VM
    # and letting the handful of whales have the big heap is the only way
    # --xmx is settable at all.
    big = chunk_dir.name in OVERSIZE_CHUNKS
    xmx = (args.oversize_xmx or args.xmx) if big else args.xmx
    dmem = (args.oversize_docker_memory or args.docker_memory) if big else args.docker_memory
    if dmem:
        mounts = ["--memory", dmem] + mounts
    # One CPU per container. Without this the cap is only nominal: each JVM
    # sizes its GC/compiler thread pools from the 72 CPUs the Docker VM
    # exposes, and 44 "single-threaded" validators were measured bursting to
    # ~47.5 cores in aggregate -- past the 46-thread ceiling this run is
    # allowed. --cpus also fixes JVM ergonomics (availableProcessors) so the
    # pools are sized for what the container actually has.
    mounts = ["--cpus", str(getattr(args, "docker_cpus", 1.0))] + mounts
    cmd = [
        "docker", "run", "--rm", *mounts,
        IMAGE, "java", f"-Xmx{xmx}", "-jar", "/tools/validator_cli.jar",
        rel_chunk,
        "-version", FHIR_VERSION,
        "-ig", f"{IG}#{args.ig_version}",
        "-profile", CELL_PROFILE[cell],
        # With -tx n/a (the default) there is no terminology SERVER, but the
        # validator still expands and enforces every value set whose content
        # ships in the -ig packages -- which, for Plan-Net's own bindings, is
        # all of them (853 NUCC codes enumerated inline). THO below adds the
        # CodeSystems r4.core lacks (v2-0360). A real -tx adds only display
        # validation and deprecated-code detection.
        "-tx", args.tx,
        "-output", rel_out,
    ]
    if args.tho_version:
        cmd[cmd.index("-profile"):cmd.index("-profile")] = [
            "-ig", f"{THO_IG}#{args.tho_version}"]
    if args.tx_cache:
        cmd += ["-txCache", "/txcache"]
    # A hard per-chunk timeout: the first full run's Network chunk hung for 6
    # hours inside the validator (normal chunks finish in ~10 min), silently
    # stalling the whole run. On expiry the container must ALSO be killed --
    # subprocess timeout only kills the docker CLIENT; the container would keep
    # running detached. --cidfile gives us its id without racing `docker ps`.
    # NOTE: --cidfile is resolved by the docker CLIENT on the host -- it must
    # be the HOST path, never a container-mount path (that variant fails with
    # exit 125 before the container even starts).
    cidfile = out_dir / f"{chunk_dir.name}.cid"
    cidfile.unlink(missing_ok=True)  # docker refuses to overwrite an existing cidfile
    cmd[cmd.index("run") + 1:cmd.index("run") + 1] = ["--cidfile", str(cidfile)]
    # Oversize chunks get their own, longer budget: a single 25MB Organization
    # with tens of thousands of addresses legitimately takes far longer than a
    # 4,000-instance chunk of ordinary resources, and holding both to one
    # timeout means either killing valid work or letting a hang run for hours.
    budget = (args.oversize_timeout if chunk_dir.name in OVERSIZE_CHUNKS
              else CHUNK_TIMEOUTS.get(cell, args.chunk_timeout or 3600))
    try:
        p = subprocess.run(cmd, capture_output=True, text=True,
                           encoding="utf-8", errors="replace",
                           timeout=budget)
        rc, tail = p.returncode, (p.stderr or p.stdout or "")[-400:]
    except subprocess.TimeoutExpired:
        rc, tail = -1, f"chunk timed out after {budget}s; container killed"
        try:
            cid = cidfile.read_text(encoding="utf-8").strip()
            if cid:
                subprocess.run(["docker", "kill", cid], capture_output=True)
        except OSError:
            pass
    finally:
        cidfile.unlink(missing_ok=True)
    return cell, out_file, rc, tail


def load_id_map(sample_dir: Path, cell: str) -> dict[str, str]:
    """instance filename -> FHIR resource id, from the sampler's sidecars.

    Written by sample_for_validation as `_ids/<cell>__<payer>.tsv`, under a
    `_`-prefixed directory so build_chunks never mistakes it for sample data.
    Loaded one cell at a time (the ingest loop is sorted by cell) because
    Organization alone is 1.86M entries.
    """
    out: dict[str, str] = {}
    ids_dir = sample_dir / "_ids"
    if not ids_dir.is_dir():
        return out
    for f in sorted(ids_dir.glob(f"{cell}__*.tsv")):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                name, _, rid = line.rstrip("\n").partition("\t")
                if name and rid:
                    out[name] = rid
    return out


def parse_output(out_file: Path, cell: str,
                 ids: dict[str, str] | None = None) -> tuple[list[tuple], list[tuple]]:
    """(instance rows, issue rows) from one validator output document.

    A multi-file chunk yields a Bundle of OperationOutcomes; a SINGLE-file
    chunk yields one bare OperationOutcome (observed live on validator
    6.10.1). Both must parse: oversize isolation makes 1-file chunks routine,
    and a bare outcome silently parsed as an empty Bundle would drop that
    instance from the run with no failure signal at all.

    Raises UnreadableOutput on a truncated or unreadable document. It used to
    print a WARN and return ([], []), which is how a chunk killed mid-write
    became zero rows, zero failures and a run that reported success while
    short by up to a whole chunk. The caller routes this into exactly the same
    failed/unvalidated accounting as a missing output file.
    """
    try:
        doc = json.loads(out_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UnreadableOutput(f"{out_file.name}: {exc}") from exc
    ids = ids or {}
    if doc.get("resourceType") == "OperationOutcome":
        outcomes = [doc]
    else:
        outcomes = [entry.get("resource") or {} for entry in doc.get("entry", []) or []]
    instances, issues = [], []
    for oo in outcomes:
        src = None
        for ext in oo.get("extension", []) or []:
            if ext.get("url") == FILE_EXT:
                src = ext.get("valueString")
        name = Path(src).name if src else "?"
        payer = name.split("__", 1)[0] if "__" in name else "?"
        nerr = nwarn = 0
        for iss in oo.get("issue", []) or []:
            sev = iss.get("severity")
            if sev in ("error", "fatal"):
                nerr += 1
            elif sev == "warning":
                nwarn += 1
            else:
                continue  # information-level notes are noise at this scale
            msgid = None
            for ext in iss.get("extension", []) or []:
                if ext.get("url") == MSGID_EXT:
                    msgid = ext.get("valueCode")
            issues.append((payer, cell, name, sev, iss.get("code"), msgid,
                           (iss.get("expression") or [None])[0],
                           ((iss.get("details") or {}).get("text") or "")[:1000]))
        instances.append((payer, cell, name, nerr, nwarn, ids.get(name)))
    return instances, issues


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-dir", required=True)
    ap.add_argument("--ig-version", default="1.1.0")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--chunk", type=int, default=None,
                     help="force ONE chunk size for every cell. Default: none "
                          "-- each cell is sized so a chunk lands near "
                          f"{TARGET_CHUNK_SECONDS}s at its measured rate (see "
                          "CELL_RATE_S). A single global size cannot work "
                          "across cells whose per-instance cost differs 8x.")
    ap.add_argument("--cell-chunk", metavar="Cell=N,...",
                     help="override the derived chunk size for named cells")
    ap.add_argument("--docker-cpus", type=float, default=1.0, metavar="N",
                     help="--cpus per validator container (default: %(default)s); "
                          "workers x this must stay inside the machine's core "
                          "allowance")
    ap.add_argument("--cell-rate", metavar="Cell=SEC,...",
                     help="override the seconds-per-instance rate used to "
                          "derive a cell's chunk size and timeout")
    ap.add_argument("--xmx", default="8g", metavar="SIZE",
                     help="JVM max heap per container (-Xmx). NOTE: this "
                          "bounds heap, not container RSS -- use "
                          "--docker-memory for that. Default: %(default)s")
    ap.add_argument("--docker-memory", default=None, metavar="SIZE",
                     help="hard container memory cap passed to `docker run "
                          "--memory` (e.g. 10g). Default: unset (a container "
                          "may take as much of the Docker VM as it likes, "
                          "which at 44 workers x 8g of heap is how the VM "
                          "gets oversubscribed).")
    ap.add_argument("--oversize-xmx", default=None, metavar="SIZE",
                     help="heap for a chunk holding an oversize (>1MB) "
                          "instance. Separate from --xmx for the same reason "
                          "--oversize-timeout is separate: one 66k-address "
                          "Organization needs a heap that 44 concurrent "
                          "containers cannot each be given. Default: --xmx")
    ap.add_argument("--memory-budget", type=float, default=92.0, metavar="GB",
                     help="total container memory limits allowed in flight at "
                          "once. The Docker VM has ~101 GB; the default leaves "
                          "headroom for the daemon. Oversize chunks acquire "
                          "--oversize-docker-memory from this, ordinary chunks "
                          "--docker-memory. Default: %(default)s")
    ap.add_argument("--oversize-docker-memory", default=None, metavar="SIZE",
                     help="container memory cap for an oversize chunk. "
                          "Default: --docker-memory")
    ap.add_argument("--keep-batches", action="store_true",
                     help="(obsolete; batches are now always restored to cell "
                          "dirs at the end of a run, never deleted)")
    ap.add_argument("--tho-version", default="6.5.0", metavar="V",
                     help=f"also load {THO_IG}#V (fixes v2-0360 resolution). "
                          "Pass '' to skip. Default: %(default)s")
    ap.add_argument("--pkg-cache", default=DEFAULT_PKG_CACHE,
                     help="host dir mounted at /root/.fhir so containers share "
                          "one FHIR package cache instead of re-downloading the "
                          "IGs (~55s each). Pass '' to disable. Default: %(default)s")
    ap.add_argument("--tx", default="n/a",
                     help="terminology server URL, or n/a (default). Local "
                          "IG-based binding checks run either way; a real "
                          "server adds display validation + deprecated codes.")
    ap.add_argument("--tx-cache", default=None,
                     help="host dir mounted+passed as -txCache so terminology "
                          "server responses persist across containers/runs")
    ap.add_argument("--per-worker-cache", action="store_true", default=True,
                     help="give each worker its own copy of the warmed package "
                          "cache. ON by default: a shared cache is corrupted by "
                          "concurrent validators (17/46 chunks died in the "
                          "cache manager's own cleanup). --no-per-worker-cache "
                          "restores sharing, only safe at --workers 1.")
    ap.add_argument("--no-per-worker-cache", dest="per_worker_cache",
                     action="store_false")
    ap.add_argument("--oversize-timeout", type=int, default=14400, metavar="SEC",
                     help="timeout for a chunk holding an oversize (>1MB) "
                          "instance. Separate from --chunk-timeout because one "
                          "66k-address Organization legitimately takes hours "
                          "while an ordinary 4,000-instance chunk takes ~10min. "
                          "Default: %(default)s (4h)")
    ap.add_argument("--chunk-timeout", type=int, default=None, metavar="SEC",
                     help="force ONE timeout for every cell, in seconds (a "
                          "hung validator once stalled a run for 6h). The "
                          "chunk is recorded failed; the rest of the run "
                          "continues. Default: none -- derived per cell from "
                          f"its rate, as {CHUNK_LOAD_SECONDS}s + "
                          f"{TIMEOUT_MARGIN}x the expected validation time, so "
                          "a chunk can never be built larger than the budget "
                          "that will kill it.")
    ap.add_argument("--reuse-outputs", action="store_true",
                     help="resume: keep _out/ and skip any chunk whose output "
                          "file already exists. Chunk names are deterministic, "
                          "so a re-run after a crash re-validates only what is "
                          "missing instead of all ~46 chunks. Safe to leave on: "
                          "the end-of-run DB load reads every output present.")
    ap.add_argument("--restore-from", default="F:/fhir_val_keep",
                     help="backup of the sample (in _batches chunk-dir layout) "
                          "to COPY back if the sample dir turns up empty. "
                          "Default: %(default)s")
    args = ap.parse_args()

    sample_dir = Path(args.sample_dir).resolve()
    if not JAR_HOST.exists():
        raise SystemExit(f"validator jar not found at {JAR_HOST}")
    if not sample_dir.is_dir():
        raise SystemExit(f"sample dir not found: {sample_dir}")
    lock_sample_dir(sample_dir)

    # ALWAYS round-trip the sample back to its canonical cell layout, including
    # when the run raises, is Ctrl-C'd, or is killed by the scheduler. This
    # used to be an unconditional statement at the END of main, which meant any
    # interruption left the entire 3.13M-file sample stranded under
    # `_batches/<cell>__NNN/` -- and `_batches` is the ONLY copy, because
    # build_chunks moves rather than copies.
    try:
        return _run(args, sample_dir)
    finally:
        try:
            restored = restore_cells(sample_dir)
            print(f"restored {restored:,} sample file(s) to cell dirs", flush=True)
        except OSError as exc:
            print(f"WARN could not restore _batches to cell dirs: {exc}\n"
                  f"  The sample is still intact under {sample_dir / '_batches'}; "
                  "the next run's build_chunks will restore it.", flush=True)


def _run(args, sample_dir: Path) -> int:
    # BEFORE anything is moved, wiped or validated: a run whose provenance
    # cannot be recorded must not start. Cheap (two small JSON reads) and it
    # fails before _out is touched.
    provenance = read_sample_provenance(sample_dir)
    print(f"sample provenance: {provenance['payer_count']} payer(s), "
          f"{provenance['cells']} cell(s), {provenance['sampled']:,} instance(s), "
          f"per_cell={provenance['per_cell'] or 'full population'}, "
          f"carin_bb_stripped={provenance['carin_bb_stripped']}"
          + (f" ({provenance['instances_stripped']:,} instance(s), "
             f"{len(provenance['canonicals_removed'])} canonical(s))"
             if provenance["instances_stripped"] else ""), flush=True)

    out_dir = sample_dir / "_out"
    # Reuse is only sound for outputs produced by the SAME validator
    # configuration. Reusing across configurations silently defeats the point
    # of the new run: run 3 exists to re-validate WITH the THO package loaded,
    # and had it inherited run 2's `-tx n/a`-without-THO outputs it would have
    # been run 2's data under a new run_id, with the v2-0360 fix never applied
    # and nothing in the results to show it. A sidecar marker makes that
    # mismatch detectable instead of invisible.
    # Chunk NAMES (`<cell>__NNN`) are a function of the chunk sizing policy, so
    # the sizing policy has to be in the reuse signature. Without it, changing
    # --chunk between attempts silently reuses `Organization__003` produced
    # from a completely different set of 4,000 files -- same name, different
    # instances, no signal anywhere. The POLICY is signed rather than the
    # resolved per-cell map so this stays a pure function of the CLI args and
    # can fail fast, before any file is restored or moved; the map is a
    # deterministic function of the policy plus which cells exist on disk.
    config_sig = json.dumps({"ig": f"{IG}#{args.ig_version}",
                             "tho": args.tho_version or None, "tx": args.tx,
                             "chunk": args.chunk,
                             "cell_chunk": args.cell_chunk or None,
                             "cell_rate": args.cell_rate or None,
                             "target_chunk_seconds": TARGET_CHUNK_SECONDS,
                             "min_chunk": MIN_CHUNK, "max_chunk": MAX_CHUNK},
                            sort_keys=True)
    marker = out_dir / "_config.json"
    if out_dir.exists() and args.reuse_outputs:
        prior = marker.read_text(encoding="utf-8") if marker.exists() else None
        if prior != config_sig:
            raise SystemExit(
                f"FATAL: --reuse-outputs, but {out_dir} holds outputs from a "
                f"different validator configuration.\n  existing: {prior or '(unmarked)'}"
                f"\n  requested: {config_sig}\nReusing them would report the OLD "
                "configuration's findings under a new run_id. Move or delete "
                f"{out_dir} to re-validate from scratch."
            )
    elif out_dir.exists():
        # Wiped by default so a fresh run cannot silently inherit a previous
        # run's outputs. --reuse-outputs is precisely the opt-in to that
        # inheritance, and must therefore NOT wipe.
        shutil.rmtree(out_dir)
    out_dir.mkdir(exist_ok=True)
    marker.write_text(config_sig, encoding="utf-8")

    # Top up the sample from the backup copy UNCONDITIONALLY, not only when
    # the sample dir looks empty: restore_from_backup is incremental (it
    # copies only files missing from the cell dirs), so a full sample costs
    # seconds of existence checks, while gating it on "looks empty" left a
    # partial-copy crash looking complete and silently running on a fraction
    # of the sample. Files currently parked in `_batches/` (interrupted run)
    # briefly get a duplicate copy here; restore_cells collapses it via
    # replace() before chunking, so nothing is ever validated twice.
    if args.restore_from:  # '' disables (Path('') is cwd, not "no backup")
        n = restore_from_backup(sample_dir, Path(args.restore_from))
        if n:
            print(f"restored {n:,} missing sample file(s) from {args.restore_from}",
                  flush=True)

    # Plan chunk sizes/timeouts from the cells actually on disk. restore_cells
    # runs first so a previous run's `_batches` is back in its cell dirs and
    # every cell is visible here, exactly as build_chunks would have seen it.
    restored = restore_cells(sample_dir)
    if restored:
        print(f"restored {restored:,} file(s) from a previous run's _batches",
              flush=True)
    present = [p.name for p in sample_dir.iterdir()
               if p.is_dir() and not p.name.startswith("_")]
    plan = chunk_plan(present, args)
    CHUNK_TIMEOUTS.clear()
    CHUNK_TIMEOUTS.update({c: t for c, (_s, t) in plan.items()})
    rates = dict(CELL_RATE_S)
    rates.update(parse_kv_overrides(args.cell_rate, float))
    for cell in sorted(plan):
        size, timeout = plan[cell]
        rate = rates.get(cell, DEFAULT_RATE_S)
        print(f"  plan {cell:26s} chunk={size:>6,}  rate={rate:>5.2f}s/inst  "
              f"est={size * rate / 3600:>5.2f}h  timeout={timeout / 3600:>5.2f}h",
              flush=True)

    chunks = build_chunks(sample_dir, {c: s for c, (s, _t) in plan.items()})
    sizes = {d.name: len(list(d.glob("*.json"))) for _, d in chunks}
    total = sum(sizes.values())
    if total == 0:
        # A zero-instance run must be a loud failure, not a quiet success: it
        # would land as the newest finished conformance_run and silently
        # become what latest_run()-based reports consider authoritative.
        raise SystemExit(
            f"FATAL: no sample instances found under {sample_dir} (and no "
            f"backup at {args.restore_from}). Re-run sample_for_validation.py, "
            "or point --restore-from at a backup of the sample."
        )
    print(f"{len(chunks)} chunk(s), {total:,} instance(s), IG {IG}#{args.ig_version}, "
          f"{args.workers} worker(s)", flush=True)
    unknown = [c for c, _ in chunks if c not in CELL_PROFILE]
    if unknown:
        raise SystemExit(f"no profile mapped for cell(s): {sorted(set(unknown))}")

    # Longest-processing-time-first. Alphabetical dispatch ran every
    # Organization chunk before the first PractitionerRole one, so the run
    # ended with the most expensive cell trickling through a draining pool.
    chunks.sort(key=lambda cd: chunk_cost(cd[0], sizes[cd[1].name], rates,
                                          args.oversize_timeout, cd[1].name),
                reverse=True)

    conn = pg_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(
            "INSERT INTO public.conformance_run "
            "(ig, ig_version, per_cell, instances, notes) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING run_id",
            (IG, args.ig_version, provenance["per_cell"], total,
             json.dumps({"profile_forced": True, "tx": args.tx,
                         # WHAT WAS DONE TO THE DATA BEFORE IT WAS VALIDATED.
                         # Without this the run row records the validator's
                         # configuration in detail and the sample's not at all,
                         # so two runs measuring completely different payer
                         # sets off differently-modified instances look
                         # identical in the database.
                         "sample": provenance,
                         "tho": f"{THO_IG}#{args.tho_version}" if args.tho_version else None,
                         "pkg_cache": bool(args.pkg_cache),
                         "tx_cache": bool(args.tx_cache),
                         "local_binding_checks": True,
                         "chunk": args.chunk, "workers": args.workers,
                         "xmx": args.xmx, "docker_memory": args.docker_memory,
                         "chunk_plan": {c: {"chunk": s, "timeout_s": t,
                                            "rate_s": rates.get(c, DEFAULT_RATE_S)}
                                        for c, (s, t) in sorted(plan.items())}})),
        )
        run_id = cur.fetchone()[0]
    print(f"conformance_run.run_id = {run_id}", flush=True)

    started = time.time()
    done = 0
    lock = threading.Lock()
    failures: list[str] = []
    # Chunk NAMES (not just log lines) so the run row can record exactly
    # which instances went unvalidated -- see the notes update at the end.
    failed_chunks: list[str] = []
    results: list[tuple[str, Path]] = []
    pending = list(chunks)

    # Warm-first: if the shared package cache does not hold the main IG yet,
    # run one chunk alone before fanning out -- N containers concurrently
    # populating the same empty cache corrupt each other's downloads. The
    # warmed chunk's result COUNTS (it is removed from the pending queue, never
    # re-run: re-running would duplicate its instances and break the PK COPY).
    if args.pkg_cache:
        pkg_root = Path(args.pkg_cache)
        pkg_root.mkdir(parents=True, exist_ok=True)
        # Integrity, not mere existence: a warm-up killed mid-download (chunk
        # timeout, Ctrl-C) can leave the package DIR present but the nested
        # package/ payload missing or partial -- existence alone would then
        # skip warm-up and let 14 containers share a corrupt cache.
        pkg_marker = pkg_root / "packages" / f"{IG}#{args.ig_version}" / "package"
        # The main-IG dir alone is NOT proof the cache is warm: the validator
        # fetches transitive packages on first use (hl7.fhir.xver-extensions,
        # r4.core's own THO pin -- observed live: a cache holding all 4 named
        # packages still downloaded two more), so N cold workers would still
        # race those downloads. The sentinel is written only after one chunk
        # has run END TO END with this exact ig/tho argument set, against THIS
        # tree -- the warm-up now mounts the shared cache (shared_cache=True),
        # so the sentinel describes the directory seed_worker_caches actually
        # clones. It previously described a tree the warm-up never wrote to.
        sentinel = (pkg_root /
                    f".provdir_warmed__{IG}#{args.ig_version}"
                    f"__tho-{args.tho_version or 'none'}")
        if (not pkg_marker.is_dir() or not sentinel.exists()) and pending:
            print("package cache is cold (or never proven warm for this IG set); "
                  "warming with one synchronous chunk...", flush=True)
            # `pending` is sorted cost-descending. pop() took the LAST element
            # -- the CHEAPEST chunk in the run -- which makes the warm-up a
            # useless gate on everything except the download: an --xmx that
            # fits a 25-file tail chunk but not a 2,790-file Organization one
            # sails through and then kills 44 workers at hour 3. Warm up on the
            # most expensive ORDINARY chunk instead: that is the heap and
            # wall-time worst case the fan-out will actually meet, and it costs
            # at most the TARGET_CHUNK_SECONDS every chunk is sized for.
            # Oversize chunks are skipped on purpose -- they run under
            # --oversize-xmx with their own longer budget, so one proves
            # nothing about the ordinary tier and would hold the run behind a
            # multi-hour whale.
            cell0, dir0 = pending.pop(pick_warmup_chunk(pending))
            print(f"  warm-up chunk: {dir0.name} "
                  f"({len(list(dir0.glob('*.json'))):,} instance(s), "
                  f"the most expensive non-oversize chunk) at --xmx {args.xmx}",
                  flush=True)
            c0, of0, rc0, tail0 = run_chunk(cell0, dir0, sample_dir, args,
                                            out_dir, shared_cache=True)
            if not of0.exists() or not output_looks_complete(of0):
                raise SystemExit(
                    f"FATAL: warm-up chunk {dir0.name} failed rc={rc0}: {tail0}\n"
                    f"  It is the most expensive ordinary chunk in the run, so "
                    f"this is the gate doing its job: --xmx {args.xmx} / "
                    f"--docker-memory {args.docker_memory} did not carry a "
                    "representative chunk. Raise the heap and lower --workers "
                    "to keep (workers x docker-memory) inside the Docker VM.")
            results.append((c0, of0))
            done += 1
            sentinel.write_text(time.strftime("%Y-%m-%dT%H:%M:%S"), encoding="utf-8")
            print(f"  [1/{len(chunks)}] {dir0.name} (warm-up)", flush=True)

    # Seed per-worker caches AFTER warm-up, so each copy is of a cache already
    # proven complete -- cloning a cold one would just replicate the download
    # storm 14 times. Skipped entirely at --workers 1 (nothing to race).
    n_workers = min(args.workers, max(1, len(pending)))
    if args.pkg_cache and args.per_worker_cache and n_workers > 1:
        src = Path(args.pkg_cache)
        one = sum(f.stat().st_size for f in src.rglob("*") if f.is_file())
        free = shutil.disk_usage(src.anchor).free
        need = one * n_workers
        print(f"seeding {n_workers} per-worker package cache copies "
              f"({one / 1e9:.1f}GB each, ~{need / 1e9:.0f}GB total, "
              f"{free / 1e9:.0f}GB free) -- prevents the concurrent-cache "
              f"corruption that killed 17 chunks last run. Kept after the run "
              f"so a resume re-copies nothing; delete "
              f"{src.parent / (src.name + '_w*')} to reclaim.", flush=True)
        if need > free * 0.5:
            raise SystemExit(
                f"FATAL: per-worker caches need ~{need / 1e9:.0f}GB but only "
                f"{free / 1e9:.0f}GB is free. Lower --workers, or pass "
                "--no-per-worker-cache (UNSAFE above --workers 1)."
            )
        seed_worker_caches(src, n_workers)
        print("caches seeded.", flush=True)

    def worker(item, widx: int):
        nonlocal done
        cell, chunk_dir = item
        c, out_file, rc, tail = run_chunk(cell, chunk_dir, sample_dir,
                                          args, out_dir, worker=widx)
        with lock:
            done += 1
            # The validator exits non-zero when it FINDS errors, so a bad exit
            # code alone is not a failure. A MISSING output is -- and so is a
            # present-but-truncated one, which is what a chunk killed mid-write
            # leaves behind and what --reuse-outputs used to accept forever.
            if not out_file.exists() or not output_looks_complete(out_file):
                if out_file.exists():
                    tail = f"output is truncated/unparseable; {tail}"
                failures.append(f"{chunk_dir.name} rc={rc}: {tail}")
                failed_chunks.append(chunk_dir.name)
                print(f"  [{done}/{len(chunks)}] FAILED {chunk_dir.name} rc={rc}",
                      flush=True)
            else:
                results.append((c, out_file))
                el = time.time() - started
                print(f"  [{done}/{len(chunks)}] {chunk_dir.name} "
                      f"({el / 60:.1f}m elapsed)", flush=True)

    threads: list[threading.Thread] = []
    qlock = threading.Lock()

    # --- container memory budget -------------------------------------------
    # The dispatch order alone cannot keep the Docker VM alive: cost-descending
    # dispatch put all 36 oversize Network chunks in flight AT ONCE, and
    # 36 x 12 GB of allowed memory against a ~101 GB VM crushed it two hours
    # into the first full run (WSL "connection forcibly closed", every
    # container dead). A chunk therefore acquires its container memory limit
    # from this budget before launching and releases it after; whales
    # self-throttle to ~budget/12 concurrent and ordinary chunks fill whatever
    # is left. Every mix of tiers is safe by construction.
    def _mem_gb(size: Optional[str]) -> float:
        if not size:
            return 2.0
        m = re.fullmatch(r"([\d.]+)([gGmM])", size.strip())
        if not m:
            return 2.0
        v = float(m.group(1))
        return v if m.group(2).lower() == "g" else v / 1024
    ordinary_gb = _mem_gb(args.docker_memory)
    oversize_gb = _mem_gb(args.oversize_docker_memory or args.docker_memory)
    budget_total = float(getattr(args, "memory_budget", 92.0) or 92.0)
    if budget_total < max(ordinary_gb, oversize_gb):
        raise SystemExit(f"FATAL: --memory-budget {budget_total}g cannot fit a "
                         f"single {max(ordinary_gb, oversize_gb)}g container")
    budget_cv = threading.Condition()
    budget_left = [budget_total]

    def _acquire(gb: float) -> None:
        with budget_cv:
            while budget_left[0] < gb:
                budget_cv.wait()
            budget_left[0] -= gb

    def _release(gb: float) -> None:
        with budget_cv:
            budget_left[0] += gb
            budget_cv.notify_all()

    def pump(widx: int):
        # Each pump thread owns one worker index for the whole run, so it also
        # owns exactly one package-cache copy -- two threads must never share
        # a cache dir (that is the race this indexing exists to prevent).
        while True:
            with qlock:
                if not pending:
                    return
                item = pending.pop(0)
            gb = oversize_gb if item[1].name in OVERSIZE_CHUNKS else ordinary_gb
            _acquire(gb)
            try:
                worker(item, widx)
            finally:
                _release(gb)

    for _w in range(min(args.workers, max(1, len(pending)))):
        t = threading.Thread(target=pump, args=(_w,), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    print(f"validation done in {(time.time() - started) / 60:.1f}m; "
          f"{len(results)} ok, {len(failures)} failed", flush=True)

    n_inst = n_iss = 0
    # Sorted by cell so the id sidecar (Organization alone is 1.86M entries) is
    # loaded once per cell and released before the next one.
    parse_failed: list[str] = []
    ids: dict[str, str] = {}
    ids_cell = None
    with conn.cursor() as cur:
        for cell, out_file in sorted(results, key=lambda r: (r[0], r[1].name)):
            if cell != ids_cell:
                ids = load_id_map(sample_dir, cell)
                ids_cell = cell
            try:
                inst, iss = parse_output(out_file, cell, ids)
            except UnreadableOutput as exc:
                # Same accounting as a missing output -- see UnreadableOutput.
                # The file is also quarantined, because otherwise the next
                # --reuse-outputs resume accepts the identical truncated file
                # again and the run is short by the same chunk forever.
                name = out_file.stem
                parse_failed.append(name)
                failures.append(f"{name} UNREADABLE OUTPUT: {exc}")
                try:
                    out_file.replace(out_file.with_suffix(".json.corrupt"))
                except OSError:
                    pass
                print(f"  FAILED {name}: unreadable output, quarantined "
                      "(re-run with --reuse-outputs to re-validate it)",
                      flush=True)
                continue
            if inst:
                with cur.copy("COPY public.conformance_instance "
                              "(run_id, payer_id, cell, instance, errors, warnings, "
                              "resource_id) FROM STDIN") as cp:
                    for r in inst:
                        cp.write_row((run_id, *r))
                n_inst += len(inst)
            if iss:
                with cur.copy("COPY public.conformance_issue (run_id, payer_id, cell, "
                              "instance, severity, code, message_id, expression, details) "
                              "FROM STDIN") as cp:
                    for r in iss:
                        cp.write_row((run_id, *r))
                n_iss += len(iss)
        # Record which chunks did NOT produce results, and how many instances
        # they covered. Without this a failed chunk is a silent absence: run 3
        # lost 30 of 46 chunks and the only evidence was a log line, so any
        # report reading the DB alone would have treated 67,744 unvalidated
        # instances as simply "not sampled" rather than "attempted and failed".
        unvalidated = 0
        failed_detail = []
        for name in failed_chunks + parse_failed:
            d = sample_dir / "_batches" / name
            n = len(list(d.glob("*.json"))) if d.is_dir() else None
            if n:
                unvalidated += n
            failed_detail.append({"chunk": name, "instances": n})

        # THE check. `chunks_failed == 0` structurally cannot detect a
        # truncated-but-non-empty output reused under --reuse-outputs, which is
        # how run 3 recorded 1,463,451 of 1,862,183 instances and still called
        # itself ok. Every instance the run attempted must be accounted for:
        # either it produced a row, or it belongs to a chunk recorded as
        # failed. If those do not add up, something ate rows silently and the
        # run is not publishable no matter what the chunk counters say.
        cur.execute("SELECT count(*) FROM public.conformance_instance "
                    "WHERE run_id = %s", (run_id,))
        recorded = int(cur.fetchone()[0])
        accounted = recorded + unvalidated
        verified = accounted == total
        # finished_at is stamped ONLY when the run verified. It used to be set
        # unconditionally, several statements ABOVE the `if not verified` check
        # -- which only prints and returns 2, long after the row is committed.
        # `finished_at IS NOT NULL` is the predicate every consumer in this
        # repo uses to mean "authoritative", so a short run was authoritative
        # the instant it stopped, no matter what it printed on the way out.
        # Leaving it NULL makes a short run indistinguishable from a crashed
        # one, which is correct: neither is publishable. notes still records
        # exactly what happened, and quality.conformance gates on notes.verified
        # as well, because one guard is how this failed the first time.
        cur.execute(
            "UPDATE public.conformance_run "
            "SET finished_at = CASE WHEN %s THEN now() ELSE finished_at END, "
            "notes = coalesce(notes, '{}'::jsonb) || %s::jsonb "
            "WHERE run_id = %s",
            (verified,
             json.dumps({"chunks_total": len(chunks),
                         "chunks_ok": len(results) - len(parse_failed),
                         "chunks_failed": len(failed_detail),
                         "chunks_unparseable": len(parse_failed),
                         "unparseable_chunks": parse_failed,
                         "instances_unvalidated": unvalidated,
                         "instances_recorded": recorded,
                         "instances_accounted": accounted,
                         "instances_expected": total,
                         "verified": verified,
                         "failed_chunks": failed_detail}), run_id),
        )
    print(f"recorded {n_inst:,} instance result(s), {n_iss:,} issue(s) "
          f"under run_id {run_id}", flush=True)
    if failures:
        print("FAILED chunks:", flush=True)
        for f in failures[:10]:
            print(f"  {f}", flush=True)
    if not verified:
        print(f"FATAL: run {run_id} does not add up. conformance_run.instances "
              f"= {total:,}, but conformance_instance holds {recorded:,} rows "
              f"and only {unvalidated:,} instance(s) are recorded as "
              f"unvalidated ({accounted:,} accounted for, "
              f"{total - accounted:,} missing).\nfinished_at was NOT stamped, "
              "so no report will treat this run as authoritative.\n"
              "Do NOT publish this run. "
              "Re-run with --reuse-outputs: quarantined outputs are "
              "re-validated and everything else is reused.", flush=True)
        conn.close()
        return 2
    if failures:
        conn.close()
        return 1
    # Completion marker for the boot-triggered resume task: a reboot AFTER the
    # run finished must not start a redundant duplicate run that reuses every
    # output and lands a second run_id holding identical data.
    (out_dir / "_complete.json").write_text(
        json.dumps({"run_id": run_id, "instances": total,
                    "finished": datetime.now().isoformat(timespec="seconds")}),
        encoding="utf-8")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

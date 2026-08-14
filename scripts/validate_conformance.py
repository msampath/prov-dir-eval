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
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

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
    PRIMARY KEY (run_id, payer_id, cell, instance));
"""


# Chunk-dir names holding an oversize instance, populated by build_chunks and
# read by run_chunk to pick the longer timeout. A module-level set rather than
# a field on the chunk tuple so the (cell, dir) shape stays what every caller
# and test already expects.
OVERSIZE_CHUNKS: set[str] = set()


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


def seed_worker_caches(pkg_cache: Path, workers: int) -> list[Path]:
    """Clone the warmed cache once per worker. Returns the per-worker paths.

    Copies only what is missing, so an interrupted seeding resumes rather than
    starting over, and a re-run after --reuse-outputs pays nothing.
    """
    out = []
    for w in range(workers):
        dest = worker_cache(pkg_cache, w)
        if not dest.exists():
            shutil.copytree(pkg_cache, dest)
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


def build_chunks(sample_dir: Path, chunk_size: int) -> list[tuple[str, Path]]:
    """Move each cell's files into per-chunk directories. Returns [(cell, dir)].

    The validator takes a directory, not a file list, so chunking has to be
    physical. Files are moved (same volume, so it is a rename) rather than
    copied to avoid a second full copy of the sample on disk.
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
        files = sorted(cell_dir.glob("*.json"))
        normal = [f for f in files if f.stat().st_size <= OVERSIZE_BYTES]
        oversize = [f for f in files if f.stat().st_size > OVERSIZE_BYTES]
        idx = 0
        # Oversize files ride in their own small chunks (see OVERSIZE_BYTES
        # above) so a --chunk-timeout kill loses only them, not their cell.
        for group, per, big in ((normal, chunk_size, False),
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


def run_chunk(cell: str, chunk_dir: Path, sample_dir: Path, args,
              out_dir: Path, worker: int = 0) -> tuple[str, Path, int, str]:
    out_file = out_dir / f"{chunk_dir.name}.json"
    if args.reuse_outputs and out_file.exists() and out_file.stat().st_size > 0:
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
        cache = Path(args.pkg_cache)
        if worker > 0 or args.per_worker_cache:
            cache = worker_cache(Path(args.pkg_cache), worker)
        mounts += ["-v", f"{cache.as_posix()}:/root/.fhir"]
    if args.tx_cache:
        mounts += ["-v", f"{Path(args.tx_cache).as_posix()}:/txcache"]
    cmd = [
        "docker", "run", "--rm", *mounts,
        IMAGE, "java", "-Xmx8g", "-jar", "/tools/validator_cli.jar",
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
              else args.chunk_timeout)
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


def parse_output(out_file: Path, cell: str) -> tuple[list[tuple], list[tuple]]:
    """(instance rows, issue rows) from one validator output document.

    A multi-file chunk yields a Bundle of OperationOutcomes; a SINGLE-file
    chunk yields one bare OperationOutcome (observed live on validator
    6.10.1). Both must parse: oversize isolation makes 1-file chunks routine,
    and a bare outcome silently parsed as an empty Bundle would drop that
    instance from the run with no failure signal at all.
    """
    try:
        doc = json.loads(out_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"  WARN unreadable output {out_file.name}: {exc}", flush=True)
        return [], []
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
        instances.append((payer, cell, name, nerr, nwarn))
    return instances, issues


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-dir", required=True)
    ap.add_argument("--ig-version", default="1.1.0")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--chunk", type=int, default=DEFAULT_CHUNK)
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
    ap.add_argument("--chunk-timeout", type=int, default=3600, metavar="SEC",
                     help="kill a chunk's container after SEC seconds (a hung "
                          "validator once stalled a run for 6h). The chunk is "
                          "recorded failed; the rest of the run continues. "
                          "Default: %(default)s")
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

    out_dir = sample_dir / "_out"
    # Reuse is only sound for outputs produced by the SAME validator
    # configuration. Reusing across configurations silently defeats the point
    # of the new run: run 3 exists to re-validate WITH the THO package loaded,
    # and had it inherited run 2's `-tx n/a`-without-THO outputs it would have
    # been run 2's data under a new run_id, with the v2-0360 fix never applied
    # and nothing in the results to show it. A sidecar marker makes that
    # mismatch detectable instead of invisible.
    config_sig = json.dumps({"ig": f"{IG}#{args.ig_version}",
                             "tho": args.tho_version or None, "tx": args.tx},
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

    chunks = build_chunks(sample_dir, args.chunk)
    total = sum(len(list(d.glob("*.json"))) for _, d in chunks)
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

    conn = pg_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(
            "INSERT INTO public.conformance_run (ig, ig_version, instances, notes) "
            "VALUES (%s, %s, %s, %s) RETURNING run_id",
            (IG, args.ig_version, total,
             json.dumps({"profile_forced": True, "tx": args.tx,
                         "tho": f"{THO_IG}#{args.tho_version}" if args.tho_version else None,
                         "pkg_cache": bool(args.pkg_cache),
                         "tx_cache": bool(args.tx_cache),
                         "local_binding_checks": True,
                         "chunk": args.chunk, "workers": args.workers})),
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
        # has run END TO END with this exact ig/tho argument set, which is the
        # only thing that proves every transitive dependency is cached.
        sentinel = (pkg_root /
                    f".provdir_warmed__{IG}#{args.ig_version}"
                    f"__tho-{args.tho_version or 'none'}")
        if (not pkg_marker.is_dir() or not sentinel.exists()) and pending:
            print("package cache is cold (or never proven warm for this IG set); "
                  "warming with one synchronous chunk...", flush=True)
            cell0, dir0 = pending.pop(0)
            c0, of0, rc0, tail0 = run_chunk(cell0, dir0, sample_dir, args, out_dir)
            if not of0.exists():
                raise SystemExit(f"FATAL: warm-up chunk failed rc={rc0}: {tail0}")
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
            # code alone is not a failure -- only a missing output file is.
            if not out_file.exists():
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

    def pump(widx: int):
        # Each pump thread owns one worker index for the whole run, so it also
        # owns exactly one package-cache copy -- two threads must never share
        # a cache dir (that is the race this indexing exists to prevent).
        while True:
            with qlock:
                if not pending:
                    return
                item = pending.pop(0)
            worker(item, widx)

    for _w in range(min(args.workers, max(1, len(pending)))):
        t = threading.Thread(target=pump, args=(_w,), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    print(f"validation done in {(time.time() - started) / 60:.1f}m; "
          f"{len(results)} ok, {len(failures)} failed", flush=True)

    n_inst = n_iss = 0
    with conn.cursor() as cur:
        for cell, out_file in results:
            inst, iss = parse_output(out_file, cell)
            if inst:
                with cur.copy("COPY public.conformance_instance "
                              "(run_id, payer_id, cell, instance, errors, warnings) "
                              "FROM STDIN") as cp:
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
        for name in failed_chunks:
            d = sample_dir / "_batches" / name
            n = len(list(d.glob("*.json"))) if d.is_dir() else None
            if n:
                unvalidated += n
            failed_detail.append({"chunk": name, "instances": n})
        cur.execute(
            "UPDATE public.conformance_run SET finished_at = now(), "
            "notes = coalesce(notes, '{}'::jsonb) || %s::jsonb "
            "WHERE run_id = %s",
            (json.dumps({"chunks_total": len(chunks),
                         "chunks_ok": len(results),
                         "chunks_failed": len(failed_detail),
                         "instances_unvalidated": unvalidated,
                         "failed_chunks": failed_detail}), run_id),
        )
    print(f"recorded {n_inst:,} instance result(s), {n_iss:,} issue(s) "
          f"under run_id {run_id}", flush=True)
    if failures:
        print("FAILED chunks:", flush=True)
        for f in failures[:10]:
            print(f"  {f}", flush=True)
    # Round-trip the sample back to its canonical cell layout. NEVER delete it:
    # _batches holds the only copy (build_chunks moves, not copies), and losing
    # it forces a not-exactly-reproducible TABLESAMPLE re-draw.
    restored = restore_cells(sample_dir)
    print(f"restored {restored:,} sample file(s) to cell dirs", flush=True)
    conn.close()
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

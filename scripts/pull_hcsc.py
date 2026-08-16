#!/usr/bin/env python
"""Pull the HCSC provider directory to NDJSON, resumably.

HCSC needs its own puller because every partition mode the shared extractor
offers is wrong for this server (see the hcsc block in config/endpoints.yaml):

  * page size is capped at 50 whatever _count asks for, so a full read is
    ~540,000 requests over ~27M records
  * _summary=count returns 400, so buckets cannot be sized before walking; the
    first page of a walk carries Bundle.total and doubles as the sizing request
  * _lastUpdated returns 400, so there is no incremental refresh
  * string search is exact match, and only a trailing '*' gives a prefix
  * PractitionerRole partitions only by specialty, in HCSC's own numeric code
    system, and 17% of the records read so far carry more than one specialty

That last point is why every record is deduplicated by id. Overlapping buckets
would otherwise write the same resource once per specialty it holds, and the
count would be meaningless. Deduplicating also makes the completeness proof
exact: distinct ids collected, compared against the Bundle.total the server
reports for the unpartitioned search. Summing bucket totals proves nothing when
buckets overlap.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

BASE = "https://api.hcsc.net/providerfinder/sapphire/fhir"
PAGE = 50                      # server cap; asking for more returns 50
SERVED = ["Organization", "Practitioner", "Location",
          "OrganizationAffiliation", "PractitionerRole"]


def client_id() -> str:
    for ln in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if ln.startswith("HCSC_CLIENT_ID="):
            return ln.partition("=")[2].strip()
    raise SystemExit("HCSC_CLIENT_ID not found in .env")


def load_specialties(path: Path) -> list[str]:
    """Codes from HCSC's published TSV (Code<TAB>Display)."""
    out: list[str] = []
    for ln in path.read_text(encoding="utf-8", errors="replace").splitlines()[1:]:
        code = ln.split("\t", 1)[0].strip()
        if code.isdigit():
            out.append(code)
    return sorted(set(out))


_ID_RE = re.compile(r'"id"\s*:\s*"([^"]+)"')


class Sink:
    """One NDJSON file per resource type, with id-level deduplication.

    Holds the seen-id set in memory. PractitionerRole is the worst case at
    ~18.5M ids; interned strings put that near 2GB, which this box has.

    The set is mirrored to a `.ids` sidecar, one id per line, because rebuilding
    it from the NDJSON is ruinous at this size. The original version re-parsed
    the whole file with json.loads on every start: 154GB off an external disk
    and 18.3M json.loads calls, which left a restarted run grinding for over two
    hours before it fetched anything. That silently negated the rest of the
    resume work, since mid-cursor checkpoints are useless if a run cannot reach
    the point of using them. The sidecar is ~350MB and loads in seconds.

    When the sidecar is missing the NDJSON is scanned with a regex for the id
    field rather than parsed, which is ~62x faster than json.loads and returns
    an identical set, and the sidecar is written on the way through so the cost
    is paid once.

    The record is written before its id, so an ill-timed crash can leave a
    record whose id is not in the sidecar. That risks re-fetching and re-writing
    that one record later, which the loader's upsert absorbs. The reverse order
    would risk dropping a record entirely, which it would not.
    """

    def __init__(self, path: Path, ids_dir: Path | None = None):
        self.path = path
        # The sidecar is read in full on every start and the NDJSON is not, so
        # it earns a faster disk. The data lives on a USB HDD because it is the
        # only volume with room for ~200GB; that device reads at ~44MB/s, which
        # is why the sidecar exists at all.
        base = ids_dir if ids_dir else path.parent
        base.mkdir(parents=True, exist_ok=True)
        self.ids_path = base / (path.stem + ".ids")
        self.lock = threading.Lock()
        self.seen: set[str] = set()
        self.written = 0
        self.dupes = 0
        if self.ids_path.exists():
            t0 = time.time()
            with self.ids_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    rid = line.strip()
                    if rid:
                        self.seen.add(rid)
            print(f"  resume: {path.name} {len(self.seen):,} ids from sidecar "
                  f"({time.time()-t0:.0f}s)", flush=True)
        elif path.exists():
            # Build to a temp file and rename only on success. A sidecar
            # truncated by a kill is indistinguishable from a complete one, and
            # the next run would trust it, believe it holds fewer ids than the
            # NDJSON does, and re-fetch every record past the truncation point.
            t0 = time.time()
            tmp = self.ids_path.with_suffix(".ids.building")
            try:
                with path.open("r", encoding="utf-8") as fh, \
                     tmp.open("w", encoding="utf-8") as out:
                    for line in fh:
                        m = _ID_RE.search(line)
                        if m and m.group(1) not in self.seen:
                            self.seen.add(m.group(1))
                            out.write(m.group(1) + "\n")
                tmp.replace(self.ids_path)
            except BaseException:
                tmp.unlink(missing_ok=True)
                raise
            print(f"  resume: {path.name} {len(self.seen):,} ids rebuilt and "
                  f"cached to {self.ids_path} ({time.time()-t0:.0f}s)", flush=True)
        self.written = len(self.seen)
        self.fh = path.open("a", encoding="utf-8")
        self.ids_fh = self.ids_path.open("a", encoding="utf-8")

    def add(self, resources: list[dict]) -> tuple[int, set[str]]:
        """Write unseen resources. Returns (new_count, specialty codes observed).

        The codes come back so the caller can queue any it has not walked yet.
        HCSC's published list is missing codes that are live in the directory
        (15 of the 136 found in the first 6,000 records), so seeding only from
        that list would silently skip every provider holding a missing code.
        """
        new = 0
        codes: set[str] = set()
        with self.lock:
            for r in resources:
                for sp in (r.get("specialty") or []):
                    for cd in (sp.get("coding") or []):
                        c = cd.get("code")
                        if c:
                            codes.add(str(c))
                rid = r.get("id")
                if not rid or rid in self.seen:
                    self.dupes += 1
                    continue
                self.seen.add(rid)
                self.fh.write(json.dumps(r, separators=(",", ":")) + "\n")
                self.ids_fh.write(rid + "\n")
                new += 1
            self.written += new
            self.fh.flush()
            self.ids_fh.flush()
        return new, codes

    def close(self):
        self.fh.close()
        self.ids_fh.close()


class Progress:
    """Checkpoint of finished buckets AND the position of unfinished ones.

    Recording only completed buckets is not enough. The unpartitioned walks are
    the long ones (Location is 99,376 pages, ~75h) and they would checkpoint
    nothing until the end, so any interruption restarts them at page 1. No data
    is lost, since the sink deduplicates, but the time is.

    Mid-walk resume works here because cursorMark is stateless: a next link
    stored now is still valid later. Payers that issue session cursors cannot
    do this, which is why the file sweep only checkpoints per unit.
    """

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.done: dict[str, dict] = {}
        self.inflight: dict[str, dict] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                raw = {}
            # Older runs stored a bare {key: stats} map with no envelope.
            if "done" in raw and "inflight" in raw:
                self.done = raw.get("done") or {}
                self.inflight = raw.get("inflight") or {}
            else:
                self.done = raw
        if self.inflight:
            print(f"  resuming {len(self.inflight)} interrupted walk(s) mid-cursor",
                  flush=True)

    def is_done(self, key: str) -> bool:
        return key in self.done

    def resume_url(self, key: str) -> str | None:
        rec = self.inflight.get(key)
        return rec.get("url") if rec else None

    def _flush_unlocked(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"done": self.done, "inflight": self.inflight}),
                       encoding="utf-8")
        tmp.replace(self.path)

    def mark(self, key: str, url: str, pages: int):
        """Record where an unfinished walk has reached."""
        with self.lock:
            self.inflight[key] = {"url": url, "pages": pages}
            self._flush_unlocked()

    def finish(self, key: str, total, fetched: int, pages: int):
        with self.lock:
            self.done[key] = {"total": total, "fetched": fetched, "pages": pages}
            self.inflight.pop(key, None)
            self._flush_unlocked()


_DIR_LOCK = None               # held for the process lifetime; OS frees it on exit


def lock_out_dir(out: Path) -> None:
    """Refuse to start if another puller already holds this output directory.

    Two pullers would append to the same NDJSON concurrently and interleave
    partial lines, and each would resume from a checkpoint the other is
    overwriting. An OS byte-range lock rather than a PID file, because the
    kernel releases it when the process dies, so a crashed or killed run leaves
    nothing stale to reap. The same collision took out a conformance run
    earlier today when a scheduled task fired alongside one already going.
    """
    global _DIR_LOCK
    lock_path = out / ".pull.lock"
    lock_path.touch(exist_ok=True)
    f = open(lock_path, "r+b")          # r+b: append mode ignores seek on write
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
            f"FATAL: another HCSC pull already holds {out}"
            + (f"\n  holder: {who}" if who else "")
            + "\nTwo pullers would interleave writes into the same NDJSON files.")
    f.seek(1)
    f.write(f"pid {os.getpid()} started "
            f"{datetime.now().isoformat(timespec='seconds')}".ljust(120).encode())
    f.flush()
    _DIR_LOCK = f


CHECKPOINT_EVERY = 20          # pages; ~1 min of walking at the measured 2.7s


def walk(c: httpx.Client, url: str, params: dict | None, sink: Sink,
         label: str, delay: float, max_pages: int | None = None,
         on_page=None, pages0: int = 0):
    """Follow cursorMark pagination to exhaustion.

    Returns (total, fetched, pages, specialty_codes_seen).

    cursorMark is stateless, unlike the session cursors other payers issue, so
    an interrupted walk can restart from the beginning of its own bucket
    without the server rejecting a stale cursor.
    """
    total, fetched, pages = None, 0, pages0
    found: set[str] = set()
    while True:
        for attempt in range(4):
            try:
                r = c.get(url, params=params, timeout=90)
                if r.status_code == 200:
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 ** attempt * 2)
                    continue
                print(f"    {label}: HTTP {r.status_code}, stopping", flush=True)
                return total, fetched, pages, found
            except Exception as e:
                if attempt == 3:
                    print(f"    {label}: {type(e).__name__}, stopping", flush=True)
                    return total, fetched, pages, found
                time.sleep(2 ** attempt * 2)
        params = None                       # next links carry their own params
        j = r.json()
        if total is None:
            total = j.get("total")
        entries = [e["resource"] for e in (j.get("entry") or []) if "resource" in e]
        n, codes = sink.add(entries)
        fetched += n
        found |= codes
        pages += 1
        nxt = [l["url"] for l in (j.get("link") or []) if l.get("relation") == "next"]
        if not nxt or not entries:
            return total, fetched, pages, found
        if max_pages and pages >= max_pages:
            print(f"    {label}: hit --max-pages {max_pages}", flush=True)
            return total, fetched, pages, found
        url = nxt[0]
        if on_page and pages % CHECKPOINT_EVERY == 0:
            on_page(url, pages)
        if delay:
            time.sleep(delay)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=r"I:\file_export\hcsc")
    ap.add_argument("--specialties", default=r"C:\Users\Admin\OneDrive\Desktop\hcsc spec list.txt")
    ap.add_argument("--resources", default=",".join(SERVED))
    ap.add_argument("--workers", type=int, default=4,
                    help="concurrent walks. Their page latency is ~2.7s and this "
                         "is a shared public API, so keep it modest.")
    ap.add_argument("--delay", type=float, default=0.0,
                    help="extra seconds between pages within one walk")
    ap.add_argument("--max-pages", type=int, default=None,
                    help="per-bucket page cap, for smoke tests")
    ap.add_argument("--pr-key", choices=["specialty", "practitioner"], default="specialty",
                    help="how to partition PractitionerRole. `specialty` is fast "
                         "(~41h) but blind to a code that never co-occurs with a "
                         "known one. `practitioner` cannot miss a role, since every "
                         "role carries a practitioner reference and the Practitioner "
                         "walk is complete, but costs one request per practitioner.")
    ap.add_argument("--ids-dir", default=r"M:\provdir_hcsc_ids",
                    help="where the .ids dedup sidecars live. Defaults to the "
                         "SSD rather than beside the data: the sidecar is read "
                         "in full on every start, the NDJSON is not, and the "
                         "data volume is a USB HDD reading at ~44MB/s.")
    ap.add_argument("--only-missing-practitioners", action="store_true",
                    help="with --pr-key practitioner, queue only practitioners that "
                         "have no role in the sink yet. Narrows a verification sweep "
                         "to the plausible gap instead of re-reading 1.47M.")
    a = ap.parse_args()

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    lock_out_dir(out)
    H = {"client_id": client_id(), "Accept": "application/fhir+json"}
    resources = [r.strip() for r in a.resources.split(",") if r.strip()]

    ids_dir = Path(a.ids_dir) if a.ids_dir else None
    sinks = {r: Sink(out / f"hcsc_{r}.ndjson", ids_dir) for r in resources}
    prog = Progress(out / "_progress.json")
    limits = httpx.Limits(max_connections=a.workers + 2,
                          max_keepalive_connections=a.workers + 2)

    # Queued codes, plus the ones discovered mid-walk. queued only ever grows,
    # so a code is walked at most once no matter how many records mention it.
    queued: set[str] = set()
    discovered: set[str] = set()          # live codes absent from the seed list
    q: "queue.Queue[tuple[str, str | None, str]]" = queue.Queue()
    qlock = threading.Lock()

    def unit(res: str, code: str | None, param: str = "specialty"):
        key = f"{res}|{code or '*'}" if param == "specialty" else f"{res}|prac:{code}"
        if prog.is_done(key):
            return
        with httpx.Client(headers=H, limits=limits, follow_redirects=True) as c:
            params = {"_count": PAGE}
            if code:
                # Reference params take a BARE id here. The standard Type/id form
                # returns 0 rather than an error: practitioner=Practitioner/123
                # gives 0, practitioner=123 gives 10.
                params[param] = code
            t0 = time.time()
            resume = prog.resume_url(key)
            if resume:
                pages0 = (prog.inflight.get(key) or {}).get("pages", 0)
                print(f"  {key}: resuming mid-cursor at page {pages0:,}", flush=True)
                total, fetched, pages, found = walk(
                    c, resume, None, sinks[res], key, a.delay, a.max_pages,
                    on_page=lambda u, n: prog.mark(key, u, n), pages0=pages0)
            else:
                total, fetched, pages, found = walk(
                    c, f"{BASE}/{res}", params, sinks[res], key, a.delay,
                    a.max_pages, on_page=lambda u, n: prog.mark(key, u, n))
        prog.finish(key, total, fetched, pages)
        if res == "PractitionerRole" and param == "specialty" and found:
            with qlock:
                fresh = found - queued
                if fresh:
                    queued.update(fresh)
                    discovered.update(fresh)
                    for f in sorted(fresh):
                        q.put((res, f, "specialty"))
            if fresh:
                print(f"    + {len(fresh)} specialty code(s) not in the seed list: "
                      f"{', '.join(sorted(fresh)[:6])}"
                      f"{' ...' if len(fresh) > 6 else ''}", flush=True)
        if total or fetched:
            print(f"  {key:32s} total={str(total):>9} new={fetched:>7,} "
                  f"pages={pages:>5} {time.time()-t0:6.0f}s "
                  f"[{sinks[res].written:,} distinct so far]", flush=True)

    for res in resources:
        if res != "PractitionerRole":
            q.put((res, None, "specialty"))
            continue
        if a.pr_key == "practitioner":
            pf = out / "hcsc_Practitioner.ndjson"
            if not pf.exists():
                raise SystemExit(f"--pr-key practitioner needs {pf}; pull Practitioner first")
            pids = []
            with pf.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        pids.append(json.loads(line)["id"])
                    except Exception:
                        continue
            pids = sorted(set(pids))
            if a.only_missing_practitioners:
                covered = set()
                rf = out / "hcsc_PractitionerRole.ndjson"
                if rf.exists():
                    with rf.open("r", encoding="utf-8") as fh:
                        for line in fh:
                            try:
                                ref = (json.loads(line).get("practitioner") or {}).get("reference")
                            except Exception:
                                continue
                            if ref:
                                covered.add(ref.split("/")[-1])
                before = len(pids)
                pids = [p for p in pids if p not in covered]
                print(f"  {before:,} practitioners, {before-len(pids):,} already have a "
                      f"role in the sink, {len(pids):,} left to sweep", flush=True)
            print(f"PractitionerRole: {len(pids):,} practitioner buckets", flush=True)
            for pid in pids:
                q.put((res, pid, "practitioner"))
        else:
            codes = load_specialties(Path(a.specialties))
            queued.update(codes)
            print(f"PractitionerRole: {len(codes):,} seed specialty buckets", flush=True)
            for c in codes:
                q.put((res, c, "specialty"))

    print(f"{q.qsize():,} seed unit(s), {a.workers} worker(s) -> {out}", flush=True)

    def worker():
        while True:
            try:
                res, code, param = q.get(timeout=2)
            except queue.Empty:
                return                     # queue drained and nothing refilling it
            try:
                unit(res, code, param)
            except Exception as e:
                print(f"  {res}|{code}: {type(e).__name__}: {e}", flush=True)
            finally:
                q.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(a.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if discovered:
        Path(out / "_discovered_codes.txt").write_text(
            "\n".join(sorted(discovered)), encoding="utf-8")
        print(f"\n{len(discovered)} specialty code(s) found in the data but absent "
              f"from the published list -> _discovered_codes.txt", flush=True)

    # A read is judged complete within DRIFT_TOLERANCE of the reported total,
    # not at exactly the total. The directory is live and grows while we read
    # it: Organization finished 22 records under a total that had moved from
    # 422,050 to 422,072, and Practitioner 77 under. Demanding got >= total
    # would mean a perfect read of a growing directory never counts as
    # complete, the marker never gets written, and the watchdog relaunches for
    # ever. 0.1% is far below any real gap (PractitionerRole's shortfall was
    # 1.3%) and far above observed drift (0.005%).
    DRIFT_TOLERANCE = 0.001
    summary: dict[str, dict] = {}
    print("\n--- distinct ids collected ---", flush=True)
    with httpx.Client(headers=H, follow_redirects=True) as c:
        for res in resources:
            got = sinks[res].written
            try:
                srv = c.get(f"{BASE}/{res}", params={"_count": 1}, timeout=60).json().get("total")
            except Exception:
                srv = None
            if srv:
                pct = 100.0 * got / srv
                short = srv - got
                ok = got >= srv * (1 - DRIFT_TOLERANCE)
                if got >= srv:
                    flag = "COMPLETE"
                elif ok:
                    flag = f"COMPLETE (within drift, {short:,} under a moving total)"
                else:
                    flag = f"SHORT by {short:,}"
                print(f"  {res:24s} {got:>10,} / {srv:>10,}  ({pct:5.1f}%)  {flag}")
                summary[res] = {"collected": got, "server_total": srv,
                                "shortfall": short, "complete": ok}
            else:
                print(f"  {res:24s} {got:>10,} / server total unavailable")
                summary[res] = {"collected": got, "server_total": None,
                                "shortfall": None, "complete": False}
            sinks[res].close()

    # A completion marker, so the watchdog trigger stops relaunching once the
    # pull is done. Without it every restart would re-read ~120GB of NDJSON to
    # rebuild the dedup set before discovering there is nothing left to do.
    # Only written when every requested resource reached its server total, so a
    # short run stays restartable.
    # The marker must mean "the whole directory is pulled", because the runner
    # exits on sight of it. A run over a subset of resources cannot say that,
    # however complete that subset is. A two-resource verification run wrote a
    # marker claiming the pull was finished while PractitionerRole sat at 99.5%,
    # which would have stopped the watchdog from ever resuming it.
    covered_all = set(resources) == set(SERVED)
    if summary and covered_all and all(v["complete"] for v in summary.values()):
        (out / "_complete.json").write_text(
            json.dumps({"finished": datetime.now().isoformat(timespec="seconds"),
                        "resources": summary}, indent=1), encoding="utf-8")
        print("every resource complete -> _complete.json written", flush=True)
    else:
        short = [r for r, v in summary.items() if not v["complete"]]
        if short:
            print(f"still short on {', '.join(short)}; re-run to continue", flush=True)
        if not covered_all:
            missing = sorted(set(SERVED) - set(resources))
            print(f"no completion marker: this run covered {len(resources)} of "
                  f"{len(SERVED)} resources, omitting {', '.join(missing)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

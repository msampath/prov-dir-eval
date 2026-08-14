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
import queue
import sys
import threading
import time
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


class Sink:
    """One NDJSON file per resource type, with id-level deduplication.

    Holds the seen-id set in memory. PractitionerRole is the worst case at
    ~18.5M ids; interned strings put that near 2GB, which this box has. The
    set is rebuilt from the file on resume rather than persisted separately,
    so there is no second thing to keep consistent with the data.
    """

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.seen: set[str] = set()
        self.written = 0
        self.dupes = 0
        if path.exists():
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        self.seen.add(json.loads(line)["id"])
                    except Exception:
                        continue
            self.written = len(self.seen)
            print(f"  resume: {path.name} already holds {self.written:,} distinct ids",
                  flush=True)
        self.fh = path.open("a", encoding="utf-8")

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
                new += 1
            self.written += new
            self.fh.flush()
        return new, codes

    def close(self):
        self.fh.close()


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
    ap.add_argument("--only-missing-practitioners", action="store_true",
                    help="with --pr-key practitioner, queue only practitioners that "
                         "have no role in the sink yet. Narrows a verification sweep "
                         "to the plausible gap instead of re-reading 1.47M.")
    a = ap.parse_args()

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    H = {"client_id": client_id(), "Accept": "application/fhir+json"}
    resources = [r.strip() for r in a.resources.split(",") if r.strip()]

    sinks = {r: Sink(out / f"hcsc_{r}.ndjson") for r in resources}
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
                flag = "COMPLETE" if got >= srv else f"SHORT by {srv-got:,}"
                print(f"  {res:24s} {got:>10,} / {srv:>10,}  ({pct:5.1f}%)  {flag}")
            else:
                print(f"  {res:24s} {got:>10,} / server total unavailable")
            sinks[res].close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
"""Paginate one payer/resource to a local ndjson file instead of Postgres.

Reuses the full extract machinery (auth, per-resource quirks/page_size, bare
pagination + partition fallback, 503 handling) by handing extract_resource a
ResourceSink whose flush writes ndjson to disk instead of upserting to the DB.

Why: for flaky deep-pagination servers (Elevance et al.) the 503s hit the OAuth
token endpoint mid-walk. The slow DB-upsert path stretches each page-to-page gap
long enough that the token expires and the refresh 503s, truncating the pull. A
fast file append needs far fewer mid-walk token refreshes, so it walks deeper. It
also yields a raw ndjson archive that PHASE 2 can ingest offline.

Resumable: a sidecar <payer>_<resource>.ckpt.json records the bare-pagination
checkpoint (page URL + page count) written after every batch. On restart it seeds
from there and appends (dedup by logical id happens at ingest). A <payer>_<resource>.done
marker means the unit finished; re-running skips it. Only bare pagination is
page-resumable; partition/adaptive sweeps are deterministic and restart whole.

    python scripts/paginate_to_file.py --payer elevance --resource PractitionerRole --out-dir I:\\file_export\\elevance
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import os
import sys
import time
from pathlib import Path

CKPT_TTL_S = 72 * 3600  # a directory can regenerate and shift offsets; don't
#                         resume a stale cursor across that boundary.
# Consecutive all-duplicate batches tolerated after a resume before declaring the
# cursor replayed. One or two is normal (a checkpoint deliberately rewinds a
# little so no record is skipped); a sustained run means the server restarted the
# chain and no amount of further paging will advance it.
REPLAY_LIMIT = 3


class ReplayedCursor(RuntimeError):
    """A resumed cursor is re-serving records instead of advancing."""


def _digest(rid: str) -> int:
    """8-byte digest of a logical id, for a memory-bounded seen-set."""
    return int.from_bytes(hashlib.blake2b(rid.encode("utf-8"), digest_size=8).digest(),
                          "big")

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import get_settings, load_manifest          # noqa: E402
from provdir.http_client import FhirSession                     # noqa: E402
from provdir.etl.extract import ResourceSink, extract_resource  # noqa: E402

# methods that mean "the unit finished" (mark .done, drop the checkpoint). Use the
# ":"-suffixed forms so failure labels don't match: "partition:<param>" is a
# successful sweep, but "partition-failed" / "needs-partition" / "blocked" are
# failures that must retry. A bare walk is only done when it exhausted.
_DONE_METHODS = ("bare", "partition:", "adaptive:", "unsupported")


async def _run(payer: str, rtype: str, out_dir: str, max_pages: int | None,
               force_bare: bool = False) -> int:
    ep = load_manifest().by_key(payer)
    settings = get_settings()
    if force_bare and (ep.quirks.adaptive or {}):
        # Drop the adaptive / reference-graph (id_chain/id_read/_include) config so
        # extract_resource runs a plain bare paginated search instead. Experiment:
        # does bare pagination work for these when it's fast + file-backed?
        ep = copy.deepcopy(ep)  # detach from the lru_cache'd manifest before mutating
        ep.quirks.adaptive = None
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    dest = out / f"{payer}_{rtype}.ndjson"
    done = out / f"{payer}_{rtype}.done"
    ckpt_path = out / f"{payer}_{rtype}.ckpt.json"

    if done.exists():
        print(f"SKIP {payer}/{rtype}: already complete", flush=True)
        return 0

    q = ep.quirks
    psize = (q.page_size_by_resource or {}).get(rtype) or q.page_size or settings.http_default_count
    # Fingerprint the request shape (base URL, resource, page size, bare vs adaptive,
    # scope params). A checkpoint only resumes when the shape still matches AND it is
    # within TTL, so a regenerated directory or a strategy switch starts fresh.
    fp = hashlib.sha256(
        f"{ep.base_url}|{rtype}|{psize}|{force_bare}|{sorted((q.base_params or {}).items())}"
        .encode()).hexdigest()[:16]

    resume_ckpt = None
    mode = "wb"
    if ckpt_path.exists() and dest.exists():
        try:
            c = json.loads(ckpt_path.read_text())
            age = time.time() - ckpt_path.stat().st_mtime
            if c.get("fp") == fp and age <= CKPT_TTL_S:
                resume_ckpt = c
                mode = "ab"
                print(f"resuming {payer}/{rtype} from page {c.get('pages_done')} "
                      f"({ckpt_path.name})", flush=True)
            else:
                why = "fingerprint changed" if c.get("fp") != fp else f"stale ({age/3600:.0f}h)"
                print(f"discarding checkpoint for {payer}/{rtype}: {why}; fresh pull", flush=True)
        except (ValueError, OSError):
            resume_ckpt = None
    if mode == "wb":
        ckpt_path.unlink(missing_ok=True)  # no stale cursor for a fresh page-1 walk

    # Ids already on disk, as 8-byte digests. Two jobs, both learned the hard
    # way on premera/Opala: (1) drop replayed records instead of appending them,
    # and (2) notice when a resumed cursor is replaying rather than advancing.
    #
    # Opala's opaque ?q= token is session-bound, like Kaiser's _getpages and
    # Aetna's _page_token. Resuming from a saved one does NOT continue the walk
    # -- the server restarts the chain. Without this guard a resume appended
    # 20,050 duplicate Organization rows over 400 pages while the distinct
    # count went DOWN, and reported it as progress the whole way.
    #
    # Digests, not ids: a full id set for a multi-million-row unit (elevance
    # PractitionerRole is 5.76M) costs hundreds of MB of strings; 8-byte
    # digests keep it to tens of MB. Collisions would drop a record, so the
    # digest is wide enough that at 10M ids the odds are ~1 in 4x10^5.
    seen: set[int] = set()
    if mode == "ab" and dest.exists():
        print(f"indexing existing {dest.name} for replay detection...", flush=True)
        with open(dest, "rb") as ex:
            for line in ex:
                line = line.strip()
                if not line:
                    continue
                try:
                    rid = json.loads(line).get("id")
                except ValueError:
                    continue
                if rid:
                    seen.add(_digest(rid))
        print(f"  indexed {len(seen):,} existing id(s)", flush=True)

    fh = open(dest, mode)
    # Count DISTINCT records on disk, not lines: a pre-guard resume may have
    # left duplicates, and reporting lines as progress is what hid the bug.
    written = {"n": len(seen) if mode == "ab" else 0}
    progress: dict = {}
    replay = {"pages": 0, "dupes": 0}

    def write_ckpt() -> None:
        # page_url is None on page 1 (fresh search); checkpoint starts at page 2+.
        if not progress.get("page_url"):
            return
        tmp = ckpt_path.with_suffix(".json.tmp")
        with open(tmp, "w") as cf:
            cf.write(json.dumps({
                "resume_url": progress.get("page_url"),
                "pages_done": progress.get("pages", 0),
                "rows_added": written["n"],
                "page_size": psize,
                "fp": fp,
            }))
            cf.flush()
            os.fsync(cf.fileno())
        os.replace(tmp, ckpt_path)

    async def flush(batch: list[dict]) -> int:
        fresh = []
        for r in batch:
            rid = r.get("id")
            d = _digest(rid) if rid else None
            if d is not None and d in seen:
                continue
            if d is not None:
                seen.add(d)
            fresh.append(r)
        dupes = len(batch) - len(fresh)
        replay["dupes"] += dupes
        # A batch that is ENTIRELY records we already hold means the cursor is
        # re-serving, not advancing. One such batch is normal right after a
        # resume (checkpoints intentionally rewind a little); a run of them
        # means the token reset the chain.
        if batch and not fresh:
            replay["pages"] += 1
            if replay["pages"] >= REPLAY_LIMIT:
                raise ReplayedCursor(
                    f"{replay['pages']} consecutive batches with no new records "
                    f"({replay['dupes']:,} duplicates skipped): the resume cursor "
                    "is replaying, not advancing"
                )
        else:
            replay["pages"] = 0

        if fresh:
            buf = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in fresh)
            fh.write(buf.encode("utf-8"))
            fh.flush()
            os.fsync(fh.fileno())  # durability before the checkpoint naming this data
        prev = written["n"]
        written["n"] += len(fresh)
        write_ckpt()
        if prev // 100000 != written["n"] // 100000:
            print(f"  written {written['n']:,} -> {dest.name}", flush=True)
        # Report what was CONSUMED, not what was kept: the caller's page
        # accounting must still line up with the server's pages.
        return len(batch)

    sink = ResourceSink(flush, batch=5000)
    print(f"paginate {payer}/{rtype} -> {dest} (mode={mode})", flush=True)
    replayed = False
    try:
        async with FhirSession(settings) as s:
            client = s.client_for(ep)
            stats = await extract_resource(client, ep, rtype, sink,
                                           max_pages=max_pages,
                                           resume_ckpt=resume_ckpt, progress=progress)
            await sink.close()
    except ReplayedCursor as exc:
        # Not a crash: the walk simply cannot continue from this cursor. Drop
        # the checkpoint so the next attempt starts a FRESH walk rather than
        # replaying again, and leave the unit un-.done so nothing mistakes the
        # partial file for a finished one.
        replayed = True
        ckpt_path.unlink(missing_ok=True)
        stats = {"method": "bare", "note": f"cursor replayed: {exc}"}
        print(f"REPLAYED premera-style cursor on {payer}/{rtype}: {exc}. "
              f"Checkpoint dropped -- this unit is NOT resumable; re-running "
              f"starts a fresh walk from page 1.", flush=True)
    finally:
        fh.close()

    method = stats.get("method") or ""
    note = stats.get("note")
    nl = (note or "").lower()
    bare_incomplete = progress.get("active") and not progress.get("exhausted")
    # Any truncation/error signal in the note means the pull did NOT finish, no
    # matter how many rows it got: partition/adaptive page caps ("budget", "our
    # cap"), mid-walk stops ("stopped"), truncated buckets, or fetch errors. This
    # is the key guard for --force-bare, where the partition fallback caps at ~40
    # pages and would otherwise mark a 0.2%-complete pull .done.
    truncated = any(k in nl for k in (
        "stopped", "budget", "truncat", "more available", "our cap",
        "fetch error", "bucket", "timeout", "rejected", "error", "failed"))
    # Even a "clean" bare walk can be silently capped by the server (exhausted, no
    # next link) far below the real total -> check against the server denominator.
    st = stats.get("server_total")
    short = isinstance(st, (int, float)) and st > 0 and written["n"] < 0.9 * st
    # `replayed` is checked explicitly rather than left to the `truncated`
    # keyword scan: a replayed cursor reports method="bare" with a note that
    # matches none of those keywords and no server_total to fail `short`, so
    # without this it would sail through and mark a partial unit .done -- the
    # exact silent-success this guard exists to prevent.
    finished = (any(method.startswith(m) for m in _DONE_METHODS)
                and not bare_incomplete and not truncated and not short
                and not replayed)
    if finished:
        ckpt_path.unlink(missing_ok=True)
        done.write_text(json.dumps({"method": method, "written": written["n"], "note": note}))
        tag = "COMPLETE"
    elif bare_incomplete:
        tag = "INCOMPLETE (resumable)"
    elif short:
        tag = f"SHORT {written['n']:,}/{st:,} (bare insufficient)"
    elif truncated:
        tag = "TRUNCATED (retry)"
    else:
        tag = f"NOT-DONE ({method})"
    print(f"{tag} {payer}/{rtype}: written={written['n']:,} method={method} "
          f"pages={stats.get('pages')} server_total={stats.get('server_total')} note={note}",
          flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payer", required=True)
    ap.add_argument("--resource", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--max-pages", type=int, default=None,
                    help="cap pages (smoke test); default unbounded")
    ap.add_argument("--force-bare", action="store_true",
                    help="ignore adaptive/reference-graph config; try plain bare pagination")
    a = ap.parse_args()
    return asyncio.run(_run(a.payer, a.resource, a.out_dir, a.max_pages, a.force_bare))


if __name__ == "__main__":
    raise SystemExit(main())

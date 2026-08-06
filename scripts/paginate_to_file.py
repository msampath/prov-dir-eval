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

    fh = open(dest, mode)
    written = {"n": (resume_ckpt or {}).get("rows_added", 0)}
    progress: dict = {}

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
        buf = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in batch)
        fh.write(buf.encode("utf-8"))
        fh.flush()
        os.fsync(fh.fileno())  # durability before the checkpoint that names this data
        prev = written["n"]
        written["n"] += len(batch)
        write_ckpt()
        if prev // 100000 != written["n"] // 100000:
            print(f"  written {written['n']:,} -> {dest.name}", flush=True)
        return len(batch)

    sink = ResourceSink(flush, batch=5000)
    print(f"paginate {payer}/{rtype} -> {dest} (mode={mode})", flush=True)
    try:
        async with FhirSession(settings) as s:
            client = s.client_for(ep)
            stats = await extract_resource(client, ep, rtype, sink,
                                           max_pages=max_pages,
                                           resume_ckpt=resume_ckpt, progress=progress)
            await sink.close()
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
    finished = (any(method.startswith(m) for m in _DONE_METHODS)
                and not bare_incomplete and not truncated and not short)
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

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
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import get_settings, load_manifest          # noqa: E402
from provdir.http_client import FhirSession                     # noqa: E402
from provdir.etl.extract import ResourceSink, extract_resource  # noqa: E402

# methods that mean "the unit finished" (mark .done, drop the checkpoint). A bare
# walk is only done when it exhausted (progress.exhausted); needs-partition and
# blocked are 503/refused failures that should retry on the next run, not be marked.
_DONE_METHODS = ("bare", "partition", "adaptive", "unsupported")


async def _run(payer: str, rtype: str, out_dir: str, max_pages: int | None) -> int:
    ep = load_manifest().by_key(payer)
    settings = get_settings()
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

    resume_ckpt = None
    mode = "wb"
    if ckpt_path.exists() and dest.exists():
        try:
            resume_ckpt = json.loads(ckpt_path.read_text())
            mode = "ab"
            print(f"resuming {payer}/{rtype} from page {resume_ckpt.get('pages_done')} "
                  f"({ckpt_path.name})", flush=True)
        except (ValueError, OSError):
            resume_ckpt = None
            mode = "wb"

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
            }))
            cf.flush()
            os.fsync(cf.fileno())
        os.replace(tmp, ckpt_path)

    async def flush(batch: list[dict]) -> int:
        buf = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in batch)
        fh.write(buf.encode("utf-8"))
        fh.flush()
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
    bare_incomplete = progress.get("active") and not progress.get("exhausted")
    finished = (any(method.startswith(m) for m in _DONE_METHODS)) and not bare_incomplete
    if finished:
        ckpt_path.unlink(missing_ok=True)
        done.write_text(json.dumps({"method": method, "written": written["n"], "note": note}))
        tag = "COMPLETE"
    else:
        tag = "INCOMPLETE (resumable)" if bare_incomplete else f"NOT-DONE ({method})"
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
    a = ap.parse_args()
    return asyncio.run(_run(a.payer, a.resource, a.out_dir, a.max_pages))


if __name__ == "__main__":
    raise SystemExit(main())

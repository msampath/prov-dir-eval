#!/usr/bin/env python
"""Pull one resource type via FHIR Bulk Data $export and load it into Postgres.

Reuses the project's transform + upsert machinery, so rows land identically to the
paginated ETL (same columns, same last_seen_at stamping, ON CONFLICT DO UPDATE).
Writes a provenance row with method=bulk-export and the bulk line count as the
server_total (the independent denominator the search API refuses to give).

    python scripts/bulk_ingest.py --payer aetna_cvs --type PractitionerRole

Resumable: Aetna's PractitionerRole export is a single ~1.6 TB ndjson file. A plain
stream cannot survive that (a mid-stream TCP drop or the signed URL expiring loses
the whole download). This script:
  * streams raw bytes (Accept-Encoding: identity, so wire offset == file offset)
    and tracks an absolute byte offset at line granularity;
  * persists {file_index, byte_offset, lines} to a sidecar checkpoint on every
    5k-row commit (fsync'd; survives process/PC restart);
  * on a connection drop, reconnects with `Range: bytes=<offset>-` and stitches
    the stream transparently (upsert is idempotent, so any boundary overlap is
    harmless);
  * detects a silent truncation (stream ends before Content-Range total) and
    reconnects rather than declaring the file complete;
  * on signed-URL expiry (403/410), re-fetches the completed export manifest from
    the status URL to get a freshly-signed URL for the *same* pre-generated file
    (identical byte layout), matched by file basename, then resumes from offset.
Re-run the same command to resume; it skips kickoff when a live checkpoint exists.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import get_settings, load_manifest              # noqa: E402
from provdir.etl.loader import (                                    # noqa: E402
    count_rows, insert_provenance, latest_prior_count, pg_connection,
    prepare_stage, schema_for, upsert_batch,
)
from provdir.etl.transform import TransformError, transform_resource  # noqa: E402
from provdir.models import RESOURCE_TABLES                          # noqa: E402
from provdir.http_client import FhirSession as FhirSessionLocal     # noqa: E402

BATCH = 5000
POLL_SECONDS = 15
MAX_POLLS = 360           # 360 * 15s = 90 min to generate the export
MAX_RECONNECTS = 40       # consecutive failures (no progress) before giving up
CKPT_DIR = REPO / "output" / "orchestrator"

# httpx exceptions that mean "the stream broke, reconnect and continue".
TRANSIENT = (
    httpx.RemoteProtocolError, httpx.ReadError, httpx.ReadTimeout,
    httpx.ConnectError, httpx.ConnectTimeout, httpx.WriteError,
    httpx.PoolTimeout,
)


class ExpiredURL(Exception):
    """The signed output URL is no longer valid; caller must refresh it."""


class TransientHTTP(Exception):
    """A retryable HTTP status (429 / 5xx) from the file host."""


def mint_auth(payer: str) -> dict:
    async def _m():
        ep = load_manifest().by_key(payer)
        async with FhirSessionLocal(get_settings()) as s:
            c = s.client_for(ep)
            return await c._auth.headers(c._client)
    return asyncio.run(_m())


def ckpt_path(payer: str, rtype: str) -> Path:
    return CKPT_DIR / f"bulk_{payer}_{rtype}.ckpt.json"


def load_ckpt(payer: str, rtype: str) -> dict | None:
    p = ckpt_path(payer, rtype)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except (ValueError, OSError):
        return None


def save_ckpt(payer: str, rtype: str, data: dict) -> None:
    p = ckpt_path(payer, rtype)
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        f.write(json.dumps(data))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)  # atomic on Windows + POSIX


def clear_ckpt(payer: str, rtype: str) -> None:
    ckpt_path(payer, rtype).unlink(missing_ok=True)


def basename_of(url: str) -> str:
    return url.rsplit("/", 1)[-1].split("?", 1)[0]


def kickoff(hx: httpx.Client, base: str, rtype: str, auth: dict) -> str:
    r = hx.get(f"{base}/$export", params={"_type": rtype},
               headers={**auth, "Accept": "application/fhir+json", "Prefer": "respond-async"})
    if r.status_code != 202:
        raise SystemExit(f"kickoff failed: {r.status_code} {r.text[:500]}")
    status_url = r.headers.get("Content-Location")
    print(f"export accepted; status_url={status_url}", flush=True)
    return status_url


def poll_manifest(hx: httpx.Client, status_url: str, auth_holder: dict, payer: str) -> dict:
    """Block until the export job completes; return its completion manifest."""
    for i in range(MAX_POLLS):
        time.sleep(POLL_SECONDS)
        pr = hx.get(status_url, headers={**auth_holder["h"], "Accept": "application/json"})
        if pr.status_code == 202:
            if i % 4 == 0:
                print(f"  [{i}] {pr.headers.get('X-Progress')}", flush=True)
            continue
        if pr.status_code == 401:
            auth_holder["h"] = mint_auth(payer)
            continue
        if pr.status_code == 200:
            return pr.json()
        raise SystemExit(f"poll error: {pr.status_code} {pr.text[:300]}")
    raise SystemExit("timed out waiting for export")


def refresh_manifest(hx: httpx.Client, status_url: str, auth_holder: dict, payer: str) -> dict:
    """Re-fetch the completed manifest (fresh signed URLs for the same files)."""
    pr = hx.get(status_url, headers={**auth_holder["h"], "Accept": "application/json"})
    if pr.status_code == 401:
        auth_holder["h"] = mint_auth(payer)
        pr = hx.get(status_url, headers={**auth_holder["h"], "Accept": "application/json"})
    pr.raise_for_status()
    return pr.json()


def _range_total(resp: httpx.Response, offset: int) -> int | None:
    """Absolute file size implied by the response headers, or None if unknowable."""
    cr = resp.headers.get("content-range")  # "bytes start-end/total"
    if cr and "/" in cr:
        tot = cr.rsplit("/", 1)[1].strip()
        if tot.isdigit():
            return int(tot)
    cl = resp.headers.get("content-length")
    if cl and cl.isdigit():
        return int(cl) if resp.status_code == 200 else offset + int(cl)
    return None


def resumable_lines(hx: httpx.Client, url_holder: dict, auth_holder: dict,
                    refresh, payer: str, start: int, send_auth: bool):
    """Yield (line_bytes, offset_after) for one ndjson file, resuming across
    connection drops, transient 5xx, and signed-URL expiry. `offset_after` is the
    absolute byte position immediately past this line's trailing newline (a safe
    resume boundary). url_holder["u"] holds the current signed URL.
    """
    offset = start
    buf = b""
    fails = 0
    total: int | None = None
    while True:
        attempt_start = offset  # to reset the failure streak on any real progress
        headers = {"Range": f"bytes={offset}-", "Accept-Encoding": "identity"}
        if send_auth:
            headers.update(auth_holder["h"])
        try:
            with hx.stream("GET", url_holder["u"], headers=headers) as resp:
                if resp.status_code == 401:
                    fails += 1
                    if fails > MAX_RECONNECTS:
                        raise ExpiredURL("repeated 401 on output URL")
                    auth_holder["h"] = mint_auth(payer)
                    time.sleep(min(5 * fails, 60))
                    continue
                if resp.status_code in (403, 410):
                    raise ExpiredURL(f"{resp.status_code} on output URL")
                if resp.status_code == 416:
                    return  # Range past EOF => file already fully consumed
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise TransientHTTP(f"{resp.status_code} on output URL")
                resp.raise_for_status()
                if resp.headers.get("content-encoding"):
                    raise SystemExit(
                        "output stream is "
                        f"{resp.headers['content-encoding']}-encoded despite "
                        "Accept-Encoding: identity; byte-range resume is unsafe")
                # a 206 must start exactly where we asked, or offsets diverge
                if resp.status_code == 206:
                    cr = resp.headers.get("content-range", "")
                    startstr = cr[6:].split("-", 1)[0].strip() if cr.startswith("bytes ") else ""
                    if startstr.isdigit() and int(startstr) != offset:
                        raise SystemExit(
                            f"206 Content-Range start {startstr} != requested {offset}")
                if total is None:
                    total = _range_total(resp, offset)
                # If the server ignored Range (200), it restarts at byte 0 and we
                # must discard the first `offset` bytes ourselves.
                skip = 0 if resp.status_code == 206 else offset
                for chunk in resp.iter_bytes():
                    if skip:
                        if len(chunk) <= skip:
                            skip -= len(chunk)
                            continue
                        chunk = chunk[skip:]
                        skip = 0
                    buf += chunk
                    nl = buf.find(b"\n")
                    while nl >= 0:
                        line = buf[:nl]
                        buf = buf[nl + 1:]
                        offset += nl + 1
                        yield line, offset
                        nl = buf.find(b"\n")
                # A length-bearing response that ended early (without raising) is a
                # silent truncation, not real EOF. Check BEFORE emitting the tail:
                # the buffered remainder is a partial record, not the file's final
                # newline-less line, so discard it (don't advance offset) and
                # reconnect — emitting it would drop that record and desync offset.
                if total is not None and offset + len(buf) < total:
                    fails = 1 if offset > attempt_start else fails + 1
                    if fails > MAX_RECONNECTS:
                        raise RuntimeError(
                            f"truncated at {offset:,}/{total:,} after {fails} tries")
                    print(f"  silent truncation at {offset:,}/{total:,}; "
                          f"reconnect #{fails}", flush=True)
                    buf = b""
                    time.sleep(min(5 * fails, 120))
                    continue
                # genuine EOF: emit a trailing newline-less final line if present
                if buf:
                    offset += len(buf)
                    tail, buf = buf, b""
                    yield tail, offset
                return
        except ExpiredURL as e:
            fails = 1 if offset > attempt_start else fails + 1
            if fails > MAX_RECONNECTS:
                raise
            print(f"  url expired ({e}); refreshing signed URL, resume at "
                  f"byte {offset:,}", flush=True)
            url_holder["u"] = refresh()
            buf = b""
            time.sleep(min(5 * fails, 60))
            continue
        except (TransientHTTP, *TRANSIENT) as e:
            fails = 1 if offset > attempt_start else fails + 1
            if fails > MAX_RECONNECTS:
                raise
            print(f"  stream drop ({type(e).__name__}: {e}); reconnect #{fails} "
                  f"Range from byte {offset:,}", flush=True)
            buf = b""
            time.sleep(min(5 * fails, 120))
            continue


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payer", required=True)
    ap.add_argument("--type", required=True, dest="rtype")
    ap.add_argument("--fresh", action="store_true",
                    help="ignore/clear any existing checkpoint and re-export")
    args = ap.parse_args()
    payer, rtype = args.payer, args.rtype

    ep = load_manifest().by_key(payer)
    base = ep.base_url
    table = RESOURCE_TABLES[rtype]
    schema = schema_for(payer)
    CKPT_DIR.mkdir(parents=True, exist_ok=True)

    if args.fresh:
        clear_ckpt(payer, rtype)
    ckpt = load_ckpt(payer, rtype)

    auth_holder = {"h": mint_auth(payer)}
    with httpx.Client(timeout=httpx.Timeout(600.0, read=600.0),
                      follow_redirects=True) as hx:
        if ckpt and ckpt.get("status_url"):
            print(f"resuming from checkpoint: file {ckpt['file_index']} "
                  f"byte {ckpt['byte_offset']:,} lines {ckpt['lines']:,}", flush=True)
            status_url = ckpt["status_url"]
            manifest = refresh_manifest(hx, status_url, auth_holder, payer)
            started = datetime.fromisoformat(ckpt["started_at"])
            file_index0 = ckpt["file_index"]
            byte_offset0 = ckpt["byte_offset"]
            total_lines = ckpt["lines"]
            errors = ckpt.get("errors", 0)
            resuming = True
        else:
            started = datetime.now(tz=timezone.utc)
            status_url = kickoff(hx, base, rtype, auth_holder["h"])
            manifest = poll_manifest(hx, status_url, auth_holder, payer)
            file_index0, byte_offset0, total_lines, errors = 0, 0, 0, 0
            resuming = False

        requires_token = manifest.get("requiresAccessToken", True)
        outputs = [o for o in (manifest.get("output") or []) if o.get("type") == rtype]
        print(f"manifest: {len(outputs)} file(s) for {rtype}; "
              f"requiresAccessToken={requires_token}", flush=True)
        if resuming and not outputs:
            raise SystemExit("resume: manifest has no output files (export purged?); "
                             "delete the .ckpt.json and re-run to re-export")

        # fresh token for the (potentially multi-day) download phase
        auth_holder["h"] = mint_auth(payer)
        conn = pg_connection(schema)
        prepare_stage(conn, table)
        batch: list[dict] = []
        fi = 0
        off = byte_offset0

        def persist(pos: int) -> None:
            save_ckpt(payer, rtype, {
                "status_url": status_url, "file_index": fi, "byte_offset": pos,
                "lines": total_lines, "errors": errors,
                "started_at": started.isoformat(),
            })

        try:
            for fi, out in enumerate(outputs):
                if fi < file_index0:
                    continue
                start = byte_offset0 if fi == file_index0 else 0
                url_holder = {"u": out["url"]}

                def refresh(want=out["url"], idx=fi):
                    fresh = [o for o in refresh_manifest(hx, status_url, auth_holder, payer)
                             .get("output", []) if o.get("type") == rtype]
                    for o in fresh:
                        if basename_of(o["url"]) == basename_of(want):
                            return o["url"]
                    if idx < len(fresh):
                        return fresh[idx]["url"]
                    raise RuntimeError(
                        f"cannot locate {basename_of(want)} in refreshed manifest")

                for line, off in resumable_lines(hx, url_holder, auth_holder,
                                                 refresh, payer, start, requires_token):
                    if not line.strip():
                        continue
                    total_lines += 1
                    try:
                        row = transform_resource(json.loads(line), payer, base)
                        batch.append(row)
                    except (TransformError, ValueError, TypeError):
                        errors += 1
                        continue
                    if len(batch) >= BATCH:
                        upsert_batch(conn, table, batch, update=True)
                        conn.commit()
                        batch = []
                        persist(off)  # off = boundary past last committed line
                        if total_lines % 500000 < BATCH:
                            print(f"  ingested ~{total_lines:,} lines "
                                  f"(byte {off:,})", flush=True)
                # flush the file's tail
                if batch:
                    upsert_batch(conn, table, batch, update=True)
                    conn.commit()
                    batch = []
                    persist(off)
                print(f"  file {fi} done; cumulative lines={total_lines:,}", flush=True)

            loaded = count_rows(conn, table)
            prior = latest_prior_count(conn, payer, rtype)
            pct = round(100.0 * (loaded - prior) / prior, 1) if prior else None
            cov = round(100.0 * loaded / total_lines, 1) if total_lines else None
            notes = {
                "method": "bulk-export", "server_total": total_lines,
                "server_total_source": "bulk", "coverage_pct": cov,
                "transform_errors": errors, "fetch_errors": 0, "extract_error": None,
                "note": None, "residual_unreachable": max(0, total_lines - loaded),
                "partitions": 0,
            }
            insert_provenance(conn, {
                "payer_id": payer, "source_base_url": base, "resource_type": rtype,
                "started_at": started, "finished_at": datetime.now(tz=timezone.utc),
                "status": "ok", "page_count": 0, "resource_count": loaded,
                "error_count": errors, "prior_count": prior, "pct_change": pct, "notes": notes,
            })
            conn.commit()
            clear_ckpt(payer, rtype)
            print(f"DONE {payer}/{rtype}: bulk_lines={total_lines:,} "
                  f"loaded_distinct={loaded:,} transform_errors={errors}", flush=True)
        except BaseException:
            # persist whatever committed so a re-run resumes instead of restarting
            if batch:
                try:
                    upsert_batch(conn, table, batch, update=True)
                    conn.commit()
                    persist(off)
                except Exception:
                    pass
            raise
        finally:
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

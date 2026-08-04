#!/usr/bin/env python
"""Pull one resource type via FHIR Bulk Data $export and load it into Postgres.

Reuses the project's transform + upsert machinery, so rows land identically to the
paginated ETL (same columns, same last_seen_at stamping, ON CONFLICT DO UPDATE).
Writes a provenance row with method=bulk-export and the bulk line count as the
server_total (the independent denominator the search API refuses to give).

    python scripts/bulk_ingest.py --payer aetna_cvs --type PractitionerRole
"""
from __future__ import annotations

import argparse
import asyncio
import json
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

BATCH = 5000
POLL_SECONDS = 15
MAX_POLLS = 360  # 360 * 15s = 90 min


def mint_auth(payer: str) -> dict:
    async def _m():
        ep = load_manifest().by_key(payer)
        async with FhirSessionLocal(get_settings()) as s:
            c = s.client_for(ep)
            return await c._auth.headers(c._client)
    return asyncio.run(_m())


# import here so the module-level import list stays clean
from provdir.http_client import FhirSession as FhirSessionLocal      # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payer", required=True)
    ap.add_argument("--type", required=True, dest="rtype")
    args = ap.parse_args()
    payer, rtype = args.payer, args.rtype

    ep = load_manifest().by_key(payer)
    base = ep.base_url
    table = RESOURCE_TABLES[rtype]
    schema = schema_for(payer)
    started = datetime.now(tz=timezone.utc)

    auth = mint_auth(payer)
    with httpx.Client(timeout=httpx.Timeout(600.0), follow_redirects=True) as hx:
        # 1. kickoff
        r = hx.get(f"{base}/$export", params={"_type": rtype},
                   headers={**auth, "Accept": "application/fhir+json", "Prefer": "respond-async"})
        if r.status_code != 202:
            print(f"kickoff failed: {r.status_code} {r.text[:500]}", flush=True)
            return 1
        status_url = r.headers.get("Content-Location")
        print(f"export accepted; status_url={status_url}", flush=True)

        # 2. poll
        manifest = None
        for i in range(MAX_POLLS):
            time.sleep(POLL_SECONDS)
            pr = hx.get(status_url, headers={**auth, "Accept": "application/json"})
            if pr.status_code == 202:
                if i % 4 == 0:
                    print(f"  [{i}] {pr.headers.get('X-Progress')}", flush=True)
                continue
            if pr.status_code == 200:
                manifest = pr.json()
                break
            print(f"poll error: {pr.status_code} {pr.text[:300]}", flush=True)
            return 1
        if manifest is None:
            print("timed out waiting for export", flush=True)
            return 1

        outputs = [o for o in (manifest.get("output") or []) if o.get("type") == rtype]
        print(f"manifest ready: {len(outputs)} file(s) for {rtype}", flush=True)

        # 3. ingest (fresh token after the poll wait)
        auth = mint_auth(payer)
        conn = pg_connection(schema)
        prepare_stage(conn, table)
        total_lines = 0
        errors = 0
        try:
            for oi, out in enumerate(outputs):
                url = out["url"]
                batch = []
                attempt = 0
                while True:
                    try:
                        with hx.stream("GET", url, headers=auth) as resp:
                            if resp.status_code == 401 and attempt == 0:
                                attempt += 1
                                auth = mint_auth(payer)
                                continue
                            resp.raise_for_status()
                            for line in resp.iter_lines():
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
                                    if total_lines % 100000 < BATCH:
                                        print(f"  ingested ~{total_lines:,} lines", flush=True)
                        break
                    except httpx.HTTPStatusError as exc:
                        print(f"  file {oi} http error: {exc}", flush=True)
                        break
                if batch:
                    upsert_batch(conn, table, batch, update=True)
                    conn.commit()
                print(f"  file {oi} done; cumulative lines={total_lines:,}", flush=True)

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
            print(f"DONE {payer}/{rtype}: bulk_lines={total_lines:,} loaded_distinct={loaded:,} "
                  f"transform_errors={errors}", flush=True)
        finally:
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

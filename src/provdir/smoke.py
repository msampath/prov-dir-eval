"""Phase 2 — Connection smoke test.

GET /metadata on each endpoint in the subset; record HTTP status, FHIR version,
software name/version, and latency to output/smoke_results.json.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Optional

from . import OUTPUT_DIR
from .config import Endpoint, Settings, get_settings, load_manifest
from .http_client import FhirSession
from .logging_setup import get_logger

log = get_logger(__name__)


async def _probe(session: FhirSession, ep: Endpoint) -> dict:
    result: dict = {
        "key": ep.key,
        "payer_name": ep.payer_name,
        "base_url": ep.base_url,
        "auth_strategy": ep.auth.strategy,
    }
    client = session.client_for(ep)
    started = time.monotonic()
    try:
        if ep.quirks.no_metadata:
            live = await client.liveness()
            result["latency_ms"] = round((time.monotonic() - started) * 1000)
            result["http_status"] = 200
            result["ok"] = True
            result["metadata_available"] = False
            result["liveness_resource"] = live["resource"]
            result["liveness_total"] = live["total"]
            log.info(
                "smoke OK  %-14s (no /metadata; live via %s, total=%s, %dms)",
                ep.key,
                live["resource"],
                live["total"],
                result["latency_ms"],
            )
            return result

        meta = await client.metadata()
        result["latency_ms"] = round((time.monotonic() - started) * 1000)
        result["http_status"] = 200
        result["ok"] = True
        result["metadata_available"] = True
        result["fhir_version"] = meta.get("fhirVersion")
        result["resource_type"] = meta.get("resourceType")
        software = meta.get("software") or {}
        result["software_name"] = software.get("name")
        result["software_version"] = software.get("version")
        rest = (meta.get("rest") or [{}])[0]
        result["declared_resources"] = sorted(
            {r.get("type") for r in rest.get("resource", []) if r.get("type")}
        )
        log.info(
            "smoke OK  %-14s %s (%s %s, %dms)",
            ep.key,
            result.get("fhir_version"),
            result.get("software_name"),
            result.get("software_version"),
            result["latency_ms"],
        )
    except Exception as exc:  # noqa: BLE001 - record any failure per endpoint
        result["latency_ms"] = round((time.monotonic() - started) * 1000)
        result["ok"] = False
        result["error"] = f"{type(exc).__name__}: {exc}"
        status = getattr(exc, "status", None) or getattr(
            getattr(exc, "response", None), "status_code", None
        )
        result["http_status"] = status
        log.warning("smoke FAIL %-14s %s", ep.key, result["error"])
    return result


async def run_smoke(
    keys: Optional[list[str]] = None,
    include_all_known: bool = False,
    settings: Optional[Settings] = None,
) -> dict:
    settings = settings or get_settings()
    manifest = load_manifest()

    if keys:
        endpoints = manifest.subset(keys)
    elif include_all_known:
        endpoints = manifest.known()
    else:
        endpoints = manifest.mvp()

    runnable, skipped = [], []
    for ep in endpoints:
        reason = ep.skip_reason(settings)
        # For smoke we still attempt 'unconfirmed-auth' open endpoints; only skip
        # those genuinely missing credentials or blocked.
        if reason and (reason.startswith("missing-credentials") or reason in {"blocked", "missing-token-url"}):
            skipped.append({"key": ep.key, "reason": reason})
            log.info("smoke SKIP %-14s (%s)", ep.key, reason)
        else:
            runnable.append(ep)

    async with FhirSession(settings) as session:
        results = await asyncio.gather(*(_probe(session, ep) for ep in runnable))

    summary = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "attempted": len(runnable),
        "ok": sum(1 for r in results if r.get("ok")),
        "failed": sum(1 for r in results if not r.get("ok")),
        "skipped": skipped,
        "results": results,
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / "smoke_results.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info(
        "smoke complete: %d/%d OK, %d failed, %d skipped -> %s",
        summary["ok"],
        summary["attempted"],
        summary["failed"],
        len(skipped),
        out,
    )
    return summary

"""Phase 1 — Endpoint inventory: validate the manifest, emit a summary.

Produces `output/inventory_manifest.json` summarizing KNOWN / UNKNOWN / BLOCKED
counts and the distinct-endpoint list, and validates that every endpoint carries
a syntactically valid HTTPS base URL (enforced at load by the pydantic model).
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import OUTPUT_DIR
from .config import Settings, get_settings, load_manifest
from .logging_setup import get_logger

log = get_logger(__name__)


def build_inventory(settings: Optional[Settings] = None) -> dict:
    settings = settings or get_settings()
    manifest = load_manifest()
    status_counts = Counter(e.status for e in manifest.endpoints)

    endpoints = []
    for ep in manifest.endpoints:
        endpoints.append(
            {
                "key": ep.key,
                "payer_name": ep.payer_name,
                "parent_org": ep.parent_org,
                "brands": ep.brands,
                "brand_count": len(ep.brands),
                "base_url": ep.base_url,
                "host": ep.host,
                "status": ep.status,
                "mvp": ep.mvp,
                "auth_strategy": ep.auth.strategy,
                "runnable": ep.is_runnable(settings),
                "skip_reason": ep.skip_reason(settings),
                "resource_subset": ep.resource_subset,
            }
        )

    distinct_hosts = sorted({e.host for e in manifest.endpoints})
    total_brands = sum(len(e.brands) for e in manifest.endpoints)

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "ig_version": manifest.ig_version,
        "fhir_version": manifest.fhir_version,
        "plannet_resources": manifest.plannet_resources,
        "counts": {
            "distinct_endpoints": len(manifest.endpoints),
            "distinct_hosts": len(distinct_hosts),
            "brands_served": total_brands,
            "known": status_counts.get("known", 0),
            "unknown": status_counts.get("unknown", 0),
            "blocked": status_counts.get("blocked", 0),
            "mvp": len(manifest.mvp()),
            "runnable": sum(1 for e in manifest.endpoints if e.is_runnable(settings)),
        },
        "endpoints": endpoints,
    }


def write_inventory(settings: Optional[Settings] = None) -> Path:
    inv = build_inventory(settings)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / "inventory_manifest.json"
    out.write_text(json.dumps(inv, indent=2), encoding="utf-8")
    c = inv["counts"]
    log.info(
        "inventory: %d distinct endpoints (%d known, %d unknown, %d blocked); "
        "%d MVP, %d runnable; %d brands",
        c["distinct_endpoints"],
        c["known"],
        c["unknown"],
        c["blocked"],
        c["mvp"],
        c["runnable"],
        c["brands_served"],
    )
    return out

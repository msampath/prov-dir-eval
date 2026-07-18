"""Phase 6 runner — per-payer data quality evaluation + roll-up.

Combines completeness + required-element conformance (evaluate.py) with
referential integrity and orphan detection (integrity.py). Writes
output/quality/<payer>_conformance.json and output/quality/summary.json.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from .. import OUTPUT_DIR
from ..config import Settings, get_settings, load_manifest
from ..etl.loader import list_payer_schemas, pg_connection, schema_for, set_search_path
from ..logging_setup import get_logger
from .evaluate import evaluate_payer_completeness
from .integrity import orphans, referential_integrity

log = get_logger(__name__)

QUALITY_DIR = OUTPUT_DIR / "quality"


def run_quality(
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

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    conn = pg_connection()
    try:
        have_data = list_payer_schemas(conn)
        reports = []
        for ep in endpoints:
            schema = schema_for(ep.key)
            if schema not in have_data:
                log.info("quality: %s has no loaded data; skipping", ep.key)
                continue
            set_search_path(conn, schema)
            expected = ep.expected_resources(manifest.plannet_resources)
            completeness = evaluate_payer_completeness(conn, ep.key, expected)
            integrity = referential_integrity(conn, ep.key)
            orph = orphans(conn, ep.key)
            report = {
                "key": ep.key,
                "payer_name": ep.payer_name,
                "evaluated_at": datetime.now(tz=timezone.utc).isoformat(),
                "total_rows": completeness["total_rows"],
                "completeness_score": completeness["completeness_score"],
                "conformance_score": completeness["conformance_score"],
                "referential_integrity_pct": integrity["overall_resolved_pct"],
                "dangling_references": integrity["dangling_references"],
                "orphans": orph,
                "completeness": completeness,
                "integrity": integrity,
            }
            (QUALITY_DIR / f"{ep.key}_conformance.json").write_text(
                json.dumps(report, indent=2, default=str), encoding="utf-8"
            )
            reports.append(report)
            log.info(
                "quality: %-14s rows=%-7d completeness=%s conformance=%s ref-integrity=%s%% dangling=%d",
                ep.key,
                report["total_rows"],
                report["completeness_score"],
                report["conformance_score"],
                report["referential_integrity_pct"],
                report["dangling_references"],
            )
    finally:
        conn.close()

    summary = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "payers": [
            {
                "key": r["key"],
                "payer_name": r["payer_name"],
                "total_rows": r["total_rows"],
                "completeness_score": r["completeness_score"],
                "conformance_score": r["conformance_score"],
                "referential_integrity_pct": r["referential_integrity_pct"],
                "dangling_references": r["dangling_references"],
            }
            for r in reports
        ],
    }
    (QUALITY_DIR / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log.info("quality complete: %d payers -> %s", len(reports), QUALITY_DIR)
    return summary

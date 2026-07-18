"""Phase 7.2/7.3 — Composite data-quality score.

Six dimensions, each normalized to 0-100:
  completeness, conformance, referential_integrity, freshness, uniqueness,
  consistency.

Weights are configurable at build time via named schemes (defaults documented
below). Dimensions with no data for a payer are excluded and the remaining
weights renormalized, so a payer that doesn't expose (say) Location isn't
penalized on consistency it could never have.

Scores persist to the `data_quality_score` table (one row per payer per scheme)
and to output/quality/scores.csv (default scheme).
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from typing import Optional

import psycopg
from psycopg.types.json import Jsonb

from .. import OUTPUT_DIR
from ..config import Settings, get_settings, load_manifest
from ..etl.loader import list_payer_schemas, pg_connection, schema_for, set_search_path
from ..logging_setup import get_logger
from .evaluate import evaluate_payer_completeness
from .integrity import orphans, referential_integrity
from .metrics import consistency, freshness, uniqueness

log = get_logger(__name__)

QUALITY_DIR = OUTPUT_DIR / "quality"

DIMENSIONS = [
    "completeness",
    "conformance",
    "referential_integrity",
    "freshness",
    "uniqueness",
    "consistency",
]

# Build-time weight schemes (documented defaults). Each is pre-generated as a
# selectable static view in the dashboard (weights are not adjustable live).
WEIGHT_SCHEMES: dict[str, dict[str, float]] = {
    "default": {
        "completeness": 0.20,
        "conformance": 0.20,
        "referential_integrity": 0.20,
        "freshness": 0.15,
        "uniqueness": 0.10,
        "consistency": 0.15,
    },
    "equal": {d: 1 / 6 for d in DIMENSIONS},
    "conformance_focused": {
        "completeness": 0.20,
        "conformance": 0.30,
        "referential_integrity": 0.25,
        "freshness": 0.10,
        "uniqueness": 0.05,
        "consistency": 0.10,
    },
}


def _composite(dims: dict[str, Optional[float]], weights: dict[str, float]) -> Optional[float]:
    num = denom = 0.0
    for d, w in weights.items():
        v = dims.get(d)
        if v is not None:
            num += w * v
            denom += w
    return round(num / denom, 1) if denom else None


def compute_payer_dimensions(conn: psycopg.Connection, ep, resource_types: list[str]) -> dict:
    completeness = evaluate_payer_completeness(conn, ep.key, resource_types)
    integrity = referential_integrity(conn, ep.key)
    fresh = freshness(conn, ep.key)
    uniq = uniqueness(conn, ep.key)
    cons = consistency(conn, ep.key)
    dims = {
        "completeness": completeness["completeness_score"],
        "conformance": completeness["conformance_score"],
        "referential_integrity": integrity["overall_resolved_pct"],
        "freshness": fresh["score"],
        "uniqueness": uniq["score"],
        "consistency": cons["score"],
    }
    return {
        "dimensions": dims,
        "total_rows": completeness["total_rows"],
        "detail": {
            "completeness": completeness,
            "integrity": integrity,
            "freshness": fresh,
            "uniqueness": uniq,
            "consistency": cons,
            "orphans": orphans(conn, ep.key),
        },
    }


def run_scoring(
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
    scored_at = datetime.now(tz=timezone.utc)
    conn = pg_connection()
    rows = []
    try:
        have_data = list_payer_schemas(conn)
        for ep in endpoints:
            schema = schema_for(ep.key)
            if schema not in have_data:
                continue
            set_search_path(conn, schema)
            expected = ep.expected_resources(manifest.plannet_resources)
            payer = compute_payer_dimensions(conn, ep, expected)
            dims = payer["dimensions"]
            composites = {scheme: _composite(dims, w) for scheme, w in WEIGHT_SCHEMES.items()}

            for scheme, weights in WEIGHT_SCHEMES.items():
                _persist_score(conn, ep, scored_at, scheme, dims, composites[scheme], payer["detail"])

            rows.append(
                {
                    "key": ep.key,
                    "payer_name": ep.payer_name,
                    "total_rows": payer["total_rows"],
                    **{d: dims[d] for d in DIMENSIONS},
                    "composite_default": composites["default"],
                    "composite_equal": composites["equal"],
                    "composite_conformance_focused": composites["conformance_focused"],
                }
            )
            log.info(
                "score: %-14s composite(default)=%s  [comp=%s conf=%s refint=%s fresh=%s uniq=%s cons=%s]",
                ep.key,
                composites["default"],
                dims["completeness"],
                dims["conformance"],
                dims["referential_integrity"],
                dims["freshness"],
                dims["uniqueness"],
                dims["consistency"],
            )
        conn.commit()
    finally:
        conn.close()

    _write_scores_csv(rows)
    payload = {
        "generated_at": scored_at.isoformat(),
        "weight_schemes": WEIGHT_SCHEMES,
        "dimensions": DIMENSIONS,
        "payers": rows,
    }
    (QUALITY_DIR / "scores.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.info("scoring complete: %d payers -> %s", len(rows), QUALITY_DIR / "scores.json")
    return payload


def _persist_score(conn, ep, scored_at, scheme, dims, composite, detail) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.data_quality_score "
            "(payer_id, scored_at, weight_scheme, completeness, conformance, referential_integrity, "
            " freshness, uniqueness, consistency, composite, detail) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (payer_id, scored_at, weight_scheme) DO UPDATE SET "
            "  completeness=EXCLUDED.completeness, conformance=EXCLUDED.conformance, "
            "  referential_integrity=EXCLUDED.referential_integrity, freshness=EXCLUDED.freshness, "
            "  uniqueness=EXCLUDED.uniqueness, consistency=EXCLUDED.consistency, "
            "  composite=EXCLUDED.composite, detail=EXCLUDED.detail",
            (
                ep.key, scored_at, scheme,
                dims["completeness"], dims["conformance"], dims["referential_integrity"],
                dims["freshness"], dims["uniqueness"], dims["consistency"],
                composite, Jsonb(detail if scheme == "default" else {}),
            ),
        )


def _write_scores_csv(rows: list[dict]):
    out = QUALITY_DIR / "scores.csv"
    cols = ["key", "payer_name", "total_rows", *DIMENSIONS,
            "composite_default", "composite_equal", "composite_conformance_focused"]
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return out

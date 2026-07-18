"""Phase 7.1 — Additional quality dimensions: freshness, uniqueness, consistency.

Completeness / conformance (evaluate.py) and referential integrity (integrity.py)
are computed elsewhere; this module adds the remaining three dimensions, each
queried directly from the loaded tables and normalized to 0-100.
"""

from __future__ import annotations

import psycopg

from ..etl.extract import US_STATES

# Tables that carry meta_last_updated worth aggregating for freshness.
_FRESHNESS_TABLES = ["organization", "practitioner", "location", "practitioner_role", "healthcare_service"]


def freshness(conn: psycopg.Connection, payer_id: str) -> dict:
    """% of resources whose meta.lastUpdated is recent. Missing timestamps count
    against the score (treated as not-fresh) but coverage is reported separately."""
    total = have_ts = fresh12 = fresh24 = 0
    with conn.cursor() as cur:
        for t in _FRESHNESS_TABLES:
            cur.execute(
                f"SELECT count(*), count(meta_last_updated), "
                f"count(*) FILTER (WHERE meta_last_updated > now() - interval '12 months'), "
                f"count(*) FILTER (WHERE meta_last_updated > now() - interval '24 months') "
                f"FROM {t} WHERE payer_id = %s",
                (payer_id,),
            )
            tt, ht, f12, f24 = cur.fetchone()
            total += tt
            have_ts += ht
            fresh12 += f12
            fresh24 += f24
    score = round(100.0 * fresh12 / total, 1) if total else None
    return {
        "total": total,
        "timestamp_coverage_pct": round(100.0 * have_ts / total, 1) if total else None,
        "fresh_within_12mo_pct": score,
        "fresh_within_24mo_pct": round(100.0 * fresh24 / total, 1) if total else None,
        "score": score,
    }


def uniqueness(conn: psycopg.Connection, payer_id: str) -> dict:
    """Duplicate detection on practitioner NPI (and organization NPI)."""
    out = {}
    with conn.cursor() as cur:
        for table in ("practitioner", "organization"):
            cur.execute(
                f"SELECT count(npi), count(DISTINCT npi) FROM {table} "
                f"WHERE payer_id = %s AND npi IS NOT NULL AND npi <> ''",
                (payer_id,),
            )
            total_npi, distinct_npi = cur.fetchone()
            total_npi, distinct_npi = int(total_npi), int(distinct_npi)
            cur.execute(f"SELECT count(*) FROM {table} WHERE payer_id = %s", (payer_id,))
            rows = int(cur.fetchone()[0])
            out[table] = {
                "rows": rows,
                "npi_coverage_pct": round(100.0 * total_npi / rows, 1) if rows else None,
                "npi_unique_pct": round(100.0 * distinct_npi / total_npi, 1) if total_npi else None,
                "duplicate_npis": total_npi - distinct_npi,
            }
    # Score driven by practitioner NPI uniqueness when present, else organization.
    prac = out.get("practitioner", {})
    score = prac.get("npi_unique_pct")
    if score is None:
        score = out.get("organization", {}).get("npi_unique_pct")
    return {"by_resource": out, "score": score}


def consistency(conn: psycopg.Connection, payer_id: str) -> dict:
    """Internal consistency: valid US state codes, ZIP format, address completeness."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS total, "
            "count(address_state) AS have_state, "
            "count(*) FILTER (WHERE upper(address_state) = ANY(%s)) AS valid_state, "
            "count(*) FILTER (WHERE address_postalcode ~ '^[0-9]{5}') AS valid_zip, "
            "count(*) FILTER (WHERE address_city IS NOT NULL AND address_state IS NOT NULL) AS city_state "
            "FROM location WHERE payer_id = %s",
            (US_STATES, payer_id),
        )
        total, have_state, valid_state, valid_zip, city_state = (int(x) for x in cur.fetchone())

    if total == 0:
        return {"total_locations": 0, "score": None}

    valid_state_pct = round(100.0 * valid_state / have_state, 1) if have_state else None
    valid_zip_pct = round(100.0 * valid_zip / total, 1)
    addr_complete_pct = round(100.0 * city_state / total, 1)
    parts = [p for p in (valid_state_pct, valid_zip_pct, addr_complete_pct) if p is not None]
    score = round(sum(parts) / len(parts), 1) if parts else None
    return {
        "total_locations": total,
        "valid_state_pct": valid_state_pct,
        "valid_zip_pct": valid_zip_pct,
        "address_complete_pct": addr_complete_pct,
        "score": score,
    }

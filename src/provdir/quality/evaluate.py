"""Phase 6.1/6.2 evaluation — completeness + required-element conformance.

For each resource type with loaded rows, a single aggregate query computes how
many rows populate each required/must-support element (per rules.RESOURCE_RULES).
From those counts we derive:

* element population %;
* a *conformance* score = mean population of REQUIRED (SHALL/min=1) elements,
  with any <100% required element listed as a violation;
* a *completeness* score = mean population across required + must-support elements.
"""

from __future__ import annotations

import psycopg

from ..models import RESOURCE_TABLES
from .rules import RESOURCE_RULES


def _resource_query(table: str, rules) -> str:
    filters = ", ".join(
        f"count(*) FILTER (WHERE {c.condition_sql}) AS e{i}" for i, c in enumerate(rules)
    )
    return f"SELECT count(*) AS total, {filters} FROM {table} WHERE payer_id = %s"


def evaluate_resource(conn: psycopg.Connection, payer_id: str, resource_type: str) -> dict | None:
    rules = RESOURCE_RULES.get(resource_type)
    table = RESOURCE_TABLES.get(resource_type)
    if not rules or table is None:
        return None
    with conn.cursor() as cur:
        cur.execute(_resource_query(table.name, rules), (payer_id,))
        row = cur.fetchone()
    total = int(row[0])
    if total == 0:
        return {"resource_type": resource_type, "row_count": 0}

    elements = {}
    required_pcts, all_pcts, violations = [], [], []
    for i, c in enumerate(rules):
        populated = int(row[i + 1])
        pct = round(100.0 * populated / total, 1)
        elements[c.label] = {"populated": populated, "pct": pct, "required": c.required}
        all_pcts.append(pct)
        if c.required:
            required_pcts.append(pct)
            if pct < 100.0:
                violations.append({"element": c.label, "populated_pct": pct})

    conformance = round(sum(required_pcts) / len(required_pcts), 1) if required_pcts else 100.0
    completeness = round(sum(all_pcts) / len(all_pcts), 1) if all_pcts else None
    return {
        "resource_type": resource_type,
        "row_count": total,
        "elements": elements,
        "required_violations": violations,
        "conformance_score": conformance,
        "completeness_score": completeness,
    }


def evaluate_payer_completeness(conn: psycopg.Connection, payer_id: str, resource_types: list[str]) -> dict:
    per_resource = {}
    for rtype in resource_types:
        res = evaluate_resource(conn, payer_id, rtype)
        if res is not None:
            per_resource[rtype] = res

    # Row-count-weighted roll-ups over resources that actually have data.
    weighted = [(r["row_count"], r) for r in per_resource.values() if r.get("row_count", 0) > 0]
    total_rows = sum(w for w, _ in weighted)

    def wmean(key: str) -> float | None:
        vals = [(w, r[key]) for w, r in weighted if r.get(key) is not None]
        tw = sum(w for w, _ in vals)
        return round(sum(w * v for w, v in vals) / tw, 1) if tw else None

    return {
        "per_resource": per_resource,
        "total_rows": total_rows,
        "completeness_score": wmean("completeness_score"),
        "conformance_score": wmean("conformance_score"),
        "resources_with_data": len(weighted),
    }

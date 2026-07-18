"""Phase 6.3/6.4 — Referential integrity + orphan detection.

For every Plan-Net reference we extracted, check that the target exists in the
loaded data *for the same payer*. References are normalized to ``Type/id``;
matching is by logical id within the payer. Counts: total / resolved / dangling.

Orphan detection flags resources the IG implies should be linked but that nothing
references (e.g. a Practitioner with no PractitionerRole).

Table/column names come from a fixed internal map (never user input), so the
dynamically-built SQL is safe.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg


@dataclass(frozen=True)
class RefCheck:
    source_table: str
    column: str
    is_array: bool
    target_table: str
    label: str


# Every reference evaluated for integrity.
REFERENCE_CHECKS: list[RefCheck] = [
    RefCheck("practitioner_role", "practitioner_ref", False, "practitioner", "PractitionerRole.practitioner"),
    RefCheck("practitioner_role", "organization_ref", False, "organization", "PractitionerRole.organization"),
    RefCheck("practitioner_role", "location_refs", True, "location", "PractitionerRole.location"),
    RefCheck("practitioner_role", "healthcare_service_refs", True, "healthcare_service", "PractitionerRole.healthcareService"),
    RefCheck("practitioner_role", "network_refs", True, "organization", "PractitionerRole.network"),
    RefCheck("organization_affiliation", "organization_ref", False, "organization", "OrganizationAffiliation.organization"),
    RefCheck("organization_affiliation", "participating_organization_ref", False, "organization", "OrganizationAffiliation.participatingOrganization"),
    RefCheck("organization_affiliation", "network_refs", True, "organization", "OrganizationAffiliation.network"),
    RefCheck("organization_affiliation", "location_refs", True, "location", "OrganizationAffiliation.location"),
    RefCheck("organization_affiliation", "healthcare_service_refs", True, "healthcare_service", "OrganizationAffiliation.healthcareService"),
    RefCheck("location", "managing_organization_ref", False, "organization", "Location.managingOrganization"),
    RefCheck("healthcare_service", "provided_by_ref", False, "organization", "HealthcareService.providedBy"),
    RefCheck("healthcare_service", "location_refs", True, "location", "HealthcareService.location"),
    RefCheck("insurance_plan", "owned_by_ref", False, "organization", "InsurancePlan.ownedBy"),
    RefCheck("insurance_plan", "administered_by_ref", False, "organization", "InsurancePlan.administeredBy"),
    RefCheck("insurance_plan", "coverage_area_refs", True, "location", "InsurancePlan.coverageArea"),
    RefCheck("insurance_plan", "network_refs", True, "organization", "InsurancePlan.network"),
    RefCheck("organization", "part_of_ref", False, "organization", "Organization.partOf"),
]


def _check_sql(chk: RefCheck) -> str:
    if chk.is_array:
        return (
            f"SELECT count(*) AS total, count(t.payer_id) AS resolved "
            f"FROM (SELECT payer_id, unnest({chk.column}) AS ref FROM {chk.source_table} "
            f"      WHERE payer_id = %s AND {chk.column} IS NOT NULL) s "
            f"LEFT JOIN {chk.target_table} t "
            f"  ON t.payer_id = s.payer_id AND t.id = split_part(s.ref, '/', 2)"
        )
    return (
        f"SELECT count(*) AS total, count(t.payer_id) AS resolved "
        f"FROM {chk.source_table} s "
        f"LEFT JOIN {chk.target_table} t "
        f"  ON t.payer_id = s.payer_id AND t.id = split_part(s.{chk.column}, '/', 2) "
        f"WHERE s.payer_id = %s AND s.{chk.column} IS NOT NULL"
    )


def referential_integrity(conn: psycopg.Connection, payer_id: str) -> dict:
    results = []
    grand_total = grand_resolved = 0
    with conn.cursor() as cur:
        for chk in REFERENCE_CHECKS:
            cur.execute(_check_sql(chk), (payer_id,))
            total, resolved = cur.fetchone()
            total, resolved = int(total), int(resolved)
            dangling = total - resolved
            grand_total += total
            grand_resolved += resolved
            results.append(
                {
                    "reference": chk.label,
                    "target": chk.target_table,
                    "total": total,
                    "resolved": resolved,
                    "dangling": dangling,
                    "resolved_pct": round(100.0 * resolved / total, 1) if total else None,
                }
            )
    return {
        "checks": results,
        "total_references": grand_total,
        "resolved_references": grand_resolved,
        "dangling_references": grand_total - grand_resolved,
        "overall_resolved_pct": round(100.0 * grand_resolved / grand_total, 1) if grand_total else None,
    }


# Orphan detection: resource present but never referenced where the IG links it.
_ORPHAN_QUERIES: list[tuple[str, str]] = [
    (
        "practitioner_without_role",
        "SELECT count(*) FROM practitioner p WHERE p.payer_id = %s AND NOT EXISTS ("
        "  SELECT 1 FROM practitioner_role r WHERE r.payer_id = p.payer_id "
        "  AND split_part(r.practitioner_ref,'/',2) = p.id)",
    ),
    (
        "location_unreferenced",
        "SELECT count(*) FROM location l WHERE l.payer_id = %s AND NOT EXISTS ("
        "  SELECT 1 FROM practitioner_role r WHERE r.payer_id = l.payer_id "
        "  AND l.id = ANY(SELECT split_part(x,'/',2) FROM unnest(r.location_refs) x))",
    ),
    (
        "organization_unreferenced",
        "SELECT count(*) FROM organization o WHERE o.payer_id = %s AND o.is_network = 0 AND NOT EXISTS ("
        "  SELECT 1 FROM practitioner_role r WHERE r.payer_id = o.payer_id "
        "  AND split_part(r.organization_ref,'/',2) = o.id)",
    ),
]


def orphans(conn: psycopg.Connection, payer_id: str) -> dict:
    out = {}
    with conn.cursor() as cur:
        for name, sql in _ORPHAN_QUERIES:
            cur.execute(sql, (payer_id,))
            out[name] = int(cur.fetchone()[0])
    return out

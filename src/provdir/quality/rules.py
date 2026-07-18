"""Phase 6.1/6.2 — Rule-based profile conformance + completeness.

Rather than invoking the HL7 Java validator (which needs the IG package and a
JVM), we encode the Plan-Net profiles' key required (SHALL, min=1) and
must-support elements per resource type and measure how completely the loaded
data populates them. This yields:

* a *conformance* signal: required elements that are not 100% populated are
  cardinality/required-element violations;
* a *completeness* signal: % populated across required + must-support elements.

Each element is expressed as a JSONB existence condition so the counts run in
the database without pulling rows into Python.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ElementCheck:
    label: str
    condition_sql: str  # boolean SQL over a row's `resource` jsonb / extracted cols
    required: bool      # True => IG min=1 (SHALL); False => must-support


# Plan-Net (v1.x) key elements. Conditions reference the `resource` JSONB column
# and, where convenient, extracted columns (npi, latitude, ...).
RESOURCE_RULES: dict[str, list[ElementCheck]] = {
    "Organization": [
        ElementCheck("active", "(resource ? 'active')", True),
        ElementCheck("name", "(resource ? 'name')", True),
        ElementCheck("identifier", "(resource ? 'identifier')", False),
        ElementCheck("type", "(resource ? 'type')", False),
        ElementCheck("telecom", "(resource ? 'telecom')", False),
        ElementCheck("address", "(resource ? 'address')", False),
    ],
    "Practitioner": [
        ElementCheck("name", "(resource ? 'name')", True),
        ElementCheck("identifier_npi", "(npi IS NOT NULL)", False),
        ElementCheck("active", "(resource ? 'active')", False),
        ElementCheck("qualification", "(resource ? 'qualification')", False),
        ElementCheck("telecom", "(resource ? 'telecom')", False),
    ],
    "PractitionerRole": [
        ElementCheck("practitioner", "(resource ? 'practitioner')", False),
        ElementCheck("organization", "(resource ? 'organization')", False),
        ElementCheck("code", "(resource ? 'code')", False),
        ElementCheck("specialty", "(resource ? 'specialty')", False),
        ElementCheck("location", "(resource ? 'location')", False),
    ],
    "Location": [
        ElementCheck("status", "(resource ? 'status')", False),
        ElementCheck("name", "(resource ? 'name')", True),
        ElementCheck("type", "(resource ? 'type')", False),
        ElementCheck("telecom", "(resource ? 'telecom')", False),
        ElementCheck("address", "(resource ? 'address')", False),
        ElementCheck("position_geo", "(latitude IS NOT NULL AND longitude IS NOT NULL)", False),
        ElementCheck("managingOrganization", "(resource ? 'managingOrganization')", False),
    ],
    "HealthcareService": [
        ElementCheck("providedBy", "(resource ? 'providedBy')", False),
        ElementCheck("location", "(resource ? 'location')", False),
        ElementCheck("type", "(resource ? 'type')", False),
        ElementCheck("specialty", "(resource ? 'specialty')", False),
        ElementCheck("name", "(resource ? 'name')", False),
    ],
    "OrganizationAffiliation": [
        ElementCheck("active", "(resource ? 'active')", False),
        ElementCheck("organization", "(resource ? 'organization')", False),
        ElementCheck("participatingOrganization", "(resource ? 'participatingOrganization')", False),
        ElementCheck("network", "(resource ? 'network')", False),
        ElementCheck("code", "(resource ? 'code')", False),
        ElementCheck("location", "(resource ? 'location')", False),
    ],
    "InsurancePlan": [
        ElementCheck("identifier", "(resource ? 'identifier')", False),
        ElementCheck("type", "(resource ? 'type')", False),
        ElementCheck("name", "(resource ? 'name')", True),
        ElementCheck("ownedBy", "(resource ? 'ownedBy')", False),
        ElementCheck("administeredBy", "(resource ? 'administeredBy')", False),
        ElementCheck("coverageArea", "(resource ? 'coverageArea')", False),
        ElementCheck("plan", "(resource ? 'plan')", False),
    ],
    "Endpoint": [
        ElementCheck("status", "(resource ? 'status')", True),
        ElementCheck("connectionType", "(resource ? 'connectionType')", True),
        ElementCheck("address", "(resource ? 'address')", True),
        ElementCheck("payloadType", "(resource ? 'payloadType')", False),
    ],
}

"""Phase 5.2 — Transform: validate, hash, and extract indexed columns.

Each FHIR resource dict is reduced to the row shape its table expects: the full
resource as JSONB plus extracted/indexed columns (names, refs, codes, geo).
References are normalized to ``Type/id`` form so referential-integrity checks
(Phase 6) can resolve them against loaded `id`s.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Optional

NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
NETWORK_TYPE_CODE = "ntwk"


def sha256_hash(resource: dict) -> str:
    payload = json.dumps(resource, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_last_updated(resource: dict) -> Optional[datetime]:
    raw = (resource.get("meta") or {}).get("lastUpdated")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def normalize_reference(ref: Any) -> Optional[str]:
    """Return a reference as ``Type/id``.

    Accepts a FHIR Reference object ({"reference": "..."}) or a raw string.
    Absolute URLs are reduced to their final ``Type/id`` segments.
    """
    if isinstance(ref, dict):
        ref = ref.get("reference")
    if not ref or not isinstance(ref, str):
        return None
    ref = ref.split("?", 1)[0].split("#", 1)[0]
    parts = [p for p in ref.split("/") if p]
    if len(parts) >= 2:
        return f"{parts[-2]}/{parts[-1]}"
    return ref


def _refs(items: Any) -> list[str]:
    if not items:
        return []
    if isinstance(items, dict):
        items = [items]
    out = []
    for it in items:
        norm = normalize_reference(it)
        if norm:
            out.append(norm)
    return out


def _codes(concepts: Any) -> list[str]:
    """Flatten CodeableConcept(s) to a list of `code` values."""
    if not concepts:
        return []
    if isinstance(concepts, dict):
        concepts = [concepts]
    out: list[str] = []
    for cc in concepts:
        for coding in (cc or {}).get("coding", []) or []:
            code = coding.get("code")
            if code:
                out.append(code)
    return out


def _identifier_value(resource: dict, system: str) -> Optional[str]:
    for ident in resource.get("identifier", []) or []:
        if ident.get("system") == system:
            return ident.get("value")
    return None


def _human_name(resource: dict) -> tuple[Optional[str], Optional[str], Optional[str]]:
    names = resource.get("name") or []
    if not names:
        return None, None, None
    n = names[0]
    family = n.get("family")
    given = " ".join(n.get("given", []) or []) or None
    text = n.get("text") or " ".join(filter(None, [given, family])) or None
    return family, given, text


def _bool_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    return 1 if val else 0


def _first_address(resource: dict) -> dict:
    addrs = resource.get("address")
    if isinstance(addrs, list):
        return addrs[0] if addrs else {}
    if isinstance(addrs, dict):
        return addrs
    return {}


def _position(resource: dict) -> tuple[Optional[float], Optional[float]]:
    pos = resource.get("position") or {}
    lat, lon = pos.get("latitude"), pos.get("longitude")
    return (float(lat) if lat is not None else None, float(lon) if lon is not None else None)


# --- per-resource extractors -----------------------------------------------
# Each returns the table-specific extracted columns (not the common columns).

def _extract_organization(r: dict) -> dict:
    type_codes = _codes(r.get("type"))
    return {
        "name": r.get("name"),
        "alias": r.get("alias") or None,
        "type_codes": type_codes or None,
        "is_network": 1 if NETWORK_TYPE_CODE in type_codes else 0,
        "npi": _identifier_value(r, NPI_SYSTEM),
        "active": _bool_int(r.get("active")),
        "part_of_ref": normalize_reference(r.get("partOf")),
    }


def _extract_practitioner(r: dict) -> dict:
    family, given, text = _human_name(r)
    quals = []
    for q in r.get("qualification", []) or []:
        quals.extend(_codes(q.get("code")))
    return {
        "family": family,
        "given": given,
        "name_text": text,
        "npi": _identifier_value(r, NPI_SYSTEM),
        "active": _bool_int(r.get("active")),
        "qualification_codes": quals or None,
    }


def _extract_practitioner_role(r: dict) -> dict:
    return {
        "practitioner_ref": normalize_reference(r.get("practitioner")),
        "organization_ref": normalize_reference(r.get("organization")),
        "location_refs": _refs(r.get("location")) or None,
        "healthcare_service_refs": _refs(r.get("healthcareService")) or None,
        "network_refs": _network_refs(r) or None,
        "specialty_codes": _codes(r.get("specialty")) or None,
        "role_codes": _codes(r.get("code")) or None,
        "active": _bool_int(r.get("active")),
    }


def _network_refs(r: dict) -> list[str]:
    """Plan-Net carries network as an extension referencing an Organization."""
    out: list[str] = []
    for ext in r.get("extension", []) or []:
        url = ext.get("url", "")
        if url.endswith("plannet-network") or "network" in url.lower():
            ref = ext.get("valueReference")
            norm = normalize_reference(ref)
            if norm:
                out.append(norm)
    return out


def _extract_location(r: dict) -> dict:
    addr = _first_address(r)
    lat, lon = _position(r)
    line = addr.get("line")
    return {
        "name": r.get("name"),
        "status": r.get("status"),
        "address_line": " ".join(line) if isinstance(line, list) else line,
        "address_city": addr.get("city"),
        "address_state": addr.get("state"),
        "address_postalcode": addr.get("postalCode"),
        "address_country": addr.get("country"),
        "latitude": lat,
        "longitude": lon,
        "managing_organization_ref": normalize_reference(r.get("managingOrganization")),
        "type_codes": _codes(r.get("type")) or None,
    }


def _extract_healthcare_service(r: dict) -> dict:
    return {
        "name": r.get("name"),
        "provided_by_ref": normalize_reference(r.get("providedBy")),
        "location_refs": _refs(r.get("location")) or None,
        "type_codes": _codes(r.get("type")) or None,
        "specialty_codes": _codes(r.get("specialty")) or None,
        "category_codes": _codes(r.get("category")) or None,
        "active": _bool_int(r.get("active")),
    }


def _extract_organization_affiliation(r: dict) -> dict:
    return {
        "organization_ref": normalize_reference(r.get("organization")),
        "participating_organization_ref": normalize_reference(r.get("participatingOrganization")),
        "network_refs": _refs(r.get("network")) or None,
        "location_refs": _refs(r.get("location")) or None,
        "healthcare_service_refs": _refs(r.get("healthcareService")) or None,
        "specialty_codes": _codes(r.get("specialty")) or None,
        "role_codes": _codes(r.get("code")) or None,
        "active": _bool_int(r.get("active")),
    }


def _extract_insurance_plan(r: dict) -> dict:
    return {
        "name": r.get("name"),
        "type_codes": _codes(r.get("type")) or None,
        "owned_by_ref": normalize_reference(r.get("ownedBy")),
        "administered_by_ref": normalize_reference(r.get("administeredBy")),
        "coverage_area_refs": _refs(r.get("coverageArea")) or None,
        "network_refs": _refs(r.get("network")) or None,
        "plan_type_codes": [
            c for p in (r.get("plan") or []) for c in _codes(p.get("type"))
        ] or None,
    }


def _extract_endpoint(r: dict) -> dict:
    ct = r.get("connectionType") or {}
    payload_types = []
    for pt in r.get("payloadType", []) or []:
        payload_types.extend(_codes(pt))
    return {
        "name": r.get("name"),
        "status": r.get("status"),
        "connection_type": ct.get("code"),
        "address": r.get("address"),
        "managing_organization_ref": normalize_reference(r.get("managingOrganization")),
        "payload_types": payload_types or None,
    }


_EXTRACTORS = {
    "Organization": _extract_organization,
    "Practitioner": _extract_practitioner,
    "PractitionerRole": _extract_practitioner_role,
    "Location": _extract_location,
    "HealthcareService": _extract_healthcare_service,
    "OrganizationAffiliation": _extract_organization_affiliation,
    "InsurancePlan": _extract_insurance_plan,
    "Endpoint": _extract_endpoint,
}


class TransformError(Exception):
    pass


def transform_resource(resource: dict, payer_id: str, source_base_url: str) -> dict:
    """Build a full DB row (common + extracted columns) for a FHIR resource."""
    rtype = resource.get("resourceType")
    extractor = _EXTRACTORS.get(rtype)
    if extractor is None:
        raise TransformError(f"No extractor for resourceType {rtype!r}")
    rid = resource.get("id")
    if not rid:
        raise TransformError(f"{rtype} resource missing id")

    row = {
        "payer_id": payer_id,
        "id": rid,
        "source_base_url": source_base_url,
        "resource": resource,
        "raw_hash": sha256_hash(resource),
        "meta_last_updated": parse_last_updated(resource),
    }
    row.update(extractor(resource))
    return row

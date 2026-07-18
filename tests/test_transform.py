"""Unit tests for ETL transform logic (no DB/network)."""

from __future__ import annotations

import pytest

from provdir.etl.transform import (
    TransformError,
    normalize_reference,
    parse_last_updated,
    sha256_hash,
    transform_resource,
)


def test_normalize_reference_relative():
    assert normalize_reference({"reference": "Practitioner/123"}) == "Practitioner/123"


def test_normalize_reference_absolute_url():
    assert normalize_reference("https://ex.com/fhir/Organization/abc") == "Organization/abc"


def test_normalize_reference_strips_query_and_fragment():
    assert normalize_reference("Location/9?_format=json") == "Location/9"


def test_normalize_reference_none():
    assert normalize_reference(None) is None
    assert normalize_reference({}) is None


def test_parse_last_updated_z_suffix():
    dt = parse_last_updated({"meta": {"lastUpdated": "2025-01-02T03:04:05Z"}})
    assert dt is not None and dt.year == 2025 and dt.tzinfo is not None


def test_sha256_hash_is_order_independent():
    a = sha256_hash({"x": 1, "y": 2})
    b = sha256_hash({"y": 2, "x": 1})
    assert a == b


def test_transform_practitioner_extracts_npi_and_name():
    res = {
        "resourceType": "Practitioner",
        "id": "p1",
        "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}],
        "name": [{"family": "Smith", "given": ["Jane", "Q"]}],
        "active": True,
    }
    row = transform_resource(res, payer_id="acme", source_base_url="https://x/y")
    assert row["payer_id"] == "acme"
    assert row["id"] == "p1"
    assert row["npi"] == "1234567890"
    assert row["family"] == "Smith"
    assert row["given"] == "Jane Q"
    assert row["active"] == 1
    assert row["raw_hash"]


def test_transform_organization_network_flag():
    res = {
        "resourceType": "Organization",
        "id": "o1",
        "name": "Net A",
        "type": [{"coding": [{"code": "ntwk"}]}],
    }
    row = transform_resource(res, "acme", "https://x")
    assert row["is_network"] == 1
    assert row["name"] == "Net A"


def test_transform_practitioner_role_refs():
    res = {
        "resourceType": "PractitionerRole",
        "id": "pr1",
        "practitioner": {"reference": "Practitioner/p1"},
        "organization": {"reference": "Organization/o1"},
        "location": [{"reference": "Location/l1"}, {"reference": "Location/l2"}],
    }
    row = transform_resource(res, "acme", "https://x")
    assert row["practitioner_ref"] == "Practitioner/p1"
    assert row["organization_ref"] == "Organization/o1"
    assert row["location_refs"] == ["Location/l1", "Location/l2"]


def test_transform_missing_id_raises():
    with pytest.raises(TransformError):
        transform_resource({"resourceType": "Organization", "name": "x"}, "acme", "https://x")


def test_transform_unknown_type_raises():
    with pytest.raises(TransformError):
        transform_resource({"resourceType": "Patient", "id": "1"}, "acme", "https://x")

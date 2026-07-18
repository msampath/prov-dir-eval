"""Unit tests for schema-per-datasource helpers (no DB)."""

from __future__ import annotations

import pytest

from provdir.etl.loader import schema_for
from provdir.models import RESOURCE_TABLE_NAMES, resource_metadata, shared_metadata


def test_schema_for_normalizes_key():
    assert schema_for("humana") == "humana"
    assert schema_for("BCBS_AZ") == "bcbs_az"
    assert schema_for("capital-blue") == "capital_blue"


def test_schema_for_rejects_unsafe():
    with pytest.raises(ValueError):
        schema_for("drop table; --")
    with pytest.raises(ValueError):
        schema_for("1bad")


def test_resource_metadata_has_eight_tables():
    assert len(RESOURCE_TABLE_NAMES) == 8
    assert set(resource_metadata.tables) == set(RESOURCE_TABLE_NAMES)


def test_shared_metadata_is_separate():
    assert set(shared_metadata.tables) == {"provenance", "data_quality_score"}
    # resource tables must NOT be in the shared (public) metadata
    assert "organization" not in shared_metadata.tables

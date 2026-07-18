"""Unit tests for config/manifest, scoring, and conformance matrix logic."""

from __future__ import annotations

import pytest

from provdir.config import Endpoint, Settings, load_manifest
from provdir.conformance.checker import (
    DECLARED_BUT_FAILING,
    NOT_APPLICABLE,
    NOT_DECLARED,
    SUPPORTED,
    DeclaredCapability,
    build_resource_matrix,
)
from provdir.conformance.ig import ResourceRequirement
from provdir.quality.scoring import WEIGHT_SCHEMES, _composite


# --- manifest --------------------------------------------------------------
def test_manifest_loads_and_mvp_subset():
    m = load_manifest()
    assert m.ig_version == "1.2.0"
    keys = {e.key for e in m.mvp()}
    assert {"humana", "banner", "bcbs_az"} <= keys
    assert "bcbs_ks" not in keys  # reclassified out (needs APIM key)


def test_endpoint_requires_https():
    with pytest.raises(ValueError):
        Endpoint(key="x", payer_name="X", base_url="http://insecure.example/fhir")


def test_endpoint_base_url_trailing_slash_stripped():
    ep = Endpoint(key="x", payer_name="X", base_url="https://ex.com/fhir/")
    assert ep.base_url == "https://ex.com/fhir"
    assert ep.metadata_url == "https://ex.com/fhir/metadata"


def test_skip_reason_missing_credentials():
    ep = Endpoint(
        key="x", payer_name="X", base_url="https://ex.com/fhir",
        auth={"strategy": "client_id_secret_headers", "secret_keys": ["FOO_ID", "FOO_SECRET"]},
    )
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    reason = ep.skip_reason(settings)
    assert reason is not None and reason.startswith("missing-credentials")
    assert not ep.is_runnable(settings)


def test_rate_limit_quirks_override_host_limiter():
    from provdir.http_client import _host_limiters, _limiter_for

    ep = Endpoint(
        key="rl", payer_name="RL", base_url="https://rl.example/fhir",
        quirks={"min_request_interval": 37.0, "max_concurrency": 1},
    )
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    try:
        lim = _limiter_for(ep, settings)
        assert lim._min_interval == 37.0
        assert lim._sem._value == 1
    finally:
        _host_limiters.pop("rl.example", None)


# --- composite scoring -----------------------------------------------------
def test_composite_weighted_mean():
    dims = {d: 100.0 for d in WEIGHT_SCHEMES["default"]}
    assert _composite(dims, WEIGHT_SCHEMES["default"]) == 100.0


def test_composite_excludes_none_and_renormalizes():
    weights = {"a": 0.5, "b": 0.5}
    # b is missing -> composite should equal a's value, not be halved.
    assert _composite({"a": 80.0, "b": None}, weights) == 80.0


def test_composite_all_none_returns_none():
    assert _composite({"a": None}, {"a": 1.0}) is None


# --- conformance matrix ----------------------------------------------------
def _req():
    return ResourceRequirement(
        resource_type="Organization",
        interactions=["read", "search-type"],
        shall_search_params=["name", "address"],
    )


def test_matrix_not_applicable_when_not_expected():
    rc = build_resource_matrix(_req(), declared=None, expected=False)
    assert rc.interactions["read"] == NOT_APPLICABLE
    assert all(v == NOT_APPLICABLE for v in rc.shall_search_params.values())


def test_matrix_not_declared_when_server_omits():
    rc = build_resource_matrix(_req(), declared=None, expected=True)
    assert rc.interactions["read"] == NOT_DECLARED
    assert rc.shall_search_params["name"] == NOT_DECLARED


def test_matrix_supported_when_declared():
    declared = DeclaredCapability(
        resource_type="Organization",
        interactions=["read", "search-type"],
        search_params=["name", "address"],
    )
    rc = build_resource_matrix(_req(), declared=declared, expected=True)
    assert rc.interactions["read"] == SUPPORTED
    # SHALL params start as "declared" (pre-probe), not yet SUPPORTED.
    assert rc.shall_search_params["name"] == "declared"


def test_fold_probes_marks_failing(monkeypatch):
    from provdir.conformance.runner import _fold_probes

    declared = DeclaredCapability(
        resource_type="Organization", interactions=["search-type"], search_params=["name"]
    )
    rc = build_resource_matrix(_req(), declared=declared, expected=True)
    rc.probes = {"search": {"ok": False, "note": "boom", "param": "name"}}
    _fold_probes(rc)
    assert rc.interactions["search-type"] == DECLARED_BUT_FAILING
    assert rc.shall_search_params["name"] == DECLARED_BUT_FAILING

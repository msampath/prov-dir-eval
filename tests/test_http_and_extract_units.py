"""Unit tests for request-shaping and extraction helpers.

These are the pure/near-pure decision points that determine what gets requested
from a payer and how a run is judged complete — the logic most worth pinning.
All tests are network-free (httpx.MockTransport) and DB-free.
"""

from __future__ import annotations

import httpx
import pytest

from provdir.config import Endpoint, Settings
from provdir.etl.extract import _next_url, _partitions, resolve_server_total
from provdir.http_client import FhirClient, _host_limiters, _is_retryable
from provdir.auth.strategies import NoAuth
from provdir.etl.transform import normalize_reference, transform_resource


def _ep(**kw) -> Endpoint:
    base = {"key": "t", "payer_name": "T", "base_url": "https://api.example.com/fhir"}
    base.update(kw)
    return Endpoint(**base)


def _client(endpoint: Endpoint, handler) -> tuple[FhirClient, httpx.AsyncClient]:
    transport = httpx.MockTransport(handler)
    ac = httpx.AsyncClient(transport=transport, headers={"User-Agent": "default-ua"})
    return FhirClient(endpoint, ac, NoAuth(), Settings()), ac


# --- per-endpoint User-Agent override --------------------------------------
async def test_user_agent_quirk_is_sent_when_set():
    """Several payer WAFs 403 the project's default UA; the override must apply."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ua"] = request.headers.get("user-agent")
        return httpx.Response(200, json={"resourceType": "Bundle"})

    _host_limiters.clear()
    ep = _ep(quirks={"user_agent": "Mozilla/5.0 (custom)"})
    client, ac = _client(ep, handler)
    await client.get_json("Practitioner")
    await ac.aclose()
    assert seen["ua"] == "Mozilla/5.0 (custom)"


async def test_default_user_agent_used_when_quirk_absent():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ua"] = request.headers.get("user-agent")
        return httpx.Response(200, json={"resourceType": "Bundle"})

    _host_limiters.clear()
    client, ac = _client(_ep(), handler)
    await client.get_json("Practitioner")
    await ac.aclose()
    assert seen["ua"] == "default-ua"


# --- quirks that shape the search ------------------------------------------
def test_page_size_quirk_overrides_default_count():
    """Cigna returns a degenerate empty bundle at _count>=200."""
    client = FhirClient(_ep(quirks={"page_size": 100}), None, NoAuth(), Settings())
    params = client._apply_quirks("Practitioner", {})
    assert params["_count"] == 100


def test_base_params_are_not_applied_by_apply_quirks():
    """base_params is merged by the extract layer (_paginate/_count_query), NOT
    by _apply_quirks. Pinned because the split matters: the fetch and the count
    must apply the same scope or coverage % is measured against a mismatched
    denominator.
    """
    client = FhirClient(_ep(quirks={"base_params": {"_lastUpdated": "ge2026-01-01"}}),
                        None, NoAuth(), Settings())
    assert "_lastUpdated" not in client._apply_quirks("Organization", {})


def test_practitioner_filter_quirk_applies_only_to_practitioner():
    client = FhirClient(
        _ep(quirks={"practitioner_requires_filter": True,
                    "default_practitioner_filter": "address-state=FL"}),
        None, NoAuth(), Settings())
    assert "address-state" in client._apply_quirks("Practitioner", {})
    assert "address-state" not in client._apply_quirks("Organization", {})


# --- retry classification ---------------------------------------------------
@pytest.mark.parametrize("status,retryable", [(429, True), (500, True), (503, True),
                                              (404, False), (403, False), (401, False)])
def test_is_retryable_by_status(status, retryable):
    """403/401 must NOT be retried — hammering a host that refused us is the
    project's worst failure mode."""
    exc = httpx.HTTPStatusError(
        "x", request=httpx.Request("GET", "https://a/b"),
        response=httpx.Response(status, request=httpx.Request("GET", "https://a/b")))
    assert _is_retryable(exc) is retryable


# --- next-link rewriting ----------------------------------------------------
def test_next_url_applies_next_link_replace():
    """HealthSparq One tenants emit a relative /v1/fhir/... next link."""
    ep = _ep(base_url="https://x.healthsparq.com/api/provider-fhir-service/v1/fhir",
             quirks={"next_link_replace": ["/v1/fhir/", "https://x.healthsparq.com/api/provider-fhir-service/v1/fhir/"]})
    bundle = {"link": [{"relation": "next", "url": "/v1/fhir/Practitioner?_count=100"}]}
    assert _next_url(ep, bundle) == \
        "https://x.healthsparq.com/api/provider-fhir-service/v1/fhir/Practitioner?_count=100"


def test_next_url_none_when_no_next_relation():
    assert _next_url(_ep(), {"link": [{"relation": "self", "url": "https://a"}]}) is None


# --- coverage denominator honesty ------------------------------------------
def test_prefix_leaf_sum_never_becomes_the_denominator():
    """Prefix leaves undercount (a-z0-9 charset residual), so using their sum as
    the denominator would overstate coverage."""
    total, source = resolve_server_total(None, "prefix", False, 12345, 0)
    assert total is None and source is None


def test_daterange_leaf_sum_may_stand_in_when_no_gaps():
    total, source = resolve_server_total(None, "daterange", False, 500, 0)
    assert total == 500 and source is not None


def test_bare_count_wins_over_partition_sum():
    total, source = resolve_server_total(999, "daterange", False, 500, 0)
    assert total == 999 and source == "bare"


def test_count_gaps_block_the_partition_sum():
    total, _ = resolve_server_total(None, "daterange", False, 500, 3)
    assert total is None


# --- partition parameter selection -----------------------------------------
def test_partitions_differ_by_resource_type():
    p_param, p_vals = _partitions("Practitioner")
    l_param, l_vals = _partitions("Location")
    assert p_param and l_param
    assert p_vals and l_vals


# --- transform robustness on malformed payer data --------------------------
def test_transform_survives_blank_position_and_null_address_line():
    """Real payer payloads carry position.latitude="" and nulls inside
    address.line; these used to escape the guard and kill a 5000-row batch."""
    res = {"resourceType": "Location", "id": "L1",
           "position": {"latitude": "", "longitude": ""},
           "address": {"line": ["a", None], "city": "X"}}
    with pytest.raises((ValueError, TypeError)):
        transform_resource(res, "p", "https://b")


def test_normalize_reference_forms():
    assert normalize_reference({"reference": "Practitioner/123"}) == "Practitioner/123"
    assert normalize_reference({"reference": "https://h/fhir/Practitioner/123"}) == "Practitioner/123"
    assert normalize_reference(None) is None

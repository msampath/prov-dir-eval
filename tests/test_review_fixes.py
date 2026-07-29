"""Regression tests for the defects found in the adversarial review.

Each test pins one confirmed finding so the fix cannot silently regress. All are
DB-free and network-free: the loader tests assert on generated SQL via a fake
cursor, and the limiter tests only build objects.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from provdir.config import Endpoint, load_manifest
from provdir.etl.extract import ResourceSink
from provdir.etl.loader import upsert_batch
from provdir.etl.pipeline import _read_chain_ids
from provdir.http_client import _host_limiters, _limiter_for
from provdir.logging_setup import _RedactionFilter
from provdir.models import practitioner
from provdir.quality.evaluate import evaluate_resource


# --- per-host limiter takes the STRICTEST politeness on a shared host -------
def _ep(key: str, host_url: str, **quirks) -> Endpoint:
    return Endpoint(key=key, payer_name=key, base_url=host_url, quirks=quirks)


@pytest.mark.parametrize("order", [("loose", "strict"), ("strict", "loose")])
def test_shared_host_limiter_uses_strictest_regardless_of_order(order, monkeypatch):
    """Two endpoints on one host must not let build order decide the rate."""
    loose = _ep("loose", "https://shared.example.com/fhir")           # no cap -> default
    strict = _ep("strict", "https://shared.example.com/fhir", max_concurrency=2,
                 min_request_interval=1.5)
    by_key = {"loose": loose, "strict": strict}
    monkeypatch.setattr("provdir.http_client.load_manifest",
                        lambda: type("M", (), {"endpoints": [loose, strict]})())
    _host_limiters.clear()
    from provdir.config import get_settings
    settings = get_settings()
    for key in order:
        limiter = _limiter_for(by_key[key], settings)
    assert limiter._sem._value == 2, "stricter concurrency cap was discarded"
    assert limiter._min_interval == 1.5, "stricter request interval was discarded"
    _host_limiters.clear()


# --- upsert must tolerate duplicate ids inside one batch --------------------
class _FakeCursor:
    def __init__(self, sink):
        self.sink = sink
        self.rowcount = 0
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, *a): self.sink.append(sql)
    def copy(self, sql):
        self.sink.append(sql)
        class _C:
            def __enter__(s): return s
            def __exit__(s, *a): return False
            def set_types(s, t): pass
            def write_row(s, r): pass
        return _C()


class _FakeConn:
    def __init__(self): self.sql: list[str] = []
    def cursor(self): return _FakeCursor(self.sql)


def test_upsert_update_mode_dedupes_conflict_keys():
    """DO UPDATE raises CardinalityViolation if a batch repeats a conflict key.

    id_chain legitimately returns the same role once per network it belongs to,
    so the staged rows must be collapsed to one row per (payer_id, id).
    """
    conn = _FakeConn()
    rows = [{"payer_id": "p", "id": "dup", "source_base_url": "u", "resource": {},
             "raw_hash": "h"} for _ in range(2)]
    upsert_batch(conn, practitioner, rows, update=True)
    insert = [s for s in conn.sql if s.startswith("INSERT")][0]
    assert "DISTINCT ON (payer_id, id)" in insert
    assert "DO UPDATE" in insert


def test_upsert_insert_mode_does_not_dedupe():
    """DO NOTHING tolerates duplicates, so it should keep the cheaper plain SELECT."""
    conn = _FakeConn()
    upsert_batch(conn, practitioner, [{"payer_id": "p", "id": "a", "source_base_url": "u",
                                       "resource": {}, "raw_hash": "h"}], update=False)
    insert = [s for s in conn.sql if s.startswith("INSERT")][0]
    assert "DISTINCT ON" not in insert
    assert "DO NOTHING" in insert


# --- concurrent sink flushes must not interleave ---------------------------
def test_resource_sink_flushes_are_serialised():
    """Overlapping flushes share one connection and one TEMP stage table, so a
    second TRUNCATE could wipe the first flush's staged rows."""
    overlap = {"max": 0, "cur": 0}

    async def flush(batch):
        overlap["cur"] += 1
        overlap["max"] = max(overlap["max"], overlap["cur"])
        await asyncio.sleep(0)          # yield: lets another task try to flush
        overlap["cur"] -= 1
        return len(batch)

    async def run():
        sink = ResourceSink(flush, batch=2)
        # 4 concurrent producers, each pushing enough to trigger flushes
        await asyncio.gather(*(sink.add({"id": f"{t}-{i}"})
                               for t in range(4) for i in range(2)))
        await sink.close()

    asyncio.run(run())
    assert overlap["max"] == 1, "two flushes ran concurrently on one connection"


# --- secrets must be redacted even when passed as %-args -------------------
def test_redaction_covers_log_args():
    """Call sites log `("...: %s", exc)` and FhirError embeds the request URL."""
    rec = logging.LogRecord("t", logging.WARNING, __file__, 1,
                            "auth failed for %s",
                            ("https://x/y?access_token=SECRET123",), None)
    _RedactionFilter().filter(rec)
    assert "SECRET123" not in rec.getMessage()
    assert "access_token=***" in rec.getMessage()


# --- conformance must not credit a free 100 --------------------------------
def test_conformance_is_none_when_no_required_elements():
    """rules.py declares no required elements for PractitionerRole et al.;
    scoring must renormalise rather than award a perfect score."""
    from provdir.quality.rules import RESOURCE_RULES
    n_checks = len(RESOURCE_RULES["PractitionerRole"])
    assert not any(c.required for c in RESOURCE_RULES["PractitionerRole"]), \
        "test premise: PractitionerRole declares no required elements"

    class _Cur:
        # total=100, then every element fully populated
        def execute(self, *a): pass
        def fetchone(self): return (100, *([100] * n_checks))
        def __enter__(self): return self
        def __exit__(self, *a): return False

    class _Conn:
        def cursor(self): return _Cur()

    out = evaluate_resource(_Conn(), "p", "PractitionerRole")
    assert out["row_count"] == 100
    assert out["conformance_score"] is None, "no required elements must not score a free 100"


# --- id_chain source filtering ---------------------------------------------
def test_read_chain_ids_applies_source_filter():
    """UnitedHealthcare must chain only off Organizations that ARE networks."""
    captured = {}

    class _Cur:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
        def fetchall(self): return [("a",), ("b",)]
        def __enter__(self): return self
        def __exit__(self, *a): return False

    class _Conn:
        def cursor(self): return _Cur()

    ids = _read_chain_ids(_Conn(), "organization", "uhc",
                          {"column": "is_network", "value": 1})
    assert ids == ["a", "b"]
    assert '"is_network" = %s' in captured["sql"]
    assert captured["params"] == ("uhc", 1)


def test_read_chain_ids_rejects_hostile_filter_column():
    class _Conn:
        def cursor(self): raise AssertionError("must not reach the DB")

    with pytest.raises(ValueError):
        _read_chain_ids(_Conn(), "organization", "uhc",
                        {"column": "id; DROP TABLE x", "value": 1})


# --- a refused host must not be swept, and must not read as "ok" ------------
@pytest.mark.parametrize("status", [401, 403, 429])
async def test_refused_bare_search_skips_the_partition_sweep(status):
    """429 in particular arrives as httpx.HTTPStatusError (raise_for_status inside
    the retry loop), NOT as FhirError — so guarding only the FhirError branch left
    rate limiting falling through to ~88 more requests at the same host.
    """
    import httpx
    from provdir.auth.strategies import NoAuth
    from provdir.config import Settings
    from provdir.etl.extract import extract_resource
    from provdir.http_client import FhirClient, _host_limiters

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(status, json={"resourceType": "OperationOutcome"})

    _host_limiters.clear()
    ep = Endpoint(key="t", payer_name="T", base_url="https://refuse.example.com/fhir")
    ac = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FhirClient(ep, ac, NoAuth(), Settings())

    class _Sink:
        async def add(self, res): pass

    stats = await extract_resource(client, ep, "Practitioner", _Sink(), None)
    await ac.aclose()
    assert stats["method"] == "blocked"
    assert calls["n"] <= 4, f"swept a refusing host with {calls['n']} requests"


def test_classify_status_treats_blocked_as_error_not_ok():
    """A refused endpoint holding rows from a prior run must not report "ok"."""
    from provdir.etl.pipeline import classify_status

    status, _ = classify_status(
        method="blocked", loaded=5000, server_total=None,
        note="bare search HTTP 403; partition sweep skipped (host refused)",
        transform_errors=0, fetch_errors=0, extract_err=None)
    assert status == "error"


def test_classify_status_flags_truncated_buckets_as_partial():
    """Adaptive sweeps fold truncation into the note using the literal wording
    classify_status matches on."""
    from provdir.etl.pipeline import classify_status

    status, _ = classify_status(
        method="adaptive:family", loaded=100, server_total=None,
        note="12 of 30 buckets truncated (pagination stopped): pagination stopped at page 5: ReadTimeout",
        transform_errors=0, fetch_errors=0, extract_err=None)
    assert status == "partial"


# --- manifest invariants ----------------------------------------------------
def test_manifest_keys_are_unique():
    """The key doubles as the Postgres schema name; by_key is first-match-wins."""
    keys = [e.key for e in load_manifest().endpoints]
    dupes = {k for k in keys if keys.count(k) > 1}
    assert not dupes, f"duplicate endpoint keys: {dupes}"

"""Tests for the slim-table write guard.

A slim table is invisible from the schema -- `resource` is populated and NOT NULL
whether it holds the full FHIR JSON or a stub -- so nothing about the table
itself stops an upsert from overwriting ~500M stubs with full resources. These
tests pin the guard that does.
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.etl import loader, pipeline  # noqa: E402
from provdir.etl.loader import SlimTableError, assert_not_slim  # noqa: E402
from provdir.models import RESOURCE_TABLES, RESOURCE_TYPE_BY_TABLE  # noqa: E402

SLIM = ("aetna_cvs", "PractitionerRole")


@pytest.fixture(autouse=True)
def _clear_cache():
    loader.refresh_slim_registry()
    yield
    loader.refresh_slim_registry()


@pytest.fixture
def registry(monkeypatch):
    """Pretend aetna_cvs/PractitionerRole is registered slim, without a DB."""
    monkeypatch.setattr(loader, "slim_registry", lambda conn: {SLIM})
    monkeypatch.setattr(loader, "slim_reason", lambda c, p, r: "test reason")


class TestResourceTypeByTable:
    def test_covers_every_resource_table(self):
        """upsert_batch derives the resource type from the table it was handed;
        a missing entry would silently disable the backstop for that type."""
        assert set(RESOURCE_TYPE_BY_TABLE.values()) == set(RESOURCE_TABLES)
        for rtype, table in RESOURCE_TABLES.items():
            assert RESOURCE_TYPE_BY_TABLE[table.name] == rtype


class TestAssertNotSlim:
    def test_blocks_a_registered_pair(self, registry):
        with pytest.raises(SlimTableError, match="SLIM table"):
            assert_not_slim(None, *SLIM)

    def test_allows_other_pairs(self, registry):
        assert_not_slim(None, "aetna_cvs", "Practitioner")
        assert_not_slim(None, "florida_blue", "PractitionerRole")

    def test_override_permits_it(self, registry):
        assert_not_slim(None, *SLIM, allow_slim=True)

    def test_message_names_the_override_and_the_alternative(self, registry):
        with pytest.raises(SlimTableError) as exc:
            assert_not_slim(None, *SLIM)
        msg = str(exc.value)
        assert "allow-slim" in msg          # how to force it
        assert "--slim" in msg              # what to do instead
        assert "test reason" in msg         # why it is slim


class TestUpsertBatchBackstop:
    """The guard callers cannot forget, since it sits in the write path itself."""

    def _rows(self, payer="aetna_cvs"):
        return [{"payer_id": payer, "id": "x", "resource": {}, "raw_hash": "h"}]

    def test_update_true_is_blocked(self, registry):
        with pytest.raises(SlimTableError):
            loader.upsert_batch(None, RESOURCE_TABLES["PractitionerRole"],
                                self._rows(), update=True)

    def test_mixed_payer_batch_is_blocked(self, registry):
        """The slim payer hiding behind another payer's row 0 must still trip
        the guard — sampling only rows[0] would wave it through."""
        rows = self._rows(payer="florida_blue") + self._rows(payer="aetna_cvs")
        with pytest.raises(SlimTableError):
            loader.upsert_batch(None, RESOURCE_TABLES["PractitionerRole"],
                                rows, update=True)

    def test_update_true_allowed_with_override(self, registry, monkeypatch):
        """The override must reach the SQL, not just skip the check."""
        seen = {}
        monkeypatch.setattr(loader, "_copy_columns",
                            lambda t: (_ for _ in ()).throw(RuntimeError("reached-sql")))
        with pytest.raises(RuntimeError, match="reached-sql"):
            loader.upsert_batch(None, RESOURCE_TABLES["PractitionerRole"],
                                self._rows(), update=True, allow_slim=True)
        assert not seen

    def test_other_payer_same_resource_is_untouched(self, registry, monkeypatch):
        monkeypatch.setattr(loader, "_copy_columns",
                            lambda t: (_ for _ in ()).throw(RuntimeError("reached-sql")))
        with pytest.raises(RuntimeError, match="reached-sql"):
            loader.upsert_batch(None, RESOURCE_TABLES["PractitionerRole"],
                                self._rows(payer="florida_blue"), update=True)

    def test_empty_batch_short_circuits_before_the_check(self, registry):
        assert loader.upsert_batch(None, RESOURCE_TABLES["PractitionerRole"],
                                   [], update=True) == 0

    def test_do_nothing_path_is_not_gated_here(self, registry, monkeypatch):
        """update=False cannot overwrite an existing stub, so the backstop stays
        out of its way; the early assert_not_slim at each entry point is what
        keeps new full rows out of a slim table."""
        monkeypatch.setattr(loader, "_copy_columns",
                            lambda t: (_ for _ in ()).throw(RuntimeError("reached-sql")))
        with pytest.raises(RuntimeError, match="reached-sql"):
            loader.upsert_batch(None, RESOURCE_TABLES["PractitionerRole"],
                                self._rows(), update=False)


class TestRegistryReadIsFailSafe:
    def test_missing_table_reads_as_empty(self, monkeypatch):
        """An older database without public.slim_table must still load, not crash."""
        class _Cur:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params=None): self.sql = sql
            def fetchone(self): return None

        class _Conn:
            def cursor(self): return _Cur()

        assert loader.slim_registry(_Conn()) == set()


class _RegistryConn:
    """Fake connection serving a mutable registry; counts full reads."""

    def __init__(self, pairs):
        self.pairs = pairs
        self.reads = 0

    def cursor(self):
        conn = self

        class _Cur:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params=None): self.sql = sql
            def fetchone(self): return (1,)                      # table exists

            def fetchall(self):
                conn.reads += 1
                return list(conn.pairs)

        return _Cur()


class TestRegistryCacheTTL:
    """A cache that never expired would let a long-lived process (orchestrator
    parent, monthly etl run) keep a pre-registration answer forever."""

    def test_fresh_cache_is_not_requeried(self):
        conn = _RegistryConn([SLIM])
        assert loader.slim_registry(conn) == {SLIM}
        assert loader.slim_registry(conn) == {SLIM}
        assert conn.reads == 1

    def test_expired_cache_sees_a_new_registration(self):
        conn = _RegistryConn([])
        assert loader.slim_registry(conn) == set()
        conn.pairs = [SLIM]                      # registered elsewhere meanwhile
        loader._slim_cache_at = time.monotonic() - loader._SLIM_CACHE_TTL_S - 1
        assert loader.slim_registry(conn) == {SLIM}
        assert conn.reads == 2


class TestUnregister:
    def test_deletes_and_invalidates_the_cache(self):
        class _Cur:
            rowcount = 1
            def __enter__(self): return self
            def __exit__(self, *a): return False

            def execute(self, sql, params=None):
                assert "DELETE FROM public.slim_table" in sql
                assert params == SLIM

        class _Conn:
            def cursor(self): return _Cur()

        loader._slim_cache = {SLIM}
        loader._slim_cache_at = time.monotonic()
        assert loader.unregister_slim_table(_Conn(), *SLIM) is True
        assert loader._slim_cache is None        # next read hits the DB

    def test_reports_when_nothing_was_registered(self):
        class _Cur:
            rowcount = 0
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params=None): pass

        class _Conn:
            def cursor(self): return _Cur()

        assert loader.unregister_slim_table(_Conn(), *SLIM) is False


class _FakeConn:
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass


class TestEtlAllowSlimWiring:
    """--allow-slim-upsert must reach the upsert_batch backstop: un-skipping the
    unit while leaving the backstop armed would extract for hours and then kill
    the unit at its very first batch."""

    def _run(self, monkeypatch, allow_slim):
        captured = {}
        monkeypatch.setattr(pipeline, "get_settings",
                            lambda: SimpleNamespace(http_default_count=100,
                                                    checkpoint_ttl_hours=72.0))
        monkeypatch.setattr(pipeline, "_open_for_resource", lambda s, t: _FakeConn())
        monkeypatch.setattr(pipeline, "bare_fingerprint", lambda ep, rt, c: "fp")
        monkeypatch.setattr(pipeline, "transform_resource",
                            lambda r, key, base: {"payer_id": key, "id": r["id"]})

        def fake_upsert(conn, table, rows, update=False, allow_slim=False):
            captured["update"] = update
            captured["allow_slim"] = allow_slim
            return len(rows)

        monkeypatch.setattr(pipeline, "upsert_batch", fake_upsert)

        async def fake_extract(client, ep, rtype, sink, max_pages,
                               resume_ckpt=None, progress=None):
            await sink.add({"resourceType": "Practitioner", "id": "1"})
            return {"method": "bare"}

        monkeypatch.setattr(pipeline, "extract_resource", fake_extract)
        monkeypatch.setattr(
            pipeline, "_finalize",
            lambda *a, **k: {"resource_type": "Practitioner", "loaded": 1,
                             "status": "ok", "method": "bare",
                             "pct_change": None, "coverage_pct": None})

        ep = SimpleNamespace(
            key="aetna_cvs", base_url="http://x",
            quirks=SimpleNamespace(page_size_by_resource={}, page_size=None,
                                   adaptive=None))
        session = SimpleNamespace(client_for=lambda ep: None)
        asyncio.run(pipeline._extract_and_load(
            session, ep, "Practitioner", None, asyncio.Semaphore(1),
            upsert=True, resume=False, allow_slim=allow_slim))
        return captured

    def test_allow_slim_reaches_upsert_batch(self, monkeypatch):
        captured = self._run(monkeypatch, allow_slim=True)
        assert captured == {"update": True, "allow_slim": True}

    def test_default_keeps_the_backstop_armed(self, monkeypatch):
        captured = self._run(monkeypatch, allow_slim=False)
        assert captured == {"update": True, "allow_slim": False}


class TestHarvestSlimGuard:
    """The _include harvest inserts with update=False, which the backstop
    deliberately does not gate — so the harvest must check the registry itself
    before opening a single connection."""

    def test_slim_base_resource_skips_the_sweep(self, monkeypatch):
        monkeypatch.setattr(pipeline, "_load_slim_units", lambda: {SLIM})
        # No further monkeypatching: reaching any DB call would blow up, which
        # is exactly what this test asserts cannot happen.
        ep = SimpleNamespace(key="aetna_cvs", base_url="http://x")
        out = asyncio.run(pipeline._harvest_one(
            None, ep, "PractitionerRole",
            {"include": ["PractitionerRole:practitioner"]}, None))
        assert out == {}

    def test_slim_target_type_is_dropped(self, monkeypatch):
        monkeypatch.setattr(pipeline, "_load_slim_units",
                            lambda: {("aetna_cvs", "Practitioner")})
        monkeypatch.setattr(pipeline, "ensure_payer_schema", lambda s: None)
        monkeypatch.setattr(pipeline, "_open_for_resource", lambda s, t: _FakeConn())

        async def fake_sweep(client, ep, base, cfg, sink, max_pages):
            return {"method": "include:PractitionerRole"}

        monkeypatch.setattr(pipeline, "include_sweep", fake_sweep)
        finalized = []

        def fake_finalize(conn, ep, rt, table, started, stats, errors, err):
            finalized.append(rt)
            return {"loaded": 0}

        monkeypatch.setattr(pipeline, "_finalize", fake_finalize)

        ep = SimpleNamespace(key="aetna_cvs", base_url="http://x")
        session = SimpleNamespace(client_for=lambda ep: None)
        out = asyncio.run(pipeline._harvest_one(
            session, ep, "PractitionerRole",
            {"include": ["PractitionerRole:practitioner"]}, None))
        assert finalized == ["PractitionerRole"]  # Practitioner dropped
        assert set(out) == {"PractitionerRole"}

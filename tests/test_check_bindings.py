"""Unit tests for scripts/check_bindings.py's SQL shapes and control flow.

No DB: a fake cursor records the SQL text/params each function issues so the
tests pin the exact predicates that matter (coalesce(cardinality(...)) > 0
for array columns, never IS NOT NULL; unnest only for array columns; the
'example'/unexpanded short-circuits that must never touch a 500M-row table).
"""
from __future__ import annotations

import importlib.util
import sys

import pytest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.quality import bindings  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "check_bindings", REPO / "scripts" / "check_bindings.py")
cb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cb)


class _FakeCopy:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def write_row(self, row):
        self.sink.append(row)


class _FakeCursor:
    """Records every execute()'s (sql, params); replays canned fetch results."""

    def __init__(self, fetchall_results=None, fetchone_results=None):
        self.executed: list[tuple[str, object]] = []
        self.copied: list[tuple] = []
        self._fetchall_queue = list(fetchall_results or [])
        self._fetchone_queue = list(fetchone_results or [])

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._fetchall_queue.pop(0) if self._fetchall_queue else []

    def fetchone(self):
        return self._fetchone_queue.pop(0) if self._fetchone_queue else None

    def copy(self, sql):
        self.executed.append((sql, None))
        return _FakeCopy(self.copied)


class _FakeConn:
    def __init__(self, cur: _FakeCursor):
        self._cur = cur
        self.committed = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True


CONN_TYPE_CHK = [c for c in bindings.CHECKS if c.table.name == "endpoint_resource"
                 and c.column == "connection_type"][0]
SPECIALTY_CHK = [c for c in bindings.CHECKS if c.table.name == "practitioner_role"
                and c.column == "specialty_codes"][0]


class TestScanDistinctCodesSQL:
    def test_array_column_uses_unnest_and_group_by(self):
        cur = _FakeCursor(fetchall_results=[[("207Q00000X", 5)]])
        conn = _FakeConn(cur)
        rows = cb._scan_distinct_codes(conn, "florida_blue", SPECIALTY_CHK)
        sql, params = cur.executed[0]
        assert "unnest(" in sql
        assert "GROUP BY 1" in sql
        assert params == {"payer": "florida_blue"}
        assert rows == [("207Q00000X", 5)]

    def test_scalar_column_has_no_unnest_and_filters_null(self):
        cur = _FakeCursor(fetchall_results=[[]])
        conn = _FakeConn(cur)
        cb._scan_distinct_codes(conn, "florida_blue", CONN_TYPE_CHK)
        sql, _params = cur.executed[0]
        assert "unnest(" not in sql
        assert 'IS NOT NULL' in sql
        assert '"connection_type"' in sql

    def test_cell_where_is_appended_when_present(self):
        org_net_chk = [c for c in bindings.CHECKS if c.resource_type == "Network"][0]
        cur = _FakeCursor(fetchall_results=[[]])
        conn = _FakeConn(cur)
        cb._scan_distinct_codes(conn, "florida_blue", org_net_chk)
        sql, _params = cur.executed[0]
        assert "is_network=1" in sql


class TestScanRollupSQL:
    def test_array_column_uses_cardinality_not_is_not_null(self):
        """The hard empty-array-vs-NULL constraint: rows_with_codes must use
        coalesce(cardinality(col),0) > 0, never IS NOT NULL (an empty array
        is NOT NULL but has zero codes)."""
        cur = _FakeCursor(fetchone_results=[(100, 80, 3)])
        conn = _FakeConn(cur)
        result = cb._scan_rollup(conn, "florida_blue", SPECIALTY_CHK, ["bogus"])
        sql, params = cur.executed[0]
        assert "coalesce(cardinality(" in sql
        assert "> 0" in sql
        assert "&&" in sql and "::text[]" in sql
        assert " IS NOT NULL" not in sql.replace("bad", "")  # no scalar-style null check
        assert params["bad"] == ["bogus"]
        assert result == (100, 80, 3)

    def test_scalar_column_uses_is_not_null_and_any(self):
        cur = _FakeCursor(fetchone_results=[(10, 4, 1)])
        conn = _FakeConn(cur)
        cb._scan_rollup(conn, "florida_blue", CONN_TYPE_CHK, ["bogus"])
        sql, _params = cur.executed[0]
        assert "cardinality(" not in sql
        assert "IS NOT NULL" in sql
        assert "= ANY(" in sql


class TestRunCheckControlFlow:
    @pytest.fixture(autouse=True)
    def _no_stale_delete(self, monkeypatch):
        """run_check clears stale resume rows first; these tests pass
        conn=None, so neutralize the helper (its SQL is asserted in
        TestStaleRowCleanup below)."""
        monkeypatch.setattr(cb, "_delete_stale_rows", lambda *a, **k: None)

    def test_example_strength_skips_scans(self, monkeypatch):
        monkeypatch.setattr(cb.bindings, "resolve_binding",
                            lambda index, chk: ("vs:x", "example"))
        monkeypatch.setattr(cb, "_scan_distinct_codes",
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError("scanned")))
        recorded = {}
        monkeypatch.setattr(cb, "_insert_element",
                            lambda conn, run_id, payer, chk, vs, strength, t, wc, wn, status:
                            recorded.update(status=status, total=t))
        status = cb.run_check(None, 1, "florida_blue", "florida_blue", SPECIALTY_CHK, {})
        assert status == "skipped-example"
        assert recorded == {"status": "skipped-example", "total": 0}

    def test_unexpandable_valueset_skips_scans(self, monkeypatch):
        monkeypatch.setattr(cb.bindings, "resolve_binding",
                            lambda index, chk: ("vs:x", "extensible"))
        monkeypatch.setattr(cb.bindings, "expand", lambda index, vs: None)
        monkeypatch.setattr(cb, "_scan_distinct_codes",
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError("scanned")))
        recorded = {}
        monkeypatch.setattr(cb, "_insert_element",
                            lambda conn, run_id, payer, chk, vs, strength, t, wc, wn, status:
                            recorded.update(status=status))
        status = cb.run_check(None, 1, "florida_blue", "florida_blue", SPECIALTY_CHK, {})
        assert status == "unexpanded"
        assert recorded == {"status": "unexpanded"}

    def test_ok_path_scans_and_classifies(self, monkeypatch):
        monkeypatch.setattr(cb.bindings, "resolve_binding",
                            lambda index, chk: ("vs:x", "required"))
        monkeypatch.setattr(cb.bindings, "expand", lambda index, vs: {"good"})
        monkeypatch.setattr(cb, "_scan_distinct_codes",
                            lambda conn, schema, chk: [("good", 8), ("bogus", 2)])
        monkeypatch.setattr(cb, "_insert_codes", lambda *a, **k: None)
        captured_bad = {}

        def fake_scan_rollup(conn, schema, chk, bad):
            captured_bad["bad"] = bad
            return (10, 10, 2)

        monkeypatch.setattr(cb, "_scan_rollup", fake_scan_rollup)
        recorded = {}
        monkeypatch.setattr(cb, "_insert_element",
                            lambda conn, run_id, payer, chk, vs, strength, t, wc, wn, status:
                            recorded.update(status=status, total=t, with_codes=wc, with_bad=wn))
        status = cb.run_check(None, 1, "florida_blue", "florida_blue", SPECIALTY_CHK, {})
        assert status == "ok"
        assert captured_bad["bad"] == ["bogus"]
        assert recorded == {"status": "ok", "total": 10, "with_codes": 10, "with_bad": 2}


class TestExistingPairs:
    def test_reads_triples_for_the_run(self):
        cur = _FakeCursor(fetchall_results=[[("florida_blue", "Location", "Location.type")]])
        conn = _FakeConn(cur)
        pairs = cb._existing_pairs(conn, 7)
        assert pairs == {("florida_blue", "Location", "Location.type")}
        sql, params = cur.executed[0]
        assert params == (7,)


class TestStaleRowCleanup:
    def test_deletes_both_tables_for_the_triple(self):
        """Resume re-runs unexpanded triples; without clearing both tables
        first the plain INSERT/COPY would PK-collide on the retry."""
        executed = []

        class _Cur:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params=None): executed.append((sql, params))

        class _Conn:
            def cursor(self): return _Cur()

        cb._delete_stale_rows(_Conn(), 7, "florida_blue", SPECIALTY_CHK)
        assert len(executed) == 2
        assert "binding_check_element" in executed[0][0]
        assert "binding_check_code" in executed[1][0]
        for sql, params in executed:
            assert params == (7, "florida_blue", SPECIALTY_CHK.resource_type,
                              SPECIALTY_CHK.element_path)

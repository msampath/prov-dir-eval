"""Checkpoint/resume for bare pagination + last_seen_at stamping.

Network-free and DB-free, following the repo's fake-FHIR-client and fake-psycopg
patterns (see tests/test_daterange_sweep.py and tests/test_review_fixes.py).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from provdir.etl.extract import (
    ResourceSink,
    _offset_of,
    _try_resume,
    bare_fingerprint,
    extract_resource,
    rewind_offset_url,
    _paginate,
)
from provdir.etl.loader import _copy_columns, delete_checkpoint, upsert_batch, upsert_checkpoint
from provdir.etl.pipeline import _validate_checkpoint
from provdir.etl.transform import transform_resource
from provdir.http_client import FhirError
from provdir.models import practitioner


# --- a fake offset-paginated FHIR server ----------------------------------
class _Quirks:
    base_params: dict = {}
    next_link_replace = None
    page_size = None
    page_size_by_resource: dict = {}
    adaptive: dict = {}


class _Endpoint:
    key = "fake"
    base_url = "https://fake.example/fhir"
    quirks = _Quirks()


class FakeOffsetServer:
    """Serves `total` resources in `page_size` pages via server-generated
    `_offset` next links — the shape Elevance/HAPI offset pagination takes."""

    def __init__(self, total: int, page_size: int, resource_type: str = "PractitionerRole"):
        self.total = total
        self.page_size = page_size
        self.rt = resource_type
        self.base = f"{_Endpoint.base_url}/{resource_type}"
        self.endpoint = _Endpoint()
        self.requested: list = []
        self.dead_cursor = False   # any offset fetch 404s (expired/stale cursor)
        self.raise_on_search = False

    def _page(self, offset: int) -> dict:
        entries = [
            {"resource": {"resourceType": self.rt, "id": f"r{i}"}}
            for i in range(offset, min(offset + self.page_size, self.total))
        ]
        bundle = {"resourceType": "Bundle", "entry": entries}
        nxt = offset + self.page_size
        if nxt < self.total:
            bundle["link"] = [{"relation": "next",
                               "url": f"{self.base}?_count={self.page_size}&_offset={nxt}"}]
        return bundle

    async def search_page(self, rt: str, params: dict) -> dict:
        self.requested.append(("search", dict(params)))
        if self.raise_on_search:
            raise FhirError("bare rejected", status=400)
        return self._page(0)

    async def get_json(self, path_or_url: str, params: dict | None = None) -> dict:
        if params and params.get("_summary") == "count":
            return {"resourceType": "Bundle", "total": self.total}
        self.requested.append(path_or_url)
        if self.dead_cursor and _offset_of(path_or_url) is not None:
            raise FhirError("dead cursor", status=404)
        return self._page(_offset_of(path_or_url) or 0)


# --- 1. rewind_offset_url pure cases --------------------------------------
def test_rewind_offset_basic():
    assert rewind_offset_url("https://x/PR?_count=1000&_offset=50000", 5, 1000) \
        == "https://x/PR?_count=1000&_offset=45000"


def test_rewind_offset_floors_at_zero():
    assert rewind_offset_url("https://x/PR?_offset=2000&_count=1000", 5, 1000) \
        == "https://x/PR?_offset=0&_count=1000"


def test_rewind_getpagesoffset_recognised():
    out = rewind_offset_url("https://x/PR?_getpagesoffset=30&_count=10", 5)
    assert "_getpagesoffset=0" in out


def test_rewind_no_offset_returns_none():
    # opaque/stateful cursor -> signal caller to retry verbatim
    assert rewind_offset_url("https://x/_getpages?_bundletype=searchset") is None


def test_rewind_uses_count_when_no_page_size_arg():
    assert rewind_offset_url("https://x/PR?_offset=9000&_count=100", 5) \
        == "https://x/PR?_offset=8500&_count=100"


def test_rewind_unknown_page_size_returns_verbatim():
    # offset present but no size to compute a rewind -> exact retry
    assert rewind_offset_url("https://x/PR?_offset=9000") == "https://x/PR?_offset=9000"


# --- 2. crash-consistency gold test ---------------------------------------
def test_resume_has_no_gap_after_simulated_kill():
    """A checkpoint is written atomically with each batch commit. Simulate a kill
    (a flush that never commits), then resume from the last durable checkpoint and
    assert the union of committed ids covers the full dataset with no gap."""
    total, page_size = 100, 10

    async def run():
        server = FakeOffsetServer(total, page_size)
        progress: dict = {}
        durable_ids: list[str] = []
        durable_ckpt = {"url": None}
        calls = {"n": 0}

        async def flush(batch):
            calls["n"] += 1
            if calls["n"] == 3:
                # process dies before this batch's txn commits: neither the rows
                # nor the checkpoint become durable.
                raise RuntimeError("simulated kill")
            durable_ckpt["url"] = progress.get("page_url")
            durable_ids.extend(r["id"] for r in batch)
            return len(batch)

        sink = ResourceSink(flush, batch=25)
        try:
            await _paginate(server, server.endpoint, "PractitionerRole", {}, sink,
                            None, progress=progress)
        except RuntimeError:
            pass

        assert durable_ckpt["url"] is not None, "a checkpoint should have been captured"

        # Resume from the last durable checkpoint.
        server2 = FakeOffsetServer(total, page_size)
        ckpt = {"resume_url": durable_ckpt["url"], "page_size": page_size}
        start = await _try_resume(server2, server2.endpoint, "PractitionerRole", ckpt)
        assert start is not None
        resume_ids: list[str] = []

        async def flush2(batch):
            resume_ids.extend(r["id"] for r in batch)
            return len(batch)

        sink2 = ResourceSink(flush2, batch=25)
        await _paginate(server2, server2.endpoint, "PractitionerRole", {}, sink2,
                        None, progress={}, start=start)

        return set(durable_ids) | set(resume_ids)

    covered = asyncio.run(run())
    assert covered == {f"r{i}" for i in range(total)}, "resume left a gap"


# --- 3. resume happy path + fallback --------------------------------------
def test_resume_happy_path_skips_page_one():
    async def run():
        server = FakeOffsetServer(total=50, page_size=10)
        ckpt = {"resume_url": f"{server.base}?_count=10&_offset=30",
                "pages_done": 3, "rows_added": 30, "page_size": 10}

        collected: list[str] = []

        async def flush(batch):
            collected.extend(r["id"] for r in batch)
            return len(batch)

        sink = ResourceSink(flush, batch=1000)
        stats = await extract_resource(server, server.endpoint, "PractitionerRole",
                                       sink, resume_ckpt=ckpt, progress={})
        await sink.close()
        return server, stats, collected

    server, stats, collected = asyncio.run(run())
    assert stats["method"] == "bare"
    assert stats["resumed_from_page"] == 3
    # page-1 search must never be issued when the resume takes
    assert not any(isinstance(r, tuple) and r[0] == "search" for r in server.requested)


def test_resume_fallback_on_dead_cursor():
    async def run():
        server = FakeOffsetServer(total=5, page_size=5)
        server.dead_cursor = True
        dead = f"{server.base}?_count=5&_offset=999999"
        ckpt = {"resume_url": dead, "pages_done": 7, "rows_added": 70, "page_size": 5}

        async def flush(batch):
            return len(batch)

        sink = ResourceSink(flush, batch=1000)
        stats = await extract_resource(server, server.endpoint, "PractitionerRole",
                                       sink, resume_ckpt=ckpt, progress={})
        await sink.close()
        return server, stats

    server, stats = asyncio.run(run())
    assert stats["method"] == "bare"              # not misclassified "unsupported"
    assert "resumed_from_page" not in stats        # fell back to fresh
    assert any(isinstance(r, tuple) and r[0] == "search" for r in server.requested)


# --- 4. _validate_checkpoint ----------------------------------------------
def test_validate_checkpoint_accepts_fresh_matching():
    ck = {"params_fingerprint": "fp", "updated_at": datetime.now(tz=timezone.utc)}
    assert _validate_checkpoint(ck, "fp", 72.0) is ck


def test_validate_checkpoint_rejects_stale():
    ck = {"params_fingerprint": "fp",
          "updated_at": datetime.now(tz=timezone.utc) - timedelta(hours=100)}
    assert _validate_checkpoint(ck, "fp", 72.0) is None


def test_validate_checkpoint_rejects_fingerprint_mismatch():
    ck = {"params_fingerprint": "old", "updated_at": datetime.now(tz=timezone.utc)}
    assert _validate_checkpoint(ck, "new", 72.0) is None


def test_validate_checkpoint_none():
    assert _validate_checkpoint(None, "fp", 72.0) is None


def test_bare_fingerprint_changes_with_page_size():
    class _Q1:
        base_params: dict = {}
        page_size = 100
        page_size_by_resource: dict = {}

    class _Q2:
        base_params: dict = {}
        page_size = 250
        page_size_by_resource: dict = {}

    class _E:
        base_url = "https://x/fhir"

    e1, e2 = _E(), _E()
    e1.quirks, e2.quirks = _Q1(), _Q2()
    assert bare_fingerprint(e1, "Practitioner", 1000) != bare_fingerprint(e2, "Practitioner", 1000)


# --- 5. checkpoint SQL (fake cursor) --------------------------------------
class _FakeCursor:
    def __init__(self, sink):
        self.sink = sink
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *a):
        self.sink.append(sql)

    def fetchone(self):
        return None

    def copy(self, sql):
        self.sink.append(sql)

        class _C:
            def __enter__(s):
                return s

            def __exit__(s, *a):
                return False

            def set_types(s, t):
                pass

            def write_row(s, r):
                pass

        return _C()


class _FakeConn:
    def __init__(self):
        self.sql: list[str] = []
        self.commits = 0

    def cursor(self):
        return _FakeCursor(self.sql)

    def commit(self):
        self.commits += 1


def test_upsert_checkpoint_sql_sets_updated_at_and_does_not_commit():
    conn = _FakeConn()
    upsert_checkpoint(conn, "p", "PractitionerRole", "u", 10, 100, 1000, "fp")
    sql = conn.sql[0]
    assert "INSERT INTO public.extract_checkpoint" in sql
    assert "ON CONFLICT (payer_id, resource_type) DO UPDATE" in sql
    assert "updated_at = now()" in sql
    assert conn.commits == 0, "loader helpers must not commit; caller owns the txn"


def test_delete_checkpoint_sql():
    conn = _FakeConn()
    delete_checkpoint(conn, "p", "PractitionerRole")
    assert "DELETE FROM public.extract_checkpoint" in conn.sql[0]
    assert conn.commits == 0


# --- 6. heartbeat ----------------------------------------------------------
def test_heartbeat_every_50_pages(caplog):
    async def run():
        server = FakeOffsetServer(total=120, page_size=1)

        async def flush(batch):
            return len(batch)

        sink = ResourceSink(flush, batch=100000)
        with caplog.at_level(logging.INFO, logger="provdir.etl.extract"):
            await _paginate(server, server.endpoint, "PractitionerRole", {}, sink,
                            None, progress={})
        return [r for r in caplog.records if "pages=" in r.getMessage()]

    beats = asyncio.run(run())
    assert len(beats) == 2, f"expected heartbeats at pages 50 and 100, got {len(beats)}"


def test_no_heartbeat_without_progress(caplog):
    async def run():
        server = FakeOffsetServer(total=120, page_size=1)

        async def flush(batch):
            return len(batch)

        sink = ResourceSink(flush, batch=100000)
        with caplog.at_level(logging.INFO, logger="provdir.etl.extract"):
            await _paginate(server, server.endpoint, "PractitionerRole", {}, sink, None)
        return [r for r in caplog.records if "pages=" in r.getMessage()]

    assert asyncio.run(run()) == []


# --- 7. partition-fallback guard ------------------------------------------
def test_partition_fallback_clears_progress_active():
    async def run():
        server = FakeOffsetServer(total=5, page_size=5)
        server.raise_on_search = True   # bare 400 -> falls out of the bare path
        progress: dict = {"active": True, "page_url": "stale"}

        async def flush(batch):
            return len(batch)

        sink = ResourceSink(flush, batch=1000)
        # PractitionerRole has no partition strategy -> returns needs-partition fast
        await extract_resource(server, server.endpoint, "PractitionerRole", sink,
                               progress=progress)
        return progress

    progress = asyncio.run(run())
    assert not progress.get("active"), "stale bare checkpoint state must be cleared"


# --- 8. last_seen_at -------------------------------------------------------
def test_transform_stamps_last_seen_at():
    row = transform_resource({"resourceType": "Practitioner", "id": "p1"}, "pk", "https://u")
    ts = row["last_seen_at"]
    assert isinstance(ts, datetime) and ts.tzinfo is not None


def test_last_seen_at_in_copy_columns_and_update_set():
    assert "last_seen_at" in _copy_columns(practitioner)
    conn = _FakeConn()
    upsert_batch(conn, practitioner, [{"payer_id": "p", "id": "a", "source_base_url": "u",
                                       "resource": {}, "raw_hash": "h"}], update=True)
    insert = [s for s in conn.sql if s.startswith("INSERT")][0]
    assert '"last_seen_at" = EXCLUDED."last_seen_at"' in insert

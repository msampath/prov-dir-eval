"""Daterange sweep vs a fake server with Humana's pathology: _summary=count
times out on wide windows but answers on narrow ones. The sweep must bisect
blind through the uncountable region instead of bare-fetching a decades-wide
window (which would trip the server's per-search cap)."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone

from provdir.config import Endpoint
from provdir.etl.extract import ResourceSink, _quantize_midpoint, adaptive_extract

COUNTABLE = timedelta(days=400)  # counts succeed only at <= ~1y widths


def _parse(bound: str) -> datetime:
    s = bound[2:]  # strip ge/lt
    if "T" in s:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    y, m, d = (int(x) for x in s.split("-"))
    return datetime(y, m, d, tzinfo=timezone.utc)


class FakeClient:
    """10 records live on 2024-06-15; counting any window wider than ~1y raises."""

    def __init__(self, endpoint=None):
        self.fetched_widths: list[timedelta] = []
        self.record_day = datetime(2024, 6, 15, tzinfo=timezone.utc)
        self.endpoint = endpoint  # real FhirClient always exposes .endpoint

    def _bounds(self, params):
        lo, hi = params["_lastUpdated"]
        return _parse(lo), _parse(hi)

    def _total(self, lo, hi):
        return 10 if lo <= self.record_day < hi else 0

    async def get_json(self, resource_type, params=None):
        lo, hi = self._bounds(params)
        if params.get("_summary") == "count":
            if hi - lo > COUNTABLE:
                raise TimeoutError("count too wide")
            return {"resourceType": "Bundle", "total": self._total(lo, hi)}
        raise AssertionError("unexpected get_json")

    async def search_page(self, resource_type, params=None):
        lo, hi = self._bounds(params)
        self.fetched_widths.append(hi - lo)
        entries = [
            {"resource": {"resourceType": resource_type, "id": f"r{i}"}}
            for i in range(self._total(lo, hi))
        ]
        return {"resourceType": "Bundle", "total": len(entries), "entry": entries}


def test_blind_bisection_through_uncountable_region():
    ep = Endpoint(key="fake", payer_name="Fake", base_url="https://fake.example/fhir")
    client = FakeClient(endpoint=ep)
    got: list[dict] = []

    async def flush(batch):
        got.extend(batch)
        return len(batch)

    async def run():
        sink = ResourceSink(flush, batch=5000)
        cfg = {"param": "_lastUpdated", "mode": "daterange",
               "start": "2000-01-01", "end": "2026-01-01", "bucket_max": 100}
        stats = await adaptive_extract(client, ep, "Practitioner", cfg, sink)
        await sink.close()
        return stats

    stats = asyncio.run(run())

    assert stats["blind_splits"] > 0, "wide uncountable windows must bisect blind"
    assert len(got) == 10, "all records recovered"
    # The decades-wide root must never be bare-fetched.
    assert all(w <= COUNTABLE for w in client.fetched_widths), (
        f"fetched a window wider than the countable region: {max(client.fetched_widths)}"
    )
    # Countable leaves contribute an exhaustive sum usable as the denominator.
    assert stats["counted_total"] == 10
    assert stats["server_total_source"] == "daterange_window_sum"


class DenseSecondClient:
    """8 records inside one calendar second: 4 at .100s, 4 at .600s. A scaled-down
    version of the azblue Organization pathology, where 16,154 records shared one
    whole-second _lastUpdated value and the (then-hardcoded) 1-second min_window
    made that window unsplittable."""

    def __init__(self, endpoint=None):
        self.fetched_widths: list[timedelta] = []
        self.endpoint = endpoint
        self.early = datetime(2024, 1, 1, 0, 0, 0, 100000, tzinfo=timezone.utc)
        self.late = datetime(2024, 1, 1, 0, 0, 0, 600000, tzinfo=timezone.utc)

    def _bounds(self, params):
        lo, hi = params["_lastUpdated"]
        return _parse(lo), _parse(hi)

    def _total(self, lo, hi):
        return (4 if lo <= self.early < hi else 0) + (4 if lo <= self.late < hi else 0)

    async def get_json(self, resource_type, params=None):
        lo, hi = self._bounds(params)
        return {"resourceType": "Bundle", "total": self._total(lo, hi)}

    async def search_page(self, resource_type, params=None):
        lo, hi = self._bounds(params)
        self.fetched_widths.append(hi - lo)
        n = self._total(lo, hi)
        entries = [{"resource": {"resourceType": resource_type, "id": f"r{i}"}} for i in range(n)]
        return {"resourceType": "Bundle", "total": n, "entry": entries}


def _run_dense_second(client, ep, min_window_seconds):
    got: list[dict] = []

    async def flush(batch):
        got.extend(batch)
        return len(batch)

    async def run():
        sink = ResourceSink(flush, batch=5000)
        cfg = {"param": "_lastUpdated", "mode": "daterange",
               "start": "2024-01-01T00:00:00Z", "end": "2024-01-01T00:00:01Z",
               "bucket_max": 4}
        if min_window_seconds is not None:
            cfg["min_window_seconds"] = min_window_seconds
        stats = await adaptive_extract(client, ep, "Organization", cfg, sink)
        await sink.close()
        return stats

    stats = asyncio.run(run())
    return stats, got


def test_default_min_window_cannot_bisect_below_one_second():
    """Unchanged behaviour: with no min_window_seconds override, a window that's
    already exactly 1 second wide is not further split, even when oversized."""
    ep = Endpoint(key="fake", payer_name="Fake", base_url="https://fake.example/fhir")
    client = DenseSecondClient(endpoint=ep)
    stats, got = _run_dense_second(client, ep, min_window_seconds=None)
    assert all(w >= timedelta(seconds=1) for w in client.fetched_widths)
    assert stats["subdivided"] == 0


def test_sub_second_min_window_bisects_the_oversized_second():
    """With min_window_seconds=0.25, the 8-record whole second (> bucket_max=4)
    splits at its midpoint into two 4-record halves, each a leaf below one
    second — proving the fix lets bisection terminate under the offset ceiling
    (azblue's real leaves were 8,083 and 8,071 records at the .250 boundary)."""
    ep = Endpoint(key="fake", payer_name="Fake", base_url="https://fake.example/fhir")
    client = DenseSecondClient(endpoint=ep)
    stats, got = _run_dense_second(client, ep, min_window_seconds=0.25)
    assert any(w < timedelta(seconds=1) for w in client.fetched_widths)
    assert stats["subdivided"] >= 1
    assert len(got) == 8, "all 8 records recovered once the oversized second is split"


WHOLE_SECOND_BOUND_RE = re.compile(r"^(ge|lt)\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class NowSpanningClient:
    """3 records scattered across a `now`-derived range: one old, one middle,
    one deliberately close to `end` (the region most exposed to the original
    bug, since the default end=now+1day is inherited unchanged down the
    entire right-hand recursion spine until it reaches a leaf). Records every
    ge/lt bound ever sent, so a test can assert none of them are fractional."""

    def __init__(self, endpoint=None):
        self.endpoint = endpoint
        self.bounds_seen: list[str] = []
        now = datetime.now(timezone.utc)
        self.instants = [
            datetime(2005, 3, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2015, 7, 15, 6, 30, 0, tzinfo=timezone.utc),
            now - timedelta(hours=2),
        ]

    def _bounds(self, params):
        lo_raw, hi_raw = params["_lastUpdated"]
        self.bounds_seen.append(lo_raw)
        self.bounds_seen.append(hi_raw)
        return _parse(lo_raw), _parse(hi_raw)

    def _total(self, lo, hi):
        return sum(1 for t in self.instants if lo <= t < hi)

    async def get_json(self, resource_type, params=None):
        lo, hi = self._bounds(params)
        return {"resourceType": "Bundle", "total": self._total(lo, hi)}

    async def search_page(self, resource_type, params=None):
        lo, hi = self._bounds(params)
        n = self._total(lo, hi)
        entries = [{"resource": {"resourceType": resource_type, "id": f"r{i}"}} for i in range(n)]
        return {"resourceType": "Bundle", "total": n, "entry": entries}


def test_default_daterange_never_emits_a_fractional_bound():
    """Regression test for the bug caught in review: with default
    min_window_seconds (no sub-second opt-in), a sweep with no explicit `end`
    (so it defaults to datetime.now(UTC) + 1 day, which carries microseconds)
    must never send a fractional _lastUpdated bound to the server. Before the
    fix, _iso() silently floored these on render; after making _iso() honest,
    the root split (and the whole right-hand spine, which inherits the
    unquantized `end`) would have started emitting `.625Z`-style bounds to
    humana and capital_blue on every run. This asserts the wire format is
    unaffected, not just the two payers checked by hand."""
    ep = Endpoint(key="fake", payer_name="Fake", base_url="https://fake.example/fhir")
    client = NowSpanningClient(endpoint=ep)
    got: list[dict] = []

    async def flush(batch):
        got.extend(batch)
        return len(batch)

    async def run():
        sink = ResourceSink(flush, batch=5000)
        cfg = {"param": "_lastUpdated", "mode": "daterange",
               "start": "2000-01-01", "bucket_max": 1}
        # No "end" key: defaults to datetime.now(UTC) + 1 day, the real trigger.
        stats = await adaptive_extract(client, ep, "Organization", cfg, sink)
        await sink.close()
        return stats

    asyncio.run(run())

    assert client.bounds_seen, "sanity: the sweep actually queried something"
    bad = [b for b in client.bounds_seen if not WHOLE_SECOND_BOUND_RE.match(b)]
    assert not bad, f"fractional _lastUpdated bound(s) leaked to the wire: {bad}"
    assert len(got) == 3, "all 3 scattered records recovered"


def test_quantize_midpoint_skips_snap_that_would_land_on_s():
    """Textbook collision: mid's whole-second floor lands EXACTLY on s (s is
    already whole; width is 1.5s so mid is s+0.75s, whose floor is s itself).
    Snapping would empty the left child and hand the right child the full
    parent range back — an infinite loop. The guard must refuse the snap."""
    s = datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    e = datetime(2024, 1, 1, 0, 0, 1, 500000, tzinfo=timezone.utc)  # width 1.5s
    mid = s + (e - s) / 2  # 00:00:00.750000; floor -> 00:00:00.000000 == s
    result = _quantize_midpoint(s, e, mid)
    assert s < result < e, "must never return a boundary outside the open window"
    assert result == mid, "collision case: guard must skip the snap, not collapse it"


def test_quantize_midpoint_skips_snap_that_would_land_before_s():
    """s itself carries a fractional offset (as it would from a caller that
    doesn't hold the whole-seconds invariant _daterange_sweep maintains) and
    the window is only slightly over 1 second wide. floor(mid) lands BEFORE
    s, which is worse than merely degenerate — an inverted/negative-width
    child. The guard must refuse the snap here too."""
    s = datetime(2024, 1, 1, 0, 0, 0, 100000, tzinfo=timezone.utc)  # .100
    e = datetime(2024, 1, 1, 0, 0, 1, 300000, tzinfo=timezone.utc)  # .300, width 1.2s
    mid = s + (e - s) / 2  # 00:00:00.700000; floor -> 00:00:00.000000 < s
    result = _quantize_midpoint(s, e, mid)
    assert s < result < e
    assert result == mid


def test_quantize_midpoint_snaps_the_ordinary_case():
    """Sanity check on the happy path so the guard tests above aren't
    vacuously trivial: an ordinary multi-second window DOES get snapped."""
    s = datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    e = datetime(2024, 1, 1, 0, 0, 5, 0, tzinfo=timezone.utc)  # width 5s
    mid = s + (e - s) / 2  # 00:00:02.500000
    result = _quantize_midpoint(s, e, mid)
    assert result == datetime(2024, 1, 1, 0, 0, 2, 0, tzinfo=timezone.utc)

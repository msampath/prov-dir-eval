"""Daterange sweep vs a fake server with Humana's pathology: _summary=count
times out on wide windows but answers on narrow ones. The sweep must bisect
blind through the uncountable region instead of bare-fetching a decades-wide
window (which would trip the server's per-search cap)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from provdir.config import Endpoint
from provdir.etl.extract import ResourceSink, adaptive_extract

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

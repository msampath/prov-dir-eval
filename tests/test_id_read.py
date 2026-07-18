"""Reference-graph id-read harvest: fetch un-searchable resources by
GET {Type}/{id} (bypasses the search pagination cap). Models Regence
Practitioner, which has no working search partition but reads fine by id."""

from __future__ import annotations

import asyncio

from provdir.config import Endpoint
from provdir.etl.extract import ResourceSink, id_read_extract
from provdir.http_client import FhirError


class FakeClient:
    """GET Practitioner/<id> returns that resource; a couple ids 404 (stale refs)."""

    def __init__(self, endpoint, known):
        self.endpoint = endpoint
        self.known = known  # set of live ids
        self.reads: list[str] = []

    async def get_json(self, path, params=None):
        rtype, _, rid = path.partition("/")
        assert rtype == "Practitioner"
        self.reads.append(rid)
        if rid not in self.known:
            raise FhirError(f"{rid} not found", status=404)
        return {"resourceType": "Practitioner", "id": rid, "name": [{"family": rid}]}


def test_id_read_harvests_by_id_and_skips_404():
    ep = Endpoint(key="regence", payer_name="Regence", base_url="https://x.example/fhir")
    ids = [f"p{i}" for i in range(12)]
    known = {f"p{i}" for i in range(10)}  # p10, p11 are stale (404)
    client = FakeClient(ep, known)
    got: list[str] = []

    async def flush(batch):
        got.extend(r["id"] for r in batch)
        return len(batch)

    async def run():
        sink = ResourceSink(flush, batch=5000)
        stats = await id_read_extract(client, ep, "Practitioner", {}, sink, ids)
        await sink.close()
        return stats

    stats = asyncio.run(run())

    assert stats["reads"] == 12
    assert stats["hits"] == 10          # only live ids landed
    assert stats["not_found"] == 2      # stale refs skipped, not errors
    assert stats["fetch_errors"] == 0
    assert set(got) == known

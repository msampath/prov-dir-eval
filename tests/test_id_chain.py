"""Id-chained harvesting of a filter-only reference resource (Excellus roles):
one PractitionerRole?practitioner=<id> query per loaded practitioner id, keeping
only the target resource type from each bundle."""

from __future__ import annotations

import asyncio

from provdir.config import Endpoint
from provdir.etl.extract import ResourceSink, id_chain_extract


class FakeClient:
    """Each practitioner id maps to a fixed number of roles. A bare/broad search
    would 400, but chained practitioner=<id> works."""

    def __init__(self, endpoint, roles_per):
        self.endpoint = endpoint
        self.roles_per = roles_per  # {practitioner_id: n_roles}
        self.chained_ids: list[str] = []

    async def search_page(self, resource_type, params=None):
        assert resource_type == "PractitionerRole"
        pid = params.get("practitioner")
        assert pid is not None, "id_chain must query by the chain param, not bare"
        self.chained_ids.append(pid)
        n = self.roles_per.get(pid, 0)
        entries = [
            {"resource": {"resourceType": "PractitionerRole", "id": f"{pid}-role{i}"}}
            for i in range(n)
        ]
        # include a stray Practitioner entry to prove filtering to target type
        entries.append({"resource": {"resourceType": "Practitioner", "id": pid}})
        return {"resourceType": "Bundle", "entry": entries}

    async def get_json(self, url, params=None):  # no pagination in this fixture
        raise AssertionError("unexpected pagination")


def test_id_chain_harvests_roles_per_practitioner():
    ep = Endpoint(key="excellus", payer_name="Excellus", base_url="https://ex.example/fhir")
    ids = [f"p{i}" for i in range(10)]
    roles_per = {f"p{i}": i % 3 for i in range(10)}  # 0,1,2,0,1,2,...
    client = FakeClient(ep, roles_per)
    got: list[str] = []

    async def flush(batch):
        got.extend(r["id"] for r in batch)
        return len(batch)

    async def run():
        sink = ResourceSink(flush, batch=5000)
        cfg = {"mode": "id_chain", "chain_param": "practitioner", "source_table": "practitioner"}
        stats = await id_chain_extract(client, ep, "PractitionerRole", cfg, sink, ids)
        await sink.close()
        return stats

    stats = asyncio.run(run())

    assert stats["queries"] == 10, "one chained query per id"
    assert set(client.chained_ids) == set(ids)
    # only PractitionerRole entries kept (the stray Practitioner entries dropped)
    assert len(got) == sum(roles_per.values())
    assert all(rid.endswith(("role0", "role1", "role2")) for rid in got)
    assert stats["source_ids"] == 10


class BatchClient:
    """Honours comma-OR: practitioner=id1,id2,... returns roles for every id in
    the group, so N ids cost one request."""

    def __init__(self, endpoint, roles_per):
        self.endpoint = endpoint
        self.roles_per = roles_per
        self.group_sizes: list[int] = []

    async def search_page(self, resource_type, params=None):
        group = params["practitioner"].split(",")
        self.group_sizes.append(len(group))
        entries = [
            {"resource": {"resourceType": "PractitionerRole", "id": f"{pid}-role{i}"}}
            for pid in group
            for i in range(self.roles_per.get(pid, 0))
        ]
        return {"resourceType": "Bundle", "entry": entries}

    async def get_json(self, url, params=None):
        raise AssertionError("unexpected pagination")


def test_id_chain_or_batches_ids():
    ep = Endpoint(key="humana", payer_name="Humana", base_url="https://h.example/api")
    ids = [f"p{i}" for i in range(10)]
    roles_per = {f"p{i}": i % 3 for i in range(10)}
    client = BatchClient(ep, roles_per)
    got: list[str] = []

    async def flush(batch):
        got.extend(r["id"] for r in batch)
        return len(batch)

    async def run():
        sink = ResourceSink(flush, batch=5000)
        cfg = {"mode": "id_chain", "chain_param": "practitioner",
               "source_table": "practitioner", "chain_batch": 3}
        stats = await id_chain_extract(client, ep, "PractitionerRole", cfg, sink, ids)
        await sink.close()
        return stats

    stats = asyncio.run(run())

    # 10 ids in groups of 3 -> 4 queries (3,3,3,1), not 10
    assert stats["queries"] == 4, "OR-batching collapses ids into fewer requests"
    assert client.group_sizes == [3, 3, 3, 1]
    assert len(got) == sum(roles_per.values()), "every id's roles still harvested"
    assert stats["source_ids"] == 10

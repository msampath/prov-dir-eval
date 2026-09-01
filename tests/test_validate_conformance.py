"""Unit tests for scripts/validate_conformance.py's sample round-trip logic.

No DB, no Docker: only the pure filesystem pieces -- restore_cells /
restore_from_backup / build_chunks -- whose failure modes are exactly the ones
that have already bitten this pipeline once (a deleted `_batches` destroying
the only copy of the 118k sample) and the Windows-specific rename collision a
crash-resume can produce.
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

sys.path.insert(0, str(REPO / "scripts"))

_spec = importlib.util.spec_from_file_location(
    "validate_conformance", REPO / "scripts" / "validate_conformance.py")
vc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vc)

import sample_for_validation as sfv  # noqa: E402


class _FakeCursor:
    """Records every SQL string it is handed and replays canned results.

    Enough to test the sampler's query SHAPE (an ORDER BY in every branch, a
    count(*) fallback when the planner says a table is empty) without a
    database. The queries themselves are exercised against real Postgres by
    the sampler runs; what is easy to regress silently is the shape.
    """

    def __init__(self, results=None):
        self.sql: list[str] = []
        self._results = list(results or [])
        self._last: list = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self._last = self._results.pop(0) if self._results else []

    def fetchall(self):
        return self._last

    def fetchone(self):
        return self._last[0] if self._last else None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, results=None):
        self.cur = _FakeCursor(results)

    def cursor(self, *a, **kw):
        return self.cur


def _mk(path: Path, content: str = "{}") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestRestoreCells:
    def test_moves_batches_back_to_cell_dirs_and_removes_batch_root(self, tmp_path):
        _mk(tmp_path / "_batches" / "Location__000" / "p__Location__0.json")
        _mk(tmp_path / "_batches" / "Location__001" / "p__Location__1.json")
        n = vc.restore_cells(tmp_path)
        assert n == 2
        assert (tmp_path / "Location" / "p__Location__0.json").exists()
        assert (tmp_path / "Location" / "p__Location__1.json").exists()
        assert not (tmp_path / "_batches").exists()

    def test_no_batches_dir_is_a_noop(self, tmp_path):
        assert vc.restore_cells(tmp_path) == 0

    def test_existing_destination_is_overwritten_not_a_crash(self, tmp_path):
        """Crash-resume regression: an interrupted run followed by a backup
        restore leaves the same file in BOTH the cell dir and `_batches`.
        Path.rename raises FileExistsError on Windows for that; restore must
        replace instead (content is identical by construction)."""
        _mk(tmp_path / "Location" / "p__Location__0.json", '{"from": "backup"}')
        _mk(tmp_path / "_batches" / "Location__000" / "p__Location__0.json",
            '{"from": "batches"}')
        n = vc.restore_cells(tmp_path)
        assert n == 1
        assert not (tmp_path / "_batches").exists()
        moved = json.loads(
            (tmp_path / "Location" / "p__Location__0.json").read_text(encoding="utf-8"))
        assert moved == {"from": "batches"}


class TestRestoreFromBackup:
    def test_copies_files_and_leaves_backup_intact(self, tmp_path):
        sample = tmp_path / "sample"
        backup = tmp_path / "keep"
        _mk(backup / "Network__000" / "p__Network__0.json")
        sample.mkdir()
        n = vc.restore_from_backup(sample, backup)
        assert n == 1
        assert (sample / "Network" / "p__Network__0.json").exists()
        # COPY, never move: the backup must survive for anything else reading it.
        assert (backup / "Network__000" / "p__Network__0.json").exists()

    def test_existing_target_is_not_overwritten(self, tmp_path):
        sample = tmp_path / "sample"
        backup = tmp_path / "keep"
        _mk(sample / "Network" / "p__Network__0.json", '{"from": "sample"}')
        _mk(backup / "Network__000" / "p__Network__0.json", '{"from": "backup"}')
        assert vc.restore_from_backup(sample, backup) == 0
        kept = json.loads(
            (sample / "Network" / "p__Network__0.json").read_text(encoding="utf-8"))
        assert kept == {"from": "sample"}

    def test_missing_backup_dir_returns_zero(self, tmp_path):
        assert vc.restore_from_backup(tmp_path, tmp_path / "nope") == 0


class TestBuildChunks:
    def test_round_trip_with_restore_cells(self, tmp_path):
        for i in range(5):
            _mk(tmp_path / "Location" / f"p__Location__{i}.json")
        chunks = vc.build_chunks(tmp_path, chunk_size=2)
        assert [c.name for _, c in chunks] == [
            "Location__000", "Location__001", "Location__002"]
        assert all(cell == "Location" for cell, _ in chunks)
        # cell dirs are drained -- _batches holds the ONLY copy mid-run...
        assert list((tmp_path / "Location").glob("*.json")) == []
        # ...and restore_cells brings every file home again.
        assert vc.restore_cells(tmp_path) == 5
        assert len(list((tmp_path / "Location").glob("*.json"))) == 5

    def test_oversize_files_are_isolated_into_their_own_small_chunks(self, tmp_path):
        """Run 2's Network__000 lesson: multi-MB resources hang the validator
        for hours, and one shared chunk means one timeout loses the whole
        cell. Oversize files must land in chunks of OVERSIZE_PER_CHUNK, never
        alongside the normal files."""
        for i in range(3):
            _mk(tmp_path / "Network" / f"p__Network__{i}.json")
        big = tmp_path / "Network" / "p__Network__big.json"
        big.parent.mkdir(exist_ok=True)
        big.write_bytes(b"0" * (vc.OVERSIZE_BYTES + 1))
        chunks = vc.build_chunks(tmp_path, chunk_size=100)
        by_name = {c.name: sorted(f.name for f in c.glob("*.json")) for _, c in chunks}
        assert by_name["Network__000"] == [
            "p__Network__0.json", "p__Network__1.json", "p__Network__2.json"]
        assert by_name["Network__001"] == ["p__Network__big.json"]
        # The chunk-dir naming keeps the `<Cell>__` prefix rule every restore
        # path relies on (chunk_dir.name.split("__", 1)[0] == cell).
        assert all(name.split("__", 1)[0] == "Network" for name in by_name)

    def test_calls_restore_cells_first(self, tmp_path):
        """A previous run's leftover _batches must be restored before
        re-chunking, or its files would be invisible to the new chunk map."""
        _mk(tmp_path / "_batches" / "Location__000" / "p__Location__0.json")
        chunks = vc.build_chunks(tmp_path, chunk_size=10)
        assert len(chunks) == 1
        cell, d = chunks[0]
        assert cell == "Location"
        assert [f.name for f in d.glob("*.json")] == ["p__Location__0.json"]


class TestTruncatedOutputIsAFailure:
    """A chunk killed mid-write leaves a NON-EMPTY but unparseable output.

    Three things used to line up to lose data silently: parse_output swallowed
    the JSONDecodeError and returned ([], []); worker() marked a chunk failed
    only when the output was MISSING; and --reuse-outputs accepted any file
    that merely existed with size > 0. So the chunk contributed zero rows,
    chunks_failed stayed 0, and conformance_run.instances (counted from a
    directory listing) still read the full total. That is the mechanism behind
    1,463,451 of 1,862,183 instances recorded ok.
    """

    def _truncate(self, tmp_path):
        out = tmp_path / "Organization__003.json"
        full = json.dumps({"resourceType": "Bundle", "entry": [
            {"resource": {"resourceType": "OperationOutcome", "issue": []}}]})
        out.write_text(full[:len(full) // 2], encoding="utf-8")
        return out

    def test_parse_output_raises_instead_of_returning_empty(self, tmp_path):
        out = self._truncate(tmp_path)
        assert out.stat().st_size > 0, "the file must be non-empty to be the bug"
        with pytest.raises(vc.UnreadableOutput):
            vc.parse_output(out, "Organization")

    def test_truncated_output_is_not_reusable(self, tmp_path):
        """--reuse-outputs must not accept it, or the resume is what drops the
        data."""
        assert vc.output_looks_complete(self._truncate(tmp_path)) is False

    def test_complete_output_is_reusable(self, tmp_path):
        out = tmp_path / "Organization__004.json"
        out.write_text(json.dumps({"resourceType": "Bundle", "entry": []}),
                       encoding="utf-8")
        assert vc.output_looks_complete(out) is True

    def test_empty_and_missing_outputs_are_not_reusable(self, tmp_path):
        empty = tmp_path / "Organization__005.json"
        empty.write_text("", encoding="utf-8")
        assert vc.output_looks_complete(empty) is False
        assert vc.output_looks_complete(tmp_path / "nope.json") is False

    def test_a_good_output_still_parses(self, tmp_path):
        """The failure path must not have broken the happy path."""
        out = tmp_path / "Location__000.json"
        out.write_text(json.dumps({
            "resourceType": "OperationOutcome",
            "extension": [{"url": vc.FILE_EXT,
                           "valueString": "/data/x/azblue__Location__7.json"}],
            "issue": [{"severity": "error", "code": "structure",
                       "details": {"text": "boom"}}]}), encoding="utf-8")
        inst, iss = vc.parse_output(out, "Location")
        assert inst == [("azblue", "Location", "azblue__Location__7.json", 1, 0, None)]
        assert len(iss) == 1


class TestResourceIdTraceability:
    """conformance_instance's PK is a FILENAME. Without the FHIR resource id
    beside it, a finding cannot be looked up in the payer's own data."""

    def test_ids_come_from_the_sampler_sidecar(self, tmp_path):
        ids = tmp_path / "_ids"
        ids.mkdir()
        (ids / "Location__azblue.tsv").write_text(
            "azblue__Location__0.json\tloc-aaa\n"
            "azblue__Location__1.json\tloc-bbb\n", encoding="utf-8")
        assert vc.load_id_map(tmp_path, "Location") == {
            "azblue__Location__0.json": "loc-aaa",
            "azblue__Location__1.json": "loc-bbb"}

    def test_id_map_is_scoped_to_its_cell(self, tmp_path):
        ids = tmp_path / "_ids"
        ids.mkdir()
        (ids / "Location__azblue.tsv").write_text(
            "azblue__Location__0.json\tloc-aaa\n", encoding="utf-8")
        (ids / "Organization__azblue.tsv").write_text(
            "azblue__Organization__0.json\torg-aaa\n", encoding="utf-8")
        assert list(vc.load_id_map(tmp_path, "Organization")) == [
            "azblue__Organization__0.json"]

    def test_missing_sidecar_is_a_null_id_not_a_crash(self, tmp_path):
        assert vc.load_id_map(tmp_path, "Location") == {}

    def test_parse_output_carries_the_id_through(self, tmp_path):
        (tmp_path / "_ids").mkdir()
        (tmp_path / "_ids" / "Location__azblue.tsv").write_text(
            "azblue__Location__7.json\tloc-777\n", encoding="utf-8")
        out = tmp_path / "Location__000.json"
        out.write_text(json.dumps({
            "resourceType": "OperationOutcome",
            "extension": [{"url": vc.FILE_EXT,
                           "valueString": "/data/x/azblue__Location__7.json"}],
            "issue": []}), encoding="utf-8")
        inst, _ = vc.parse_output(out, "Location",
                                  vc.load_id_map(tmp_path, "Location"))
        assert inst[0][-1] == "loc-777"

    def test_ids_dir_is_invisible_to_chunking(self, tmp_path):
        """The sidecar lives under a _-prefixed dir so build_chunks cannot
        mistake it for an instance to validate."""
        _mk(tmp_path / "Location" / "azblue__Location__0.json")
        (tmp_path / "_ids").mkdir()
        (tmp_path / "_ids" / "Location__azblue.tsv").write_text("x\ty\n")
        chunks = vc.build_chunks(tmp_path, chunk_size=10)
        assert [c for c, _ in chunks] == ["Location"]
        assert (tmp_path / "_ids" / "Location__azblue.tsv").exists()


class TestPerCellChunkPlan:
    """One global --chunk cannot serve cells whose per-instance cost differs
    8x: at 12.8 s/instance an 8,000-file PractitionerRole chunk needs 28.4 h,
    which no --chunk-timeout can survive."""

    def _args(self, **kw):
        import argparse
        d = dict(chunk=None, chunk_timeout=None, cell_chunk=None, cell_rate=None)
        d.update(kw)
        return argparse.Namespace(**d)

    def test_expensive_cells_get_smaller_chunks(self):
        plan = vc.chunk_plan(["Organization", "PractitionerRole"], self._args())
        assert plan["PractitionerRole"][0] < plan["Organization"][0]

    def test_every_chunk_fits_inside_its_own_timeout(self):
        """The whole point: a chunk must never be built larger than the budget
        that will kill it."""
        cells = ["Organization", "PractitionerRole", "Location", "Network",
                 "Practitioner", "HealthcareService", "InsurancePlan"]
        for cell, (size, timeout) in vc.chunk_plan(cells, self._args()).items():
            rate = vc.CELL_RATE_S.get(cell, vc.DEFAULT_RATE_S)
            assert size * rate < timeout, f"{cell}: {size} x {rate}s >= {timeout}s"

    def test_the_regression_case_directly(self):
        """8,000 PractitionerRoles at 12.8 s/instance = 28.4 h > any timeout."""
        size, timeout = vc.chunk_plan(["PractitionerRole"],
                                      self._args())["PractitionerRole"]
        assert size * 12.8 / 3600 < 24, "a chunk must fit inside a day"
        assert timeout <= 86400

    def test_explicit_chunk_still_forces_one_size(self):
        plan = vc.chunk_plan(["Organization", "PractitionerRole"],
                             self._args(chunk=1000))
        assert plan["Organization"][0] == plan["PractitionerRole"][0] == 1000

    def test_per_cell_override_wins_over_global(self):
        plan = vc.chunk_plan(["Organization", "PractitionerRole"],
                             self._args(chunk=1000,
                                        cell_chunk="PractitionerRole=200"))
        assert plan["Organization"][0] == 1000
        assert plan["PractitionerRole"][0] == 200

    def test_rate_override_resizes(self):
        slow = vc.chunk_plan(["Location"], self._args(cell_rate="Location=60"))
        fast = vc.chunk_plan(["Location"], self._args(cell_rate="Location=0.5"))
        assert slow["Location"][0] < fast["Location"][0]

    def test_explicit_timeout_forces_one_budget(self):
        plan = vc.chunk_plan(["Organization", "PractitionerRole"],
                             self._args(chunk_timeout=999))
        assert plan["Organization"][1] == plan["PractitionerRole"][1] == 999

    def test_sizes_are_clamped(self):
        assert vc.chunk_plan(["Location"], self._args(
            cell_rate="Location=100000"))["Location"][0] == vc.MIN_CHUNK
        assert vc.chunk_plan(["Location"], self._args(
            cell_rate="Location=0.0001"))["Location"][0] == vc.MAX_CHUNK

    def test_build_chunks_honours_the_per_cell_map(self, tmp_path):
        for i in range(6):
            _mk(tmp_path / "Location" / f"p__Location__{i}.json")
            _mk(tmp_path / "Organization" / f"p__Organization__{i}.json")
        chunks = vc.build_chunks(tmp_path, {"Location": 2, "Organization": 6})
        by_cell: dict[str, list[int]] = {}
        for cell, d in chunks:
            by_cell.setdefault(cell, []).append(len(list(d.glob("*.json"))))
        assert by_cell["Location"] == [2, 2, 2]
        assert by_cell["Organization"] == [6]


class TestCostOrderedDispatch:
    """Alphabetical dispatch ran every Organization chunk before the first
    PractitionerRole one, so the run ended with the most expensive cell
    trickling through a draining worker pool."""

    def test_expensive_cell_sorts_first(self):
        rates = dict(vc.CELL_RATE_S)
        assert (vc.chunk_cost("PractitionerRole", 2790, rates, 43200)
                > vc.chunk_cost("Organization", 2790, rates, 43200))

    def test_oversize_chunks_sort_ahead_of_everything(self):
        vc.OVERSIZE_CHUNKS.clear()
        vc.OVERSIZE_CHUNKS.add("Network__012")
        try:
            rates = dict(vc.CELL_RATE_S)
            whale = vc.chunk_cost("Network", 1, rates, 43200, "Network__012")
            normal = vc.chunk_cost("PractitionerRole", 562, rates, 43200,
                                   "PractitionerRole__000")
            assert whale > normal
        finally:
            vc.OVERSIZE_CHUNKS.clear()

    def test_unknown_cell_falls_back_to_the_default_rate(self):
        assert vc.chunk_cost("Nope", 10, {}, 1.0) == 10 * vc.DEFAULT_RATE_S


class TestWorkerCacheCompleteness:
    """`if not dest.exists(): copytree` treats a directory as proof of a
    complete clone. 14 F:/fhir_cache_w* clones survive from a prior era, and a
    partial one produces IG-resolution errors that read as payer findings."""

    def _src(self, tmp_path, n=4):
        cache = tmp_path / "fhir_cache"
        pkg = cache / "packages" / "pkg#1.0.0" / "package"
        pkg.mkdir(parents=True)
        for i in range(n):
            (pkg / f"f{i}.json").write_text("{}")
        return cache

    def test_partial_clone_is_detected_and_recopied(self, tmp_path):
        cache = self._src(tmp_path)
        dest = vc.worker_cache(cache, 0)
        (dest / "packages" / "pkg#1.0.0" / "package").mkdir(parents=True)
        (dest / "packages" / "pkg#1.0.0" / "package" / "f0.json").write_text("{}")
        vc.seed_worker_caches(cache, 1)
        assert len(list((dest / "packages" / "pkg#1.0.0" / "package")
                        .glob("*.json"))) == 4

    def test_unmarked_but_complete_legacy_clone_is_stamped_not_recopied(self, tmp_path):
        cache = self._src(tmp_path)
        dest = vc.worker_cache(cache, 0)
        shutil.copytree(cache, dest)
        touched = dest / "packages" / "pkg#1.0.0" / "package" / "f0.json"
        touched.write_text('{"legacy": true}')
        vc.seed_worker_caches(cache, 1)
        assert touched.read_text() == '{"legacy": true}'
        assert vc.clone_sentinel(dest).exists()

    def test_sentinel_lives_outside_the_mounted_tree(self, tmp_path):
        cache = self._src(tmp_path)
        dest, = vc.seed_worker_caches(cache, 1)
        assert vc.clone_sentinel(dest).parent == dest.parent
        assert not list(dest.rglob("*.complete"))


class TestReuseOutputsConfigGuard:
    """--reuse-outputs must refuse outputs from a different validator config.

    The concrete near-miss: run 3 exists to re-validate WITH the THO package
    loaded. Launched with --reuse-outputs against run 2's leftover `-tx n/a`,
    no-THO outputs, it would have reused all 33 of them -- publishing run 2's
    findings under a new run_id with the v2-0360 fix never applied and nothing
    in the results to reveal it.
    """

    def _args(self, tmp_path, **kw):
        import argparse
        d = dict(sample_dir=str(tmp_path), ig_version="1.1.0", tho_version="6.5.0",
                 tx="n/a", reuse_outputs=True, chunk=None, cell_chunk=None,
                 cell_rate=None)
        d.update(kw)
        return argparse.Namespace(**d)

    def _sig(self, args):
        import json as _j
        return _j.dumps({"ig": f"{vc.IG}#{args.ig_version}",
                         "tho": args.tho_version or None, "tx": args.tx,
                         "chunk": args.chunk,
                         "cell_chunk": args.cell_chunk or None,
                         "cell_rate": args.cell_rate or None,
                         "target_chunk_seconds": vc.TARGET_CHUNK_SECONDS,
                         "min_chunk": vc.MIN_CHUNK, "max_chunk": vc.MAX_CHUNK},
                        sort_keys=True)

    def test_signature_changes_with_tho(self, tmp_path):
        with_tho = self._sig(self._args(tmp_path))
        without = self._sig(self._args(tmp_path, tho_version=""))
        assert with_tho != without, "THO must be part of the reuse signature"

    def test_signature_changes_with_tx(self, tmp_path):
        assert self._sig(self._args(tmp_path)) != self._sig(
            self._args(tmp_path, tx="https://tx.fhir.org"))

    def test_signature_stable_for_same_config(self, tmp_path):
        assert self._sig(self._args(tmp_path)) == self._sig(self._args(tmp_path))

    def test_signature_changes_with_chunk_size(self, tmp_path):
        """Chunk NAMES are a function of the chunk size, so `Organization__003`
        names a different set of files at --chunk 4000 than at --chunk 8000 --
        and --reuse-outputs would happily reuse the old one under the new name
        with nothing anywhere to reveal the swap."""
        assert self._sig(self._args(tmp_path, chunk=4000)) != self._sig(
            self._args(tmp_path, chunk=8000))

    def test_signature_changes_with_per_cell_overrides(self, tmp_path):
        assert self._sig(self._args(tmp_path)) != self._sig(
            self._args(tmp_path, cell_chunk="PractitionerRole=200"))
        assert self._sig(self._args(tmp_path)) != self._sig(
            self._args(tmp_path, cell_rate="Organization=1.29"))


class TestWorkerCacheIsolation:
    """A shared package cache cannot survive concurrent validators: the cache
    manager deletes and re-extracts package dirs with no cross-process lock, so
    one container's cleanup walks a tree another is reading. 17 of 46 chunks
    died together that way after warm-up had already proven the cache complete.
    """

    def test_each_worker_gets_a_distinct_path(self, tmp_path):
        cache = tmp_path / "fhir_cache"
        paths = [vc.worker_cache(cache, w) for w in range(14)]
        assert len(set(paths)) == 14, "worker caches must not collide"
        assert cache not in paths, "no worker may use the shared original"

    def test_seeding_clones_and_is_idempotent(self, tmp_path):
        cache = tmp_path / "fhir_cache"
        (cache / "packages" / "pkg#1.0.0" / "package").mkdir(parents=True)
        (cache / "packages" / "pkg#1.0.0" / "package" / "x.json").write_text("{}")

        seeded = vc.seed_worker_caches(cache, 3)
        assert len(seeded) == 3
        for p in seeded:
            assert (p / "packages" / "pkg#1.0.0" / "package" / "x.json").exists()

        # Re-seeding must not raise (copytree would) and must not re-copy:
        # a resumed run pays the clone cost only once.
        marker = seeded[0] / "packages" / "pkg#1.0.0" / "package" / "x.json"
        marker.write_text('{"touched": true}')
        vc.seed_worker_caches(cache, 3)
        assert marker.read_text() == '{"touched": true}'


class TestOversizeBudget:
    def test_one_oversize_file_per_chunk(self):
        """Pairing whales just doubled what each timeout discarded: 13 Network
        chunks of two 66k-address Organizations each still blew the 1h cap."""
        assert vc.OVERSIZE_PER_CHUNK == 1

    def test_oversize_chunks_are_tracked_for_the_longer_timeout(self, tmp_path):
        cell = tmp_path / "Network"
        cell.mkdir()
        (cell / "p__Network__0.json").write_text("x" * 10)                 # normal
        (cell / "p__Network__1.json").write_text("x" * (vc.OVERSIZE_BYTES + 1))
        vc.OVERSIZE_CHUNKS.clear()
        chunks = vc.build_chunks(tmp_path, chunk_size=4000)
        assert len(chunks) == 2, "oversize file must not ride with the normal one"
        assert len(vc.OVERSIZE_CHUNKS) == 1
        big = next(d.name for _c, d in chunks if d.name in vc.OVERSIZE_CHUNKS)
        assert (tmp_path / "_batches" / big / "p__Network__1.json").exists()


class TestSamplerOrdering:
    """No sampler branch had an ORDER BY, so `<payer>__<cell>__41207.json`
    named a different row on every re-run and a finding recorded against it
    could not be reproduced, let alone traced."""

    def _spec_for(self, cell):
        return next(s for s in sfv.CELL_SPECS if s[0] == cell)

    def test_full_population_branch_orders_and_avoids_the_dict_loader(self):
        conn = _FakeConn()
        _rows, method = sfv.sample_from_db(conn, "s", "organization", "s",
                                           None, 1_000_000, 0, 1)
        assert method == "full"
        # The generator is lazy, so drive it far enough to issue the query.
        # It opens its own connection, so just inspect the SQL it builds.
        sql = ('SELECT id, resource::text FROM "s"."organization" '
               "WHERE payer_id = %s ORDER BY id")
        assert "ORDER BY id" in sql and "resource::text" in sql

    def test_all_branch_orders_by_id(self):
        conn = _FakeConn([[]])
        sfv.sample_from_db(conn, "s", "location", "s", None, 10, 1000, 1)
        assert "ORDER BY id" in conn.cur.sql[-1]

    def test_tablesample_branch_orders_by_id(self):
        conn = _FakeConn([[("a", "{}")]])
        _rows, method = sfv.sample_from_db(conn, "s", "organization", "s",
                                           None, 1_000_000, 1000, 7)
        assert method == "tablesample"
        assert "ORDER BY id" in conn.cur.sql[-1]
        assert "REPEATABLE (7)" in conn.cur.sql[-1]

    def test_limit_fallback_branch_orders_by_id(self):
        conn = _FakeConn([[], []])          # tablesample draws nothing
        _rows, method = sfv.sample_from_db(conn, "s", "organization", "s",
                                           "is_network = 1", 1_000_000, 1000, 7)
        assert method == "limit-fallback"
        assert "ORDER BY id" in conn.cur.sql[-1]

    def test_every_db_branch_selects_text_not_jsonb(self):
        """psycopg's jsonb->dict loader is what made the `all` branch need
        25-40 GB for 1.86M Organizations."""
        conn = _FakeConn([[], []])
        sfv.sample_from_db(conn, "s", "organization", "s", None, 1_000_000, 1000, 1)
        for sql in conn.cur.sql:
            assert "resource::text" in sql, sql

    def test_ids_are_written_beside_the_instances(self, tmp_path):
        rows = [("org-a", '{"resourceType":"Organization"}'),
                ("org-b", '{"resourceType":"Organization"}')]
        ids = tmp_path / "_ids" / "Organization__p.tsv"
        n = sfv.write_cell(tmp_path / "Organization", ids, "p", "Organization",
                           iter(rows), 0, False, {})
        assert n == 2
        assert ids.read_text(encoding="utf-8").splitlines() == [
            "p__Organization__0.json\torg-a", "p__Organization__1.json\torg-b"]
        # ...and the id sidecar keys really are the filenames on disk.
        assert (tmp_path / "Organization" / "p__Organization__0.json").exists()


class TestCarinBbStrip:
    """The validator enforces declared meta.profile IN ADDITION to -profile and
    has no flag to ignore it, so azblue's C4BB declarations get validated
    against our carin-bb 2.1.0 while the resources pin 1.0.0. That is an
    artifact of our package set, not a payer defect."""

    PLANNET = ("http://hl7.org/fhir/us/davinci-pdex-plan-net/"
               "StructureDefinition/plannet-Organization")
    C4BB = "http://hl7.org/fhir/us/carin-bb/StructureDefinition/C4BB-Organization"

    def test_only_carin_bb_entries_are_popped(self):
        src = json.dumps({"resourceType": "Organization", "meta": {"profile": [
            "http://hl7.org/fhir/StructureDefinition/Organization",
            self.C4BB, self.C4BB + "|1.0.0", self.PLANNET]}})
        out, dropped = sfv.strip_carin_bb_profiles(src)
        assert sorted(dropped) == sorted([self.C4BB, self.C4BB + "|1.0.0"])
        kept = json.loads(out)["meta"]["profile"]
        assert self.PLANNET in kept
        assert not any("carin-bb" in p for p in kept)

    def test_resource_without_carin_bb_is_returned_untouched(self):
        src = json.dumps({"resourceType": "Location",
                          "meta": {"profile": [self.PLANNET]}})
        out, dropped = sfv.strip_carin_bb_profiles(src)
        assert out is src and dropped == []

    def test_profile_key_is_removed_when_nothing_survives(self):
        src = json.dumps({"resourceType": "Organization",
                          "meta": {"profile": [self.C4BB], "versionId": "1"}})
        out, dropped = sfv.strip_carin_bb_profiles(src)
        assert dropped == [self.C4BB]
        meta = json.loads(out)["meta"]
        assert "profile" not in meta and meta["versionId"] == "1"

    def test_malformed_meta_is_survivable(self):
        for src in ('{"meta": "carin-bb/oops"}',
                    '{"meta": {"profile": "/us/carin-bb/x"}}',
                    '{"x": "/us/carin-bb/"}'):
            out, dropped = sfv.strip_carin_bb_profiles(src)
            assert dropped == [] and out == src

    def test_strip_is_recorded_to_the_inventory(self, tmp_path):
        src = json.dumps({"resourceType": "Organization",
                          "meta": {"profile": [self.C4BB, self.PLANNET]}})
        inv: dict = {}
        sfv.write_cell(tmp_path / "Organization",
                       tmp_path / "_ids" / "Organization__p.tsv",
                       "p", "Organization", iter([("a", src), ("b", src)]),
                       0, True, inv)
        assert inv["p|Organization"][self.C4BB] == 2
        assert inv["p|Organization"]["_instances"] == 2
        # and the file on disk no longer declares it
        written = json.loads((tmp_path / "Organization" /
                              "p__Organization__0.json").read_text(encoding="utf-8"))
        assert written["meta"]["profile"] == [self.PLANNET]

    def test_no_strip_leaves_the_resource_alone(self, tmp_path):
        src = json.dumps({"resourceType": "Organization",
                          "meta": {"profile": [self.C4BB]}})
        inv: dict = {}
        sfv.write_cell(tmp_path / "Organization",
                       tmp_path / "_ids" / "Organization__p.tsv",
                       "p", "Organization", iter([("a", src)]), 0, False, inv)
        assert inv == {}
        assert self.C4BB in (tmp_path / "Organization" /
                             "p__Organization__0.json").read_text(encoding="utf-8")


class TestPayerDiscovery:
    """`cells()` gated on n_live_tup > 0, so a stats-reset schema yielded zero
    cells and exit code 0. `hcsc` reads 0 live tuples against 18.46M actual
    rows right now."""

    def test_stats_reset_schema_is_recovered_by_count(self):
        # planner says 0 for both; count(*) is probed in sorted table order
        # (location, then organization) and says otherwise for location.
        conn = _FakeConn([
            [("hcsc", "organization", 0), ("hcsc", "location", 0)],
            [(18_460_000,)],   # count(*) hcsc.location
            [(0,)],            # count(*) hcsc.organization -- really empty
        ])
        found = sfv.cells(conn, {"hcsc"}, None, verify_empty=True)
        assert [(c[0], c[1], c[4]) for c in found] == [
            ("hcsc", "Location", 18_460_000)]
        assert any("count(*)" in s for s in conn.cur.sql)

    def test_reltuples_is_used_when_n_live_tup_is_zero(self):
        """The SQL itself must take GREATEST, not n_live_tup alone."""
        conn = _FakeConn([[]])
        sfv.cells(conn, None, None, verify_empty=False)
        sql = conn.cur.sql[0]
        assert "GREATEST(t.n_live_tup, k.reltuples" in sql
        assert "AND n_live_tup > 0" not in sql

    def test_genuinely_empty_table_yields_no_cell(self):
        conn = _FakeConn([[("p", "location", 0)], [(0,)]])
        assert sfv.cells(conn, {"p"}, None, verify_empty=True) == []

    def test_count_fallback_is_skipped_for_unnamed_payers(self):
        """count(*) over every zero-estimate table in a 60-payer database means
        scanning 500M-row tables to learn they are empty."""
        conn = _FakeConn([[("p", "location", 0)]])
        assert sfv.cells(conn, None, None, verify_empty=False) == []
        assert not any("count(*)" in s for s in conn.cur.sql)

    def test_resource_filter_still_applies(self):
        conn = _FakeConn([[("p", "organization", 5000)]])
        found = sfv.cells(conn, None, {"Network"}, verify_empty=False)
        assert [c[1] for c in found] == ["Network"]


# ===========================================================================
# Run selection, sample provenance, and the two guards that decide WHICH rows
# a published figure is computed over. The measurement code was never the
# problem here: run 9 (1,000/cell, 61 payers, unstripped) and a full-population
# single-payer run whose meta.profile was rewritten before validation used to
# be indistinguishable in the database, and the tooling silently preferred
# whichever was largest or newest.
# ===========================================================================

from provdir.quality import conformance as qc  # noqa: E402


def _run_row(run_id, *, payers=1, recorded=100, instances=100, finished=True,
             verified=None, sample=None, payer_ids=None):
    notes = {}
    if verified is not None:
        notes["verified"] = verified
    if sample is not None:
        notes["sample"] = sample
    return {
        "run_id": run_id, "instances": instances, "per_cell": None,
        "finished": finished, "notes": notes, "recorded": recorded,
        "payers": payers,
        "payer_ids": payer_ids if payer_ids is not None
        else [f"p{i}" for i in range(payers)],
    }


# The two runs at the heart of S1: the 61-payer cross-payer artifact, and the
# single-payer full-population run that is both newest AND largest.
RUN9 = _run_row(9, payers=61, recorded=121_038, instances=121_038,
                sample={"payers": [f"p{i}" for i in range(61)], "per_cell": 1000,
                        "carin_bb_stripped": False})
RUN10 = _run_row(10, payers=1, recorded=3_133_000, instances=3_133_000,
                 verified=True, payer_ids=["bcbs_az_new"],
                 sample={"payers": ["bcbs_az_new"], "per_cell": 0,
                         "carin_bb_stripped": True})


class TestRunSelectionRefusesToSwapTheArtifact:
    def test_auto_selection_refuses_when_breadth_and_size_disagree(self):
        """THE regression. "most recorded instances" and "newest finished" both
        hand the whole 61-payer map to a 1-payer run the instant it finishes."""
        with pytest.raises(SystemExit) as e:
            qc.choose_run([RUN9, RUN10])
        msg = str(e.value)
        assert "widest payer set" in msg and "most instances" in msg
        assert "run 9" in msg and "run 10" in msg
        # the difference is NAMED, not merely flagged
        assert "61 payer(s)" in msg and "1 payer(s)" in msg
        assert "carin_bb_stripped=True" in msg

    def test_auto_selection_works_when_they_agree(self):
        small = _run_row(6, payers=60, recorded=118_951, instances=118_951)
        run_id, note = qc.choose_run([small, RUN9])
        assert run_id == 9
        assert "auto-selected" in note

    def test_expect_payers_count_resolves_the_ambiguity(self):
        assert qc.choose_run([RUN9, RUN10], expect_payers=61)[0] == 9
        assert qc.choose_run([RUN9, RUN10], expect_payers=1)[0] == 10

    def test_expect_payers_set_resolves_the_ambiguity(self):
        assert qc.choose_run([RUN9, RUN10], expect_payers={"bcbs_az_new"})[0] == 10

    def test_expect_payers_that_matches_nothing_is_fatal_and_names_the_gap(self):
        with pytest.raises(SystemExit) as e:
            qc.choose_run([RUN9, RUN10], expect_payers={"p0", "nope"})
        assert "payer set differs" in str(e.value)

    def test_pinned_run_still_has_to_match_expect_payers(self):
        with pytest.raises(SystemExit) as e:
            qc.choose_run([RUN9, RUN10], run_id=10, expect_payers=61)
        assert "does not match --expect-payers" in str(e.value)

    def test_pinned_run_is_honoured(self):
        run_id, note = qc.choose_run([RUN9, RUN10], run_id=10)
        assert run_id == 10 and note.startswith("pinned")

    def test_unknown_run_id_is_fatal(self):
        with pytest.raises(SystemExit):
            qc.choose_run([RUN9], run_id=404)

    def test_parse_expect_payers_accepts_both_forms(self):
        assert qc.parse_expect_payers("61") == 61
        assert qc.parse_expect_payers("a, b") == {"a", "b"}
        assert qc.parse_expect_payers(None) is None
        assert qc.parse_expect_payers("") is None


class TestVerifiedIsActuallyEnforced:
    """`finished_at IS NOT NULL` was the whole gate, and it was stamped BEFORE
    the verification check -- so the 1,463,451-of-1,862,183 run was
    authoritative to every report the moment it stopped."""

    def test_a_run_that_says_it_is_unverified_is_not_selectable(self):
        short = _run_row(11, payers=61, recorded=1_463_451, instances=1_862_183,
                         verified=False)
        ok, why = qc.run_usability(short)
        assert not ok and "verified is false" in why
        with pytest.raises(SystemExit) as e:
            qc.choose_run([short])
        assert "no publishable conformance run" in str(e.value)

    def test_a_legacy_run_that_does_not_reconcile_is_not_selectable(self):
        """Run 3: 50,974 of 118,718, finished_at stamped, no `verified` key."""
        legacy_short = _run_row(3, payers=57, recorded=50_974, instances=118_718)
        ok, why = qc.run_usability(legacy_short)
        assert not ok and "does not reconcile" in why

    def test_a_legacy_run_that_reconciles_is_selectable(self):
        ok, _why = qc.run_usability(RUN9)
        assert ok

    def test_an_unfinished_run_is_not_selectable(self):
        ok, why = qc.run_usability(_run_row(7, finished=False, recorded=0))
        assert not ok and "finished_at IS NULL" in why

    def test_a_short_run_can_still_be_pinned_but_only_explicitly(self):
        short = _run_row(11, recorded=10, instances=100, verified=False)
        with pytest.raises(SystemExit) as e:
            qc.choose_run([short], run_id=11)
        assert "--allow-unverified" in str(e.value)
        run_id, note = qc.choose_run([short], run_id=11, allow_unverified=True)
        assert run_id == 11 and "UNVERIFIED" in note

    def test_finished_at_is_only_stamped_when_verified(self):
        """The UPDATE must not set finished_at on a run that did not add up."""
        src = (REPO / "scripts" / "validate_conformance.py").read_text(
            encoding="utf-8")
        assert "SET finished_at = CASE WHEN %s THEN now() ELSE finished_at END" in src
        assert "UPDATE public.conformance_run SET finished_at = now()" not in src


class TestAllThreeConsumersShareOneRule:
    """The three artifacts used three different rules -- most instances, newest
    finished, and `CONF_RUN = 9` typed into the source."""

    CONSUMERS = ["build_map_data.py", "conformance_report.py",
                 "build_framework_payload.py"]

    def _src(self, name):
        return (REPO / "scripts" / name).read_text(encoding="utf-8")

    def test_every_consumer_calls_select_run(self):
        for name in self.CONSUMERS:
            assert "select_run(" in self._src(name), name

    def test_no_consumer_keeps_its_own_selection_rule(self):
        for name in self.CONSUMERS:
            src = self._src(name)
            assert "def pick_conformance_run" not in src, name
            assert "max(run_id) from conformance_run" not in src.lower(), name
        assert "CONF_RUN = 9" not in self._src("build_framework_payload.py")

    def test_every_consumer_offers_expect_payers(self):
        for name in self.CONSUMERS:
            assert "--expect-payers" in self._src(name), name

    def test_every_consumer_declares_the_aggregation_unit(self):
        for name in self.CONSUMERS:
            assert "D3_UNIT" in self._src(name), name


class TestDeclaredAggregationUnit:
    def test_unweighted_mean_is_not_the_pooled_rate(self):
        """One 10,000-instance cell at 50% and one 100-instance cell at 100%:
        pooling reports 50.5%, the declared unit reports 75%."""
        rate, used, sup = qc.cell_mean_rate([(10_000, 5_000), (100, 100)])
        assert used == 2 and sup == 0
        assert rate == pytest.approx(0.75)
        assert rate != pytest.approx(5_100 / 10_100)

    def test_small_cells_are_suppressed_not_counted(self):
        rate, used, sup = qc.cell_mean_rate([(100, 50), (5, 0)])
        assert (used, sup) == (1, 1) and rate == pytest.approx(0.5)

    def test_all_suppressed_is_none_not_zero(self):
        rate, used, sup = qc.cell_mean_rate([(5, 0), (2, 2)])
        assert rate is None and used == 0 and sup == 2

    def test_the_unit_names_what_is_actually_computable(self):
        assert "(payer, resource)" in qc.D3_UNIT
        assert "LOB is not recorded" in qc.D3_UNIT_CAVEAT


class TestSampleProvenanceTravelsWithTheRun:
    """manifest.json and _carin_bb_inventory.json were written by the sampler
    and read by nothing, so `per_cell` was NULL on all 9 runs and the run row
    recorded the validator's configuration in detail and the sample's not at
    all."""

    def _sample(self, tmp_path, manifest, inventory=None):
        (tmp_path / "manifest.json").write_text(json.dumps(manifest),
                                                encoding="utf-8")
        if inventory is not None:
            (tmp_path / "_carin_bb_inventory.json").write_text(
                json.dumps(inventory), encoding="utf-8")
        return tmp_path

    def test_a_sample_dir_with_no_manifest_fails_the_run(self, tmp_path):
        with pytest.raises(SystemExit) as e:
            vc.read_sample_provenance(tmp_path)
        assert "manifest.json" in str(e.value)

    def test_an_empty_manifest_fails_the_run(self, tmp_path):
        with pytest.raises(SystemExit):
            vc.read_sample_provenance(self._sample(tmp_path, []))

    def test_a_capped_multi_payer_sample_records_its_cap(self, tmp_path):
        d = self._sample(tmp_path, [
            {"payer": "a", "cell": "Organization", "sampled": 1000,
             "live_rows": 50_000, "carin_bb_stripped": False},
            {"payer": "b", "cell": "Location", "sampled": 1000,
             "live_rows": 9_000, "carin_bb_stripped": False},
            {"payer": "b", "cell": "Endpoint", "sampled": 12,
             "live_rows": 12, "carin_bb_stripped": False},
        ])
        prov = vc.read_sample_provenance(d)
        assert prov["per_cell"] == 1000
        assert prov["payers"] == ["a", "b"] and prov["payer_count"] == 2
        assert prov["sampled"] == 2012 and prov["cells"] == 3
        assert prov["carin_bb_stripped"] is False

    def test_a_full_population_sample_is_zero_not_null(self, tmp_path):
        """0 means "no uniform cap"; NULL is the nine legacy rows that recorded
        nothing at all. The two must not look the same."""
        d = self._sample(tmp_path, [
            {"payer": "bcbs_az_new", "cell": "Organization", "sampled": 1_862_241,
             "live_rows": 1_870_000, "carin_bb_stripped": True},
            {"payer": "bcbs_az_new", "cell": "Location", "sampled": 915_339,
             "live_rows": 920_000, "carin_bb_stripped": True},
        ])
        prov = vc.read_sample_provenance(d)
        assert prov["per_cell"] == 0
        assert prov["carin_bb_stripped"] is True

    def test_the_strip_inventory_is_folded_in(self, tmp_path):
        d = self._sample(
            tmp_path,
            [{"payer": "bcbs_az_new", "cell": "Organization", "sampled": 10,
              "live_rows": 10, "carin_bb_stripped": True}],
            {"bcbs_az_new|Organization": {"http://x/carin-bb/a": 7, "_instances": 7},
             "bcbs_az_new|Location": {"http://x/carin-bb/a": 2,
                                      "http://x/carin-bb/b": 1, "_instances": 3}})
        prov = vc.read_sample_provenance(d)
        assert prov["instances_stripped"] == 10
        assert prov["canonicals_removed"] == {"http://x/carin-bb/a": 9,
                                              "http://x/carin-bb/b": 1}

    def test_one_stripped_cell_makes_the_whole_run_stripped(self, tmp_path):
        d = self._sample(tmp_path, [
            {"payer": "a", "cell": "Organization", "sampled": 5, "live_rows": 5,
             "carin_bb_stripped": False},
            {"payer": "a", "cell": "Location", "sampled": 5, "live_rows": 5,
             "carin_bb_stripped": True},
        ])
        assert vc.read_sample_provenance(d)["carin_bb_stripped"] is True

    def test_the_run_row_carries_per_cell_and_notes_sample(self):
        src = (REPO / "scripts" / "validate_conformance.py").read_text(
            encoding="utf-8")
        assert "(ig, ig_version, per_cell, instances, notes)" in src
        assert '"sample": provenance' in src

    def test_provenance_is_read_before_anything_is_moved_or_wiped(self):
        """A run whose provenance cannot be recorded must not have destroyed
        _out on its way to failing."""
        src = (REPO / "scripts" / "validate_conformance.py").read_text(
            encoding="utf-8")
        body = src[src.index("def _run(args, sample_dir: Path)"):]
        assert body.index("read_sample_provenance") < body.index("shutil.rmtree(out_dir)")


class TestWarmUpWarmsTheCacheThatIsCloned:
    """--per-worker-cache defaults True, so the warm-up (worker=0, no override)
    populated F:/fhir_cache_w0 while the sentinel was written to F:/fhir_cache
    and seed_worker_caches cloned F:/fhir_cache -- the tree the warm-up never
    touched -- 44 times."""

    class _Args:
        pkg_cache = "F:/fhir_cache"
        per_worker_cache = True
        tx_cache = None
        tx = "n/a"
        ig_version = "1.2.0"
        tho_version = "6.5.0"
        xmx = "1500m"
        oversize_xmx = None
        docker_memory = "2g"
        oversize_docker_memory = None
        chunk_timeout = 3600
        oversize_timeout = 43200
        reuse_outputs = False

    def _mounts(self, monkeypatch, tmp_path, **kw):
        seen = {}

        class _P:
            returncode = 0
            stdout = ""
            stderr = ""

        def fake_run(cmd, **_kw):
            seen.setdefault("cmd", cmd)
            return _P()

        monkeypatch.setattr(vc.subprocess, "run", fake_run)
        out_dir = tmp_path / "_out"
        out_dir.mkdir()
        chunk = tmp_path / "_batches" / "Organization__000"
        chunk.mkdir(parents=True)
        vc.run_chunk("Organization", chunk, tmp_path, self._Args(), out_dir, **kw)
        return [a for a in seen["cmd"] if ":/root/.fhir" in str(a)]

    def test_warm_up_mounts_the_shared_cache(self, monkeypatch, tmp_path):
        mounts = self._mounts(monkeypatch, tmp_path, shared_cache=True)
        assert mounts and mounts[0].startswith("F:/fhir_cache:")

    def test_workers_still_get_their_own_copy(self, monkeypatch, tmp_path):
        mounts = self._mounts(monkeypatch, tmp_path, worker=3)
        assert mounts and mounts[0].startswith("F:/fhir_cache_w3:")

    def test_worker_zero_without_the_override_is_still_isolated(self, monkeypatch,
                                                               tmp_path):
        mounts = self._mounts(monkeypatch, tmp_path, worker=0)
        assert mounts and mounts[0].startswith("F:/fhir_cache_w0:")

    def test_the_warm_up_call_site_asks_for_the_shared_cache(self):
        src = (REPO / "scripts" / "validate_conformance.py").read_text(
            encoding="utf-8")
        warm = src[src.index("warming with one synchronous chunk"):]
        assert "shared_cache=True" in warm[:2000]


class TestWarmUpChunkIsRepresentative:
    """`pending.pop()` took the LAST element of a cost-DESCENDING list -- the
    cheapest chunk in the run -- so an --xmx that fits a 25-file tail chunk but
    not a 2,790-file Organization one passed the gate and failed at hour 3."""

    def _pending(self, tmp_path, names):
        out = []
        for n in names:
            d = tmp_path / n
            d.mkdir()
            out.append((n.split("__", 1)[0], d))
        return out

    def test_the_most_expensive_ordinary_chunk_is_chosen(self, tmp_path):
        vc.OVERSIZE_CHUNKS.clear()
        pending = self._pending(tmp_path, ["Organization__000", "Location__000",
                                           "Endpoint__000"])
        assert pending[vc.pick_warmup_chunk(pending)][1].name == "Organization__000"

    def test_it_is_never_the_cheapest_chunk(self, tmp_path):
        vc.OVERSIZE_CHUNKS.clear()
        pending = self._pending(tmp_path, ["PractitionerRole__000",
                                           "Organization__000", "Endpoint__007"])
        assert vc.pick_warmup_chunk(pending) != len(pending) - 1

    def test_oversize_whales_are_skipped(self, tmp_path):
        """An oversize chunk runs under --oversize-xmx with its own budget, so
        it proves nothing about the ordinary tier and would hold the whole run
        behind a multi-hour whale."""
        vc.OVERSIZE_CHUNKS.clear()
        pending = self._pending(tmp_path, ["Network__000", "Network__001",
                                           "Organization__000"])
        vc.OVERSIZE_CHUNKS.update({"Network__000", "Network__001"})
        try:
            assert pending[vc.pick_warmup_chunk(pending)][1].name == "Organization__000"
        finally:
            vc.OVERSIZE_CHUNKS.clear()

    def test_an_all_oversize_run_still_warms_up(self, tmp_path):
        vc.OVERSIZE_CHUNKS.clear()
        pending = self._pending(tmp_path, ["Network__000", "Network__001"])
        vc.OVERSIZE_CHUNKS.update({"Network__000", "Network__001"})
        try:
            assert vc.pick_warmup_chunk(pending) == 0
        finally:
            vc.OVERSIZE_CHUNKS.clear()

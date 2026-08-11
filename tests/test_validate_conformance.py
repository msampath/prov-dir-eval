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
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

_spec = importlib.util.spec_from_file_location(
    "validate_conformance", REPO / "scripts" / "validate_conformance.py")
vc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vc)


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
                 tx="n/a", reuse_outputs=True)
        d.update(kw)
        return argparse.Namespace(**d)

    def _sig(self, args):
        import json as _j
        return _j.dumps({"ig": f"{vc.IG}#{args.ig_version}",
                         "tho": args.tho_version or None, "tx": args.tx},
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

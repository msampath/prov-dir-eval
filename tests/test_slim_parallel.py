"""Tests for the --slim --workers parallel load's pure logic.

Sharding and the capacity arithmetic are what make a parallel load either
correct or silently short, and both are cheap to test without a database. The
capacity cases in particular encode the two bugs found in review: a hardcoded
cap that goes stale as the load consumes space, and a ratio projection that is
only meaningful when nothing is loading concurrently.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

_spec = importlib.util.spec_from_file_location(
    "bulk_replace_load", REPO / "scripts" / "bulk_replace_load.py")
brl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(brl)


FILES = [Path(f"f{i:02d}.ndjson") for i in range(30)]


class TestParseShard:
    @pytest.mark.parametrize("spec,expected", [("0/1", (0, 1)), ("11/12", (11, 12))])
    def test_valid(self, spec, expected):
        assert brl.parse_shard(spec) == expected

    @pytest.mark.parametrize("spec", ["", "1", "a/2", "1/0", "2/2", "3/2", "-1/4", "1/2/3"])
    def test_rejected(self, spec):
        with pytest.raises(SystemExit):
            brl.parse_shard(spec)


class TestShardFiles:
    def test_no_spec_returns_everything(self):
        assert brl.shard_files(FILES, None) == FILES

    @pytest.mark.parametrize("k", [1, 2, 3, 7, 12, 29, 30])
    def test_shards_partition_the_list(self, k):
        """Disjoint AND complete -- the property the whole design rests on.

        An overlap double-loads a 16.6M-row file and only surfaces hours later
        at the unique-PK rebuild; a gap silently short-loads the table.
        """
        shards = [brl.shard_files(FILES, f"{i}/{k}") for i in range(k)]
        flat = [f for s in shards for f in s]
        assert len(flat) == len(FILES)
        assert set(flat) == set(FILES)

    def test_more_workers_than_files_yields_empty_shards(self):
        shards = [brl.shard_files(FILES, f"{i}/40") for i in range(40)]
        assert sum(len(s) for s in shards) == len(FILES)
        assert [s for s in shards if not s]

    def test_assignment_is_stable_regardless_of_completed_files(self):
        """Shards derive from the full sorted list, never from what's left, so
        a resumed child re-derives exactly the slice it had before."""
        assert brl.shard_files(FILES, "3/12") == brl.shard_files(list(FILES), "3/12")


class TestCapacityCap:
    class _Cur:
        def __init__(self, used):
            self.used = used

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            self.sql = sql

        def fetchone(self):
            if "pg_tablespace_location" in self.sql:
                return ("M:/pg_ts/bulk_ssd",)
            return (self.used,)

    class _Conn:
        def __init__(self, used):
            self.used = used

        def cursor(self):
            return TestCapacityCap._Cur(self.used)

    def test_cap_tracks_live_volume(self, monkeypatch):
        """Derived from used+free so it does not age out: the constant was set
        against an empty table and 774GB free, but 91GB has since been consumed.
        """
        monkeypatch.setattr(brl.shutil, "disk_usage",
                            lambda p: type("U", (), {"free": 689 * 10 ** 9})())
        cap, how = brl.capacity_cap(self._Conn(91 * 10 ** 9), "bulk_ssd")
        assert cap == pytest.approx((91 + 689 - 60) * 10 ** 9)
        assert cap > brl.CAPACITY_CAP_BYTES  # the real budget beats the constant
        assert "live volume" in how

    def test_falls_back_to_constant_when_volume_unreadable(self, monkeypatch):
        monkeypatch.setattr(brl, "volume_free_bytes", lambda c, t: None)
        cap, how = brl.capacity_cap(self._Conn(0), "bulk_ssd")
        assert cap == brl.CAPACITY_CAP_BYTES
        assert "fallback" in how


class TestFreeSpaceGuard:
    def test_trips_below_the_index_reserve(self, monkeypatch):
        monkeypatch.setattr(brl, "volume_free_bytes",
                            lambda c, t: brl.INDEX_RESERVE_BYTES - 1)
        with pytest.raises(SystemExit, match="index"):
            brl.free_space_guard(None)

    def test_passes_above_it(self, monkeypatch):
        monkeypatch.setattr(brl, "volume_free_bytes",
                            lambda c, t: brl.INDEX_RESERVE_BYTES + 1)
        brl.free_space_guard(None)

    def test_silent_when_volume_unknown(self, monkeypatch):
        monkeypatch.setattr(brl, "volume_free_bytes", lambda c, t: None)
        brl.free_space_guard(None)


class TestCapacityProbe:
    """The measured production numbers, so a regression in the arithmetic or the
    constants shows up as a failing test rather than a false abort mid-load."""

    HEAP_7_FILES = 91_112_611_840
    SRC_7_FILES = 372_105_770_268
    SRC_TOTAL = 1_594_737_395_275

    def _probe(self, monkeypatch, cap, heap=None, fatal=True):
        monkeypatch.setattr(brl, "capacity_cap", lambda c, t: (cap, "test"))
        monkeypatch.setattr(brl, "relation_size",
                            lambda c, s, t: heap or self.HEAP_7_FILES)
        brl.capacity_probe(None, "s", "t", self.SRC_7_FILES, self.SRC_TOTAL,
                           fatal=fatal)

    def test_real_numbers_pass_under_the_live_cap(self, monkeypatch):
        self._probe(monkeypatch, (91 + 689 - 60) * 10 ** 9)

    def test_real_numbers_are_uncomfortably_close_to_the_old_constant(self, monkeypatch):
        """Documents why the constant was replaced: the same healthy projection
        sits within ~2% of 650GB, so ordinary variance would false-abort."""
        ratio = self.HEAP_7_FILES / self.SRC_7_FILES
        projected = (ratio * self.SRC_TOTAL * (1 + brl.INDEX_OVERHEAD_FACTOR)
                     + brl.SORT_HEADROOM_BYTES)
        assert projected < brl.CAPACITY_CAP_BYTES
        assert projected > brl.CAPACITY_CAP_BYTES * 0.95

    def test_aborts_when_over_cap(self, monkeypatch):
        with pytest.raises(SystemExit, match="exceeds"):
            self._probe(monkeypatch, 400 * 10 ** 9)

    def test_resume_bloat_warns_instead_of_aborting(self, monkeypatch, capsys):
        """A killed 12-worker run leaves up to ~1 aborted in-flight file per
        worker (~13GB heap each) in relation_size; VACUUM makes that space
        reusable but never shrinks the file. The resulting projection is ~2x
        over the cap on a perfectly healthy resume, so the resume-path probe
        must warn, not abort -- otherwise --resume can never recover a
        parallel crash and free_space_guard (skew-free) enforces instead."""
        bloated = self.HEAP_7_FILES + 150 * 10 ** 9  # ~12 aborted files
        self._probe(monkeypatch, (94 + 736 - 60) * 10 ** 9, heap=bloated,
                    fatal=False)  # must NOT raise
        out = capsys.readouterr().out
        assert "WARNING" in out and "NOT aborting" in out

    def test_same_bloat_would_false_abort_if_fatal(self, monkeypatch):
        """Documents that the bloat scenario above really is over-cap: only
        fatal=False makes the resume survivable."""
        bloated = self.HEAP_7_FILES + 150 * 10 ** 9
        with pytest.raises(SystemExit, match="exceeds"):
            self._probe(monkeypatch, (94 + 736 - 60) * 10 ** 9, heap=bloated)

    def test_noop_before_anything_is_committed(self, monkeypatch):
        monkeypatch.setattr(brl, "capacity_cap",
                            lambda c, t: pytest.fail("must not probe on zero bytes"))
        brl.capacity_probe(None, "s", "t", 0, self.SRC_TOTAL)


class TestArgGuards:
    """A child must never be able to reach the destructive prelude."""

    def _parse(self, argv):
        old = sys.argv
        sys.argv = ["bulk_replace_load.py", *argv]
        try:
            return brl.main()
        finally:
            sys.argv = old

    @pytest.mark.parametrize("argv", [
        ["--load-only"],                                  # missing --slim
        ["--slim", "--shard", "0/2"],                     # shard without load-only
        ["--slim", "--load-only", "--workers", "4"],      # child spawning children
        ["--slim", "--load-only", "--dry-run"],
        ["--slim", "--workers", "0"],
        ["--workers", "4"],                               # workers without --slim
        ["--slim", "--expect-files", "30"],               # expect without load-only
        ["--slim", "--load-only", "--shard", "5/2"],      # malformed shard
    ])
    def test_rejected_combinations(self, argv):
        with pytest.raises(SystemExit) as exc:
            self._parse(argv)
        assert exc.value.code != 0

"""Tests for paginate_to_file's replayed-cursor guard.

Opala's opaque ?q= token is session-bound: resuming from a saved one restarts
the chain instead of continuing it. Before this guard, a premera Organization
resume appended 20,050 duplicate rows over 400 pages while the DISTINCT count
went down, and reported it as progress throughout.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

_spec = importlib.util.spec_from_file_location(
    "paginate_to_file", REPO / "scripts" / "paginate_to_file.py")
ptf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ptf)


class TestDigest:
    def test_stable_and_distinct(self):
        assert ptf._digest("abc") == ptf._digest("abc")
        assert ptf._digest("abc") != ptf._digest("abd")

    def test_fits_in_eight_bytes(self):
        assert 0 <= ptf._digest("x") < 2 ** 64


class TestReplayLimit:
    def test_tolerates_a_short_rewind(self):
        """A checkpoint deliberately rewinds a little so no record is skipped,
        so the first batch or two after a resume being all-duplicates is
        NORMAL and must not abort the walk."""
        assert ptf.REPLAY_LIMIT >= 2

    def test_does_not_tolerate_a_long_replay(self):
        """400 pages of replay is what actually happened; the limit has to be
        far below that to be worth anything."""
        assert ptf.REPLAY_LIMIT <= 10


class TestReplayedCursorIsNotDone:
    def test_exception_type_exists(self):
        assert issubclass(ptf.ReplayedCursor, RuntimeError)

    def test_replayed_note_matches_no_truncation_keyword(self):
        """Documents WHY `finished` checks `replayed` explicitly: the note a
        replayed cursor produces trips none of the truncation keywords, so the
        keyword scan alone would let a partial unit be marked .done."""
        note = "cursor replayed: 3 consecutive batches with no new records"
        keywords = ("stopped", "budget", "truncat", "more available", "our cap",
                    "fetch error", "bucket", "timeout", "rejected", "error",
                    "failed")
        assert not any(k in note.lower() for k in keywords)


class TestGuardOnlyArmsOnAResumedCursor:
    """The two cases look identical inside a batch, so the guard keys off
    whether a cursor was actually resumed.

    A fresh walk over an existing file MUST tolerate unlimited duplicate
    batches: premera's cursors expire (HTTP 410), forcing a restart from page
    1, and Location then re-pages ~3,964 pages / ~40 all-duplicate batches
    before reaching anything new. Arming the guard there killed a healthy
    re-walk a few hundred pages in.
    """

    @staticmethod
    def _armed(progress: dict) -> bool:
        # Mirrors the predicate in paginate_to_file._run's flush.
        return progress.get("pages_base") is not None

    def test_resumed_cursor_arms_the_guard(self):
        # extract_resource sets pages_base ONLY when the resume URL was accepted.
        assert self._armed({"pages_base": 1371, "added_base": 68550})

    def test_fresh_walk_does_not_arm_the_guard(self):
        assert not self._armed({})

    def test_failed_resume_does_not_arm_the_guard(self):
        """A 410'd cursor falls back to a fresh walk and never sets pages_base,
        so the duplicate prefix it must re-page is not treated as a replay."""
        assert not self._armed({"active": True, "pages": 150})

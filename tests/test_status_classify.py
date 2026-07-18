"""Unit tests for provenance status classification and coverage-denominator
resolution — the truth layer that keeps extraction failures from reporting ok."""

from __future__ import annotations

from provdir.etl.extract import resolve_server_total
from provdir.etl.pipeline import classify_status


def _cls(**kw):
    defaults = dict(
        method="bare", note=None, fetch_errors=0, transform_errors=0,
        extract_err=None, loaded=100, server_total=100,
    )
    defaults.update(kw)
    return classify_status(**defaults)


# --- classify_status ---------------------------------------------------------
def test_clean_run_is_ok():
    assert _cls() == ("ok", None)


def test_unsupported_is_skipped():
    assert _cls(method="unsupported", loaded=0, server_total=None)[0] == "skipped"


def test_zero_rows_against_reported_total_is_error():
    # Premera Practitioner regression: sweep landed 0 of a server-reported 50,860
    # and was previously classified "ok".
    status, note = _cls(loaded=0, server_total=50860)
    assert status == "error"
    assert "50860" in note


def test_zero_rows_unknown_total_is_empty_unverified():
    status, note = _cls(loaded=0, server_total=None)
    assert status == "empty-unverified"
    assert note


def test_zero_rows_zero_total_is_ok():
    assert _cls(loaded=0, server_total=0) == ("ok", None)


def test_fetch_errors_make_partial():
    assert _cls(fetch_errors=3)[0] == "partial"


def test_budget_note_makes_partial():
    assert _cls(note="partition sweep page budget exhausted (cap=40; our cap, not the server's)")[0] == "partial"
    assert _cls(note="page budget reached at 15 pages with more available (our cap)")[0] == "partial"


def test_extract_error_with_rows_is_partial_without_is_error():
    assert _cls(extract_err="boom", loaded=10)[0] == "partial"
    assert _cls(extract_err="boom", loaded=0)[0] == "error"


def test_pagination_stopped_is_partial():
    assert _cls(note="pagination stopped at page 3: TimeoutError: x")[0] == "partial"


# --- resolve_server_total ----------------------------------------------------
def test_bare_count_wins():
    assert resolve_server_total(853420, "prefix", False, 500, 0) == (853420, "bare")


def test_daterange_window_sum_is_exhaustive():
    assert resolve_server_total(None, "daterange", False, 11129311, 0) == (
        11129311, "daterange_window_sum",
    )


def test_match_all_count_is_exhaustive():
    assert resolve_server_total(None, "values", True, 2200000, 0) == (
        2200000, "match_all_count",
    )


def test_prefix_sum_never_used_as_denominator():
    # Prefix leaf sums miss the charset residual -> using them would overstate coverage.
    assert resolve_server_total(None, "prefix", False, 565000, 0) == (None, None)


def test_count_gaps_disqualify_the_sum():
    assert resolve_server_total(None, "daterange", False, 100000, 2) == (None, None)

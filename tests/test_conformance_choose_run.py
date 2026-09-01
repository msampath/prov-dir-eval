"""Unit tests for provdir.quality.conformance.choose_run.

The rule the function encodes is load-bearing: three artifacts read a
conformance_run and each used to pick it differently, so a single-payer
full-population run (run 14) landing next to a 61-payer historical run (run 9
et al.) has to refuse rather than guess. This test file locks that behavior in
before the report driver depends on it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.quality import conformance  # noqa: E402
from provdir.quality.conformance import (  # noqa: E402
    choose_run,
    parse_expect_payers,
    run_usability,
)


def _row(run_id, *, payers=1, recorded=100, instances=None, finished=True,
         verified=True, per_cell=None, payer_ids=None):
    return {
        "run_id": run_id,
        "instances": instances if instances is not None else recorded,
        "per_cell": per_cell,
        "finished": finished,
        "notes": {"verified": verified} if verified is not None else {},
        "recorded": recorded,
        "payers": payers,
        "payer_ids": payer_ids if payer_ids is not None else [f"p{i}" for i in range(payers)],
    }


# ---- run_usability ------------------------------------------------------

def test_run_usability_unfinished():
    ok, why = run_usability(_row(1, finished=False, verified=None))
    assert not ok
    assert "never finished" in why


def test_run_usability_verified_true():
    ok, why = run_usability(_row(1, verified=True))
    assert ok
    assert why == "notes.verified"


def test_run_usability_verified_false():
    ok, why = run_usability(_row(1, verified=False, recorded=50, instances=100))
    assert not ok
    assert "50 of 100" in why


def test_run_usability_preverified_reconciles():
    ok, _ = run_usability(_row(1, verified=None, recorded=100, instances=100))
    assert ok


def test_run_usability_preverified_shortfall():
    ok, why = run_usability(_row(1, verified=None, recorded=90, instances=100))
    assert not ok
    assert "90 of 100" in why


# ---- choose_run: the ambiguity refusal ---------------------------------
#
# The one that motivated the whole selector: run 14 is single-payer
# full-population, run 9 is 61-payer sampled. Widest payer set and largest
# instance count point at different runs -- auto-select must refuse.

def _run14_vs_run9():
    return [
        _row(9, payers=61, recorded=118_000,
             payer_ids=[f"payer{i:02d}" for i in range(61)]),
        _row(14, payers=1, recorded=3_133_014, payer_ids=["bcbs_az_new"]),
    ]


def test_choose_run_refuses_when_widest_and_biggest_disagree():
    with pytest.raises(SystemExit) as exc:
        choose_run(_run14_vs_run9())
    msg = str(exc.value)
    assert "refusing to auto-select" in msg
    assert "widest payer set" in msg
    assert "most instances" in msg


def test_choose_run_pins_by_run_id():
    picked, why = choose_run(_run14_vs_run9(), run_id=14)
    assert picked == 14
    assert "pinned" in why


def test_choose_run_pin_missing_run_id_exits():
    with pytest.raises(SystemExit) as exc:
        choose_run(_run14_vs_run9(), run_id=99)
    assert "no conformance_run with run_id 99" in str(exc.value)


def test_choose_run_pin_unverified_requires_flag():
    rows = [_row(3, payers=1, recorded=50, instances=100, verified=False)]
    with pytest.raises(SystemExit) as exc:
        choose_run(rows, run_id=3)
    assert "not publishable" in str(exc.value)
    picked, why = choose_run(rows, run_id=3, allow_unverified=True)
    assert picked == 3
    assert "UNVERIFIED" in why


def test_choose_run_expect_payers_by_count_picks_matched():
    rows = _run14_vs_run9()
    picked, why = choose_run(rows, expect_payers=1)
    assert picked == 14
    assert "matched --expect-payers" in why


def test_choose_run_expect_payers_by_set_picks_matched():
    rows = _run14_vs_run9()
    picked, _ = choose_run(rows, expect_payers={"bcbs_az_new"})
    assert picked == 14


def test_choose_run_expect_payers_no_match_exits():
    rows = _run14_vs_run9()
    with pytest.raises(SystemExit) as exc:
        choose_run(rows, expect_payers={"nonexistent_payer"})
    assert "no publishable conformance run matches" in str(exc.value)


def test_choose_run_pin_expect_payers_mismatch_exits():
    rows = _run14_vs_run9()
    with pytest.raises(SystemExit) as exc:
        choose_run(rows, run_id=14, expect_payers=61)
    assert "does not match --expect-payers" in str(exc.value)


def test_choose_run_no_publishable_run_exits():
    rows = [_row(1, verified=False, recorded=50, instances=100)]
    with pytest.raises(SystemExit) as exc:
        choose_run(rows)
    assert "no publishable conformance run" in str(exc.value)


def test_choose_run_single_usable_run_auto_selects():
    rows = [_row(14, payers=1, recorded=3_133_014, payer_ids=["bcbs_az_new"])]
    picked, why = choose_run(rows)
    assert picked == 14
    assert "auto-selected" in why


# ---- parse_expect_payers ------------------------------------------------

def test_parse_expect_payers_none():
    assert parse_expect_payers(None) is None
    assert parse_expect_payers("") is None
    assert parse_expect_payers("  ") is None


def test_parse_expect_payers_int():
    assert parse_expect_payers("61") == 61
    assert parse_expect_payers("1") == 1


def test_parse_expect_payers_set():
    assert parse_expect_payers("a,b,c") == {"a", "b", "c"}
    assert parse_expect_payers("bcbs_az_new") == {"bcbs_az_new"}
    assert parse_expect_payers(" a , b ,, c ") == {"a", "b", "c"}


# ---- CATEGORY_SQL sanity: parameter set is well-formed -----------------

def test_category_params_are_complete():
    # A regression check that _PARAMS matches the placeholders in CATEGORY_SQL.
    for key in ("tx", "bindreq", "bindext", "bindsev", "ext", "bp"):
        assert key in conformance._PARAMS
        assert isinstance(conformance._PARAMS[key], list)
        assert conformance._PARAMS[key] == sorted(conformance._PARAMS[key])

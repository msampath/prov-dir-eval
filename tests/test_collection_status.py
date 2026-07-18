"""The collection-status dashboard builders — pure rendering over sample data
(no DB), so the HTML shape and coverage-bucket coloring are covered."""

from __future__ import annotations

from provdir.quality.collection_status import _cell, _tier, build_fragment, build_standalone

SAMPLE = {
    "generated_at": "2026-07-08T12:00:00+00:00",
    "grand_total": 1_234_567,
    "payer_count": 2,
    "payers": [
        {"key": "oscar", "name": "Oscar Health", "tier": "open", "total": 1_000_000,
         "res": {"Org": {"n": 458000, "cov": 100.0}, "Prac": {"n": 700000, "cov": 100.0},
                 "Role": {"n": 0, "cov": None}, "Loc": {"n": 767000, "cov": None},
                 "HCS": {"n": 0, "cov": None}, "OrgAff": {"n": 0, "cov": None},
                 "Plan": {"n": 0, "cov": None}, "Endpt": {"n": 0, "cov": None}}},
        {"key": "regence", "name": "Regence", "tier": "public-token", "total": 234567,
         "res": {"Org": {"n": 300, "cov": 91.0}, "Prac": {"n": 122000, "cov": 8.0},
                 "Role": {"n": 50000, "cov": 60.0}, "Loc": {"n": 5000, "cov": None},
                 "HCS": {"n": 0, "cov": None}, "OrgAff": {"n": 100, "cov": 83.0},
                 "Plan": {"n": 27, "cov": 100.0}, "Endpt": {"n": 0, "cov": None}}},
    ],
}


def test_tier_from_strategy():
    assert _tier("none") == "open"
    assert _tier("healthsparq_public_token") == "public-token"
    assert _tier("oauth2_client_credentials") == "gated"


def test_cell_coverage_buckets():
    assert "bg-success" in _cell({"n": 100, "cov": 95.0})   # >=90 green
    assert "bg-warning" in _cell({"n": 100, "cov": 70.0})   # 55-89 amber
    assert "bg-danger" in _cell({"n": 100, "cov": 8.0})     # <55 red
    assert "surface-2" in _cell({"n": 100, "cov": None})    # present, unmeasured
    assert "·" in _cell({"n": 0, "cov": None})              # not served


def test_fragment_renders_all_payers_and_disclaimer():
    html = build_fragment(SAMPLE)
    assert "Oscar Health" in html and "Regence" in html
    assert "1.2M" in html                       # grand total card
    assert "sr-only" in html                    # accessibility summary
    assert "no warranty" in html                # disclaimer present
    assert html.count("<tr>") == 3              # header + one row per payer


def test_standalone_is_self_contained():
    html = build_standalone(SAMPLE)
    assert html.startswith("<!doctype html>")
    assert "--surface-1" in html                # defines its own CSS vars
    assert "prefers-color-scheme:dark" in html  # theme-aware
    assert "cdn" not in html.lower()            # no external deps

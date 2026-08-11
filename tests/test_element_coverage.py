"""Unit tests for the element-coverage engine + USCDI/MPF rule sets (no DB)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.models import RESOURCE_TABLES  # noqa: E402
from provdir.quality import mpf, uscdi  # noqa: E402
from provdir.quality.element_coverage import (  # noqa: E402
    CoverageCheck, _build_merged_query, _measure_from_sample, _payers_from_db_rows,
)

# Extracted ARRAY columns across all 8 resource tables -- any CoverageCheck
# testing one of these MUST use coalesce(cardinality(col),0) > 0, never
# `col IS NOT NULL` (arrays store `[]`, not NULL, when empty; IS NOT NULL
# would count an empty array as populated).
_ARRAY_COLUMNS = {
    c.name for t in RESOURCE_TABLES.values() for c in t.columns
    if type(c.type).__name__ == "ARRAY"
}


class TestQueryBuilder:
    def test_single_check_no_cell_where(self):
        items = [("uscdi", CoverageCheck("a", "Practitioner", "family IS NOT NULL", True, None))]
        sql = _build_merged_query("practitioner", items)
        assert sql == (
            "SELECT count(*) AS total, count(*) AS d_0, "
            "count(*) FILTER (WHERE family IS NOT NULL) AS p_0 "
            "FROM practitioner WHERE payer_id = %(payer)s"
        )

    def test_cell_where_ANDs_into_populated_but_not_denominator_alone(self):
        items = [("mpf", CoverageCheck("npi", "Organization", "npi IS NOT NULL", True, None,
                                       cell_where="coalesce(is_network, 0) = 0"))]
        sql = _build_merged_query("organization", items)
        assert "count(*) FILTER (WHERE coalesce(is_network, 0) = 0) AS d_0" in sql
        assert ("count(*) FILTER (WHERE (coalesce(is_network, 0) = 0) "
                "AND (npi IS NOT NULL)) AS p_0") in sql

    def test_multiple_checks_are_merged_into_one_select(self):
        items = [
            ("uscdi", CoverageCheck("a", "Practitioner", "family IS NOT NULL", True, None)),
            ("mpf", CoverageCheck("b", "Practitioner", "given IS NOT NULL", True, None)),
        ]
        sql = _build_merged_query("practitioner", items)
        assert sql.count("SELECT") == 1  # one merged scan, not one query per check
        assert "AS d_0" in sql and "AS p_0" in sql
        assert "AS d_1" in sql and "AS p_1" in sql

    def test_positional_order_matches_input_order(self):
        items = [
            ("uscdi", CoverageCheck("first", "Practitioner", "1=1", True, None)),
            ("uscdi", CoverageCheck("second", "Practitioner", "2=2", True, None)),
            ("uscdi", CoverageCheck("third", "Practitioner", "3=3", True, None)),
        ]
        sql = _build_merged_query("practitioner", items)
        assert sql.index("1=1") < sql.index("2=2") < sql.index("3=3")


class TestArrayCardinalityConvention:
    """Regression guard for the array-vs-NULL footgun: `col IS NOT NULL` is
    always true for an empty array (`[]` is not NULL), so any check over an
    extracted ARRAY column must use `coalesce(cardinality(col),0) > 0`.
    """

    def _offenders(self, checks: list[CoverageCheck]) -> list[str]:
        bad = []
        for c in checks:
            if c.condition_sql is None:
                continue
            for col in _ARRAY_COLUMNS:
                if f"{col} IS NOT NULL" in c.condition_sql:
                    bad.append(f"{c.resource_type}.{c.element} ({col})")
        return bad

    def test_uscdi_checks(self):
        assert self._offenders(uscdi.USCDI_CHECKS) == []

    def test_mpf_checks(self):
        assert self._offenders(mpf.MPF_CHECKS) == []

    def test_plannet_ext_checks(self):
        assert self._offenders(mpf.PLANNET_EXT_CHECKS) == []


class TestJsonbStructuralSafety:
    """jsonb_array_elements RAISES on a non-array value, and one malformed row
    aborts its payer's whole merged scan hours into a sweep. Every JSONB
    condition must therefore either use lax jsonb_path_exists (structural
    mismatch = no match) or guard the array with jsonb_typeof first."""

    def _unguarded(self, checks) -> list[str]:
        bad = []
        for c in checks:
            sql = c.condition_sql or ""
            if "jsonb_array_elements" in sql and "jsonb_typeof" not in sql:
                bad.append(f"{c.resource_type}.{c.element}")
        return bad

    def test_uscdi_checks(self):
        assert self._unguarded(uscdi.USCDI_CHECKS) == []

    def test_mpf_checks(self):
        assert self._unguarded(mpf.MPF_CHECKS) == []

    def test_plannet_ext_checks(self):
        assert self._unguarded(mpf.PLANNET_EXT_CHECKS) == []


class TestRfc5646R4Shape:
    """Regression guard: Practitioner.communication in R4 is CodeableConcept[]
    (coding directly on each entry); the R5 `.language.coding` path matched
    nothing in this corpus and scored a payer publishing bcp:47 codes at 0%."""

    _CHECK = next(c for c in uscdi.USCDI_CHECKS
                  if c.element == "demographics_communication_rfc5646")

    def test_sql_uses_r4_path(self):
        assert "$.communication[*].coding[*]" in self._CHECK.condition_sql
        assert ".language" not in self._CHECK.condition_sql

    def test_predicate_matches_r4_shape(self):
        r4 = {"communication": [{"coding": [{"code": "en",
                                             "system": "urn:ietf:bcp:47"}]}]}
        assert self._CHECK.json_predicate(r4)

    def test_predicate_rejects_display_only_coding(self):
        # humana-style: display text, no code -- the strict tier must not count it
        assert not self._CHECK.json_predicate(
            {"communication": [{"coding": [{"display": "Spanish"}]}]})


class TestMeasureFromSample:
    def _write(self, root: Path, subdir: str, name: str, obj: dict) -> None:
        d = root / subdir
        d.mkdir(parents=True, exist_ok=True)
        (d / name).write_text(json.dumps(obj), encoding="utf-8")

    def test_flat_layout(self, tmp_path):
        self._write(tmp_path, "PractitionerRole", "aetna_cvs__PractitionerRole__0.json",
                    {"telecom": [{"system": "phone", "value": "555-1212"}]})
        self._write(tmp_path, "PractitionerRole", "aetna_cvs__PractitionerRole__1.json", {})
        checks = [CoverageCheck("telecom_phone", "PractitionerRole", "x", False,
                                lambda r: any(t.get("system") == "phone" for t in (r.get("telecom") or [])))]
        out = _measure_from_sample([str(tmp_path)], "aetna_cvs", "PractitionerRole", checks)
        assert out["telecom_phone"] == {"denominator": 2, "populated": 1, "method": "sample"}

    def test_batched_layout_is_found_via_rglob(self, tmp_path):
        # validate_conformance.py's mid-run layout: _batches/<Cell>__NNN/
        self._write(tmp_path, "_batches/PractitionerRole__000",
                   "aetna_cvs__PractitionerRole__0.json", {"telecom": []})
        checks = [CoverageCheck("telecom_phone", "PractitionerRole", "x", False, lambda r: bool(r.get("telecom")))]
        out = _measure_from_sample([str(tmp_path)], "aetna_cvs", "PractitionerRole", checks)
        assert out["telecom_phone"]["denominator"] == 1
        assert out["telecom_phone"]["populated"] == 0

    def test_zero_files_is_unmeasured_not_zero_denominator_silently(self, tmp_path):
        checks = [CoverageCheck("x", "PractitionerRole", "x", False, lambda r: True)]
        out = _measure_from_sample([str(tmp_path)], "no_such_payer", "PractitionerRole", checks)
        assert out["x"] == {"denominator": 0, "populated": 0, "method": "unmeasured"}

    def test_none_predicate_is_always_unmeasured_even_with_files_present(self, tmp_path):
        self._write(tmp_path, "PractitionerRole", "aetna_cvs__PractitionerRole__0.json", {})
        checks = [CoverageCheck("x", "PractitionerRole", "x", False, None)]
        out = _measure_from_sample([str(tmp_path)], "aetna_cvs", "PractitionerRole", checks)
        assert out["x"] == {"denominator": 0, "populated": 0, "method": "unmeasured"}

    def test_multiple_roots_dedupe_by_filename(self, tmp_path):
        root_a = tmp_path / "a"
        root_b = tmp_path / "b"
        self._write(root_a, "PractitionerRole", "aetna_cvs__PractitionerRole__0.json", {"telecom": []})
        self._write(root_b, "PractitionerRole", "aetna_cvs__PractitionerRole__0.json", {"telecom": []})
        checks = [CoverageCheck("x", "PractitionerRole", "x", False, lambda r: True)]
        out = _measure_from_sample([str(root_a), str(root_b)], "aetna_cvs", "PractitionerRole", checks)
        assert out["x"]["denominator"] == 1  # same filename in both roots counts once

    def test_malformed_json_is_skipped_not_fatal(self, tmp_path):
        d = tmp_path / "PractitionerRole"
        d.mkdir()
        (d / "aetna_cvs__PractitionerRole__0.json").write_text("{not json", encoding="utf-8")
        self._write(tmp_path, "PractitionerRole", "aetna_cvs__PractitionerRole__1.json", {"telecom": []})
        checks = [CoverageCheck("x", "PractitionerRole", "x", False, lambda r: True)]
        out = _measure_from_sample([str(tmp_path)], "aetna_cvs", "PractitionerRole", checks)
        # both files counted toward `total` file-glob... but the malformed one
        # never parses, so it cannot contribute a populated hit; only the
        # readable file can.
        assert out["x"]["populated"] == 1


class TestPayersFromDbRows:
    """The resume-artifact invariant: the JSON artifact is rebuilt from the
    run's FULL DB rows, so a resumed invocation must emit every payer under
    the run -- not just the tail it measured itself."""

    _ROW_A = ("payer_a", "Practitioner", "family", 100, 90, "sql", True, "inferred", {"note": ""})
    _ROW_B = ("payer_b", "Practitioner", "family", 50, 10, "sql", True, "inferred", {"note": ""})

    def test_all_payers_in_rows_appear(self):
        payers = _payers_from_db_rows([self._ROW_A, self._ROW_B], {})
        assert set(payers) == {"payer_a", "payer_b"}
        assert payers["payer_a"]["elements"][0]["populated"] == 90

    def test_asof_taken_from_map_and_none_when_absent(self):
        payers = _payers_from_db_rows(
            [self._ROW_A, self._ROW_B], {"payer_a": "2026-08-01T00:00:00"})
        assert payers["payer_a"]["asof"] == "2026-08-01T00:00:00"
        assert payers["payer_b"]["asof"] is None

    def test_multiple_rows_group_under_one_payer(self):
        row2 = ("payer_a", "Location", "phone", 10, 5, "sql", False, "analog", None)
        payers = _payers_from_db_rows([self._ROW_A, row2], {})
        assert len(payers["payer_a"]["elements"]) == 2


class TestMAPlanIdFormat:
    """Verified against real sample values quoted in the approved plan."""

    def _normalized_match(self, raw: str) -> bool:
        import re
        normalized = re.sub(r"[^A-Za-z0-9]", "", raw)
        return bool(mpf.MA_PLAN_ID_FORMAT_RE.match(normalized))

    def test_devoted_form_matches(self):
        assert self._normalized_match("H0413-003-000")

    def test_humana_form_matches(self):
        assert self._normalized_match("H5525-049-000-2021")

    def test_uhc_concatenated_form_matches(self):
        assert self._normalized_match("H1234123123")

    def test_kaiser_xv_prefix_fails(self):
        assert not self._normalized_match("XV1234567")

    def test_fallon_bare_id_fails(self):
        assert not self._normalized_match("31")
        assert not self._normalized_match("3100123")

    def test_lowercase_h_prefix_fails_case_sensitive(self):
        # regexp_replace/regex here is case-sensitive on purpose: real CMS
        # contract ids are always upper-case; a lowercase 'h...' is not one.
        assert not self._normalized_match("h0413003000")


class TestClassifierSignalSql:
    def test_no_signal_uses_a_bare_MA_token(self):
        """Regression guard for the Fallon '(MA)'=Massachusetts trap: a bare
        \\bma\\b token in the plan-name signal would false-positive on every
        Massachusetts-named plan, MA line-of-business or not."""
        name_sig = next(s for s in mpf.MA_SIGNALS if s.name == "insurance_plan_name")
        assert r"\bma\b" not in name_sig.sql

    def test_organization_type_signal_excludes_medicaid(self):
        sig = next(s for s in mpf.MA_SIGNALS if s.name == "organization_type_medicare_like")
        assert "medicaid" in sig.sql

    def test_every_signal_is_scoped_by_payer_id_param(self):
        for sig in mpf.MA_SIGNALS:
            assert "payer_id = %s" in sig.sql, sig.name

    def test_plan_type_signal_checks_expected_ma_codes(self):
        sig = next(s for s in mpf.MA_SIGNALS if s.name == "insurance_plan_plan_type")
        for code in ("MA", "MA-PD", "MAPD", "MMP"):
            assert code in sig.sql


class _FakeCursor:
    def __init__(self, responder):
        self._responder = responder
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._rows = self._responder(sql, params)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, responder):
        self._responder = responder

    def cursor(self):
        return _FakeCursor(self._responder)


class TestClassifyMaOverrides:
    """classify_ma with a fake connection: every signal query returns no rows,
    so only the override dicts should be able to flip the classification."""

    def _manifest_with(self, keys):
        from types import SimpleNamespace
        eps = [SimpleNamespace(key=k) for k in keys]
        return SimpleNamespace(known=lambda: eps)

    def _no_signal_conn(self):
        # list_payer_schemas/set_search_path are monkeypatched at the call
        # sites below, so every query this fake connection actually sees is
        # one of MA_SIGNALS -- always answer "no matches".
        return _FakeConn(lambda sql, params: [])

    def test_override_include_forces_classification_with_no_signals(self, monkeypatch):
        monkeypatch.setattr(mpf, "list_payer_schemas", lambda conn: {"devoted"})
        monkeypatch.setattr(mpf, "set_search_path", lambda conn, schema: None)
        conn = self._no_signal_conn()
        out = mpf.classify_ma(conn, self._manifest_with(["devoted"]))
        assert out[0]["payer"] == "devoted"
        assert out[0]["classified"] is True
        assert out[0]["evidence"] == []
        assert "included" in out[0]["override"]

    def test_no_signal_no_override_is_not_classified(self, monkeypatch):
        monkeypatch.setattr(mpf, "list_payer_schemas", lambda conn: {"cigna"})
        monkeypatch.setattr(mpf, "set_search_path", lambda conn, schema: None)
        conn = self._no_signal_conn()
        out = mpf.classify_ma(conn, self._manifest_with(["cigna"]))
        assert out[0]["classified"] is False
        assert out[0]["override"] is None

    def test_missing_schema_is_skipped(self, monkeypatch):
        monkeypatch.setattr(mpf, "list_payer_schemas", lambda conn: set())
        conn = self._no_signal_conn()
        out = mpf.classify_ma(conn, self._manifest_with(["not_loaded"]))
        assert out == []

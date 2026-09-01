"""Regression tests for the places a number's meaning can flip ([O-16]).

Covers: report-category routing for every message id that moves a headline
rate, the standard-layer classifier, band edges, badge gating (row floor
included), census-cell rendering, classify_path grains, and the
reference-parser pattern. SQL classifiers run against the local Postgres
via VALUES rows; those tests skip if no DB is reachable.
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.quality.findings import (  # noqa: E402
    MATERIAL_CATEGORIES,
    REPORT_CATEGORY_SQL_TEMPLATE,
    RESOURCE_ROOTS,
    STANDARD_LAYER_SQL,
    classify_path,
)

_spec = importlib.util.spec_from_file_location(
    "azblue_html", REPO / "scripts" / "build_azblue_report_html.py")
_html = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_html)


# ---- DB-backed classifier tests ----------------------------------------

def _db():
    try:
        from provdir.etl.loader import pg_connection
        conn = pg_connection()
        conn.autocommit = True
        return conn
    except Exception:
        pytest.skip("no local Postgres reachable")


def _classify(conn, sql_case: str, rows: list[tuple[str | None, str, str]],
              params: dict) -> list[str]:
    """rows: (message_id, severity, details)"""
    out = []
    with conn.cursor() as cur:
        for mid, sev, det in rows:
            cur.execute(
                "SELECT " + sql_case.replace("message_id", "%(mid)s::text")
                .replace("severity", "%(sev)s::text")
                .replace("details", "%(det)s::text"),
                {**params, "mid": mid, "sev": sev, "det": det})
            out.append(cur.fetchone()[0])
    return out


@pytest.fixture(scope="module")
def report_category_sql():
    from provdir.quality.conformance import CATEGORY_SQL, _PARAMS
    return (REPORT_CATEGORY_SQL_TEMPLATE.format(base_category_sql=CATEGORY_SQL),
            dict(_PARAMS))


def test_category_routing_headline_movers(report_category_sql):
    """The ids whose routing changes published rates ([O-5], [F-1], [F-3])."""
    sql, params = report_category_sql
    conn = _db()
    try:
        got = _classify(conn, sql, [
            ("VALUESET_VALUE_MISMATCH_DEFAULT", "warning", "version mismatch"),
            ("UNKNOWN_CODESYSTEM_VERSION", "warning", "v 4.0.1 not found"),
            ("UNKNOWN_CODESYSTEM_VERSION_NONE", "warning", "no versions known"),
            ("Unknown_Code_in_Version", "warning", "code not in version"),
            ("TYPE_SPECIFIC_CHECKS_DT_PRIMITIVE_REGEX_EXCEPTION", "error",
             "Regex evaluation timed out"),
            ("Extension_EXT_Unknown_NotHere", "error",
             "The extension ex:createdAt could not be found"),
        ], params)
    finally:
        conn.close()
    assert got == ["version-stamping", "version-stamping", "version-stamping",
                   "version-stamping", "validator-limitation",
                   "publisher-infrastructure"]


def test_category_routing_material_untouched(report_category_sql):
    sql, params = report_category_sql
    conn = _db()
    try:
        got = _classify(conn, sql, [
            ("Validation_VAL_Profile_Minimum", "error", "minimum required = 2"),
            ("_DT_Fixed_Wrong", "error", "Value is 'false' but is fixed"),
            (None, "error", "Unrecognized property 'geolocation'"),
            # non-createdAt unknown extension stays extension-unresolvable
            ("Extension_EXT_Unknown_NotHere", "error",
             "The extension http://example.org/x could not be found"),
            ("http://hl7.org/fhir/StructureDefinition/DomainResource#dom-6",
             "warning", "should have narrative"),
        ], params)
    finally:
        conn.close()
    assert got[0] == "profile"
    assert got[1] == "profile"
    assert got[2] == "profile"
    assert got[3] == "extension-unresolvable"
    assert got[4] == "best-practice"


def test_standard_layer_classifier():
    conn = _db()
    try:
        got = _classify(conn, STANDARD_LAYER_SQL, [
            ("http://hl7.org/fhir/us/core/StructureDefinition/us-core-practitionerrole#pd-1",
             "error", ""),
            ("http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-Organization#us-core-16",
             "error", ""),
            ("http://hl7.org/fhir/StructureDefinition/DomainResource#dom-6",
             "warning", ""),
            ("_DT_Fixed_Wrong", "error",
             "fixed to 'true' in the profile http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-Organization"),
            ("Type_Specific_Checks_DT_DateTime_Reasonable", "warning",
             "outside the range of reasonable years"),
            # [O-20]: a message that merely CITES a value set does not name
            # the rule's owner -- these go to code-set-membership, never to
            # the value set's publisher.
            ("Terminology_TX_NoValid_1_CC", "error",
             "value set 'USPS Two Letter' (http://hl7.org/fhir/us/core/ValueSet/us-core-usps-state|7.0.0)"),
            ("UNKNOWN_CODESYSTEM", "warning",
             "CodeSystem 'http://hl7.org/fhir/us/carin-bb/CodeSystem/x'"),
            ("Validation_VAL_Profile_Minimum", "error",
             "minimum required = 2 (from http://hl7.org/fhir/StructureDefinition/geolocation|5.3.0)"),
        ], {})
    finally:
        conn.close()
    assert got == ["us-core", "plan-net", "r4-base", "plan-net",
                   "r4-base", "code-set-membership", "code-set-membership",
                   "r4-base"]


# ---- classify_path grains ----------------------------------------------

LUT = {"Organization.type": "mandatory",
       "PractitionerRole.extension.extension.value[x]": "mandatory",
       "Practitioner.name": "must-support"}


def test_classify_resource_root_is_record_level():
    for root in ("Organization", "PractitionerRole"):
        assert classify_path(root, LUT) == ("record-level", "resource-level", False)
    assert classify_path("(resource-level)", LUT)[1] == "resource-level"


def test_classify_exact():
    assert classify_path("Organization.type", LUT) == ("mandatory", "exact", False)


def test_classify_ancestor_depths():
    # one segment below a known element -> ancestor-1
    assert classify_path("Practitioner.name.family", LUT) == \
        ("must-support", "ancestor-1", False)
    # two below -> ancestor-2
    assert classify_path("Organization.type.coding.system", LUT) == \
        ("mandatory", "ancestor-2", False)
    # value[x] chains resolve through the collapsed key
    assert classify_path(
        "PractitionerRole.extension.extension.value[x].coding.system", LUT) == \
        ("mandatory", "ancestor-2", False)


def test_classify_residual_is_base_fhir_mechanism():
    # Unit test of the walk mechanism on a controlled fixture: no element
    # within two levels above -> base-fhir.
    assert classify_path("Practitioner.a.b.c.d", LUT) == \
        ("base-fhir", "none", True)


def test_classify_production_shape_ancestor_inheritance():
    lut = {"Practitioner.address": "must-support"}
    assert classify_path("Practitioner.address.period.end", lut) == \
        ("must-support", "ancestor-2", False)


@pytest.fixture(scope="module")
def real_plannet_lut():
    """The actual Plan-Net 1.2.0 lookup — [O-24]: the base-fhir guard is
    meaningless unless exercised against the real element universe."""
    from provdir.quality.findings import build_requirement_lookup
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT element_path, min_card, must_support "
                "FROM public.plannet_element "
                "WHERE ig_package LIKE %s", ("%plan-net#1.2.0%",))
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        pytest.skip("plannet_element not loaded")
    return build_requirement_lookup(rows)


def test_classify_against_real_plannet_lookup(real_plannet_lut):
    lut = real_plannet_lut
    # genuinely unreachable on the real lookup ([O-24]'s supplied paths)
    assert classify_path(
        "Practitioner.address.period.end.extension.url", lut) == \
        ("base-fhir", "none", True)
    assert classify_path(
        "Organization.contact.purpose.coding.userSelected.id", lut) == \
        ("base-fhir", "none", True)
    # ancestor inheritance as production actually resolves it
    # (Practitioner.address.period is itself enumerated -> depth 1)
    assert classify_path("Practitioner.address.period.end", lut) == \
        ("other", "ancestor-1", False)


def test_resource_roots_cover_run_cells():
    for cell in ("Organization", "Location", "Practitioner",
                 "PractitionerRole", "HealthcareService", "Network",
                 "InsurancePlan"):
        assert cell in RESOURCE_ROOTS


# ---- renderer: bands, badges, census cells -----------------------------

def test_band_edges_are_20_points():
    assert _html.band(0.0) == "b0"
    assert _html.band(0.19) == "b0"
    assert _html.band(0.20) == "b1"
    assert _html.band(0.399) == "b1"
    assert _html.band(0.40) == "b2"
    assert _html.band(0.60) == "b3"
    assert _html.band(0.80) == "b4"
    assert _html.band(1.0) == "b4"


def test_badge_gated_by_row_floor():
    # InsurancePlan n=1 must NOT produce a badge ([O-6.3])
    assert _html.badge_for(1.0, True, 1) is None
    assert _html.badge_for(0.0, False, _html.ROW_FLOOR - 1) is None


def test_badge_rules():
    assert _html.badge_for(0.05, False, 1000) == ("Good", "good")
    # error severity blocks Good
    assert _html.badge_for(0.05, True, 1000) == ("OK", "ok")
    # warnings alone cannot badge Bad
    assert _html.badge_for(0.99, False, 1000) == ("OK", "ok")
    assert _html.badge_for(0.45, True, 1000) == ("Bad", "bad")


def test_render_rate_census_small_cell_exact_count():
    out = _html.render_rate(1, 1)
    assert "1 of 1 records" in out
    assert "%" not in out


def test_render_rate_no_signal_and_rounding():
    assert "no signal" in _html.render_rate(0, 0)
    # census exactness ([O-21]): true zero and full-cell render exact counts
    assert "0 of 1,000 records" in _html.render_rate(0, 1000)
    assert "all 1,000 records" in _html.render_rate(1000, 1000)
    # near-boundaries still round per §9.1 rule 9
    assert "&gt;99.9%" in _html.render_rate(999999, 1000000)
    assert "&lt;0.1%" in _html.render_rate(1, 1000000)


def test_material_categories_exclude_environment():
    for cat in ("best-practice", "terminology-unavailable",
                "validator-limitation", "version-stamping",
                "publisher-infrastructure", "extension-unresolvable"):
        assert cat not in MATERIAL_CATEGORIES


# ---- finding_label slice-resolution guard ([O-23] / R-1) ---------------

_F_BASE = {
    "element_path": "PractitionerRole.extension.extension.value[x]",
    "slice_paths": [
        "PractitionerRole.extension:qualification.extension:status.value[x]",
        "PractitionerRole.extension:qualification.extension:whereValid.value[x]",
    ],
    "sample_details": "not in the value set 'X' (http://hl7.org/fhir/us/core/ValueSet/us-core-usps-state|7.0.0)",
}


def test_finding_label_resolves_only_within_own_slices():
    bindings = [{"element_path":
                 "PractitionerRole.extension:qualification.extension:whereValid.value[x]",
                 "binding_valueset":
                 "http://hl7.org/fhir/us/core/ValueSet/us-core-usps-state",
                 "binding_strength": "required"}]
    label = _html.finding_label(dict(_F_BASE), bindings)
    assert "whereValid" in label
    assert "slice identified" in label


def test_finding_label_never_resolves_outside_own_slices():
    # A binding on the same value set but NOT one of this finding's slices
    # must not produce a name ([O-23]).
    bindings = [{"element_path":
                 "Practitioner.extension:foo.value[x]",
                 "binding_valueset":
                 "http://hl7.org/fhir/us/core/ValueSet/us-core-usps-state",
                 "binding_strength": "required"}]
    label = _html.finding_label(dict(_F_BASE), bindings)
    assert "foo" not in label
    assert "spans 2 profile slices" in label


def test_finding_label_ambiguous_same_valueset_stays_generic():
    vs = "http://hl7.org/fhir/us/core/ValueSet/us-core-usps-state"
    bindings = [
        {"element_path": _F_BASE["slice_paths"][0], "binding_valueset": vs,
         "binding_strength": "required"},
        {"element_path": _F_BASE["slice_paths"][1], "binding_valueset": vs,
         "binding_strength": "required"},
    ]
    label = _html.finding_label(dict(_F_BASE), bindings)
    assert "slice identified" not in label
    assert "spans 2 profile slices" in label


# ---- network-name scrub guard ([O-28] / Opus R3 debt note) -------------

def test_network_name_scrub_is_pinned():
    spec = importlib.util.spec_from_file_location(
        "azblue_driver", REPO / "scripts" / "build_azblue_report.py")
    drv = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(drv)
    # only placeholder names are quotable in external artifacts
    assert drv._QUOTABLE_NET_NAMES == frozenset(
        {"INVALID", "NOT APPLICABLE", "UNKNOWN", "UNKNOWN-ERROR",
         "NONE", "TEST"})
    # branded / geographic names must not be quotable
    for name in ("MA BLUEVISTA PIMA HMO", "BLUEDISTINCTION", "SELECT NETWORK"):
        assert name.upper() not in drv._QUOTABLE_NET_NAMES


# ---- reference parser pattern ([O-12]) ---------------------------------

_REF_RE = re.compile(r"^([A-Za-z]+)/([A-Za-z0-9._-]{1,64})$")


@pytest.mark.parametrize("ref,ok", [
    ("Practitioner/abc-123", True),
    ("Location/8f0d52ae", True),
    ("http://host/fhir/Location/abc", False),   # absolute URL -> unparseable
    ("urn:uuid:1234-5678", False),
    ("#contained1", False),
    ("Location/", False),
    ("noslash", False),
])
def test_ref_parser_forms(ref, ok):
    assert bool(_REF_RE.match(ref)) is ok

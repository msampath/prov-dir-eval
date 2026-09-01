"""Unit tests for provdir.quality.findings.normalize_path and helpers.

The normalizer is the load-bearing correctness risk in the whole report
pipeline: a broken rule silently collapses two different Plan-Net elements
into one finding, and the F2 tripwire only catches SYSTEMIC failure, not the
elements it happens to mis-normalize into the matched bucket. Fixtures assert
the TARGET plannet_element.element value for each raw expression -- not just
structural stability -- because a normalizer that eats `.value` inside
Extension.value.ofType(...) would round-trip a plausible-looking string with
the wrong join key.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.quality.findings import (  # noqa: E402
    ANCESTOR_WALK_MAX_DEPTH,
    _split_segments,
    ancestors,
    build_requirement_lookup,
    normalize_path,
    requirement_class,
    strip_slice_discriminators,
)


# ---- Empty / NULL / blank ----------------------------------------------

@pytest.mark.parametrize("expr", [None, "", "   ", "\t\n"])
def test_normalize_empty_routes_to_resource_level(expr):
    assert normalize_path(expr) == "(resource-level)"


# ---- Rule 1: leading/trailing parens -----------------------------------

def test_rule1_wrap_parens_stripped():
    assert normalize_path("(Organization.contact.telecom.value)") == \
        "Organization.contact.telecom.value"


def test_rule1_multiple_wrap_parens():
    assert normalize_path("((Practitioner.name.given))") == \
        "Practitioner.name.given"


def test_rule1_partial_parens_not_stripped():
    # `(a.b).c` outermost parens do NOT wrap the whole -- leave them.
    r = normalize_path("(Organization.contact).telecom")
    # rule 1 leaves them; downstream rules don't touch bare parens either.
    assert "Organization" in r and "telecom" in r


# ---- Rule 2: terminal navigator functions ------------------------------

def test_rule2_first():
    assert normalize_path("Organization.contact.name.given.first()") == \
        "Organization.contact.name.given"


def test_rule2_exists():
    assert normalize_path("Practitioner.identifier.exists()") == \
        "Practitioner.identifier"


@pytest.mark.parametrize("fn", ["first", "last", "single", "tail",
                                 "exists", "hasValue", "empty", "count"])
def test_rule2_all_terminal_navs(fn):
    assert normalize_path(f"Organization.name.{fn}()") == "Organization.name"


def test_rule2_only_terminal_not_middle():
    # .exists() only stripped from tail. In the middle it's part of a
    # where() and rule 3 handles it.
    r = normalize_path("Organization.contact.where(name.exists()).telecom")
    # rule 3 collapses the where()
    assert r == "Organization.contact.telecom"


# ---- Rule 3: where() -----------------------------------------------------

def test_rule3_simple_where():
    assert normalize_path("Organization.contact.telecom.where(system='phone')") == \
        "Organization.contact.telecom"


def test_rule3_nested_where():
    r = normalize_path(
        "PractitionerRole.code.coding.where(system='http://x' and code='y').code")
    assert r == "PractitionerRole.code.coding.code"


# ---- Rule 4: ofType() collapse + Extension.value rewrite ---------------

def test_rule4_oftype_outside_extension_plain_collapse():
    r = normalize_path("Practitioner.qualification.code.ofType(CodeableConcept)")
    # No `extension` segment earlier in raw -> plain collapse; qualification.code
    # is the target element in plannet_element.
    assert r == "Practitioner.qualification.code"


def test_rule4_bracket_indexed_extension_still_rewrites():
    # Real validator output: bracket-indexed extension chain, no URL.
    # Rule 4's predicate must recognize this as Extension context.
    r = normalize_path(
        "PractitionerRole.extension[0].extension[1].value.ofType(CodeableConcept)")
    assert r.endswith(".value[x]")


def test_rule4_bare_extension_recognized():
    r = normalize_path("PractitionerRole.extension.extension.value.ofType(String)")
    assert r.endswith(".value[x]")


def test_rule4_midpath_value_gets_bracket_x():
    # Real validator output: .value.ofType(X) mid-path, continues into coding.
    # The [x] must be inserted at the mid-path .value, not only at tail.
    r = normalize_path(
        "PractitionerRole.extension[0].extension[8648].value.ofType(CodeableConcept)"
        ".coding[0].system")
    assert ".value[x].coding.system" in r
    assert r.startswith("PractitionerRole.extension.extension.value[x].coding.system")


def test_rule4_midpath_valueString_choice_type():
    # Extension.valueString.something is a concrete choice-type usage that
    # rule 5 rewrites to .value[x].something.
    r = normalize_path("Practitioner.extension[0].valueCodeableConcept.coding[0].code")
    assert ".value[x].coding.code" in r


def test_rule4_oftype_inside_extension_rewrite_to_value_x():
    r = normalize_path(
        "PractitionerRole.extension('http://hl7.org/fhir/us/davinci-pdex-plan-net/"
        "StructureDefinition/qualification').value.ofType(CodeableConcept)")
    # Extension context -> .value.ofType(X) becomes .value[x]
    assert r.endswith(".value[x]")
    assert "PractitionerRole" in r


# ---- Rule 5: choice-type endings ---------------------------------------

def test_rule5_valueString_to_value_x():
    assert normalize_path("Extension.valueString") == "Extension.value[x]"


def test_rule5_valueCodeableConcept():
    r = normalize_path(
        "Practitioner.extension('http://x').valueCodeableConcept.coding.code")
    # rule 5 rewrites the .valueCodeableConcept to .value[x];
    # rule 6 rewrites extension('http://x') to extension(<http://x>)
    assert ".value[x].coding.code" in r
    assert "extension(<http://x>)" in r


def test_rule5_does_not_touch_non_choice_element():
    # An element named `value` alone must NOT be rewritten; only .valueX where
    # X is an enumerated FHIR type.
    r = normalize_path("Organization.contact.telecom.value")
    assert r == "Organization.contact.telecom.value"


# ---- Rule 6: extension URL preservation --------------------------------

def test_rule6_single_quoted_extension():
    r = normalize_path("Practitioner.extension('http://foo/bar')")
    assert r == "Practitioner.extension(<http://foo/bar>)"


def test_rule6_double_quoted_extension():
    r = normalize_path('Practitioner.extension("http://foo/bar")')
    assert r == "Practitioner.extension(<http://foo/bar>)"


def test_rule6_angle_bracket_extension_passthrough():
    r = normalize_path("Practitioner.extension(<http://foo/bar>)")
    assert r == "Practitioner.extension(<http://foo/bar>)"


def test_rule6_url_verbatim_no_scheme_rewrite():
    # trailing slash, https vs http, %-escapes: all MUST pass through verbatim,
    # so a match against plannet_element (byte-equal) works.
    r = normalize_path("Practitioner.extension('https://a/b/')")
    assert "extension(<https://a/b/>)" in r


def test_rule6_two_nested_extensions_both_preserved():
    r = normalize_path(
        "PractitionerRole.extension('http://qual').extension('http://code').value")
    assert "extension(<http://qual>)" in r
    assert "extension(<http://code>)" in r


# ---- Rule 7: array indices --------------------------------------------

def test_rule7_numeric_index():
    assert normalize_path("Organization.contact[0].name") == \
        "Organization.contact.name"


def test_rule7_large_index():
    assert normalize_path("Practitioner.qualification[42].code") == \
        "Practitioner.qualification.code"


def test_rule7_bare_brackets():
    assert normalize_path("Location.address[].line") == "Location.address.line"


# ---- Rule composition (real validator expressions) --------------------

def test_composed_where_index_first():
    r = normalize_path(
        "Organization.contact[0].telecom.where(system='phone').first()")
    assert r == "Organization.contact.telecom"


def test_composed_extension_choice_index():
    r = normalize_path(
        "PractitionerRole.extension('http://qual').extension('http://code')"
        "[0].valueCodeableConcept.coding[0].code")
    # extension URLs preserved, indices stripped, choice-type collapsed
    assert "extension(<http://qual>)" in r
    assert "extension(<http://code>)" in r
    assert "coding.code" in r
    assert ".value[x]" in r
    # numeric indices stripped; only [x] from value[x] may remain
    import re
    assert not re.search(r"\[\d+\]|\[\]", r)


def test_composed_practitioner_qualification_coding_chain():
    r = normalize_path(
        "Practitioner.qualification[0].code.coding[0].code")
    assert r == "Practitioner.qualification.code.coding.code"


# ---- _split_segments: extension URL is atomic --------------------------

def test_split_segments_extension_atomic():
    segs = _split_segments("PractitionerRole.extension(<http://a.com/b>).value[x]")
    assert segs == ["PractitionerRole", "extension(<http://a.com/b>)", "value[x]"]


def test_split_segments_simple():
    assert _split_segments("a.b.c") == ["a", "b", "c"]


def test_split_segments_empty():
    assert _split_segments("") == []


# ---- ancestors --------------------------------------------------------

def test_ancestors_bounded_depth_2():
    # Practitioner.qualification.code.coding.code
    # depth-1 ancestor: Practitioner.qualification.code.coding
    # depth-2 ancestor: Practitioner.qualification.code
    # resource root excluded
    got = ancestors("Practitioner.qualification.code.coding.code")
    assert got == [
        ("Practitioner.qualification.code.coding", 1),
        ("Practitioner.qualification.code", 2),
    ]


def test_ancestors_resource_root_excluded():
    # Practitioner.name -> depth-1 would be Practitioner (root); excluded.
    assert ancestors("Practitioner.name") == []


def test_ancestors_stops_before_root():
    # a.b.c -> depth-1: a.b (kept); depth-2 would be `a` (root); stops
    got = ancestors("a.b.c", max_depth=3)
    assert got == [("a.b", 1)]


def test_ancestors_single_segment_no_ancestors():
    assert ancestors("Practitioner") == []


def test_ancestors_extension_url_stays_atomic():
    got = ancestors(
        "PractitionerRole.extension(<http://q>).value[x].coding.code",
        max_depth=3,
    )
    # depth-1: PractitionerRole.extension(<http://q>).value[x].coding
    # depth-2: PractitionerRole.extension(<http://q>).value[x]
    # depth-3: PractitionerRole.extension(<http://q>)
    assert ("PractitionerRole.extension(<http://q>).value[x].coding", 1) in got
    assert ("PractitionerRole.extension(<http://q>).value[x]", 2) in got


# ---- Slice collapse + requirement classification (F4) ------------------

def test_strip_slice_single():
    assert strip_slice_discriminators(
        "HealthcareService.extension:newpatients.value[x]") == \
        "HealthcareService.extension.value[x]"


def test_strip_slice_nested():
    assert strip_slice_discriminators(
        "PractitionerRole.extension:qualification.extension:whereValid.value[x]"
        ".coding.system") == \
        "PractitionerRole.extension.extension.value[x].coding.system"


def test_strip_slice_noop_on_plain_path():
    assert strip_slice_discriminators("Practitioner.name.given") == \
        "Practitioner.name.given"


def test_requirement_class_mandatory():
    assert requirement_class(1, True) == "mandatory"
    assert requirement_class(2, False) == "mandatory"


def test_requirement_class_must_support():
    assert requirement_class(0, True) == "must-support"
    assert requirement_class(None, True) == "must-support"


def test_requirement_class_other():
    assert requirement_class(0, False) == "other"
    assert requirement_class(None, None) == "other"


def test_lookup_exact_and_collapsed_keys():
    lut = build_requirement_lookup([
        ("HealthcareService.extension:newpatients", 0, True),
    ])
    assert lut["HealthcareService.extension:newpatients"] == "must-support"
    assert lut["HealthcareService.extension"] == "must-support"


def test_lookup_collision_becomes_mixed():
    # Two slices of the same element with different classes: the collapsed
    # key is ambiguous and must say so.
    lut = build_requirement_lookup([
        ("PractitionerRole.extension:a", 1, True),   # mandatory
        ("PractitionerRole.extension:b", 0, True),   # must-support
    ])
    assert lut["PractitionerRole.extension"] == "mixed"
    # exact keys keep their own class
    assert lut["PractitionerRole.extension:a"] == "mandatory"
    assert lut["PractitionerRole.extension:b"] == "must-support"


def test_lookup_agreeing_slices_stay_classified():
    lut = build_requirement_lookup([
        ("Location.extension:x", 0, True),
        ("Location.extension:y", 0, True),
    ])
    assert lut["Location.extension"] == "must-support"


# ---- Constants ---------------------------------------------------------

def test_ancestor_walk_max_depth_default_is_2():
    assert ANCESTOR_WALK_MAX_DEPTH == 2

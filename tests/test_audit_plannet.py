"""Tests for the Plan-Net IG audit: path compiler, shred SQL, and one
end-to-end shred against a real Postgres.

The compiler tests are the important ones. Every number the audit reports is
downstream of one jsonpath per IG element, and a WRONG jsonpath does not fail
loudly -- it silently reports 0%, or reports a parent's numbers as a slice's.
The specific cases pinned here are the four ways that has actually happened:

  * a nested extension (newpatients -> fromNetwork) read as "not published"
  * a slice's children losing the slice's own url filter and matching any
    extension of any extension
  * a pattern-discriminated identifier slice matching every identifier
  * a choice element tested only against its first declared type
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "scripts"))

PKG = Path("F:/fhir_cache/packages/hl7.fhir.us.davinci-pdex-plan-net#1.2.0/package")
pytestmark = pytest.mark.skipif(
    not PKG.is_dir(),
    reason="Plan-Net 1.2.0 package not present in the local FHIR package cache")

FROM_NETWORK = "PractitionerRole.extension:newpatients.extension:fromNetwork.value[x]"
FROM_NETWORK_JP = (
    '$.extension[*] ? (@.url == "http://hl7.org/fhir/us/davinci-pdex-plan-net/'
    'StructureDefinition/newpatients").extension[*] ? (@.url == "fromNetwork")'
    '.valueReference'
)


@pytest.fixture(scope="module")
def compiled():
    import audit_plannet as A
    checks, index, packages = A.load_surface(str(PKG))
    return A, checks, index, packages


def by_path(checks, profile, path):
    hits = [c for c in checks if c.profile == profile and c.path == path]
    assert hits, f"{profile} {path} missing from the compiled surface"
    return hits


# --------------------------------------------------------------------------
# the element surface
# --------------------------------------------------------------------------

def test_surface_is_the_whole_snapshot(compiled):
    """750 raw IG records, 737 distinct elements: 13 are emitted twice."""
    import ig_elements

    raw = ig_elements.build(str(PKG))
    assert len(raw) == 750
    assert sum(1 for e in raw if e.must_support) == 299
    assert sum(1 for e in raw if e.min > 0) == 114
    assert sum(1 for e in raw if e.is_reference) == 78
    assert sum(1 for e in raw if e.binding_valueset) == 126

    _, checks, _, _ = compiled
    assert len(checks) == 737
    assert len({(c.profile, c.path) for c in checks}) == len(checks)
    assert len({c.profile for c in checks}) == 9


def test_inlined_extension_is_deduped_to_the_profiles_own_binding(compiled):
    """The 13 duplicates are the PractitionerRole qualification family, and
    the copy that survives is the profile snapshot's -- which narrows the
    binding the shared Qualification extension leaves wider."""
    import ig_elements
    from collections import Counter

    raw = ig_elements.build(str(PKG))
    dup = {k for k, n in Counter((e.profile, e.path) for e in raw).items() if n > 1}
    assert len(dup) == 13
    assert all(p == "PlannetPractitionerRole"
               and path.startswith("PractitionerRole.extension:qualification")
               for p, path in dup)

    _, checks, _, _ = compiled
    code = by_path(checks, "PlannetPractitionerRole",
                   "PractitionerRole.extension:qualification.extension:code.value[x]")
    assert len(code) == 1
    assert code[0].binding_valueset.endswith(
        "IndividualSpecialtyAndDegreeLicenseCertificateVS")


def test_every_check_maps_to_a_table_and_a_jsonpath(compiled):
    A, checks, _, _ = compiled
    for c in checks:
        assert c.table in {t.name for t in
                           __import__("provdir.models", fromlist=["x"]).RESOURCE_TABLES.values()}
        assert c.jsonpaths and all(jp.startswith("$") for jp in c.jsonpaths)
        assert c.cell in (None, "network", "not_network")


# --------------------------------------------------------------------------
# the four failure modes
# --------------------------------------------------------------------------

def test_nested_extension_reference_compiles_to_the_published_path(compiled):
    """The linkage an earlier column-based audit reported as absent."""
    _, checks, _, _ = compiled
    c = by_path(checks, "PlannetPractitionerRole", FROM_NETWORK)[0]
    assert c.jsonpaths == [FROM_NETWORK_JP]
    assert c.is_reference
    assert c.ref_targets == ["plannet-Network"]


def test_slice_children_keep_the_slice_discriminator(compiled):
    """PlannetPractitionerRole inlines the qualification extension, so its
    sub-elements arrive from ig_elements as `$.extension[*].extension[*]`."""
    _, checks, _, _ = compiled
    pr = [c for c in checks if c.profile == "PlannetPractitionerRole"]
    generic = [c for c in pr if any(jp.startswith("$.extension[*].extension[*]")
                                    for jp in c.jsonpaths)]
    assert generic == [], [c.path for c in generic]
    c = by_path(checks, "PlannetPractitionerRole",
                "PractitionerRole.extension:qualification.extension:status.value[x]")[0]
    assert c.jsonpaths[0] == (
        '$.extension[*] ? (@.url == "http://hl7.org/fhir/us/davinci-pdex-plan-net/'
        'StructureDefinition/qualification").extension[*] ? (@.url == "status").valueCode')


def test_pattern_discriminated_identifier_slices(compiled):
    _, checks, _, _ = compiled
    npi = by_path(checks, "PlannetOrganization", "Organization.identifier:NPI")[0]
    assert npi.jsonpaths == [
        '$.identifier[*] ? (@.system == "http://hl7.org/fhir/sid/us-npi")']
    clia = by_path(checks, "PlannetOrganization", "Organization.identifier:CLIA")[0]
    assert 'urn:oid:2.16.840.1.113883.4.7' in clia.jsonpaths[0]


def test_choice_element_covers_every_declared_type(compiled):
    _, checks, _, _ = compiled
    c = by_path(checks, "PlannetPractitionerRole",
                "PractitionerRole.extension:qualification.extension:whereValid.value[x]")[0]
    assert len(c.jsonpaths) == 2
    assert any(jp.endswith(".valueCodeableConcept") for jp in c.jsonpaths)
    assert any(jp.endswith(".valueReference") for jp in c.jsonpaths)


def test_open_type_value_is_probed_by_key_prefix(compiled):
    A, checks, _, _ = compiled
    c = by_path(checks, "PlannetPractitionerRole",
                "PractitionerRole.extension:newpatients.value[x]")[0]
    assert len(c.types) > A.OPEN_TYPE_THRESHOLD
    assert c.open_value_parent is not None
    assert c.max_card == "0"
    assert "starts_with" in A.arr_expr(c)
    assert c.card_method == "open-value-key-probe"


def test_value_discriminated_slice_uses_its_bound_codesystem(compiled):
    _, checks, _, _ = compiled
    c = by_path(checks, "PlannetHealthcareService",
                "HealthcareService.category:plan-net-category")[0]
    assert "HealthcareServiceCategoryCS" in c.jsonpaths[0]


# --------------------------------------------------------------------------
# cardinality classification
# --------------------------------------------------------------------------

def test_cardinality_method_reflects_where_the_parent_is(compiled):
    """Both methods are exact; they differ only in what they group by."""
    _, checks, _, _ = compiled
    top = by_path(checks, "PlannetPractitionerRole", "PractitionerRole.active")[0]
    assert top.is_root_parented and top.card_method == "root"
    assert top.parent_idx is None and top.parent_jsonpath == "$"
    nested = by_path(checks, "PlannetPractitioner", "Practitioner.name.family")[0]
    assert not nested.is_root_parented and nested.card_method == "per-parent"
    assert nested.parent_idx is not None


def test_root_parented_is_exactly_parent_idx_none(compiled):
    """The two ways of asking "is this element's parent the resource?" must
    agree, because audit_schema picks the cardinality denominator off one and
    the shred picks its UNION branch off the other."""
    _, checks, _, _ = compiled
    assert ({c.idx for c in checks if c.parent_idx is None}
            == {c.idx for c in checks if c.parent_jsonpath == "$"})


def test_every_parent_is_in_the_catalogue_and_prefixes_its_children(compiled):
    """The two facts the hierarchical shred rests on.

    Relative extraction is only exact because a parent's jsonpath is a literal
    prefix of its children's; and a child's parent-instance denominator is only
    available because its parent is itself a catalogued element with its own
    occurrence count. Either failing would make cardinality unmeasurable for
    part of the surface, silently.
    """
    A, checks, _, _ = compiled
    by_idx = {c.idx: c for c in checks}
    for c in checks:
        pjp = c.parent_jsonpath or "$"
        for jp in c.jsonpaths:
            assert A.rel_jsonpath(jp, pjp).startswith("$")
        if c.is_root_parented:
            continue
        parent = by_idx[c.parent_idx]
        assert parent.jsonpaths == [pjp], (c.path, parent.jsonpaths, pjp)
        assert parent.open_value_parent is None
        # the parent's own occurrence expression IS the child branch's
        # enumeration, which is what makes n_occurrences a valid denominator
        assert A.arr_expr(parent) == (
            f"jsonb_path_query_array(t.resource, {A._lit(pjp)}::jsonpath)")


def test_rel_jsonpath_refuses_a_path_outside_its_parent(compiled):
    A, _, _, _ = compiled
    assert A.rel_jsonpath("$.a[*].b", "$.a[*]") == "$.b"
    assert A.rel_jsonpath("$.a", "$") == "$.a"
    with pytest.raises(ValueError):
        A.rel_jsonpath("$.other.b", "$.a[*]")


def test_organization_and_network_split_the_same_table(compiled):
    _, checks, _, _ = compiled
    org = by_path(checks, "PlannetOrganization", "Organization.name")[0]
    net = by_path(checks, "PlannetNetwork", "Organization.name")[0]
    assert org.table == net.table == "organization"
    assert (org.cell, net.cell) == ("not_network", "network")


# --------------------------------------------------------------------------
# generated SQL
# --------------------------------------------------------------------------

def test_lit_escapes_quotes(compiled):
    A, _, _, _ = compiled
    assert A._lit("a'b") == "'a''b'"


def test_shred_sql_has_one_entry_per_element_and_no_percent(compiled):
    """A literal `%` in driver-level SQL is read by psycopg as a placeholder,
    so the generated shred must not contain one."""
    A, checks, _, _ = compiled
    hs = [c for c in checks if c.table == "healthcare_service"]
    sql = A.build_shred_insert("bcbs_az_new", "healthcare_service",
                               "HealthcareService", hs)
    assert "%" not in sql
    for c in hs:
        assert f"({c.idx}, '{c.path}'" in sql
    assert "WITH ORDINALITY" in sql
    assert "jsonb_path_query_array" in sql
    assert "parent_ordinal" in sql


def test_shred_sql_has_one_branch_per_distinct_parent_path(compiled):
    """The shape: one UNION ALL branch per parent jsonpath, every jsonpath a
    plan-time literal. Carrying paths as data in a VALUES list would re-parse
    them once per row per element."""
    A, checks, _, _ = compiled
    pr = [c for c in checks if c.table == "practitioner_role"]
    branches = A.shred_branches(pr)
    assert len(branches) == len({c.parent_jsonpath for c in pr})
    assert sum(1 for b in branches if "parent_ord" in b) == len(branches)
    # exactly one root branch, and it is the only one without a parent
    # enumeration
    roots = [b for b in branches if "0::int AS parent_ord" in b]
    assert len(roots) == 1
    assert "pj(pv, ord)" not in roots[0]
    assert "::jsonpath" in A.build_shred_select(
        "s", "practitioner_role", "PractitionerRole", pr)


def test_open_value_probe_goes_relative_under_a_nested_parent(compiled):
    """The open value[x] key probe has to follow the same relative rewrite as
    everything else, or the branch would re-scan every parent occurrence for
    each one."""
    A, checks, _, _ = compiled
    c = by_path(checks, "PlannetPractitionerRole",
                "PractitionerRole.extension:newpatients.value[x]")[0]
    rel = A.arr_expr_rel(c, "pj.pv")
    assert "jsonb_each(pj.pv)" in rel
    assert "jsonb_path_query_array" not in rel


def test_presence_columns_line_up_with_their_index_constants(compiled):
    """audit_schema reads the measurement pass positionally. Eleven columns and
    eleven constants drifting apart would not raise -- it would put the
    max-cardinality count in the min column and report it."""
    import re

    A, _, _, _ = compiled
    order = [
        ("m_elem", A.M_ELEM), ("m_profile", A.M_PROFILE), ("m_lob", A.M_LOB),
        ("n_present", A.M_PRESENT), ("n_occurrences", A.M_OCC),
        ("n_max_violation", A.M_MAXV), ("n_min_satisfied", A.M_MINSAT),
        ("n_concept_no_coding", A.M_NOCODING),
        ("n_concept_text_only", A.M_TEXTONLY),
        ("n_invalid_occ", A.M_INVOCC), ("n_invalid_rows", A.M_INVROWS),
        ("n_pre0a", A.M_PRE0A),
    ]
    assert re.findall(r"AS ([mn]_\w+)", A.presence_sql()) == [n for n, _ in order]
    assert [i for _, i in order] == list(range(len(order)))


def test_ddl_constants_carry_no_bare_percent(compiled):
    """Same trap as the generated shred, and it has already been sprung once:
    psycopg reads a literal `%` in driver-level SQL as a placeholder, so a `%`
    inside a SQL COMMENT aborts the whole DDL block with 'incomplete
    placeholder'."""
    A, _, _, _ = compiled
    for name in ("SHRED_DDL", "AUDIT_DDL"):
        assert "%" not in getattr(A, name), name


def test_shred_sql_applies_the_network_cell_split(compiled):
    A, checks, _, _ = compiled
    org = [c for c in checks if c.table == "organization"]
    sql = A.build_shred_insert("bcbs_az_new", "organization", "Organization", org)
    # The network predicate is hoisted into the fenced `tc` lateral and
    # evaluated ONCE per resource row; the VALUES rows reference the flag.
    # Inlining it per element re-detoasts the whole document per parent
    # occurrence -- hours per 25 MB network whale.
    assert sql.count('jsonb_path_exists(t.resource') == 1
    assert '@.code == "ntwk"' in sql
    assert 'AS is_net OFFSET 0) tc' in sql
    assert 'NOT tc.is_net' in sql and 'tc.is_net' in sql
    assert 'tc.lob AS lob' in sql


def test_validity_sql_covers_the_declared_primitive_types(compiled):
    A, checks, _, _ = compiled
    kinds = {c.validity_kind for c in checks if c.validity_kind}
    assert {"code", "boolean", "positiveInt", "instant", "uri", "Reference"} <= kinds
    for kind in kinds:
        assert kind in A.VALIDITY_SQL


# --------------------------------------------------------------------------
# end-to-end: compile -> SQL -> Postgres -> rows
# --------------------------------------------------------------------------

FIXTURE = {
    "id": "pr-1",
    "meta": {"source": "urn:source_name:azblue"},
    "active": True,
    "extension": [
        {"url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/newpatients",
         "extension": [
             {"url": "fromNetwork", "valueReference": {"reference": "Organization/net-1"}},
             {"url": "acceptingPatients", "valueCodeableConcept": {"coding": [
                 {"code": "newpt", "system": "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                                             "CodeSystem/AcceptingPatientsCS"}]}},
         ]},
        {"url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/newpatients",
         "extension": [
             {"url": "fromNetwork", "valueReference": {"reference": "Organization/net-2"}},
         ]},
    ],
    "location": [{"reference": "Location/loc-1"}, {"reference": "Location/loc-2"}],
    "specialty": [{"coding": [{"code": "208D00000X", "system": "http://nucc.org/provider-taxonomy"},
                              {"code": "bogus code", "system": "http://example.org/other"}]}],
    "practitioner": {"display": "no reference here"},
}

# The three shapes a flat shred cannot tell apart, in one row each.
#
# pr-2  parent occurrence 1 has NO child, occurrence 2 has two. It is the
#       counter-example to every cheap stand-in for count(DISTINCT resource_id):
#       no row carries parent_ordinal 1, so a `parent_ordinal = 1` filter drops
#       the resource from the numerator entirely, and `ordinal = 1` counts the
#       two parent instances that DO have children as two resources.
#       It also carries a genuine per-parent max=1 breach (two fromNetwork
#       sub-extensions inside ONE newpatients) and a genuine min=1 breach (one
#       of them has no value[x] at all).
# pr-3  no newpatients extension at all: the min=1 element underneath owes
#       nothing, because its parent never occurs.
FIXTURE_2 = {
    "id": "pr-2",
    "meta": {"source": "urn:source_name:azblue"},
    "extension": [
        {"url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/newpatients",
         "extension": [
             {"url": "acceptingPatients", "valueCodeableConcept": {"text": "call us"}},
         ]},
        {"url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/newpatients",
         "extension": [
             {"url": "fromNetwork", "valueReference": {"reference": "Organization/net-3"}},
             {"url": "fromNetwork"},
         ]},
    ],
}
FIXTURE_3 = {"id": "pr-3", "meta": {"source": "urn:source_name:azblue"}, "active": False}

NEWPATIENTS = "PractitionerRole.extension:newpatients"
FROM_NETWORK_EXT = "PractitionerRole.extension:newpatients.extension:fromNetwork"


def _shred(conn, A, checks, fixtures):
    """Run the generated shred SELECT over a temp table of `fixtures`."""
    import json

    pr = [c for c in checks if c.table == "practitioner_role"]
    conn.exec_driver_sql("SET statement_timeout = 0")
    conn.exec_driver_sql(A.SHRED_DDL)
    conn.exec_driver_sql("DROP TABLE IF EXISTS pg_temp.practitioner_role")
    conn.exec_driver_sql(
        "CREATE TEMP TABLE practitioner_role (id text, resource jsonb)")
    for f in fixtures:
        conn.exec_driver_sql(
            "INSERT INTO practitioner_role (id, resource) VALUES (%(i)s, %(r)s)",
            {"i": f["id"], "r": json.dumps(f)})
    return A.build_shred_select("pg_temp", "practitioner_role",
                                "PractitionerRole", pr)


@pytest.fixture(scope="module")
def pg_conn(compiled):
    try:
        from provdir.db import get_engine
        engine = get_engine()
        conn = engine.connect()
        conn.exec_driver_sql("SELECT 1")
    except Exception as exc:  # noqa: BLE001 - no database available in this environment
        pytest.skip(f"Postgres not reachable: {exc}")
    yield conn
    conn.rollback()
    conn.close()


def test_end_to_end_shred_of_one_resource(compiled, pg_conn):
    """Compile, generate, execute -- and read the shredded rows back.

    Runs the shred's SELECT (not its INSERT) against a TEMP table shaped like a
    payer table, so it exercises the real generated SQL without writing to the
    audit's own tables or touching a payer schema.
    """
    A, checks, _, _ = compiled
    select = _shred(pg_conn, A, checks, [FIXTURE])
    rows = pg_conn.exec_driver_sql(
        f"SELECT element_path, ordinal, coding_ordinal, value_text, ref_type, ref_id, "
        f"code_system, code_code, lob, parent_ordinal FROM ({select}) q").all()

    by_elem: dict[str, list] = {}
    for r in rows:
        by_elem.setdefault(r[0], []).append(r)

    # the nested-extension network linkage, twice, with distinct targets. Each
    # sits at ordinal 1 of its OWN newpatients occurrence -- flat it would read
    # as one element repeating twice, which for a max=1 element is a breach that
    # is not there.
    fn = sorted(by_elem[FROM_NETWORK], key=lambda r: r[9])
    assert [(r[9], r[1]) for r in fn] == [(1, 1), (2, 1)]
    assert [(r[4], r[5]) for r in fn] == [("Organization", "net-1"),
                                          ("Organization", "net-2")]
    assert {r[8] for r in fn} == {"azblue"}

    # the parent itself is root-parented, so its own ordinals still run 1..n
    npat = sorted(by_elem[NEWPATIENTS], key=lambda r: r[1])
    assert [(r[9], r[1]) for r in npat] == [(0, 1), (0, 2)]

    # a repeating top-level reference keeps its ordinals
    loc = sorted(by_elem["PractitionerRole.location"], key=lambda r: r[1])
    assert [(r[1], r[5]) for r in loc] == [(1, "loc-1"), (2, "loc-2")]

    # a CodeableConcept fans out to one row per coding
    spec = sorted(by_elem["PractitionerRole.specialty"], key=lambda r: r[2])
    assert [(r[2], r[7]) for r in spec] == [(1, "208D00000X"), (2, "bogus code")]

    # a Reference carrying only a display is present but names nothing
    prac = by_elem["PractitionerRole.practitioner"]
    assert len(prac) == 1
    assert prac[0][4] is None and prac[0][3] == "no reference here"

    # a scalar
    assert by_elem["PractitionerRole.active"][0][1] == 1

    pg_conn.rollback()


def test_cardinality_is_counted_within_each_parent_occurrence(compiled, pg_conn):
    """0a. A multi-occurrence parent whose occurrences hold different numbers
    of children -- the shape the flat shred could not express.

    pr-2's newpatients #1 has no fromNetwork; #2 has two. Flat, `fromNetwork`
    reads as one element occurring twice on the resource, and a `max = 1`
    element occurring twice is a breach. Per parent it is what it actually is:
    one parent instance with none and one with a real breach.
    """
    A, checks, _, _ = compiled
    select = _shred(pg_conn, A, checks, [FIXTURE, FIXTURE_2, FIXTURE_3])
    fn = A._fetch(pg_conn, f"""
        SELECT resource_id, parent_ordinal, ordinal FROM ({select}) q
        WHERE element_path = '{FROM_NETWORK_EXT}' AND coding_ordinal = 1
        ORDER BY 1, 2, 3""")
    assert [tuple(r) for r in fn] == [
        ("pr-1", 1, 1), ("pr-1", 2, 1),      # one per newpatients occurrence
        ("pr-2", 2, 1), ("pr-2", 2, 2),      # both inside newpatients #2
    ]

    c = by_path(checks, "PlannetPractitionerRole", FROM_NETWORK_EXT)[0]
    assert c.max_n == 1 and c.card_method == "per-parent"
    # exactly one parent instance breaches max=1, and it is pr-2's second
    breach = A._fetch(pg_conn, f"""
        SELECT resource_id, parent_ordinal FROM ({select}) q
        WHERE element_path = '{FROM_NETWORK_EXT}' AND coding_ordinal = 1
          AND ordinal = {c.max_n + 1}""")
    assert [tuple(r) for r in breach] == [("pr-2", 2)]
    # the flat test the fix replaces would have charged pr-1 too
    flat = A._fetch(pg_conn, f"""
        SELECT count(DISTINCT resource_id) FROM ({select}) q
        WHERE element_path = '{FROM_NETWORK_EXT}' AND coding_ordinal = 1""")[0][0]
    assert flat == 2
    pg_conn.rollback()


def test_min_is_conditioned_on_parent_presence(compiled, pg_conn):
    """0b. min=1 under an optional parent, three ways.

    `fromNetwork.value[x]` is min=1 inside `fromNetwork`, which is itself
    optional. pr-1 satisfies it twice; pr-2 has two fromNetwork parents and only
    one carries a value; pr-3 has no parent at all and therefore owes nothing.
    The old rule -- `n_rows - n_present`, gated on whether an ancestor REPEATS
    rather than whether it is OPTIONAL -- charges pr-3 with a violation it
    cannot commit.
    """
    A, checks, _, _ = compiled
    child = by_path(checks, "PlannetPractitionerRole", FROM_NETWORK)[0]
    parent = by_path(checks, "PlannetPractitionerRole", FROM_NETWORK_EXT)[0]
    assert child.min == 1 and parent.min == 0 and parent.max_card == "1"

    select = _shred(pg_conn, A, checks, [FIXTURE, FIXTURE_2, FIXTURE_3])
    n_rows, n_parent, n_sat, n_present = A._fetch(pg_conn, f"""
        SELECT (SELECT count(*) FROM pg_temp.practitioner_role),
               count(*) FILTER (WHERE element_path = '{FROM_NETWORK_EXT}'
                                AND coding_ordinal = 1),
               count(*) FILTER (WHERE element_path = '{FROM_NETWORK}'
                                AND coding_ordinal = 1 AND ordinal = {child.min}),
               count(DISTINCT resource_id) FILTER (
                   WHERE element_path = '{FROM_NETWORK}')
        FROM ({select}) q""")[0]
    assert (n_rows, n_parent, n_sat, n_present) == (3, 4, 3, 2)

    assert A.min_violation(child, n_parent, n_sat) == 1     # pr-2's empty one
    # what the old rule produced: every row without the element, parent or no
    assert n_rows - n_present == 1 + 0  # pr-3, which owes nothing
    # and min = 0 stays unmeasured rather than being reported as clean
    assert A.min_violation(parent, n_rows, 0) is None
    pg_conn.rollback()


def test_n_present_must_be_distinct_resources(compiled, pg_conn):
    """0c. Both cheap stand-ins for count(DISTINCT resource_id) are wrong once
    ordinals restart inside every parent, and they are wrong in opposite
    directions on the same data.

    `ordinal = 1` counts parent instances and calls them resources -- pr-2
    contributes two. `parent_ordinal = 1` misses pr-2 entirely, because its only
    populated parent is occurrence 2 and no row carries occurrence 1.
    """
    A, checks, _, _ = compiled
    select = _shred(pg_conn, A, checks, [FIXTURE, FIXTURE_2, FIXTURE_3])
    distinct, by_ordinal, by_parent_ordinal = A._fetch(pg_conn, f"""
        SELECT count(DISTINCT resource_id),
               count(*) FILTER (WHERE ordinal = 1 AND coding_ordinal = 1),
               count(*) FILTER (WHERE ordinal = 1 AND parent_ordinal = 1
                                AND coding_ordinal = 1)
        FROM ({select}) q WHERE element_path = '{FROM_NETWORK_EXT}'""")[0]
    assert distinct == 2                 # pr-1 and pr-2 publish it
    assert by_ordinal == 3               # over-counts: 3 parent instances
    assert by_parent_ordinal == 1        # under-counts: pr-2 vanishes
    pg_conn.rollback()


def test_text_only_concept_is_visible_as_uncoded(compiled, pg_conn):
    """A CodeableConcept with `text` and no `coding` is fully populated and
    completely uncoded. The shred has to carry enough to say so."""
    A, checks, _, _ = compiled
    select = _shred(pg_conn, A, checks, [FIXTURE_2])
    path = ("PractitionerRole.extension:newpatients.extension:"
            "acceptingPatients.value[x]")
    c = by_path(checks, "PlannetPractitionerRole", path)[0]
    assert c.is_coded and c.binding_strength == "required"
    rows = A._fetch(pg_conn, f"""
        SELECT value_text, code_code, coding_ordinal FROM ({select}) q
        WHERE element_path = '{path}'""")
    # value_text carries the concept's `text` only when there is no coding at
    # all, so "populated but uncoded under a required binding" is countable
    assert [tuple(r) for r in rows] == [("call us", None, 1)]

    # and the coded case is untouched: text stays out of value_text
    select = _shred(pg_conn, A, checks, [FIXTURE])
    rows = A._fetch(pg_conn, f"""
        SELECT value_text, code_code FROM ({select}) q
        WHERE element_path = '{path}'""")
    assert [tuple(r) for r in rows] == [(None, "newpt")]
    pg_conn.rollback()


def test_shred_select_preserves_the_flat_occurrence_total(compiled, pg_conn):
    """The hierarchical enumeration must be a PARTITION of the flat one.

    That is what makes an element's own occurrence count a valid denominator
    for its children, and it is the check a distinct-resource_id comparison
    structurally cannot make: duplication into a partition with no unique
    constraint changes the total while leaving the distinct count alone.
    """
    A, checks, _, _ = compiled
    pr = [c for c in checks if c.table == "practitioner_role"]
    _shred(pg_conn, A, checks, [FIXTURE, FIXTURE_2, FIXTURE_3])
    src = A.source_occurrences(pg_conn, "pg_temp", "practitioner_role", pr)
    sel = A.select_occurrences(pg_conn, "pg_temp", "practitioner_role",
                               "PractitionerRole", pr)
    assert A.compare_occurrences(src, sel) == []
    assert sum(src.values()) > 0
    pg_conn.rollback()


@pytest.mark.skipif(os.environ.get("PROVDIR_SKIP_LIVE_AUDIT") == "1",
                    reason="live-data regression disabled")
def test_regression_fromnetwork_share(compiled, pg_conn):
    """The invariant the whole path compiler is judged against: the nested
    newpatients -> fromNetwork reference is present on 29,350 of bcbs_az_new's
    96,027 PractitionerRole rows (30.6%), not on none of them."""
    A, checks, _, _ = compiled
    c = by_path(checks, "PlannetPractitionerRole", FROM_NETWORK)[0]
    have = pg_conn.exec_driver_sql(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'bcbs_az_new' AND table_name = 'practitioner_role'"
    ).all()[0][0]
    if not have:
        pytest.skip("bcbs_az_new not present in this database")
    pg_conn.exec_driver_sql("SET statement_timeout = 0")
    n_present, n_rows = pg_conn.exec_driver_sql(
        f"SELECT count(*) FILTER (WHERE jsonb_array_length({A.arr_expr(c)}) > 0), "
        f"count(*) FROM bcbs_az_new.practitioner_role t").all()[0]
    assert (n_present, n_rows) == (29350, 96027)


# --------------------------------------------------------------------------
# resume safety: unlogged shred vs logged progress
# --------------------------------------------------------------------------

def test_reconcile_discards_progress_whose_shred_data_is_gone(compiled, pg_conn):
    """A crash truncates the UNLOGGED shred partitions but leaves the LOGGED
    progress table intact, so every chunk still reads as done. Resuming would
    then skip the whole shred and the audit would report 0% over an empty
    table -- a silent empty success. The reconciliation must catch that.

    Deliberately touches no shared object: it stands up a throwaway table
    under the name reconcile_progress() derives for its cell, so it never
    takes DDL locks on plannet_element_value or ig_shred_progress (doing so
    would stall a shred running in another session).
    """
    A, _, _, _ = compiled
    pg_conn.exec_driver_sql("SET statement_timeout = 0")
    have_col = pg_conn.exec_driver_sql(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'ig_shred_progress' "
        "AND column_name = 'postmaster_start'").all()[0][0]
    if not have_col:
        pytest.skip("ig_shred_progress.postmaster_start not migrated yet")

    schema, table = "reconcile_probe", "practitioner_role"
    assert A.leaf_partition(schema, table) ==         "public.pev_practitioner_role_reconcile_probe"
    pg_conn.exec_driver_sql(
        "CREATE UNLOGGED TABLE IF NOT EXISTS "
        "public.pev_practitioner_role_reconcile_probe "
        "(resource_id text, parent_ordinal integer)")
    try:
        # the crash shape: partition emptied, progress row survives
        pg_conn.exec_driver_sql(
            "INSERT INTO public.ig_shred_progress (schema_name, table_name, chunk_lo,"
            " chunk_hi, n_values, seconds, postmaster_start)"
            " VALUES (%(s)s, %(t)s, 0, 4000, 820125, 52.0, pg_postmaster_start_time())",
            {"s": schema, "t": table})
        pg_conn.commit()

        assert A.reconcile_progress(pg_conn, schema, [table]) == [f"{schema}.{table}"]
        left = pg_conn.exec_driver_sql(
            "SELECT count(*) FROM public.ig_shred_progress WHERE schema_name = %(s)s",
            {"s": schema}).all()[0][0]
        assert left == 0, "stale progress must not survive, or the resume skips the cell"

        # a cell whose rows are actually there is left alone
        pg_conn.exec_driver_sql(
            "INSERT INTO public.pev_practitioner_role_reconcile_probe "
            "SELECT 'r' || g, 0 FROM generate_series(1, 5) g")
        pg_conn.exec_driver_sql(
            "INSERT INTO public.ig_shred_progress (schema_name, table_name, chunk_lo,"
            " chunk_hi, n_values, seconds, postmaster_start)"
            " VALUES (%(s)s, %(t)s, 0, 4000, 5, 1.0, pg_postmaster_start_time())",
            {"s": schema, "t": table})
        pg_conn.commit()
        assert A.reconcile_progress(pg_conn, schema, [table]) == []

        # a row written before the current postmaster start cannot be trusted,
        # however many rows the partition happens to hold
        pg_conn.exec_driver_sql(
            "UPDATE public.ig_shred_progress SET postmaster_start = "
            "pg_postmaster_start_time() - interval '1 day' WHERE schema_name = %(s)s",
            {"s": schema})
        pg_conn.commit()
        assert A.reconcile_progress(pg_conn, schema, [table]) == [f"{schema}.{table}"]

        # and a cell with no progress at all is not spuriously reported
        assert A.reconcile_progress(pg_conn, schema, [table]) == []

        # a cell shredded BEFORE parent_ordinal existed is not a resumable
        # cell: its `ordinal` counts occurrences flat across the resource, so
        # measuring it with the per-parent queries fabricates breaches. Row
        # counts cannot tell the two shapes apart, so this must stop.
        pg_conn.exec_driver_sql(
            "INSERT INTO public.pev_practitioner_role_reconcile_probe "
            "SELECT 'r' || g, CASE WHEN g = 1 THEN NULL ELSE 0 END "
            "FROM generate_series(1, 5) g")
        pg_conn.exec_driver_sql(
            "INSERT INTO public.ig_shred_progress (schema_name, table_name, chunk_lo,"
            " chunk_hi, n_values, seconds, postmaster_start)"
            " VALUES (%(s)s, %(t)s, 0, 4000, 5, 1.0, pg_postmaster_start_time())",
            {"s": schema, "t": table})
        pg_conn.commit()
        with pytest.raises(SystemExit) as exc:
            A.reconcile_progress(pg_conn, schema, [table])
        assert "--reset-shred" in str(exc.value)
    finally:
        pg_conn.rollback()
        pg_conn.exec_driver_sql(
            "DELETE FROM public.ig_shred_progress WHERE schema_name = %(s)s",
            {"s": schema})
        pg_conn.exec_driver_sql(
            "DROP TABLE IF EXISTS public.pev_practitioner_role_reconcile_probe")
        pg_conn.commit()


@pytest.mark.skipif(os.environ.get("PROVDIR_SKIP_LIVE_AUDIT") == "1",
                    reason="live-data regression disabled")
def test_regression_fromnetwork_share_through_the_hierarchical_shred(compiled, pg_conn):
    """The same invariant, but read through the generated shred SELECT rather
    than a flat jsonb_path_query_array -- so it pins the rewrite, not just the
    path compiler. Also asserts the per-parent max=1 test finds nothing, where
    the flat one found 28,992 of 29,350.
    """
    A, checks, _, _ = compiled
    c = by_path(checks, "PlannetPractitionerRole", FROM_NETWORK)[0]
    have = pg_conn.exec_driver_sql(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'bcbs_az_new' AND table_name = 'practitioner_role'"
    ).all()[0][0]
    if not have:
        pytest.skip("bcbs_az_new not present in this database")
    pg_conn.exec_driver_sql("SET statement_timeout = 0")
    select = A.build_shred_select("bcbs_az_new", "practitioner_role",
                                  "PractitionerRole", [c])
    n_present, n_max_viol = pg_conn.exec_driver_sql(
        f"SELECT count(DISTINCT resource_id), "
        f"count(*) FILTER (WHERE coding_ordinal = 1 AND ordinal = {c.max_n + 1}) "
        f"FROM ({select}) q").all()[0]
    n_rows = pg_conn.exec_driver_sql(
        "SELECT count(*) FROM bcbs_az_new.practitioner_role").all()[0][0]
    assert (n_present, n_rows) == (29350, 96027)
    assert n_max_viol == 0
    pg_conn.rollback()



# --------------------------------------------------------------------------
# execution tests over the PRODUCTION audit query
#
# Everything below runs the REAL presence_sql() / measure_presence() /
# audit_rows() against fixture data, rather than hand-rolling a parallel SQL
# statement and comparing the code to a restatement of itself. That difference
# is the point: the two tests that previously "proved" n_present and
# min_violation (test_n_present_must_be_distinct_resources,
# test_min_is_conditioned_on_parent_presence) both write their own SELECT and
# assert on its output, so they pass with presence_sql() and min_violation()
# reverted to the shapes they were written to kill. These do not.
#
# They also cover the two resource types the execution-level fixtures never
# touched. Every prior end-to-end fixture was practitioner_role, so
# Organization -- the majority of the corpus, and the only table where two
# profiles are split by an `in_cell` predicate rather than by the table they
# live in -- and Location had no mechanical validation of the hierarchical
# shred at all. `test_organization_and_network_split_the_same_table` regexed
# the generated SQL string; it never ran it.
# --------------------------------------------------------------------------

QUAL_URL = ("http://hl7.org/fhir/us/davinci-pdex-plan-net/"
            "StructureDefinition/qualification")
GEO_URL = "http://hl7.org/fhir/StructureDefinition/geolocation"
CREATED_AT = "ex:createdAt"
ORG_TYPE_CS = ("http://hl7.org/fhir/us/davinci-pdex-plan-net/"
               "CodeSystem/OrgTypeCS")
PHYS_TYPE_CS = "http://terminology.hl7.org/CodeSystem/location-physical-type"

# A Network: same table as Organization, separated only by type.coding.code.
# `alias` twice is an element present twice in ONE resource; `meta.extension`
# is an extension URL at a depth the old key-space census never looked at.
ORG_NETWORK = {
    "id": "org-net",
    "meta": {"source": "urn:source_name:azblue",
             "extension": [{"url": CREATED_AT,
                            "valueInstant": "2026-07-17T18:54:04.553520Z"}]},
    "active": True,
    "name": "AZ Blue Network",
    "alias": ["ABN", "AZ Blue Net"],
    "type": [{"coding": [{"code": "ntwk", "system": ORG_TYPE_CS}]}],
    "partOf": {"reference": "Organization/org-plain"},
}

# Not a Network. Carries the two shapes a flat measurement cannot express:
#   * `type` with TWO codings -- one occurrence, two coding_ordinal rows
#   * `qualification` twice, and only the SECOND carries the min=1 `code`
#     sub-extension, so `code` exists on the resource while parent occurrence 1
#     owes one and has not paid it
# plus a geolocation extension nested under address, two levels below the
# deepest path the old extension census reached.
ORG_PLAIN = {
    "id": "org-plain",
    "meta": {"source": "urn:source_name:azblue",
             "extension": [{"url": CREATED_AT,
                            "valueInstant": "2026-07-17T18:54:04.553520Z"}]},
    "active": True,
    "name": "Desert Care Group",
    "type": [{"coding": [{"code": "prov", "system": ORG_TYPE_CS},
                         {"code": "dept", "system": ORG_TYPE_CS}]}],
    "address": [{"city": "Phoenix", "state": "AZ",
                 "extension": [{"url": GEO_URL, "extension": [
                     {"url": "latitude", "valueDecimal": 33.4},
                     {"url": "longitude", "valueDecimal": -112.0}]}]}],
    "extension": [
        {"url": QUAL_URL,
         "extension": [{"url": "status", "valueCode": "active"}]},
        {"url": QUAL_URL,
         "extension": [{"url": "status", "valueCode": "active"},
                       {"url": "code", "valueCodeableConcept": {
                           "coding": [{"code": "MD",
                                       "system": "http://nucc.org/provider-taxonomy"}]}}]},
    ],
}

# The second LOB, and the only row of it: this makes PlannetNetwork x hcaz a
# genuine zero-row cell -- one Network exists, it does not carry hcaz -- which
# is the exact shape the report must render as `not measured` rather than 0.0%.
ORG_HCAZ = {
    "id": "org-hcaz",
    "meta": {"source": "urn:source_name:hcaz"},
    "active": True,
    "name": "Health Choice Clinic",
    "type": [{"coding": [{"code": "prov", "system": ORG_TYPE_CS}]}],
}

# Location, which no execution fixture had ever covered.
#   * telecom twice           -- one resource, two occurrences
#   * physicalType 2 codings  -- one occurrence, two coding rows
#   * hoursOfOperation twice, and only the SECOND carries openingTime
#   * position present, so its min=1 children are owed and paid
LOC_AZ = {
    "id": "loc-az",
    "meta": {"source": "urn:source_name:azblue",
             "extension": [{"url": CREATED_AT,
                            "valueInstant": "2026-07-17T18:54:04.553520Z"}]},
    "status": "active",
    "name": "Camelback Clinic",
    "position": {"latitude": 33.5, "longitude": -112.1},
    "physicalType": {"coding": [{"code": "si", "system": PHYS_TYPE_CS},
                                {"code": "bu", "system": PHYS_TYPE_CS}]},
    "telecom": [{"system": "phone", "value": "602-555-0100"},
                {"system": "fax", "value": "602-555-0101"}],
    "hoursOfOperation": [{"daysOfWeek": ["mon"]},
                         {"daysOfWeek": ["tue"], "openingTime": "09:00:00"}],
    "address": {"city": "Phoenix",
                "extension": [{"url": GEO_URL, "extension": [
                    {"url": "latitude", "valueDecimal": 33.5}]}]},
}

# No position at all: its min=1 children owe nothing, because their parent
# never occurs. Same claim as pr-3, on a different resource type and through a
# real backbone element rather than a nested extension.
LOC_HCAZ = {
    "id": "loc-hcaz",
    "meta": {"source": "urn:source_name:hcaz"},
    "status": "active",
    "name": "Mesa Annex",
}


def _fixture_audit(conn, A, checks, table, rtype, fixtures):
    """Shred `fixtures` with the generated SELECT, measure the result with the
    REAL presence_sql(), and assemble the REAL ig_audit rows over it.

    Fixture shred and fixture catalogue are TEMP tables, so nothing here writes
    to public.plannet_element_value or takes a lock a running shred would wait
    on -- but they are read by the production query, not by a copy of it.
    """
    import json

    tchecks = [c for c in checks if c.table == table]
    conn.exec_driver_sql("SET statement_timeout = 0")
    conn.exec_driver_sql(A.SHRED_DDL)
    for t in (table, "ig_fix_pev", "ig_fix_elem"):
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS pg_temp.{t}")
    conn.exec_driver_sql(f"CREATE TEMP TABLE {table} (id text, resource jsonb)")
    for f in fixtures:
        conn.exec_driver_sql(
            f"INSERT INTO {table} (id, resource) VALUES (%(i)s, %(r)s)",
            {"i": f["id"], "r": json.dumps(f)})
    conn.exec_driver_sql(
        f"CREATE TEMP TABLE ig_fix_elem (LIKE {A.CATALOGUE_TABLE} INCLUDING ALL)")
    conn.exec_driver_sql(f"CREATE TEMP TABLE ig_fix_pev (LIKE {A.SHRED_TABLE})")
    A.write_catalogue(conn, checks, str(PKG), table="pg_temp.ig_fix_elem")
    select = A.build_shred_select("pg_temp", table, rtype, tchecks)
    conn.exec_driver_sql(
        f"INSERT INTO pg_temp.ig_fix_pev ({A.SHRED_COLUMNS}) {select}")

    rows = A.measure_presence(conn, "pg_temp", rtype, shred="pg_temp.ig_fix_pev",
                              catalogue="pg_temp.ig_fix_elem")
    measured = {(r[A.M_ELEM], r[A.M_LOB] or "(unknown)"): r for r in rows}
    denom = A.denominators(conn, "pg_temp", table)
    lobs = sorted({k[1] for k in denom}) or ["(unknown)"]
    audit = A.audit_rows(1, "pg_temp", rtype, tchecks, lobs, denom, measured)
    return measured, denom, {(r["elem_idx"], r["lob"]): r for r in audit}


def _raw(conn, elem_idx):
    """Every fixture shred row for one element, ordered as stored."""
    return [tuple(r) for r in conn.exec_driver_sql(
        "SELECT resource_id, lob, parent_ordinal, ordinal, coding_ordinal, "
        "value_text, value_num, code_code FROM pg_temp.ig_fix_pev "
        "WHERE elem_idx = %(i)s ORDER BY 1, 3, 4, 5", {"i": elem_idx}).all()]


def test_organization_network_cell_split_executes(compiled, pg_conn):
    """(a) The Network / non-Network split, RUN rather than regexed.

    Both profiles compile against `organization`, and the only thing keeping
    their measurements apart is the per-element `in_cell` predicate inside the
    generated SQL. Nothing had ever executed that: the existing test asserts
    `org.cell == 'not_network'` on the compiled object and greps two substrings
    out of the SQL string. A predicate that is present in the text and wrong in
    effect -- inverted, or applied to the wrong branch -- passes that and fails
    this.
    """
    A, checks, _, _ = compiled
    _measured, denom, audit = _fixture_audit(
        pg_conn, A, checks, "organization", "Organization",
        [ORG_NETWORK, ORG_PLAIN, ORG_HCAZ])

    # the denominator the split produces, per cell and per lob
    assert denom[("network", "azblue")] == 1
    assert denom[("not_network", "azblue")] == 1
    assert denom[("network", "hcaz")] == 0
    assert denom[("not_network", "hcaz")] == 1
    assert denom[(None, "azblue")] == 2

    net_name = by_path(checks, "PlannetNetwork", "Organization.name")[0]
    org_name = by_path(checks, "PlannetOrganization", "Organization.name")[0]

    # each profile sees exactly its own row, and only in the lob that row has
    assert [(r[0], r[5]) for r in _raw(pg_conn, net_name.idx)] == [
        ("org-net", "AZ Blue Network")]
    assert [r[0] for r in _raw(pg_conn, org_name.idx)] == ["org-hcaz", "org-plain"]

    # and that reaches ig_audit as a per-cell row count, not the table's
    assert audit[(net_name.idx, "azblue")]["n_rows"] == 1
    assert audit[(net_name.idx, "azblue")]["n_present"] == 1
    assert audit[(org_name.idx, "azblue")]["n_rows"] == 1
    assert audit[(org_name.idx, "azblue")]["n_present"] == 1

    # PlannetNetwork x hcaz: a real profile, a real lob, and no rows at all.
    # n_present 0 out of n_rows 0 is not 0% -- it is unmeasured, and the report
    # renders it as such.
    z = audit[(net_name.idx, "hcaz")]
    assert (z["n_rows"], z["n_present"], z["n_parent_instances"]) == (0, 0, 0)
    assert net_name.min == 1 and z["n_min_violation"] == 0
    pg_conn.rollback()


def test_organization_repeats_and_coding_fanout_execute(compiled, pg_conn):
    """(c) an element present twice in one resource, and (e) a coded element
    fanning to several coding_ordinal rows.

    Both are places where "occurrences" and "rows" come apart, and
    presence_sql() is the only thing that decides which of the two every
    downstream figure gets. n_occurrences filters coding_ordinal = 1 precisely
    so a two-coding concept stays ONE occurrence; drop that filter and every
    binding denominator in the report doubles for the concepts carrying two
    codes.
    """
    A, checks, _, _ = compiled
    measured, _denom, _audit = _fixture_audit(
        pg_conn, A, checks, "organization", "Organization",
        [ORG_NETWORK, ORG_PLAIN, ORG_HCAZ])

    alias = by_path(checks, "PlannetNetwork", "Organization.alias")[0]
    m = measured[(alias.idx, "azblue")]
    assert (int(m[A.M_PRESENT]), int(m[A.M_OCC])) == (1, 2)
    assert [(r[3], r[5]) for r in _raw(pg_conn, alias.idx)] == [
        (1, "ABN"), (2, "AZ Blue Net")]

    otype = by_path(checks, "PlannetOrganization", "Organization.type")[0]
    rows = [r for r in _raw(pg_conn, otype.idx) if r[0] == "org-plain"]
    assert [(r[3], r[4], r[7]) for r in rows] == [(1, 1, "prov"), (1, 2, "dept")]
    m = measured[(otype.idx, "azblue")]
    # two shred rows, ONE occurrence, one resource
    assert (int(m[A.M_PRESENT]), int(m[A.M_OCC])) == (1, 1)
    pg_conn.rollback()


def test_organization_min_is_owed_per_parent_occurrence(compiled, pg_conn):
    """(d) a parent occurring twice where only the SECOND has the child.

    `qualification` occurs twice on org-plain; its min=1 `code` sub-extension
    is on occurrence 2 only. The resource therefore PUBLISHES `code` -- so
    `n_rows - n_present`, the rule 0b replaced, scores it clean -- while one of
    the two parent instances is genuinely short. Only a per-parent denominator
    can tell those apart, and this asserts on the number audit_rows() actually
    stores rather than on a recomputation of it.
    """
    A, checks, _, _ = compiled
    _measured, _denom, audit = _fixture_audit(
        pg_conn, A, checks, "organization", "Organization",
        [ORG_NETWORK, ORG_PLAIN, ORG_HCAZ])

    qual = by_path(checks, "PlannetOrganization",
                   "Organization.extension:qualification")[0]
    code = by_path(checks, "PlannetOrganization",
                   "Organization.extension:qualification.extension:code")[0]
    assert code.min == 1 and code.parent_idx == qual.idx
    assert code.card_method == "per-parent"

    # the parent occurs twice; the child sits under occurrence 2 only
    assert [(r[2], r[3]) for r in _raw(pg_conn, qual.idx)] == [(0, 1), (0, 2)]
    assert [(r[2], r[3]) for r in _raw(pg_conn, code.idx)] == [(2, 1)]

    a = audit[(code.idx, "azblue")]
    assert a["n_parent_instances"] == 2      # both qualification occurrences
    assert a["n_present"] == 1               # the resource does publish it
    assert a["n_occurrences"] == 1
    assert a["n_min_violation"] == 1         # occurrence 1 owes one, has none

    # the pre-0b rule, on the same data: the resource has the element, so it
    # scores clean and the real shortfall disappears
    assert a["n_rows"] - a["n_present"] == 0

    # the parent itself is min=0 and stays unmeasured rather than clean
    assert audit[(qual.idx, "azblue")]["n_min_violation"] is None
    pg_conn.rollback()


def test_location_shred_and_measurement_execute(compiled, pg_conn):
    """(b) Location, plus (c)/(d)/(e) again on a resource type whose repeating
    parents are backbone elements rather than nested extensions.

    Location had no execution coverage of any kind. `hoursOfOperation` is the
    parent-restart shape on a plain FHIR backbone: two occurrences, the second
    carrying `openingTime`. Both cheap stand-ins for count(DISTINCT
    resource_id) are wrong on it, in opposite directions, on one resource.
    """
    A, checks, _, _ = compiled
    measured, denom, audit = _fixture_audit(
        pg_conn, A, checks, "location", "Location", [LOC_AZ, LOC_HCAZ])
    assert denom[(None, "azblue")] == 1 and denom[(None, "hcaz")] == 1

    tel = by_path(checks, "PlannetLocation", "Location.telecom")[0]
    m = measured[(tel.idx, "azblue")]
    assert (int(m[A.M_PRESENT]), int(m[A.M_OCC])) == (1, 2)

    phys = by_path(checks, "PlannetLocation", "Location.physicalType")[0]
    assert [r[7] for r in _raw(pg_conn, phys.idx)] == ["si", "bu"]
    assert int(measured[(phys.idx, "azblue")][A.M_OCC]) == 1

    hoo = by_path(checks, "PlannetLocation", "Location.hoursOfOperation")[0]
    days = by_path(checks, "PlannetLocation",
                   "Location.hoursOfOperation.daysOfWeek")[0]
    opens = by_path(checks, "PlannetLocation",
                    "Location.hoursOfOperation.openingTime")[0]
    assert [(r[2], r[3]) for r in _raw(pg_conn, hoo.idx)] == [(0, 1), (0, 2)]
    # one child under each parent occurrence: ordinal restarts, so `ordinal = 1`
    # counts TWO and calls them resources
    assert [(r[2], r[3]) for r in _raw(pg_conn, days.idx)] == [(1, 1), (2, 1)]
    # and the other child exists only under parent 2, so `parent_ordinal = 1`
    # counts ZERO and drops the resource entirely
    assert [(r[2], r[3]) for r in _raw(pg_conn, opens.idx)] == [(2, 1)]
    assert int(measured[(days.idx, "azblue")][A.M_PRESENT]) == 1
    assert int(measured[(opens.idx, "azblue")][A.M_PRESENT]) == 1

    wrong = pg_conn.exec_driver_sql(
        "SELECT count(*) FILTER (WHERE elem_idx = %(d)s AND ordinal = 1 "
        "                        AND coding_ordinal = 1), "
        "       count(*) FILTER (WHERE elem_idx = %(o)s AND ordinal = 1 "
        "                        AND parent_ordinal = 1 AND coding_ordinal = 1) "
        "FROM pg_temp.ig_fix_pev", {"d": days.idx, "o": opens.idx}).all()[0]
    assert tuple(wrong) == (2, 0)

    # a min=1 element under an optional parent that is ABSENT owes nothing --
    # the same claim as pr-3, through a backbone element on another table
    lat = by_path(checks, "PlannetLocation", "Location.position.latitude")[0]
    pos = by_path(checks, "PlannetLocation", "Location.position")[0]
    assert lat.min == 1 and pos.min == 0 and lat.parent_idx == pos.idx
    assert audit[(lat.idx, "azblue")]["n_parent_instances"] == 1
    assert audit[(lat.idx, "azblue")]["n_min_violation"] == 0
    assert audit[(lat.idx, "hcaz")]["n_parent_instances"] == 0
    assert audit[(lat.idx, "hcaz")]["n_min_violation"] == 0

    # a root-parented min=1 element is scored against the row count
    status = by_path(checks, "PlannetLocation", "Location.status")[0]
    assert status.card_method == "root"
    assert audit[(status.idx, "hcaz")]["n_parent_instances"] == 1
    assert audit[(status.idx, "hcaz")]["n_min_violation"] == 0
    pg_conn.rollback()


def test_extension_url_census_walks_to_any_depth(compiled, pg_conn):
    """The key-space census must find extension URLs wherever they are.

    It used to query exactly `$.extension[*].url` and
    `$.extension[*].extension[*].url`. Measured against a recursive walk on
    bcbs_az_new (first 3,000 ctid blocks per table) those two paths saw 19 of
    the 32 URLs present, and missed `ex:createdAt` -- a vendor extension in
    every one of the eight tables, and precisely the thing this function exists
    to find. This fixture reproduces the missed shapes.
    """
    A, checks, _, _ = compiled
    _fixture_audit(pg_conn, A, checks, "organization", "Organization",
                   [ORG_NETWORK, ORG_PLAIN, ORG_HCAZ])
    org = [c for c in checks if c.table == "organization"]
    keys, urls = A.key_space_inventory(pg_conn, "pg_temp", "organization", org)
    found = {u: (n, declared) for u, n, declared in urls}

    assert "meta" in dict(keys) and "extension" in dict(keys)
    # meta.extension -- a depth no fixed pair of paths reached
    assert found[CREATED_AT][0] == 2
    # address[*].extension, and its own children one level deeper again
    assert found[GEO_URL][0] == 1
    assert found["latitude"][0] == 1 and found["longitude"][0] == 1
    # the shapes the old census DID reach are still there
    assert found[QUAL_URL][0] == 2
    assert found["code"][0] == 1 and found["status"][0] == 2

    old = {r[0] for r in A._fetch(pg_conn, """
        SELECT u #>> '{}' FROM pg_temp.organization t,
             LATERAL jsonb_path_query(t.resource, '$.extension[*].url') u
        UNION
        SELECT u #>> '{}' FROM pg_temp.organization t,
             LATERAL jsonb_path_query(t.resource,
                 '$.extension[*].extension[*].url') u""")}
    assert old < set(found)
    assert not (old & {CREATED_AT, GEO_URL, "latitude", "longitude"})
    pg_conn.rollback()


def test_extension_url_census_scores_declared_against_this_table_only(
        compiled, pg_conn):
    """`declared` is the checks for the table under census, and nothing else.

    `geolocation` is declared on PlannetOrganization's address and on no
    Location element. Scored against the whole 737-check surface it would come
    back `declared` on Location too, and the undeclared count -- the one number
    this function produces -- would be biased low by every URL any other
    profile happens to declare.
    """
    A, checks, _, _ = compiled
    org = [c for c in checks if c.table == "organization"]
    loc = [c for c in checks if c.table == "location"]

    _fixture_audit(pg_conn, A, checks, "organization", "Organization", [ORG_PLAIN])
    _keys, urls = A.key_space_inventory(pg_conn, "pg_temp", "organization", org)
    assert {u: d for u, _n, d in urls}[GEO_URL] is True

    _fixture_audit(pg_conn, A, checks, "location", "Location", [LOC_AZ])
    _keys, urls = A.key_space_inventory(pg_conn, "pg_temp", "location", loc)
    declared = {u: d for u, _n, d in urls}
    assert GEO_URL in declared and declared[GEO_URL] is False
    assert declared[CREATED_AT] is False

    # and it refuses a mixed surface rather than quietly over-declaring
    with pytest.raises(ValueError) as exc:
        A.key_space_inventory(pg_conn, "pg_temp", "location", org + loc)
    assert "one table at a time" in str(exc.value)
    pg_conn.rollback()


def test_occurrence_verification_is_per_lob(compiled, pg_conn):
    """The recount compares (elem_idx, lob) cells, not per-element totals.

    n_parent_instances is used PER LOB in audit_rows(), so a miscount that
    moves occurrences from one LOB to another leaves every per-element total
    untouched -- and an elem_idx-only comparison, which is what this was,
    reports no disagreement while both LOBs' denominators are wrong.
    """
    A, checks, _, _ = compiled
    _fixture_audit(pg_conn, A, checks, "organization", "Organization",
                   [ORG_NETWORK, ORG_PLAIN, ORG_HCAZ])
    org = [c for c in checks if c.table == "organization"]
    src = A.source_occurrences(pg_conn, "pg_temp", "organization", org)
    sel = A.select_occurrences(pg_conn, "pg_temp", "organization",
                               "Organization", org)
    assert A.compare_occurrences(src, sel) == []
    assert {lob for _idx, lob in src} == {"azblue", "hcaz"}

    name = by_path(checks, "PlannetOrganization", "Organization.name")[0]
    assert src[(name.idx, "azblue")] == 1 and src[(name.idx, "hcaz")] == 1
    moved = dict(sel)
    moved[(name.idx, "azblue")] += 1
    moved[(name.idx, "hcaz")] -= 1
    assert sum(moved.values()) == sum(src.values())          # totals agree
    assert A.compare_occurrences(src, moved) == [            # cells do not
        ((name.idx, "azblue"), 1, 2), ((name.idx, "hcaz"), 1, 0)]
    pg_conn.rollback()


# --------------------------------------------------------------------------
# the report's guards
# --------------------------------------------------------------------------

def test_empty_audit_run_table_names_the_fix():
    """An empty public.ig_audit_run used to surface as a bare IndexError from
    `[0]` on a `WHERE run_id = NULL` lookup, several frames from anything an
    operator could act on."""
    import ig_audit_report as R

    with pytest.raises(SystemExit) as e:
        R.require_run(None, [])
    assert "audit_plannet.py" in str(e.value)
    assert "empty" in str(e.value)

    with pytest.raises(SystemExit) as e:
        R.require_run(4242, [])
    assert "4242" in str(e.value)
    assert "ig_audit_run" in str(e.value)

    row = ("r",)
    assert R.require_run(7, [row]) is row


def test_report_reports_unmeasured_as_unmeasured():
    import ig_audit_report as R

    assert R.num(None) == "unmeasured"
    assert R.num(0) == "0"
    assert R.num(1234567) == "1,234,567"


def test_no_denominator_never_renders_as_a_percentage():
    """A cell with no rows must not be typeset like a cell that scored zero.

    `PlannetNetwork x hcaz` rendered `0 (0.0%)` in the completeness table two
    sections above the paragraph saying a profile with no rows "has not been
    measured at 0%". Same glyphs as a genuine 0.0%, opposite meaning.
    """
    import ig_audit_report as R

    assert R.pct(0, 0) == "not measured"
    assert R.pct(5, 0) == "not measured"
    assert R.pct(0, 76) == "0.0%"


def test_zero_row_cells_are_told_apart():
    """Three different findings used to render as one unlabelled zero: a
    resource type we never captured, a profile the payer publishes none of,
    and a profile whose rows all carry a different LOB. Only the first two say
    anything about the payer."""
    import ig_audit_report as R

    assert "nothing captured" in R.zero_row_reason(0, 0, table_absent=True)
    assert "present and empty" in R.zero_row_reason(0, 0, table_absent=False)
    assert R.zero_row_reason(1_862_183, 0, False) == (
        "table has rows, but none match this profile")
    assert R.zero_row_reason(1_862_183, 36, False) == (
        "profile has rows, but none carry this lob")

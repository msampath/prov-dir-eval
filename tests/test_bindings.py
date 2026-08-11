"""Unit tests for the offline value-set expansion engine (no DB, no network).

Fixture value sets/code systems below mirror the REAL shapes found in the
Plan-Net/US-Core/R4-core/THO packages (verified against the actual downloaded
packages during implementation): inline concept lists, whole-CodeSystem
includes, recursive valueSet references, exclude subtraction, and THO's flat
v3 code systems that express hierarchy via a `subsumedBy` concept property
rather than nested `.concept` children.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.quality import bindings  # noqa: E402
from provdir.quality.bindings import (  # noqa: E402
    CHECKS,
    binding_for,
    expand,
    is_array_column,
    resolve_binding,
)


def _vs(url: str, compose: dict) -> dict:
    return {"resourceType": "ValueSet", "url": url, "compose": compose}


def _cs(url: str, content: str, concept: list[dict]) -> dict:
    return {"resourceType": "CodeSystem", "url": url, "content": content, "concept": concept}


class TestExpandConceptList:
    def test_inline_concepts(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "sys:x", "concept": [{"code": "1"}, {"code": "2"}]},
            ]}),
        }
        assert expand(index, "vs:a") == {"1", "2"}

    def test_version_suffix_is_stripped(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "sys:x", "concept": [{"code": "1"}]},
            ]}),
        }
        assert expand(index, "vs:a|3.2.1") == {"1"}


class TestExpandWholeCodeSystem:
    def test_complete_codesystem_is_fully_enumerated(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [{"system": "cs:x"}]}),
            "cs:x": _cs("cs:x", "complete", [{"code": "a"}, {"code": "b"}, {"code": "c"}]),
        }
        assert expand(index, "vs:a") == {"a", "b", "c"}

    def test_nested_concept_tree_is_walked(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [{"system": "cs:x"}]}),
            "cs:x": _cs("cs:x", "complete", [
                {"code": "parent", "concept": [
                    {"code": "child1"},
                    {"code": "child2", "concept": [{"code": "grandchild"}]},
                ]},
            ]),
        }
        assert expand(index, "vs:a") == {"parent", "child1", "child2", "grandchild"}

    def test_non_complete_codesystem_is_unexpandable(self):
        """content='fragment'/'not-present' cannot be trusted to enumerate
        every member -- returning a partial set would silently under-flag
        nonmembers, so this must return None (status='unexpanded')."""
        index = {
            "vs:a": _vs("vs:a", {"include": [{"system": "cs:x"}]}),
            "cs:x": _cs("cs:x", "fragment", [{"code": "a"}]),
        }
        assert expand(index, "vs:a") is None

    def test_missing_codesystem_is_unexpandable(self):
        index = {"vs:a": _vs("vs:a", {"include": [{"system": "cs:missing"}]})}
        assert expand(index, "vs:a") is None


class TestExpandValueSetReferences:
    def test_union_across_include_entries(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"valueSet": ["vs:b"]}, {"valueSet": ["vs:c"]},
            ]}),
            "vs:b": _vs("vs:b", {"include": [{"system": "s", "concept": [{"code": "1"}]}]}),
            "vs:c": _vs("vs:c", {"include": [{"system": "s", "concept": [{"code": "2"}]}]}),
        }
        assert expand(index, "vs:a") == {"1", "2"}

    def test_intersection_within_one_include_entry(self):
        """A single include naming >=1 valueSets contributes their
        INTERSECTION, per FHIR compose.include semantics."""
        index = {
            "vs:a": _vs("vs:a", {"include": [{"valueSet": ["vs:b", "vs:c"]}]}),
            "vs:b": _vs("vs:b", {"include": [
                {"system": "s", "concept": [{"code": "1"}, {"code": "2"}]},
            ]}),
            "vs:c": _vs("vs:c", {"include": [
                {"system": "s", "concept": [{"code": "2"}, {"code": "3"}]},
            ]}),
        }
        assert expand(index, "vs:a") == {"2"}

    def test_unresolvable_nested_valueset_propagates_none(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [{"valueSet": ["vs:missing"]}]}),
        }
        assert expand(index, "vs:a") is None

    def test_self_referential_cycle_does_not_hang(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"valueSet": ["vs:a"]},
                {"system": "s", "concept": [{"code": "1"}]},
            ]}),
        }
        assert expand(index, "vs:a") == {"1"}


class TestExpandExclude:
    def test_exclude_subtracts_from_include(self):
        index = {
            "vs:a": _vs("vs:a", {
                "include": [{"system": "cs:x"}],
                "exclude": [{"system": "cs:x", "concept": [{"code": "b"}]}],
            }),
            "cs:x": _cs("cs:x", "complete", [{"code": "a"}, {"code": "b"}, {"code": "c"}]),
        }
        assert expand(index, "vs:a") == {"a", "c"}


class TestExpandFilters:
    """Mirrors THO's v3-RoleCode encoding: a FLAT concept list (no nested
    `.concept`) where hierarchy is expressed via a `subsumedBy` property
    whose value is the parent's code -- this is what
    v3-ServiceDeliveryLocationRoleType (Location.type) actually uses."""

    def _flat_cs(self):
        return _cs("cs:v3", "complete", [
            {"code": "root"},
            {"code": "mid", "property": [{"code": "subsumedBy", "valueCode": "root"}]},
            {"code": "leaf1", "property": [{"code": "subsumedBy", "valueCode": "mid"}]},
            {"code": "leaf2", "property": [{"code": "subsumedBy", "valueCode": "mid"}]},
            {"code": "unrelated"},
        ])

    def test_descendent_of_via_subsumed_by_property(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "cs:v3", "filter": [
                    {"property": "concept", "op": "descendent-of", "value": "root"},
                ]},
            ]}),
            "cs:v3": self._flat_cs(),
        }
        assert expand(index, "vs:a") == {"mid", "leaf1", "leaf2"}

    def test_is_a_includes_the_anchor_itself(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "cs:v3", "filter": [
                    {"property": "concept", "op": "is-a", "value": "mid"},
                ]},
            ]}),
            "cs:v3": self._flat_cs(),
        }
        assert expand(index, "vs:a") == {"mid", "leaf1", "leaf2"}

    def test_hierarchy_via_nested_concept_also_works(self):
        """Genuinely hierarchical code systems nest `.concept` directly
        instead of (or in addition to) using `subsumedBy` -- both encodings
        must resolve the same descendant set."""
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "cs:x", "filter": [
                    {"property": "concept", "op": "descendent-of", "value": "root"},
                ]},
            ]}),
            "cs:x": _cs("cs:x", "complete", [
                {"code": "root", "concept": [
                    {"code": "child1"}, {"code": "child2", "concept": [{"code": "gc"}]},
                ]},
            ]),
        }
        assert expand(index, "vs:a") == {"child1", "child2", "gc"}

    def test_unsupported_filter_op_is_unexpandable(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "cs:v3", "filter": [
                    {"property": "concept", "op": "exists", "value": "true"},
                ]},
            ]}),
            "cs:v3": self._flat_cs(),
        }
        assert expand(index, "vs:a") is None

    def test_filter_over_non_complete_codesystem_is_unexpandable(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "cs:v3", "filter": [
                    {"property": "concept", "op": "descendent-of", "value": "root"},
                ]},
            ]}),
            "cs:v3": _cs("cs:v3", "fragment", [{"code": "root"}]),
        }
        assert expand(index, "vs:a") is None

    def test_filter_anchor_not_in_codesystem_is_unexpandable(self):
        index = {
            "vs:a": _vs("vs:a", {"include": [
                {"system": "cs:v3", "filter": [
                    {"property": "concept", "op": "descendent-of", "value": "nope"},
                ]},
            ]}),
            "cs:v3": self._flat_cs(),
        }
        assert expand(index, "vs:a") is None


class TestExpandTopLevel:
    def test_unknown_valueset_url_is_unexpandable(self):
        assert expand({}, "vs:missing") is None

    def test_non_valueset_resource_at_url_is_unexpandable(self):
        index = {"vs:a": {"resourceType": "CodeSystem", "url": "vs:a"}}
        assert expand(index, "vs:a") is None

    def test_no_compose_is_unexpandable(self):
        index = {"vs:a": {"resourceType": "ValueSet", "url": "vs:a"}}
        assert expand(index, "vs:a") is None


class TestBindingFor:
    def test_finds_strength_and_valueset(self):
        sd = {"snapshot": {"element": [
            {"path": "Foo.bar", "binding": {"strength": "required", "valueSet": "vs:x|1.0"}},
        ]}}
        assert binding_for(sd, "Foo.bar") == ("vs:x", "required")

    def test_no_matching_path_returns_none(self):
        sd = {"snapshot": {"element": [{"path": "Foo.baz"}]}}
        assert binding_for(sd, "Foo.bar") is None

    def test_path_present_without_binding_returns_none(self):
        sd = {"snapshot": {"element": [{"path": "Foo.bar"}]}}
        assert binding_for(sd, "Foo.bar") is None

    def test_skips_unbound_slice_and_finds_the_bound_one(self):
        """A differential slice can repeat a .path without repeating the
        binding; the first (unbound) match must not shadow a later bound
        one at the same path."""
        sd = {"snapshot": {"element": [
            {"path": "Foo.bar", "id": "Foo.bar:sliceA"},
            {"path": "Foo.bar", "id": "Foo.bar:sliceB",
             "binding": {"strength": "extensible", "valueSet": "vs:y"}},
        ]}}
        assert binding_for(sd, "Foo.bar") == ("vs:y", "extensible")


class TestChecksSafety:
    def test_fifteen_checks(self):
        assert len(CHECKS) == 15

    def test_every_check_column_exists_on_its_table(self):
        # Import-time assertion in bindings.py already enforces this; this
        # test just pins the invariant so a future edit that breaks it fails
        # a fast unit test, not a mid-sweep KeyError hours in.
        for chk in CHECKS:
            assert chk.column in chk.table.c

    def test_only_endpoint_connection_type_is_scalar(self):
        scalar = [(c.table.name, c.column) for c in CHECKS if not is_array_column(c)]
        assert scalar == [("endpoint_resource", "connection_type")]

    def test_organization_network_split_shares_column_and_path(self):
        org = [c for c in CHECKS if c.resource_type == "Organization"][0]
        net = [c for c in CHECKS if c.resource_type == "Network"][0]
        assert org.table is net.table and org.column == net.column == "type_codes"
        assert org.element_path == net.element_path == "Organization.type"
        assert org.cell_where != net.cell_where


class TestResolveBinding:
    def test_raises_for_unknown_profile(self):
        chk = bindings.Check(bindings.organization, "type_codes", None,
                             "Organization", "Organization.type", "no-such-profile")
        with pytest.raises(KeyError):
            resolve_binding({}, chk)

    def test_raises_for_unbound_element(self):
        index = {bindings.PROFILE_BASE + "plannet-Organization":
                {"resourceType": "StructureDefinition", "snapshot": {"element": []}}}
        chk = bindings.Check(bindings.organization, "type_codes", None,
                             "Organization", "Organization.type", "plannet-Organization")
        with pytest.raises(KeyError):
            resolve_binding(index, chk)

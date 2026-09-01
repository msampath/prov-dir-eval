#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build the real-data payload for the framework report artifact.

    .venv\\Scripts\\python.exe scripts\\build_framework_payload.py

READ-ONLY against the database: no INSERT/UPDATE/DELETE/DDL, no VACUUM, no ANALYZE.
Output: ``output/quality/framework_mockup_data.json``.

This script is the executable statement of ``docs/evaluation-framework.md`` v0.3.0's
arithmetic (spec §9.0). Where the prose and this file disagree, one of them is wrong and
the discrepancy is a finding.

What it computes, and the spec section each rule comes from:

* §5.1  rule B — concept mean of cell means, with declared ``concept_id``, the declared
        alias collapse, the absolute floor (n >= 30) and the method floor (no ``sample``
        cell enters a mean). The v0.2.0 relative floor (0.1% of published rows) is gone.
        Every concept also carries its denominator span and its row-pooled rate.
* §1.1.1 ``path_status`` — ``communication-proficiency`` is ``refuted`` and leaves every
        headline. ``signal`` — evaluated only for proxy/metadata checks.
* §D5e  ``provenance_author_timestamp`` demoted to ``weak-proxy``, out of the USCDI
        headline.
* §5.4 G1 — per (payer, resource) coverage bound from the latest provenance row, then the
        stated per-payer precedence (superset > incomplete > not-assessable > bounded).
        ``conjuncts_evaluated`` is 1 of 3 and is carried on every result.
* §5.4 G2 — the actual InsurancePlan -> Organization[network] <- PractitionerRole ->
        Practitioner traversal. Exact where the scan completes; a bounded existence sample
        otherwise, in which case a state is published and no rate is.
* §D3   cell-mean aggregation with n < 30 cells suppressed, plus the matched-instance
        run-6 -> run-9 decomposition by sole-cause error class.
* §D10  the ``pd-1`` unreachable-listing proxy.

The payer set is **pinned** to the payers in ``map_data.json`` (the frozen run-3 / run-9
run set). Schemas that exist in the database but are outside that set are listed under
``payer_set.schemas_outside_run_set`` and are not measured — a concurrent extraction track
adds schemas while this runs, and a census over "whatever exists at build time" is not
reproducible (spec §12 item 16).
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from sqlalchemy import text                                     # noqa: E402

from provdir.db import get_engine                               # noqa: E402
from provdir.quality.conformance import (                       # noqa: E402
    D3_MIN_CELL, D3_UNIT, D3_UNIT_CAVEAT, cell_mean_rate, parse_expect_payers,
    select_run,
)
from provdir.quality.uscdi import USCDI_CHECKS                  # noqa: E402


def _identical_sql_pairs():
    """§5.1 declared aliases, derived mechanically.

    Two checks are "two obligations, one observation" when they resolve to an identical
    ``(resource_type, condition_sql, cell_where)`` triple. v0.2.0's builder inferred the
    alias from equal (denominator, populated) counts, which is the right answer today by
    luck: two genuinely different checks can coincide on a payer. Reading the check
    definitions removes the guess.
    """
    by_sql = collections.defaultdict(list)
    for c in USCDI_CHECKS:
        if not c.condition_sql or not c.required:
            continue
        by_sql[(c.resource_type, c.condition_sql, c.cell_where)].append(c.element)
    pairs = []
    for (rt, _sql, _w), elements in by_sql.items():
        names = sorted(set(elements))
        if len(names) > 1:
            pairs.append((rt, names))
    return pairs


ALIAS_PAIRS = _identical_sql_pairs()

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
MAP_DATA = os.path.join(REPO, "output", "quality", "map_data.json")
OUT = os.path.join(REPO, "output", "quality", "framework_mockup_data.json")

COV_RUN, CONF_RUN_PRIOR, BIND_RUN = 3, 6, 2
# CONF_RUN used to be pinned here as `9`, in the source, while build_map_data.py
# picked its run by "most recorded instances" and conformance_report.py by
# "newest finished". Three rules, one of them a literal: the moment a run lands
# that is both newest and largest, the other two artifacts move and this one
# does not, and the published figures disagree with nothing to explain it.
# The run is now selected at runtime by the SAME shared rule as the other two
# (provdir.quality.conformance.choose_run), which refuses to guess.
FRAMEWORK_VERSION = "0.3.0"

EXT = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"
TABLES = ("practitioner", "practitioner_role", "organization", "location",
          "healthcare_service", "insurance_plan", "organization_affiliation",
          "endpoint_resource")
RES_TABLE = {"Practitioner": "practitioner", "PractitionerRole": "practitioner_role",
             "Organization": "organization", "Location": "location",
             "HealthcareService": "healthcare_service", "InsurancePlan": "insurance_plan",
             "OrganizationAffiliation": "organization_affiliation",
             "Endpoint": "endpoint_resource"}

# §1.1.1 axis 1: a property of the check, established by reading the StructureDefinition.
PATH_REFUTED = {
    EXT + "communication-proficiency":
        "the Plan-Net profile slices this extension under Practitioner.communication in "
        "both 1.1.0 and 1.2.0; the check reads Practitioner.extension (root). Its "
        "published values are exactly inverted (spec §8.3).",
}
# §D5e: demoted to weak-proxy, out of the USCDI headline.
DEMOTED = {"uscdi:provenance_author_timestamp"}
# §1.1.1 axis 2 scope: signal is evaluated only for these (proxy / capture-metadata) ids.
SIGNAL_SCOPE = {"uscdi:provenance_author_timestamp",
                "mpf:PractitionerRole.last_updated", "mpf:Organization.last_updated"}

ALIAS_GROUPS = {
    "amerihealth_deex": "amerihealth-caritas-next", "amerihealth_flex": "amerihealth-caritas-next",
    "amerihealth_laex": "amerihealth-caritas-next", "amerihealth_ncex": "amerihealth-caritas-next",
    "amerihealth_scex": "amerihealth-caritas-next",
    "wellmark": "wellmark", "wellmark_advantage": "wellmark",
}

G1_PRECEDENCE = ["capture-suspect-superset", "capture-incomplete",
                 "not-assessable", "capture-bounded"]

# G2 traversal budgets. Exact scan first; bounded existence sample on timeout.
G2_EXACT_TIMEOUT = "75s"
G2_SAMPLE_TIMEOUT = "60s"
G2_SAMPLE_ROWS = 200000


def concept_id(assessment: str, resource_type: str, element: str) -> str:
    """§5.1: concept identity is declared per assessment, never derived from the string."""
    if assessment == "plannet_ext":
        b = element.replace("delivery_method_virtual_modalities",
                            "delivery-method#virtualModalities")
        b = b.replace("communication_proficiency", "communication-proficiency")
        b = b.replace("delivery_method", "delivery-method")
        return EXT + b
    if assessment == "mpf":
        # a CMS element-list row is per (resource, element): `name` on Organization and
        # `name` on InsurancePlan are DIFFERENT rows of the guide.
        return "mpf:%s.%s" % (resource_type, element)
    if assessment == "uscdi":
        return "uscdi:%s" % element      # USCDI classes are resource-agnostic by design
    return "%s:%s.%s" % (assessment, resource_type, element)


# --------------------------------------------------------------------------- rule B
# §1.1.1 axis 2: which table's timestamp column each in-scope concept reads.
SIGNAL_TABLE = {"mpf:Organization.last_updated": "organization",
                "mpf:PractitionerRole.last_updated": "practitioner_role",
                "uscdi:provenance_author_timestamp": "location"}


def concept_signal(cid, entropy_for):
    """§1.1.1: `signal` is evaluated ONLY for proxy / capture-metadata checks, and it is
    evaluated on the column's **distinct-value count**, not on the presence rate. A column
    can be 100% populated and still hold one bulk-load value."""
    if cid not in SIGNAL_SCOPE:
        return "sound"                       # out of scope: the axis does not apply
    e = entropy_for(SIGNAL_TABLE.get(cid))
    if not e or not e["n"]:
        return "not-evaluated"               # honest: we did not sample this column
    return "degenerate" if e["distinct"] <= 1 else "sound"


def rule_b(cells, entropy_for=lambda t: None):
    """§5.1 rule B: concept mean of cell means, with the two floors and the alias rule."""
    ok, below, unmeasurable, refuted = [], [], [], []
    for x in cells:
        if not x["required"] or x["mapping"] == "weak-proxy" or x["cid"] in DEMOTED:
            continue
        if x["cid"] in PATH_REFUTED:
            refuted.append(x)
            continue
        if x["method"] == "unmeasured" or x["d"] == 0:
            unmeasurable.append(x)
            continue
        if x["d"] < 30:                       # absolute floor
            below.append(dict(x, why="n < 30"))
            continue
        if x["method"] == "sample":           # method floor
            below.append(dict(x, why="sampled, not a full scan"))
            continue
        ok.append(x)

    by_concept = collections.defaultdict(list)
    for x in ok:
        by_concept[x["cid"]].append(x)

    # §5.1 declared alias: two USCDI elements resolving to identical SQL on the same
    # resource are two obligations measured by ONE observation. Count it once; the detail
    # view lists both concept ids. The pairs are read from the check definitions
    # (`ALIAS_PAIRS`), never inferred from coincidentally equal counts.
    alias = False
    alias_notes = []
    for rt, names in ALIAS_PAIRS:
        present = [n for n in names
                   if any(y["rt"] == rt for y in by_concept.get("uscdi:%s" % n, []))]
        if len(present) < 2:
            continue
        alias = True
        alias_notes.append(dict(resource_type=rt, concepts=["uscdi:%s" % n for n in names],
                                kept="uscdi:%s" % present[0]))
        for n in present[1:]:                       # keep the first, drop the duplicates
            key = "uscdi:%s" % n
            keep = [y for y in by_concept[key] if y["rt"] != rt]
            if keep:
                by_concept[key] = keep
            else:
                del by_concept[key]

    concepts = []
    for cid, cs in sorted(by_concept.items()):
        ns = [x["d"] for x in cs]
        concepts.append(dict(
            cid=cid,
            rate=sum(x["pop"] / x["d"] for x in cs) / len(cs),
            pooled=sum(x["pop"] for x in cs) / sum(ns),     # §5.1: published beside the mean
            n_min=min(ns), n_max=max(ns),
            signal=concept_signal(cid, entropy_for),
            cells=[dict(rt=x["rt"], el=x["el"], d=x["d"], pop=x["pop"], m=x["method"])
                   for x in sorted(cs, key=lambda z: z["rt"])]))
    rate = sum(c["rate"] for c in concepts) / len(concepts) if concepts else None
    return dict(rate=rate, n_concepts=len(concepts), n_cells=len(ok),
                n_below_floor=len(below), n_unmeasurable=len(unmeasurable),
                n_path_refuted=len(refuted), alias_collapsed=alias, aliases=alias_notes,
                below=[dict(rt=x["rt"], el=x["el"], d=x["d"], pop=x["pop"], why=x["why"])
                       for x in sorted(below, key=lambda z: z["d"])[:8]],
                refuted=[dict(rt=x["rt"], el=x["el"], d=x["d"], pop=x["pop"],
                              cid=x["cid"], note=PATH_REFUTED[x["cid"]]) for x in refuted],
                concepts=concepts)


def rule_a(cells):
    """The current (superseded) arithmetic, kept for the side-by-side comparison."""
    cs = [x for x in cells if x["required"] and x["method"] != "unmeasured" and x["d"] > 0]
    return (sum(x["pop"] / x["d"] for x in cs) / len(cs)) if cs else None


# --------------------------------------------------------------------------- G1
def g1_resource_state(v):
    """§5.4 G1, conjunct 1 of 3 (the only one evaluable)."""
    if v["cov"] is None:
        if not v["server_total"] and not v["resource_count"]:
            return "nothing-served"
        return "not-assessable"
    if v["cov"] > 100.5 or v["misreported"]:
        return "capture-suspect-superset"
    if v["cov"] < 99.9:
        return "capture-incomplete"
    return "capture-bounded"


def g1_payer_state(per_resource):
    """§5.4 G1 per-payer precedence. `nothing-served` is excluded, not counted unknown."""
    live = [s for s in per_resource.values() if s != "nothing-served"]
    for s in G1_PRECEDENCE:
        if s in live:
            return s
    return "not-assessable"


# --------------------------------------------------------------------------- G2
def g2_for_schema(conn, schema, rel_role, rel_prac, exact_rows):
    """§5.4 G2: walk InsurancePlan -> Organization[network] <- PractitionerRole -> Practitioner.

    Returns a dict carrying the state, the reason, the method, and — only where the exact
    scan completed — the practitioner-level chain rate.
    """
    out = dict(state=None, reason="", method=None, chain_rate=None, named_chain_rate=None,
               plans=None, plans_with_network_ref=None, plans_named=None,
               plan_network_refs=None, plan_network_refs_resolved=None,
               roles=None, roles_to_plan_network=None, practitioners_in_chain=None,
               sample_n=None)

    role_rows = exact_rows.get("practitioner_role")
    prac_rows = exact_rows.get("practitioner")
    if not role_rows and not prac_rows:
        out.update(state="not-assessable", reason="no Practitioner and no PractitionerRole rows held")
        return out

    conn.execute(text("SET statement_timeout='60s'"))
    try:
        r = conn.execute(text(
            "select count(*), count(*) filter (where network_refs is not null "
            "and cardinality(network_refs) > 0), count(*) filter (where name is not null "
            "and btrim(name) <> '') from %s.insurance_plan" % schema)).one()
        out["plans"], out["plans_with_network_ref"], out["plans_named"] = int(r[0]), int(r[1]), int(r[2])
    except Exception:
        conn.rollback()
        out["plans"] = 0

    refs, refs_named = [], []
    if out["plans"]:
        try:
            rows = conn.execute(text(
                "select u, bool_or(name is not null and btrim(name) <> '') "
                "from %s.insurance_plan ip, unnest(ip.network_refs) u group by u" % schema)).all()
            refs = [x[0] for x in rows]
            refs_named = [x[0] for x in rows if x[1]]
        except Exception:
            conn.rollback()
    out["plan_network_refs"] = len(refs)

    resolved, resolved_named = [], []
    if refs:
        ids = [x.split("/")[-1] for x in refs]
        try:
            got = set(x[0] for x in conn.execute(
                text("select id from %s.organization where id = any(:i)" % schema), {"i": ids}))
            resolved = [x for x in refs if x.split("/")[-1] in got]
            resolved_named = [x for x in refs_named if x.split("/")[-1] in got]
        except Exception:
            conn.rollback()
    out["plan_network_refs_resolved"] = len(resolved)

    if not role_rows:
        out.update(state="chain-does-not-resolve",
                   reason="publishes no PractitionerRole: no network edge exists")
        return out
    if not out["plans"]:
        out.update(state="chain-does-not-resolve",
                   reason="publishes no InsurancePlan: the chain has no terminus")
        return out
    if not out["plans_with_network_ref"]:
        out.update(state="chain-does-not-resolve",
                   reason="no InsurancePlan carries a network reference")
        return out
    if not resolved:
        out.update(state="chain-does-not-resolve",
                   reason="no InsurancePlan network reference resolves to a published Organization")
        return out

    try:                                                    # exact traversal
        conn.execute(text("SET statement_timeout='%s'" % G2_EXACT_TIMEOUT))
        r = conn.execute(text(
            "select count(*), count(*) filter (where network_refs && :r), "
            "count(distinct practitioner_ref) filter (where network_refs && :r), "
            "count(distinct practitioner_ref) filter (where network_refs && :n) "
            "from %s.practitioner_role" % schema),
            {"r": resolved, "n": resolved_named}).one()
        out["method"] = "sql"
        out["roles"], out["roles_to_plan_network"] = int(r[0]), int(r[1])
        out["practitioners_in_chain"] = int(r[2])
        if prac_rows:
            out["chain_rate"] = min(int(r[2]) / prac_rows, 1.0)
            out["named_chain_rate"] = min(int(r[3]) / prac_rows, 1.0)
    except Exception:
        conn.rollback()
        try:                                                # bounded existence sample
            conn.execute(text("SET statement_timeout='%s'" % G2_SAMPLE_TIMEOUT))
            r = conn.execute(text(
                "select count(*), count(*) filter (where network_refs && :r) from "
                "(select network_refs from %s.practitioner_role limit %d) t"
                % (schema, G2_SAMPLE_ROWS)), {"r": resolved}).one()
            out["method"] = "sample"
            out["sample_n"], out["roles_to_plan_network"] = int(r[0]), int(r[1])
        except Exception:
            conn.rollback()
            out.update(state="not-assessable", method="timeout",
                       reason="the traversal query did not complete inside the budget")
            return out

    if out["roles_to_plan_network"] == 0:
        out.update(state="chain-partially-resolves",
                   reason="every link is published but no PractitionerRole references a "
                          "network any InsurancePlan references")
    else:
        out["state"] = "chain-resolves"
    return out


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conformance-run", type=int, default=None,
                    help="report on this run explicitly. Without it the run is "
                         "chosen by provdir.quality.conformance.choose_run -- "
                         "the same rule build_map_data.py and "
                         "conformance_report.py use, which REFUSES rather than "
                         "guess when the widest payer set and the largest "
                         "instance count are different runs.")
    ap.add_argument("--expect-payers", default=None, metavar="N|a,b,c",
                    help="required payer count, or the exact comma-separated "
                         "payer set. Same flag, same meaning, in the other two "
                         "consumers.")
    ap.add_argument("--allow-unverified", action="store_true",
                    help="accept a run that did not account for every instance "
                         "it attempted.")
    ap.add_argument("--conformance-run-prior", type=int, default=CONF_RUN_PRIOR,
                    metavar="N",
                    help="run to diff against for the IG version delta. 0 "
                         "disables the delta -- which is what it MUST be when "
                         "the selected run does not share a payer set with it: "
                         "the delta joins on (payer, cell, instance) and a "
                         "disjoint pair silently yields an empty diff that "
                         "reads as 'nothing changed'. Default: %(default)s")
    ap.add_argument("--reuse-g2", action="store_true",
                    help="development convenience: reuse the previous "
                         "payload's G2 traversal instead of recomputing it")
    args = ap.parse_args()

    started = time.time()
    engine = get_engine()
    with engine.connect() as _c:
        conf_run, conf_note = select_run(
            _c, run_id=args.conformance_run,
            expect_payers=parse_expect_payers(args.expect_payers),
            allow_unverified=args.allow_unverified)
    conf_run_prior = args.conformance_run_prior or None
    print("conformance run selection: %s" % conf_note)
    print("  prior run for the IG delta: %s"
          % (conf_run_prior if conf_run_prior else "disabled"))

    mapd = json.load(open(MAP_DATA, encoding="utf-8"))
    pinned = [p["id"] for p in mapd["payers"]]
    pinned_set = set(pinned)
    label = {p["id"]: p["name"] for p in mapd["payers"]}
    states = {p["id"]: p.get("states", {}) for p in mapd["payers"]}
    ma = set(p["id"] for p in mapd["payers"] if p.get("is_ma"))

    per = collections.defaultdict(lambda: collections.defaultdict(list))
    with engine.connect() as c:
        c.execute(text("SET statement_timeout='300s'"))

        for (payer, assessment, rt, el, d, pop, method, required, mapping) in c.execute(text(
                "select payer_id, assessment, resource_type, element, denominator, populated,"
                " method, required, mapping from public.element_coverage where run_id=:r"),
                {"r": COV_RUN}):
            per[payer][assessment].append(dict(
                rt=rt, el=el, d=int(d), pop=int(pop), method=method, required=bool(required),
                mapping=mapping, cid=concept_id(assessment, rt, el)))

        reltuples = {}
        for schema, table, n in c.execute(text(
                "select n.nspname, cl.relname, cl.reltuples::bigint from pg_class cl "
                "join pg_namespace n on n.oid = cl.relnamespace where cl.relkind='r' "
                "and cl.relname = any(:t)"), {"t": list(TABLES)}):
            reltuples.setdefault(schema, {})[table] = int(n)
        live_schemas = sorted(reltuples)

        # the selected conformance run, per (payer, resource) cell
        conf = collections.defaultdict(dict)
        for payer, cell, n, clean in c.execute(text(
                "select payer_id, split_part(cell,'__',1), count(*), "
                "count(*) filter (where errors=0) from public.conformance_instance "
                "where run_id=:r group by 1,2"), {"r": conf_run}):
            conf[payer][cell] = (int(n), int(clean))
        # The version delta joins run-to-run on (payer, cell, instance). Two
        # runs over different payer sets share no such key, so the join
        # silently returns nothing and the payload reports "no instance
        # changed state" -- which is not the same statement as "the delta was
        # not measured". --conformance-run-prior 0 says so explicitly.
        conf_prior = collections.defaultdict(dict)
        newly_failing = collections.defaultdict(collections.Counter)
        newly_clean = collections.defaultdict(collections.Counter)
        if conf_run_prior:
            _load_prior(c, conf_run, conf_run_prior, conf_prior,
                        newly_failing, newly_clean)

        # §D10 pd-1 unreachable-listing proxy
        pd1 = {}
        for payer, n, k in c.execute(text(
                "with pr as (select payer_id, count(*) n from public.conformance_instance "
                "  where run_id=:r and split_part(cell,'__',1)='PractitionerRole' group by 1), "
                " f as (select payer_id, count(distinct instance) k from public.conformance_issue "
                "  where run_id=:r and message_id like '%%us-core-practitionerrole#pd-1' group by 1) "
                " select pr.payer_id, pr.n, coalesce(f.k,0) from pr left join f using(payer_id)"),
                {"r": conf_run}):
            pd1[payer] = dict(n=int(n), fail=int(k))

        binding = collections.defaultdict(dict)
        for payer, strength, rows_with_codes, rows_with_nonmember in c.execute(text(
                "select payer_id, strength, sum(rows_with_codes), sum(rows_with_nonmember) "
                "from public.binding_check_element where run_id=:r and status='ok' "
                "group by 1,2"), {"r": BIND_RUN}):
            binding[payer][strength or "unknown"] = (int(rows_with_codes or 0),
                                                     int(rows_with_nonmember or 0))

        # provenance history, for G1 and for the monotonicity indicator
        hist = collections.defaultdict(list)
        for run, payer, rt, status, rc, notes in c.execute(text(
                "select run_id, payer_id, resource_type, status, resource_count, notes "
                "from public.provenance order by run_id")):
            nn = notes if isinstance(notes, dict) else (json.loads(notes) if notes else {})
            hist[(payer, rt)].append(dict(run=run, rc=rc, cov=nn.get("coverage_pct"),
                                          server_total=nn.get("server_total"),
                                          src=nn.get("server_total_source"),
                                          misreported=bool(nn.get("server_total_misreported")),
                                          method=nn.get("method")))
        prov = {}
        for (payer, rt), h in hist.items():
            h.sort(key=lambda x: x["run"])
            last = dict(h[-1])
            counts = [x["rc"] for x in h if x["rc"] is not None]
            last["resource_count"] = last.pop("rc")
            last["n_loads"] = len(counts)
            last["never_decreased"] = all(counts[i + 1] >= counts[i]
                                          for i in range(len(counts) - 1)) if counts else None
            prov.setdefault(payer, {})[rt] = last

        # D8 timestamp entropy, bounded sample per (payer, table). §D8's threshold, now
        # stated: degenerate when the sampled column is all-null or holds <=1 distinct value.
        entropy = collections.defaultdict(dict)
        c.execute(text("SET statement_timeout='20s'"))
        for schema in live_schemas:
            for tbl in ("location", "organization", "practitioner_role"):
                try:
                    r = c.execute(text(
                        "select count(*), count(distinct meta_last_updated), "
                        "count(*) filter (where meta_last_updated is null) from "
                        "(select meta_last_updated from %s.%s limit 50000) s"
                        % (schema, tbl))).one()
                    if r[0]:
                        entropy[schema][tbl] = dict(n=int(r[0]), distinct=int(r[1]),
                                                    nulls=int(r[2]))
                except Exception:
                    c.rollback()

        # exact row counts for the small tables G2 needs to branch on
        exact = {}
        c.execute(text("SET statement_timeout='120s'"))
        for schema in live_schemas:
            if schema not in pinned_set:
                continue
            e = {}
            for tbl in ("practitioner", "practitioner_role"):
                est = reltuples.get(schema, {}).get(tbl, 0)
                if est > 0:
                    e[tbl] = est
                else:                       # 0 or the -1 never-analysed sentinel
                    try:
                        e[tbl] = int(c.execute(
                            text("select count(*) from %s.%s" % (schema, tbl))).scalar())
                    except Exception:
                        c.rollback()
                        e[tbl] = 0
            exact[schema] = e

        g2 = {}
        if args.reuse_g2 and os.path.exists(OUT):
            # development convenience only: the G2 traversal is the slow part (~10 min).
            # The default path recomputes it; this reads the previous payload's results.
            prev = json.load(open(OUT, encoding="utf-8"))
            g2 = {x["id"]: x["gates"]["g2"] for x in prev["payers"]}
            print("  G2 reused from", OUT, "(--reuse-g2)")
        for schema in pinned:
            if schema in g2:
                continue
            if schema not in reltuples:
                g2[schema] = dict(state="not-assessable", reason="no tables loaded",
                                  method=None, chain_rate=None, named_chain_rate=None)
                continue
            g2[schema] = g2_for_schema(c, schema,
                                       reltuples.get(schema, {}).get("practitioner_role", 0),
                                       reltuples.get(schema, {}).get("practitioner", 0),
                                       exact.get(schema, {}))
            print("  G2 %-24s %-26s %s" % (schema, g2[schema]["state"], g2[schema]["method"]),
                  flush=True)

    # ---------------------------------------------------------------- assemble
    payers = []
    for p in pinned:
        t = reltuples.get(p, {})
        total_rows = sum(max(v, 0) for v in t.values())
        present = [k for k in TABLES if t.get(k, 0) > 0]

        pr = {rt: g1_resource_state(v) for rt, v in prov.get(p, {}).items()}
        g1_state = g1_payer_state(pr) if pr else "not-assessable"
        never_dec = [rt for rt, v in prov.get(p, {}).items()
                     if v.get("n_loads", 0) >= 2 and v.get("never_decreased")]

        # The declared unit, from the shared helper -- so this artifact, the
        # map and the report cannot drift apart on what the number means or on
        # where the §8.4 small-cell floor sits.
        cells = [(cell, n, clean) for cell, (n, clean) in conf.get(p, {}).items()]
        d3_rate, d3_used, d3_sup = cell_mean_rate([(n, cl) for _c, n, cl in cells])
        prior_cells = [(cell, n, clean) for cell, (n, clean) in conf_prior.get(p, {}).items()]
        d3_prior, _pu, _ps = cell_mean_rate([(n, cl) for _c, n, cl in prior_cells])

        nf = newly_failing.get(p, collections.Counter())
        nc = newly_clean.get(p, collections.Counter())

        bq = binding.get(p, {}).get("required")
        ent = entropy.get(p, {})
        e = ent.get("location")

        def entropy_for(tbl, _ent=ent):
            return _ent.get(tbl)

        payers.append(dict(
            id=p, name=label.get(p, p), is_ma=p in ma, alias_group=ALIAS_GROUPS.get(p),
            resources=dict((k, t.get(k, 0)) for k in TABLES[:7]),
            total_rows=total_rows, n_types=len(present), states=states.get(p, {}),
            gates=dict(
                g1=dict(state=g1_state, conjuncts_evaluated=1, conjuncts_total=3,
                        per_resource=pr,
                        coverage=dict((rt, v["cov"]) for rt, v in prov.get(p, {}).items()),
                        row_count_never_decreased=sorted(never_dec)),
                g2=g2.get(p, dict(state="not-assessable", reason="not measured"))),
            d3=dict(rate=d3_rate, prior_rate=d3_prior,
                    delta=(d3_rate - d3_prior) if (d3_rate is not None and d3_prior is not None) else None,
                    unit=D3_UNIT, n_cells=d3_used, n_cells_suppressed=d3_sup,
                    cells=[dict(rt=cell, n=n, clean=cl, suppressed=n < D3_MIN_CELL)
                           for cell, n, cl in sorted(cells)],
                    newly_failing=[[k, v] for k, v in nf.most_common(4)],
                    newly_clean=[[k, v] for k, v in nc.most_common(4)]),
            d4=dict(rate=(1 - bq[1] / bq[0]) if bq and bq[0] else None,
                    rows=bq[0] if bq else 0),
            d8=(dict(n=e["n"], distinct=e["distinct"], nulls=e["nulls"],
                     degenerate=(e["distinct"] <= 1)) if e else None),
            pd1=pd1.get(p),
            d8_tables=dict((t, v) for t, v in ent.items()),
            cov=dict((a, dict(ruleB=rule_b(per[p][a], entropy_for),
                              ruleA=rule_a(per[p][a])))
                     for a in per.get(p, {})) if p in per else {}))

    g1_counts = collections.Counter(x["gates"]["g1"]["state"] for x in payers)
    g2_counts = collections.Counter(x["gates"]["g2"]["state"] for x in payers)
    g2_reasons = collections.Counter(x["gates"]["g2"].get("reason") for x in payers
                                     if x["gates"]["g2"]["state"] == "chain-does-not-resolve")
    g1_res = collections.Counter(s for x in payers for s in x["gates"]["g1"]["per_resource"].values())
    pd1_n = sum(v["n"] for v in pd1.values())
    pd1_f = sum(v["fail"] for v in pd1.values())

    doc = dict(
        generated=time.strftime("%Y-%m-%d"),
        framework_version=FRAMEWORK_VERSION,
        runs=dict(coverage=COV_RUN, conformance=conf_run,
                  conformance_selection=conf_note,
                  conformance_unit=D3_UNIT, conformance_unit_caveat=D3_UNIT_CAVEAT,
                  conformance_prior=conf_run_prior,
                  binding=BIND_RUN, ig="1.2.0", ig_prior="1.1.0"),
        payer_set=dict(
            basis="pinned to output/quality/map_data.json — the frozen run-3 / run-9 set",
            n=len(pinned),
            schemas_outside_run_set=sorted(set(live_schemas) - pinned_set)),
        corpus=dict(
            g1=dict(payer=dict(g1_counts), resource=dict(g1_res),
                    conjuncts_evaluated=1, conjuncts_total=3),
            g2=dict(payer=dict(g2_counts), reasons=dict(g2_reasons),
                    exact_rates=sorted(round(x["gates"]["g2"]["chain_rate"], 4)
                                       for x in payers
                                       if x["gates"]["g2"].get("chain_rate") is not None)),
            pd1=dict(instances=pd1_n, failing=pd1_f, payers=len(pd1),
                     at_100=sum(1 for v in pd1.values() if v["n"] and v["fail"] == v["n"]),
                     at_0=sum(1 for v in pd1.values() if v["fail"] == 0)),
            floors=dict(
                measurable_required_cells=sum(
                    1 for p in per for a in per[p] for x in per[p][a]
                    if x["required"] and x["method"] != "unmeasured" and x["d"] > 0),
                below_n30=sum(1 for p in per for a in per[p] for x in per[p][a]
                              if x["required"] and x["method"] != "unmeasured"
                              and 0 < x["d"] < 30),
                sampled=sum(1 for p in per for a in per[p] for x in per[p][a]
                            if x["required"] and x["method"] == "sample"),
                excluded_headline_eligible=sum(x["cov"][a]["ruleB"]["n_below_floor"]
                                               for x in payers for a in x["cov"])),
            alias_collapses=sum(1 for x in payers for a in x["cov"]
                                if x["cov"][a]["ruleB"]["alias_collapsed"])),
        payers=payers)

    json.dump(doc, open(OUT, "w", encoding="utf-8"), indent=1)
    print("\npayers: %d -> %s (%.0fs)" % (len(payers), OUT, time.time() - started))
    print("G1:", dict(g1_counts))
    print("G2:", dict(g2_counts))
    print("floors:", doc["corpus"]["floors"])
    print("alias collapses:", doc["corpus"]["alias_collapses"])
    ex = [x for x in payers if x["id"] == "bcbs_az"][0]["cov"]["plannet_ext"]
    print("bcbs_az plannet_ext  ruleA=%.1f%%  ruleB=%.1f%%  concepts=%d  refuted=%d"
          % (100 * ex["ruleA"], 100 * ex["ruleB"]["rate"], ex["ruleB"]["n_concepts"],
             ex["ruleB"]["n_path_refuted"]))


def _load_prior(c, conf_run, conf_run_prior, conf_prior, newly_failing,
                newly_clean):
    """Populate the IG-version delta structures from the prior conformance run.

    Lifted out of main() so the delta can be skipped wholesale when no prior
    run is comparable, rather than run against a disjoint payer set and report
    an empty diff as "nothing changed".
    """
    for payer, cell, n, clean in c.execute(text(
            "select payer_id, split_part(cell,'__',1), count(*), "
            "count(*) filter (where errors=0) from public.conformance_instance "
            "where run_id=:r group by 1,2"), {"r": conf_run_prior}):
        conf_prior[payer][cell] = (int(n), int(clean))

    # §D3 decomposition: matched instances, attributed to their sole-cause error set
    for payer, mids in c.execute(text(
            "with o as (select payer_id,cell,instance from public.conformance_instance "
            "  where run_id=:o and errors=0), "
            " n as (select payer_id,cell,instance from public.conformance_instance "
            "  where run_id=:n and errors>0), "
            " f as (select * from o join n using(payer_id,cell,instance)) "
            " select f.payer_id, array_agg(distinct coalesce(i.message_id,'(null)')) "
            " from f join public.conformance_issue i on i.run_id=:n "
            "   and i.payer_id=f.payer_id and i.cell=f.cell and i.instance=f.instance "
            "   and i.severity='error' group by f.payer_id, f.cell, f.instance"),
            {"o": conf_run_prior, "n": conf_run}):
        newly_failing[payer][" + ".join(sorted(short_class(m) for m in mids))] += 1
    for payer, mids, cnt in c.execute(text(
            "with o as (select payer_id,cell,instance from public.conformance_instance "
            "  where run_id=:o and errors>0), "
            " n as (select payer_id,cell,instance from public.conformance_instance "
            "  where run_id=:n and errors=0), "
            " f as (select * from o join n using(payer_id,cell,instance)), "
            " g as (select f.payer_id p, array_agg(distinct coalesce(i.message_id,'(null)')) m "
            "   from f join public.conformance_issue i on i.run_id=:o "
            "     and i.payer_id=f.payer_id and i.cell=f.cell and i.instance=f.instance "
            "     and i.severity='error' group by f.payer_id, f.cell, f.instance) "
            " select p, m, count(*) from g group by 1,2"),
            {"o": conf_run_prior, "n": conf_run}):
        newly_clean[payer][" + ".join(sorted(short_class(m) for m in mids))] += int(cnt)


def short_class(message_id: str) -> str:
    """Human-readable error-class name for the version-delta decomposition."""
    if not message_id:
        return "(none)"
    if "#" in message_id:
        return message_id.rsplit("#", 1)[-1]
    return message_id


if __name__ == "__main__":
    main()

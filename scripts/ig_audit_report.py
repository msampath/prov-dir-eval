"""Render the Plan-Net IG audit as markdown, straight from the audit tables.

Everything here is a read of public.ig_audit* + public.plannet_element +
public.ig_shred_progress, plus one read-only pass over the payer tables for the
capture window. No measurement happens in this module, so the report can be
regenerated (or re-cut differently) without re-running a multi-hour shred, and
every figure in it is traceable to a row someone else can query.

THREE RULES run through the whole file.

1. **No count appears as a literal in prose.** Every caption is formatted from
   the result set of the query producing the table it captions. Three different
   figures (376 / 362 / 360) once circulated for the same "number of elements"
   because three people described three predicates in three sentences; a
   literal in a caption is a number that cannot be re-derived and drifts the
   moment the data does. The capture window obeys the same rule -- it comes
   from min/max(ingested_at), never from a restated date.

2. **A caption formatted from a `sum()` over a result set names the ROW of that
   result set, not the entity underneath it.** `sum(n_min_violation)` over
   (schema, profile, element, lob) cells is a count of element-level
   shortfalls; one parent instance short on five elements is in it five times,
   so calling it "parent instances" states a number that can exceed the row
   count of the schema it describes.

3. **A row whose denominator is zero renders `not measured`, never a
   percentage.** `0.0%` over an empty cell is arithmetically true and
   rhetorically false: it is typographically identical to a populated cell that
   genuinely publishes nothing, and a reader cannot tell them apart.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from provdir.db import get_engine  # noqa: E402
from provdir.models import RESOURCE_TABLES  # noqa: E402

LOB_NOTE = (
    "`urn:source_name:` in each resource's `meta.source` is a **provenance** "
    "field: it records which upstream feed the record arrived on. Two of its "
    "values line up with lines of business; one does not.\n\n"
    "| value | what it is |\n|---|---|\n"
    "| `azblue` | ACA (individual/marketplace) **and** Medicare Advantage |\n"
    "| `hcaz` | Medicaid - Health Choice Arizona |\n"
    "| `ods` | a **source system**. Not a line of business, not a peer of the "
    "other two, and not something to put in the same column as them without "
    "saying so. |\n\n"
    "Both lines of business are in CMS-9115-F scope. Neither is \"commercial\".\n\n"
    "Read the mapping as an **inference layered on a provenance field**, not as "
    "a published product taxonomy. The payer publishes the source name; \"azblue "
    "= ACA + Medicare Advantage\" is our reading of it. Nothing in the data "
    "asserts it, and no resource carries a product identifier that would "
    "confirm it.\n"
)

# An internal detail of the path compiler, not a limit on what was tested: it
# records that an element's jsonpath was rebuilt parent-first so a slice's
# children keep the slice's own discriminator. Left in the "could not be fully
# tested" buckets it was that section's single largest entry and made the audit
# look far more hobbled than it is.
INTERNAL_NOTE = "jsonpath rebuilt from the ancestor chain"


def q(conn, sql: str, params=None):
    return conn.exec_driver_sql(sql, params or ()).all()


def pct(n, d) -> str:
    """A rate, or `not measured` when there is no denominator.

    Never `0.0%`. A cell with nothing in it has not scored zero -- it has not
    been scored, and the two must not render identically two sections apart
    from the paragraph that says so.
    """
    return f"{100.0 * n / d:.1f}%" if d else "not measured"


def num(v) -> str:
    return "unmeasured" if v is None else f"{v:,}"


def table(headers: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(out) + "\n"


def require_run(run_id: Optional[int], rows: list) -> tuple:
    """The audit-run row, or a message naming the fix.

    Without this, an empty public.ig_audit_run makes run_id None, the lookup
    runs `WHERE run_id = NULL`, returns no rows, and `[0]` raises a bare
    IndexError several frames away from anything an operator can act on.
    """
    if run_id is None:
        raise SystemExit(
            "public.ig_audit_run is empty - no audit has ever been recorded, so "
            "there is nothing to report.\n"
            "Fix: run the audit first, e.g.\n"
            "  python scripts/audit_plannet.py --stage audit --schema bcbs_az_new")
    if not rows:
        raise SystemExit(
            f"no audit run {run_id} in public.ig_audit_run.\n"
            "Fix: pick an existing one with\n"
            "  SELECT run_id, started_at, finished_at FROM public.ig_audit_run "
            "ORDER BY run_id DESC;\n"
            "or omit --run-id to use the most recent.")
    return rows[0]


def zero_row_reason(tbl_rows: int, prof_rows: int, table_absent: bool) -> str:
    """Why a (schema, profile, lob) cell has no rows.

    Four different findings used to render as one unlabelled `0`. Only the
    first two say anything about what the payer publishes; the last says
    nothing at all about the payer and must never be read as a gap.
    """
    if int(tbl_rows) == 0:
        if table_absent:
            return "table not present in the schema - nothing captured"
        return ("table present and empty - the payer published none, or the "
                "capture returned none")
    if int(prof_rows) == 0:
        return "table has rows, but none match this profile"
    return "profile has rows, but none carry this lob"


def capture_window(conn, schemas: list[str]) -> list[tuple]:
    """(schema, resource table, first ingest, last ingest, rows) per cell.

    Derived, never restated. A capture window written down in prose is a date
    that was true once; this one is whatever the rows say today.

    Empty and absent tables are KEPT, with rows = 0 and no timestamps. Skipping
    them dropped exactly the tables where the report's zero-row section has to
    tell three different facts apart -- a table the payer publishes nothing
    for, a table we captured nothing from, and a profile that does not apply --
    and left the reader to infer which from a table that was not there.
    """
    out = []
    for schema in schemas:
        for tbl in RESOURCE_TABLES.values():
            present = q(conn, "SELECT to_regclass(%(n)s)", {"n": f"{schema}.{tbl.name}"})
            if not present or present[0][0] is None:
                out.append((schema, tbl.name, None, None, None))
                continue
            r = q(conn, f"SELECT min(ingested_at), max(ingested_at), count(*)::bigint "
                        f'FROM "{schema}"."{tbl.name}"')[0]
            out.append((schema, tbl.name, r[0], r[1], int(r[2])))
    return out


def build(run_id: Optional[int], out_path: str) -> str:
    engine = get_engine()
    with engine.connect() as conn:
        conn.exec_driver_sql("SET statement_timeout = 0")
        if run_id is None:
            run_id = q(conn, "SELECT max(run_id) FROM public.ig_audit_run")[0][0]
        run = require_run(run_id, q(
            conn, "SELECT run_id, started_at, finished_at, ig_package, schemas, "
                  "n_elements, notes FROM public.ig_audit_run WHERE run_id = %(r)s",
            {"r": run_id}))
        schemas = list(run[4])

        L = [f"# Plan-Net 1.2.0 conformance audit - {', '.join(schemas)}", ""]
        L.append(f"IG package: `{run[3]}`  ")
        L.append(f"Audit run: **{run[0]}**, started {run[1]}, finished {run[2]}  ")
        L.append(f"Elements tested: **{run[5]}** (the complete Plan-Net 1.2.0 "
                 f"snapshot surface across 9 profiles)")
        L.append("")
        L.append("Every figure below is over **100% of the rows we hold**. Nothing "
                 "is sampled. Every element is read from the `resource` JSONB, "
                 "never from an ETL-extracted column.")
        L.append("")

        # -- capture window ---------------------------------------------
        window = capture_window(conn, schemas)
        stamped = [w for w in window if w[2]]
        if stamped:
            L.append("## What was captured, and when")
            L.append("")
            lo = min(w[2] for w in stamped)
            hi = max(w[3] for w in stamped)
            L.append(f"Ingest timestamps on the audited rows run from **{lo}** to "
                     f"**{hi}**. Both ends are `min`/`max` of `ingested_at` over the "
                     f"tables listed here, not a date anyone typed.")
            L.append("")
            L.append(f"{len(window) - len(stamped)} of the {len(window)} "
                     f"(schema, table) pairs hold no rows and carry no window. "
                     f"They are listed rather than dropped: a table that is "
                     f"absent, a table that is present and empty, and a table "
                     f"full of rows that score 0% are three different facts, and "
                     f"the sections below can only keep them apart if this one "
                     f"says which is which.")
            L.append("")
            L.append(table(["schema", "table", "first ingest", "last ingest", "rows"],
                           [[a, b, c or "not measured", d or "not measured",
                             "table absent" if e is None else f"{e:,}"]
                            for a, b, c, d, e in window]))

        # -- shred ------------------------------------------------------
        L.append("## The shred")
        L.append("")
        L.append("`public.plannet_element_value` holds one row per (resource, IG "
                 "element, parent occurrence, occurrence, coding). `ordinal` is the "
                 "occurrence's index **within its parent instance** and "
                 "`parent_ordinal` says which parent instance that is - so FHIR "
                 "cardinality, which is defined per parent instance, is counted the "
                 "way FHIR defines it. Checks are SQL over that table.")
        L.append("")
        shred = q(conn, """
            SELECT schema_name, resource_type, count(*)::bigint,
                   count(DISTINCT resource_id)::bigint
            FROM public.plannet_element_value GROUP BY 1, 2 ORDER BY 1, 2""")
        L.append(table(["schema", "resource type", "shred rows", "resources shredded"],
                       [[a, b, f"{c:,}", f"{d:,}"] for a, b, c, d in shred]))
        total = q(conn, "SELECT count(*)::bigint, "
                        "pg_size_pretty(pg_total_relation_size('public.plannet_element_value'))"
                        " FROM public.plannet_element_value")[0]
        secs = q(conn, "SELECT sum(seconds) FROM public.ig_shred_progress")[0][0] or 0
        L.append(f"**Total: {total[0]:,} shred rows, {total[1]} on disk, "
                 f"{secs / 3600:.1f} h of shred time.**")
        L.append("")

        # -- LOB --------------------------------------------------------
        L.append("## Lines of business, and one thing that is not one")
        L.append("")
        L.append(LOB_NOTE)
        lob = q(conn, """
            SELECT schema_name, resource_type, lob, max(n_rows)::bigint
            FROM public.ig_audit WHERE run_id = %(r)s
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""", {"r": run_id})
        L.append(table(["schema", "resource type", "lob", "rows"],
                       [[a, b, c, f"{d:,}"] for a, b, c, d in lob]))
        ods = q(conn, """
            SELECT a.schema_name, max(a.n_rows) FILTER (WHERE a.lob = 'ods')::bigint,
                   max(a.n_rows)::bigint
            FROM public.ig_audit a
            WHERE a.run_id = %(r)s AND a.resource_type = 'Organization'
            GROUP BY 1 HAVING max(a.n_rows) FILTER (WHERE a.lob = 'ods') > 0
            ORDER BY 1""", {"r": run_id})
        if ods:
            # n_to_ods is counted over RESOLVED references only (measure_refs
            # can only read meta.source off a row that exists), so n_resolved
            # is its denominator. Rendering it over n_refs -- which includes
            # dangling -- understates the ods share by exactly the dangling
            # rate, and the dangling rate here is not small. The whole "nothing
            # points at ods" conclusion rests on this figure.
            inbound = q(conn, """
                SELECT r.schema_name, r.element_path, sum(r.n_refs)::bigint,
                       sum(r.n_resolved)::bigint, sum(r.n_to_ods)::bigint
                FROM public.ig_audit_ref r
                WHERE r.run_id = %(r)s AND r.target_type = 'Organization'
                GROUP BY 1, 2 ORDER BY 3 DESC""", {"r": run_id})
            L.append("")
            for schema, n_ods, n_max in ods:
                L.append(f"In `{schema}`, **{n_ods:,} of {n_max:,} Organizations "
                         f"({pct(n_ods, n_max)})** carry `ods`. Whether that "
                         f"population belongs beside the other two depends on "
                         f"whether anything points at it:")
            L.append("")
            L.append(table(["schema", "referencing element",
                            "references to an Organization",
                            "of those, resolved",
                            "of the resolved, to an `ods` Organization"],
                           [[a, f"`{b}`", f"{c:,}", f"{d:,} ({pct(d, c)})",
                             f"{e:,} ({pct(e, d)})"]
                            for a, b, c, d, e in inbound]))
            L.append("")
            L.append("The `ods` share is taken over **resolved** references, not "
                     "over all of them: a dangling reference names an Organization "
                     "that is not in the table, so nothing can be read off its "
                     "`meta.source` and it belongs in neither the numerator nor the "
                     "denominator. The dangling column is left in view because it "
                     "bounds how much of this is knowable at all.")
            L.append("")
            L.append("An Organization population that nothing references is a "
                     "provenance dump, not a line of business. Read the `ods` rows "
                     "as records the payer holds, not as a product anyone can be "
                     "enrolled in, and do not average it against the other two.")
            L.append("")

        # -- completeness ----------------------------------------------
        L.append("## Completeness by profile")
        L.append("")
        L.append("`elements populated` counts elements with at least one populated "
                 "row. There is no row-weighted mustSupport column here, and that "
                 "is deliberate - see *What this does not show*.")
        L.append("")
        L.append("A cell with **no rows** renders `not measured`, not `0.0%`. "
                 "Every element in an empty cell is trivially unpopulated, so the "
                 "rate is arithmetically 0 and means nothing; printed as a "
                 "percentage it is indistinguishable from a cell full of rows that "
                 "genuinely publish nothing.")
        L.append("")
        comp = q(conn, """
            SELECT a.schema_name, a.profile, a.lob, max(a.n_rows)::bigint AS n_rows,
                   count(DISTINCT a.elem_idx)::bigint AS n_elements,
                   count(DISTINCT a.elem_idx) FILTER (WHERE a.n_present > 0)::bigint,
                   count(DISTINCT a.elem_idx) FILTER (WHERE d.must_support)::bigint,
                   count(DISTINCT a.elem_idx) FILTER (WHERE d.must_support
                                                      AND a.n_present > 0)::bigint
            FROM public.ig_audit a JOIN public.plannet_element d USING (elem_idx)
            WHERE a.run_id = %(r)s
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""", {"r": run_id})
        L.append(table(
            ["schema", "profile", "lob", "rows", "elements", "populated",
             "mustSupport", "MS populated"],
            [[a, b, c, f"{d:,}", e,
              f"{f_} ({pct(f_, e)})" if d else "not measured",
              g, f"{h} ({pct(h, g)})" if d else "not measured"]
             for a, b, c, d, e, f_, g, h in comp]))

        # -- scope: zero-row cells --------------------------------------
        L.append("## Scope: cells with no rows to test")
        L.append("")
        # A zero-row cell is not one fact. `PlannetEndpoint` with no rows at all
        # and `PlannetNetwork x hcaz` -- 36 Networks exist, none carry that LOB
        # -- rendered identically when this grouped by (schema, profile, lob)
        # alone, collapsing "the payer publishes none", "we captured none" and
        # "the profile does not apply to this LOB" into a single unlabelled 0.
        # tbl_rows and prof_rows are what tell them apart, and the capture
        # window above is what separates an absent table from an empty one.
        absent = {(s, t) for s, t, _c, _d, n in window if n is None}
        tbl_of = {rt: t.name for rt, t in RESOURCE_TABLES.items()}
        nodata = q(conn, """
            WITH z AS (
              SELECT schema_name, profile, resource_type, lob,
                     count(DISTINCT elem_idx)::bigint AS n_elements
              FROM public.ig_audit WHERE run_id = %(r)s AND n_rows = 0
              GROUP BY 1, 2, 3, 4),
            tot AS (
              SELECT schema_name, resource_type, max(n_rows)::bigint AS tbl_rows
              FROM public.ig_audit WHERE run_id = %(r)s GROUP BY 1, 2),
            prof AS (
              SELECT schema_name, profile, max(n_rows)::bigint AS prof_rows
              FROM public.ig_audit WHERE run_id = %(r)s GROUP BY 1, 2)
            SELECT z.schema_name, z.profile, z.resource_type, z.lob, z.n_elements,
                   t.tbl_rows, p.prof_rows
            FROM z JOIN tot t USING (schema_name, resource_type)
                   JOIN prof p USING (schema_name, profile)
            ORDER BY 1, 2, 4""", {"r": run_id})

        def why(schema, rtype, tbl_rows, prof_rows) -> str:
            return zero_row_reason(tbl_rows, prof_rows,
                                   (schema, tbl_of.get(rtype, rtype)) in absent)

        if nodata:
            L.append(f"**{len(nodata)} (schema, profile, lob) cell(s) hold zero "
                     f"rows**, covering {sum(int(r[4]) for r in nodata):,} element "
                     f"measurements with a denominator of 0.")
            L.append("")
            L.append("This is a **scope figure, not a score**. A profile with no "
                     "rows has not been measured at 0% - it has not been measured. "
                     "Nothing here is evidence of a defect and nothing here should "
                     "be averaged into a rate. What it does bound is the reach of "
                     "every other number in this report.")
            L.append("")
            L.append("The `why` column is derived, not asserted: it is read off "
                     "the table's own row count and the profile's own row count in "
                     "this same run, plus the capture window above. These are four "
                     "different findings - a resource type we never captured, a "
                     "resource type the payer publishes nothing for, a profile with "
                     "no instances, and a profile whose instances all carry a "
                     "different LOB - and only the middle two say anything about "
                     "what the payer publishes.")
            L.append("")
            L.append(table(["schema", "profile", "lob", "elements with no rows",
                            "rows in the table", "why"],
                           [[a, b, d, e, f"{int(f_):,}", why(a, c, f_, g)]
                            for a, b, c, d, e, f_, g in nodata]))
        else:
            L.append("Every (schema, profile, lob) cell in this run holds at least "
                     "one row.")
        L.append("")

        # -- required elements -----------------------------------------
        L.append("## Required elements (min > 0) that are absent or under-populated")
        L.append("")
        L.append("FHIR's `min` is per **parent instance**, so the denominator is the "
                 "number of parent occurrences the element could legally have "
                 "appeared under - `parent instances` below - and not the row count. "
                 "`method = root` means the parent IS the resource, so the two "
                 "coincide. `method = per-parent` means the element sits inside a "
                 "repeating or optional parent, where they do not: an element with "
                 "`min=1` under a `min=0` parent owes nothing at all on a row whose "
                 "parent is absent. Both methods are exact.")
        L.append("")
        req = q(conn, """
            SELECT a.schema_name, a.profile, a.element_path, a.lob, d.min_card,
                   d.card_method, a.n_rows::bigint, a.n_parent_instances::bigint,
                   a.n_min_violation::bigint, d.must_support
            FROM public.ig_audit a JOIN public.plannet_element d USING (elem_idx)
            WHERE a.run_id = %(r)s AND d.min_card > 0 AND a.n_min_violation > 0
            ORDER BY a.n_min_violation DESC, a.schema_name, a.profile, a.element_path""",
            {"r": run_id})
        # sum(n_min_violation) is a sum over the ROWS OF THIS RESULT SET, and
        # each row is a (schema, profile, element, lob) cell -- not a parent
        # instance. One parent instance short on five elements contributes five
        # here, so this total can exceed the number of resources in the schema.
        # Captioning it "parent instances" put a cell-sum in the same
        # rhetorical slot, and the same order of magnitude, as the ~3,525,000
        # fabricated violations the parent-presence fix was written to kill.
        L.append(f"**{len(req)} (schema, profile, element, lob) cells breach a "
                 f"min>0 cardinality**, totalling "
                 f"{sum(int(r[8]) for r in req):,} element-level shortfalls - one "
                 f"per (element, parent instance) pair. That is NOT a count of "
                 f"parent instances: a parent instance that is short on five "
                 f"elements is counted five times, once in each element's cell. "
                 f"Per-cell denominators are in the `parent instances` column "
                 f"below, and they are the only place a rate can honestly be "
                 f"taken.")
        L.append("")
        L.append(table(["schema", "profile", "element", "lob", "min", "method",
                        "rows", "parent instances", "violations", "mustSupport"],
                       [[a, b, f"`{c}`", d, e, f_, f"{g:,}", num(h),
                         f"{i:,} ({pct(i, h)})" if h else f"{i:,}",
                         "yes" if j else ""]
                        for a, b, c, d, e, f_, g, h, i, j in req]))
        minstate = q(conn, """
            SELECT CASE WHEN a.n_min_violation IS NULL THEN 'not measured'
                        WHEN a.n_min_violation < 0 THEN 'INCONSISTENT (negative)'
                        WHEN a.n_min_violation = 0 THEN 'fully populated'
                        ELSE 'breached' END AS state,
                   count(*)::bigint
            FROM public.ig_audit a JOIN public.plannet_element d USING (elem_idx)
            WHERE a.run_id = %(r)s AND d.min_card > 0
            GROUP BY 1 ORDER BY 2 DESC""", {"r": run_id})
        L.append("")
        L.append("Every `min > 0` cell in the run, by state. `not measured` is "
                 "reported as itself and never folded into `fully populated` - a "
                 "missing measurement is not a clean result. A negative count would "
                 "mean the shred holds more parent instances than the parent element "
                 "does, i.e. duplicated rows, and is surfaced rather than clamped:")
        L.append("")
        L.append(table(["state", "cells"], [[a, f"{b:,}"] for a, b in minstate]))
        L.append("")

        # -- max cardinality -------------------------------------------
        L.append("## Cardinality breaches (numeric max exceeded)")
        L.append("")
        L.append("A parent instance breaches `max = N` iff it holds an occurrence at "
                 "index N+1 **within that parent**. The count below is of breaching "
                 "parent instances, against the parent instances at risk.")
        L.append("")
        mx = q(conn, """
            SELECT a.schema_name, a.profile, a.element_path, a.lob, d.max_card,
                   d.card_method, a.n_parent_instances::bigint,
                   a.n_max_violation::bigint
            FROM public.ig_audit a JOIN public.plannet_element d USING (elem_idx)
            WHERE a.run_id = %(r)s AND coalesce(a.n_max_violation, 0) > 0
            ORDER BY a.n_max_violation DESC""", {"r": run_id})
        if mx:
            L.append(table(["schema", "profile", "element", "lob", "max", "method",
                            "parent instances", "breaches"],
                           [[a, b, f"`{c}`", d, e, f_, num(g),
                             f"{h:,} ({pct(h, g)})" if g else f"{h:,}"]
                            for a, b, c, d, e, f_, g, h in mx]))
        else:
            L.append("None. No element exceeds its declared numeric maximum in any "
                     "parent instance anywhere in the audited data.")
        L.append("")

        # -- validity ---------------------------------------------------
        L.append("## Invalid values by type")
        L.append("")
        cover = q(conn, """
            SELECT count(*)::bigint,
                   count(*) FILTER (WHERE validity_kind IS NOT NULL)::bigint
            FROM public.plannet_element""")[0]
        L.append(f"A value-level type check exists for **{cover[1]} of the "
                 f"{cover[0]} elements** ({pct(cover[1], cover[0])}). The other "
                 f"{cover[0] - cover[1]} declare a type this audit has no lexical "
                 f"test for - complex types, backbone elements and open choice "
                 f"elements are structural, not lexical - so a clean run says "
                 f"nothing about them either way. They are enumerated at the end of "
                 f"this section rather than left implied.")
        L.append("")
        L.append("Checks are chosen by the element's declared type: `date` / "
                 "`dateTime` / `instant` / `time` against the FHIR regex, `uri` / "
                 "`canonical` non-empty and whitespace-free, `url` additionally "
                 "requiring a scheme, `code` non-empty and whitespace-free (stricter "
                 "than FHIR's own regex, which permits single internal spaces), "
                 "`boolean` actually boolean, `positiveInt`/`unsignedInt`/`integer` "
                 "integral and in range, `Reference` carrying at least one of "
                 "reference / identifier / display.")
        L.append("")
        inv = q(conn, """
            SELECT a.schema_name, a.profile, a.element_path, a.lob, d.validity_kind,
                   a.n_occurrences::bigint, a.n_invalid_occ::bigint,
                   a.n_invalid_rows::bigint
            FROM public.ig_audit a JOIN public.plannet_element d USING (elem_idx)
            WHERE a.run_id = %(r)s AND coalesce(a.n_invalid_occ, 0) > 0
            ORDER BY a.n_invalid_occ DESC""", {"r": run_id})
        # Grouped by (schema, profile, lob, type), NOT by type alone. Pooling
        # sum(n_occurrences) across every (profile, LOB) cell is the §4a defect
        # in Track A's own report: Organization is the majority of rows and
        # `ods` is the majority of Organization, so a pooled rate is
        # structurally the ods dump's rate diluted by everything else. The
        # report's own non-conclusions say `ods` must not go into a per-LOB
        # comparison; dissolving it into a single total is worse than comparing
        # it, because there is then nothing to point at.
        summary = q(conn, """
            SELECT a.schema_name, a.profile, a.lob, d.validity_kind,
                   sum(a.n_occurrences)::bigint, sum(a.n_invalid_occ)::bigint,
                   count(*) FILTER (WHERE a.n_invalid_occ > 0)::bigint
            FROM public.ig_audit a JOIN public.plannet_element d USING (elem_idx)
            WHERE a.run_id = %(r)s AND d.validity_kind IS NOT NULL
            GROUP BY 1, 2, 3, 4 HAVING sum(a.n_occurrences) > 0
            ORDER BY 6 DESC, 5 DESC""", {"r": run_id})
        L.append(f"One row per (schema, profile, lob, type) cell - "
                 f"{len(summary)} of them, worst first. There is deliberately no "
                 f"pooled all-profiles row: `ods` alone is most of the Organization "
                 f"population, so a total would report its rate diluted by row "
                 f"count and call it the corpus's.")
        L.append("")
        L.append(table(["schema", "profile", "lob", "type", "occurrences checked",
                        "invalid", "cells affected"],
                       [[a, b, c, d, f"{e:,}", f"{f_:,} ({pct(f_, e)})", g]
                        for a, b, c, d, e, f_, g in summary[:60]]))
        if len(summary) > 60:
            L.append(f"...and {len(summary) - 60} more cells with at least one "
                     f"type-checked occurrence.")
            L.append("")
        if inv:
            L.append("")
            L.append("Detail, worst first:")
            L.append("")
            L.append(table(["schema", "profile", "element", "lob", "type",
                            "occurrences", "invalid occ", "rows affected"],
                           [[a, b, f"`{c}`", d, e, f"{f_:,}", f"{g:,} ({pct(g, f_)})",
                             f"{h:,}"] for a, b, c, d, e, f_, g, h in inv]))
        else:
            L.append("")
            L.append("No invalid values found in any type-checked element.")
        L.append("")
        novalid = q(conn, """
            SELECT d.profile, d.element_path, array_to_string(d.types, ', ')
            FROM public.plannet_element d
            WHERE d.validity_kind IS NULL ORDER BY 1, 2""")
        L.append(f"**{len(novalid)} elements carry no value-level type check.** They "
                 f"are listed rather than merely counted, so nobody reads their "
                 f"silence as a pass:")
        L.append("")
        L.append(table(["profile", "element", "declared types"],
                       [[a, f"`{b}`", c] for a, b, c in novalid[:60]]))
        if len(novalid) > 60:
            L.append(f"...and {len(novalid) - 60} more (`SELECT profile, "
                     f"element_path FROM public.plannet_element WHERE validity_kind "
                     f"IS NULL`).")
        L.append("")

        # -- bindings ---------------------------------------------------
        L.append("## Terminology bindings")
        L.append("")
        L.append("Value sets are expanded offline from the local package closure by "
                 "`provdir.quality.bindings.expand`. That expander returns **codes "
                 "only** - it was written against ETL columns that discard the "
                 "coding `system`. Reading the JSONB restores the system, so "
                 "membership is checked on both halves here: `code_member` is the "
                 "code-level check the earlier binding runs reported, "
                 "`system_member` is new and catches a right code carried under the "
                 "wrong CodeSystem.")
        L.append("")
        # (schema, profile, lob, strength), for the same reason as the validity
        # summary above: grouping by (schema, strength) pools every profile and
        # every LOB into one rate, and the largest contributor to that pool is
        # the `ods` Organization dump the report elsewhere refuses to put in a
        # per-LOB comparison at all.
        bsum = q(conn, """
            SELECT c.schema_name, c.profile, c.lob, c.strength,
                   sum(c.n_occurrences)::bigint,
                   sum(c.n_occurrences) FILTER (WHERE c.code_member IS FALSE)::bigint,
                   sum(c.n_occurrences) FILTER (WHERE c.system_member IS FALSE)::bigint,
                   sum(c.n_occurrences) FILTER (WHERE c.code_member IS NULL)::bigint
            FROM public.ig_audit_code c WHERE c.run_id = %(r)s
            GROUP BY 1, 2, 3, 4 ORDER BY 5 DESC""", {"r": run_id})
        L.append(f"One row per (schema, profile, lob, strength) cell - "
                 f"{len(bsum)} of them, largest first. Not pooled, for the reason "
                 f"given under *Invalid values by type*.")
        L.append("")
        L.append(table(["schema", "profile", "lob", "strength", "coded occurrences",
                        "code not in VS", "system not in VS", "VS not expandable"],
                       [[a, b, c, d, f"{e:,}", f"{f_ or 0:,} ({pct(f_ or 0, e)})",
                         f"{g or 0:,} ({pct(g or 0, e)})", f"{h or 0:,}"]
                        for a, b, c, d, e, f_, g, h in bsum[:60]]))
        if len(bsum) > 60:
            L.append(f"...and {len(bsum) - 60} more cells.")
            L.append("")
        L.append("")

        # -- concepts with no code --------------------------------------
        L.append("### CodeableConcepts that carry no code")
        L.append("")
        L.append("A `CodeableConcept` published as `{\"text\": \"Cardiology\"}` with "
                 "no `coding` array scores as fully populated in the completeness "
                 "table above and never enters the binding denominator at all - the "
                 "code inventory is built from occurrences that have a code. Under a "
                 "**required** binding that is a conformance failure rendered as a "
                 "clean pass, so it is counted here instead of vanishing.")
        L.append("")
        txt = q(conn, """
            SELECT a.schema_name, a.profile, a.element_path, a.lob,
                   coalesce(d.binding_strength, '(unbound)'),
                   a.n_occurrences::bigint, a.n_concept_no_coding::bigint,
                   a.n_concept_text_only::bigint
            FROM public.ig_audit a JOIN public.plannet_element d USING (elem_idx)
            WHERE a.run_id = %(r)s AND coalesce(a.n_concept_no_coding, 0) > 0
            ORDER BY a.n_concept_no_coding DESC""", {"r": run_id})
        if txt:
            L.append(f"**{len(txt)} (schema, profile, element, lob) cells publish a "
                     f"CodeableConcept with no `coding`**, and summing those cells' "
                     f"`no coding` counts gives "
                     f"{sum(int(r[6]) for r in txt):,} such occurrences, of which "
                     f"{sum(int(r[6]) for r in txt if r[4] == 'required'):,} sit "
                     f"under a **required** binding, where a code is not optional.")
            L.append("")
            L.append(table(["schema", "profile", "element", "lob", "binding",
                            "occurrences", "no coding", "of those, text only"],
                           [[a, b, f"`{c}`", d, e, f"{f_:,}",
                             f"{g:,} ({pct(g, f_)})", f"{h:,}"]
                            for a, b, c, d, e, f_, g, h in txt]))
        else:
            L.append("None. Every CodeableConcept occurrence in the audited data "
                     "carries at least one `coding`.")
        L.append("")
        L.append("Non-membership by value set:")
        L.append("")
        bvs = q(conn, """
            SELECT c.schema_name, d.binding_valueset, c.strength,
                   sum(c.n_occurrences)::bigint,
                   sum(c.n_occurrences) FILTER (WHERE c.code_member IS FALSE)::bigint,
                   count(DISTINCT c.code) FILTER (WHERE c.code_member IS FALSE)::bigint,
                   sum(c.n_occurrences) FILTER (WHERE c.system_member IS FALSE)::bigint
            FROM public.ig_audit_code c JOIN public.plannet_element d USING (elem_idx)
            WHERE c.run_id = %(r)s
            GROUP BY 1, 2, 3
            HAVING sum(c.n_occurrences) FILTER (WHERE c.code_member IS FALSE) > 0
                OR sum(c.n_occurrences) FILTER (WHERE c.system_member IS FALSE) > 0
            ORDER BY 5 DESC NULLS LAST""", {"r": run_id})
        L.append(table(["schema", "value set", "strength", "occurrences",
                        "code non-member", "distinct bad codes", "system non-member"],
                       [[a, f"`{(b or '').rsplit('/', 1)[-1]}`", c, f"{d:,}",
                         f"{e or 0:,}", f_ or 0, f"{g or 0:,}"]
                        for a, b, c, d, e, f_, g in bvs]))
        L.append("")
        L.append("Worst individual non-member codes:")
        L.append("")
        bad = q(conn, """
            SELECT c.schema_name, c.element_path, c.lob, c.system, c.code,
                   c.n_occurrences::bigint, c.n_rows::bigint, c.strength
            FROM public.ig_audit_code c
            WHERE c.run_id = %(r)s AND c.code_member IS FALSE
            ORDER BY c.n_occurrences DESC LIMIT 60""", {"r": run_id})
        L.append(table(["schema", "element", "lob", "system", "code", "occurrences",
                        "rows", "strength"],
                       [[a, f"`{b}`", c, d, f"`{e}`", f"{f_:,}", f"{g:,}", h]
                        for a, b, c, d, e, f_, g, h in bad]))
        L.append("")

        # -- referential integrity --------------------------------------
        L.append("## Referential integrity")
        L.append("")
        L.append("Every `reference` string at every Reference-typed element, split "
                 "into `Type/id` and resolved against the same schema's table for "
                 "that Type. **Nested extension references are in here on the same "
                 "footing as top-level ones** - they are just rows in the shred.")
        L.append("")
        refs = q(conn, """
            SELECT schema_name, profile, element_path, lob, target_type, declared_ok,
                   n_refs::bigint, n_distinct::bigint, n_resolved::bigint,
                   n_dangling::bigint, n_to_network::bigint
            FROM public.ig_audit_ref WHERE run_id = %(r)s
            ORDER BY schema_name, profile, element_path, lob, target_type""",
            {"r": run_id})
        L.append(table(["schema", "profile", "element", "lob", "target type",
                        "declared?", "refs", "distinct targets", "resolved",
                        "dangling", "to a Network"],
                       [[a, b, f"`{c}`", d, e, "yes" if f_ else "**NO**", f"{g:,}",
                         f"{h:,}", f"{i:,} ({pct(i, g)})", f"{j:,}", f"{k:,}"]
                        for a, b, c, d, e, f_, g, h, i, j, k in refs]))
        L.append("")
        untouched = q(conn, """
            SELECT d.profile, d.element_path, d.ref_targets
            FROM public.plannet_element d
            WHERE d.is_reference AND NOT EXISTS (
              SELECT 1 FROM public.ig_audit_ref r
              WHERE r.run_id = %(r)s AND r.elem_idx = d.elem_idx)
            ORDER BY 1, 2""", {"r": run_id})
        n_ref_elems = q(conn, "SELECT count(*) FROM public.plannet_element "
                              "WHERE is_reference")[0][0]
        L.append(f"**{len(untouched)} of the {n_ref_elems} Reference-typed elements "
                 f"carry no reference at all:**")
        L.append("")
        L.append(table(["profile", "element", "declared targets"],
                       [[a, f"`{b}`", ", ".join(c)] for a, b, c in untouched]))
        L.append("")

        # -- untestable -------------------------------------------------
        L.append("## Elements that could not be fully tested, and why")
        L.append("")
        noted = q(conn, """
            SELECT profile, element_path, card_method, notes
            FROM public.plannet_element
            WHERE array_length(notes, 1) > 0 ORDER BY profile, element_path""")
        buckets: dict[str, list] = {}
        n_internal = 0
        for prof, path, method, notes in noted:
            for n in notes:
                key = n.split(";")[0].split("(")[0].strip()
                if key == INTERNAL_NOTE:
                    n_internal += 1
                    continue
                buckets.setdefault(key, []).append((prof, path))
        for key, items in sorted(buckets.items(), key=lambda kv: -len(kv[1])):
            L.append(f"**{key}** - {len(items)} element(s)")
            L.append("")
            L.append(table(["profile", "element"],
                           [[p, f"`{e}`"] for p, e in items[:40]]))
            if len(items) > 40:
                L.append(f"...and {len(items) - 40} more.")
            L.append("")
        if n_internal:
            L.append(f"({n_internal} elements also carry the note "
                     f"*\"{INTERNAL_NOTE}\"*. That is a detail of the path compiler, "
                     f"not a limitation: the element's path was rebuilt parent-first "
                     f"so a slice's children keep the slice's own discriminator. "
                     f"Those elements are tested normally and are not listed here.)")
            L.append("")
        unexp = q(conn, """
            SELECT DISTINCT d.binding_valueset, d.binding_strength
            FROM public.ig_audit_code c JOIN public.plannet_element d USING (elem_idx)
            WHERE c.run_id = %(r)s AND c.code_member IS NULL
            ORDER BY 1""", {"r": run_id})
        if unexp:
            L.append("**Value sets that could not be expanded offline**, so their "
                     "bound elements have a code inventory but no membership verdict:")
            L.append("")
            L.append(table(["value set", "strength"], [[f"`{a}`", b] for a, b in unexp]))
            L.append("")

        # -- non-conclusions --------------------------------------------
        ms_max0 = q(conn, """
            SELECT profile, element_path FROM public.plannet_element
            WHERE must_support AND max_card = '0' ORDER BY 1, 2""")
        L.append("## What this does not show")
        L.append("")
        L.append("- **This is not a D3 structural conformance result.** Nothing here "
                 "ran the HL7 validator. It is an IG-driven read of the published "
                 "JSON: completeness, cardinality, type validity, terminology "
                 "binding and referential integrity. A resource can pass every check "
                 "above and still fail structural validation.")
        L.append("- **`n_present / n_rows` is not a compliance score.** It is how "
                 "often an element is populated. Most Plan-Net elements are "
                 "`mustSupport`, which means *send it if you have it* - not *you "
                 "must populate it*. A low number can be a data gap, a population "
                 "that genuinely lacks the attribute, or an element that does not "
                 "apply.")
        if ms_max0:
            L.append(f"- **There is no row-weighted mustSupport figure in this "
                     f"report, on purpose.** Its denominator treats every "
                     f"(mustSupport element x row) pair as an obligation, which "
                     f"mustSupport is not, and it ignores element depth entirely - a "
                     f"nested sub-element counts the same as a top-level one. "
                     f"{len(ms_max0)} element{'s are' if len(ms_max0) > 1 else ' is'} "
                     f"`mustSupport` **and** pinned to `max=0`, i.e. must not be sent "
                     f"at all (`{ms_max0[0][0]}` / `{ms_max0[0][1]}`); a row-weighted "
                     f"average scores those as missing on every row.")
        L.append("- **A `min>0` breach under an optional parent is not a "
                 "violation.** Cardinality here is counted per parent instance. An "
                 "element with `min=1` inside a `min=0` parent owes nothing when the "
                 "parent is absent, and this report does not charge it.")
        L.append("- **A zero is not proof of absence.** It means the compiled "
                 "jsonpath matched nothing in the rows we hold. That is normally a "
                 "payer fact - the paths come verbatim from the StructureDefinitions "
                 "- but it is not the same claim.")
        L.append("- **`ods` is not a line of business.** It is a source system in a "
                 "provenance field. Do not put it in a per-LOB comparison.")
        L.append("- **\"100% of rows\" means 100% of the rows we hold.** It is not a "
                 "claim about the payer's directory in full. What we captured, and "
                 "when, is at the top of this report.")
        L.append("- **Only what the IG declares was tested.** The element surface is "
                 "the whole Plan-Net 1.2.0 snapshot and nothing beyond it. Vendor "
                 "extensions and undeclared keys are invisible to an IG-driven audit "
                 "by construction; `audit_plannet.py --stage verify` inventories "
                 "them separately, by a **recursive** walk for extension `url` "
                 "values at any depth (`strict $.**.url`) rather than at the two "
                 "depths an IG-shaped guess would look at, and it scores each URL "
                 "against the checks compiled for that table alone.")
        L.append("")

    text = "\n".join(L) + "\n"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(text, encoding="utf-8")
    return text


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", type=int, default=None)
    ap.add_argument("--out", default="output/quality/ig_audit_bcbs_az_new.md")
    args = ap.parse_args(argv)
    text = build(args.run_id, args.out)
    print(f"wrote {args.out} ({len(text):,} chars)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

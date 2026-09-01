"""Classify and aggregate HL7 validator findings into per-payer conformance.

The validator emits every issue it can see, which at this scale is a mix of
three very different things. Reporting them as one number would be wrong in
both directions, so they are separated here rather than at ingest -- the raw
issues stay complete in ``public.conformance_issue`` and this classification is
a reporting decision that can be revised without re-validating 118k instances.

``profile``
    Genuine violations of the Plan-Net profile: cardinality (a SHALL element
    absent), a fixed value not matched, wrong type, a failed invariant. This is
    the headline conformance signal.

``binding-required`` / ``binding-extensible``
    Genuine value-set binding failures, decided LOCALLY: even with ``-tx n/a``
    the validator expands value sets whose content ships in the loaded IG
    packages, and Plan-Net enumerates all of its required bindings inline (853
    NUCC codes, 14 complete CodeSystems). A ``Terminology_TX_NoValid_*`` issue
    therefore reflects the payer's data, not the missing terminology server --
    an earlier revision of this module mis-bucketed these as
    ``terminology-unavailable`` and silently discarded 12k required-binding
    errors. ``binding-required`` (a required-strength binding not satisfied) is
    a conformance failure in its own right, reported as a score column separate
    from structural ``profile`` violations; ``binding-extensible`` is
    reportable but advisory, matching the binding strength's meaning.

``extension-unresolvable``
    The instance carries an extension whose StructureDefinition cannot be
    resolved -- almost always a payer's own unpublished extension. Real, and
    worth reporting (a consumer cannot interpret it either), but it measures
    "publishes private extensions", not "violates the profile". Left in the
    headline it would swamp everything: it alone puts one large payer at a 100%
    error rate.

``terminology-unavailable``
    True artifacts of running without a terminology server: code systems whose
    CONTENT the loaded packages do not carry (``us-npi``, ``urn:ietf:bcp:47``,
    usps displays, payer-proprietary systems) cannot be resolved locally, so
    "unknown code system" says nothing about the payer. These MUST NOT count
    against a score. The proprietary-system inventory is still reportable --
    see unknown_systems().

``best-practice``
    Advisory constraints such as dom-6 (narrative present). Not conformance.
"""

from __future__ import annotations

import json

import psycopg

# Locally-decided binding failures: the validator HAD the value-set content
# (from the IG packages) and the code was not a member. Real findings.
BINDING_REQUIRED_IDS = {
    "Terminology_TX_NoValid_1_CC",
    "Terminology_TX_NoValid_1",
}
BINDING_EXTENSIBLE_IDS = {
    "Terminology_TX_NoValid_2_CC",
    "Terminology_TX_NoValid_2",
}
# Binding failures whose id does not encode the binding strength, so the
# severity the validator assigned routes them (error => the binding was
# required, warning => extensible). Verified against run-2 details text:
# NoValid_16/_17 are the primitive-code variants ("value 'Ohio' not in USPS",
# "'m' not in DaysOfWeek ... a code is required"); Code_ValueSet[_Ext] fire
# when a CodeableConcept carries NO code at all where the binding demands one
# ("No code provided, and a code is required/should be provided");
# UNABLE_TO_INFER_CODESYSTEM fires for a code invalid in every system of the
# bound VS ('website' in contact-point-system) -- inference over locally
# expanded content only fails when the code is not a member.
BINDING_SEVERITY_ROUTED_IDS = {
    "None_of_the_provided_codes_are_in_the_value_set_one",
    "Terminology_TX_NoValid_16",
    "Terminology_TX_NoValid_17",
    "Terminology_TX_Code_ValueSet",
    "Terminology_TX_Code_ValueSet_Ext",
    "UNABLE_TO_INFER_CODESYSTEM",
}
# True no-tx artifacts: the code SYSTEM's content is absent from the loaded
# packages (or, for the _VERSION_NONE / TX_WARNING pair, the versioned lookup
# or server round-trip is impossible locally), so nothing about the payer's
# code can be decided.
TERMINOLOGY_IDS = {
    "Terminology_TX_System_Unknown",
    "Unknown_Code_in_Version",
    "UNKNOWN_CODESYSTEM",
    "UNKNOWN_CODESYSTEM_VERSION",
    "UNKNOWN_CODESYSTEM_VERSION_NONE",
    "TERMINOLOGY_TX_WARNING",
    "Terminology_TX_Code_ValueSet_MISSING",
}
EXTENSION_IDS = {"Extension_EXT_Unknown_NotHere", "Extension_EXT_Unknown"}
# Advisory data-quality notes about codings that cannot be validated at all
# (no system / no code present) -- kin to dom-6, not profile violations.
BEST_PRACTICE_IDS = {
    "Coding_has_no_system__cannot_validate",
    "TERMINOLOGY_TX_SYSTEM_NO_CODE",
    "NO_CODE_PROVIDED_VERSION",
}

# A single CASE expression, so every consumer of these categories classifies
# identically instead of each report inventing its own filter. Order matters:
# artifact checks run before binding checks so an unknown-system issue never
# counts as a binding failure, and the severity-routed ids (binding failures
# whose message id does not encode the strength) land by strength.
CATEGORY_SQL = """
CASE
  WHEN message_id = ANY(%(tx)s) THEN 'terminology-unavailable'
  WHEN message_id IS NULL AND details LIKE 'A definition for CodeSystem%%'
       THEN 'terminology-unavailable'
  WHEN details LIKE 'Unknown Code System%%' THEN 'terminology-unavailable'
  WHEN message_id = ANY(%(bindreq)s) THEN 'binding-required'
  WHEN message_id = ANY(%(bindext)s) THEN 'binding-extensible'
  WHEN message_id = ANY(%(bindsev)s)
       THEN CASE WHEN severity IN ('error','fatal')
                 THEN 'binding-required' ELSE 'binding-extensible' END
  WHEN message_id = ANY(%(ext)s) THEN 'extension-unresolvable'
  WHEN message_id = ANY(%(bp)s) THEN 'best-practice'
  WHEN message_id LIKE 'http://hl7.org/fhir/StructureDefinition/DomainResource#dom-%%'
       THEN 'best-practice'
  ELSE 'profile'
END
"""

_PARAMS = {
    "tx": sorted(TERMINOLOGY_IDS),
    "bindreq": sorted(BINDING_REQUIRED_IDS),
    "bindext": sorted(BINDING_EXTENSIBLE_IDS),
    "bindsev": sorted(BINDING_SEVERITY_ROUTED_IDS),
    "ext": sorted(EXTENSION_IDS),
    "bp": sorted(BEST_PRACTICE_IDS),
}


# --------------------------------------------------------------------------
# Run selection. ONE rule, shared by every consumer.
#
# Three artifacts read a conformance run and each picked it differently: the
# map picked "most recorded instances", the report picked "newest finished",
# and the framework payload had ``CONF_RUN = 9`` typed into its source. Those
# rules agree only by accident, and they stop agreeing the moment a
# single-payer full-population run lands: it is both the newest AND the
# largest, so two of the three silently swap a 61-payer cross-payer artifact
# for a 1-payer run whose data was modified before validation, while the third
# keeps reporting the old run -- two published artifacts disagreeing with
# nothing anywhere to say why.
#
# The rule below refuses rather than guesses. Auto-selection must satisfy BOTH
# "widest payer set" and "most instances"; when those point at different runs
# the situation is exactly the one above, and the caller has to say which it
# meant (--conformance-run / --expect-payers).
# --------------------------------------------------------------------------

RUNS_SQL = """
SELECT r.run_id,
       r.instances,
       r.per_cell,
       (r.finished_at IS NOT NULL) AS finished,
       r.notes,
       count(i.*)::bigint AS recorded,
       count(DISTINCT i.payer_id)::int AS payers,
       coalesce(array_agg(DISTINCT i.payer_id)
                FILTER (WHERE i.payer_id IS NOT NULL), ARRAY[]::text[]) AS payer_ids
FROM public.conformance_run r
LEFT JOIN public.conformance_instance i ON i.run_id = r.run_id
GROUP BY r.run_id, r.instances, r.per_cell, r.finished_at, r.notes
ORDER BY r.run_id
"""


def _fetch_all(conn, sql: str) -> list[tuple]:
    """Run a parameterless query on either a psycopg or a SQLAlchemy connection.

    The consumers of this module do not share a driver handle -- the two report
    scripts hold a psycopg connection, the map builder holds a SQLAlchemy one --
    and duplicating the selection query per driver is exactly how the three
    selection rules drifted apart in the first place.
    """
    if hasattr(conn, "exec_driver_sql"):          # SQLAlchemy Connection
        return [tuple(r) for r in conn.exec_driver_sql(sql)]
    with conn.cursor() as cur:                    # psycopg Connection
        cur.execute(sql)
        return cur.fetchall()


def _as_notes(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return {}
    return {}


def fetch_runs(conn) -> list[dict]:
    """Every conformance_run, with what selection needs in order to judge it."""
    out = []
    for (run_id, instances, per_cell, finished, notes,
         recorded, payers, payer_ids) in _fetch_all(conn, RUNS_SQL):
        out.append({
            "run_id": int(run_id),
            "instances": int(instances) if instances is not None else None,
            "per_cell": per_cell,
            "finished": bool(finished),
            "notes": _as_notes(notes),
            "recorded": int(recorded or 0),
            "payers": int(payers or 0),
            "payer_ids": sorted(payer_ids or []),
        })
    return out


def run_usability(row: dict) -> tuple[bool, str]:
    """(usable, why). A run is usable only if it accounted for its own work.

    ``finished_at IS NOT NULL`` was the whole test, and it is not one: the
    stamp used to be written unconditionally, BEFORE the verification check,
    so a run that recorded 1,463,451 of 1,862,183 instances looked exactly like
    a complete one to every downstream report.

    ``notes.verified`` is the gate now. Runs predating that key are judged by
    the same arithmetic it encodes -- every instance the run claimed must have
    produced a row -- which is computable from the run row itself and so needs
    no backfill of historical rows.
    """
    if not row["finished"]:
        return False, "never finished (finished_at IS NULL)"
    verified = row["notes"].get("verified")
    n, planned = row["recorded"], row["instances"]
    if verified is True:
        return True, "notes.verified"
    if verified is False:
        return False, (f"notes.verified is false ({n:,} of {planned:,} "
                       "instances recorded)" if planned
                       else "notes.verified is false")
    if planned and n < planned:
        return False, ("pre-verification run, and it does not reconcile: "
                       f"{n:,} of {planned:,} instances recorded")
    return True, "pre-verification run, but its instance count reconciles"


def parse_expect_payers(raw: str | None):
    """``--expect-payers`` -> an exact count (int) or an exact set of payer ids.

    Shared so the CLIs cannot disagree about what the flag means.
    """
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    return {p.strip() for p in raw.split(",") if p.strip()}


def sample_provenance(row: dict) -> dict:
    """What was done to the data before it was validated. ``{}`` if unrecorded."""
    s = row["notes"].get("sample")
    return s if isinstance(s, dict) else {}


def describe_run(row: dict) -> str:
    prov = sample_provenance(row)
    bits = [f"run {row['run_id']}", f"{row['payers']} payer(s)"]
    if row["instances"] and row["recorded"] != row["instances"]:
        bits.append(f"{row['recorded']:,} of {row['instances']:,} instances")
    else:
        bits.append(f"{row['recorded']:,} instances")
    if prov:
        bits.append("per_cell=" + ("full population" if not prov.get("per_cell")
                                   else f"{prov['per_cell']:,}"))
        bits.append(f"carin_bb_stripped={bool(prov.get('carin_bb_stripped'))}")
    else:
        bits.append("sample provenance UNRECORDED")
    return ", ".join(bits)


def _payers_match(row: dict, expect) -> tuple[bool, str]:
    if expect is None:
        return True, ""
    if isinstance(expect, int):
        if row["payers"] == expect:
            return True, ""
        return False, f"has {row['payers']} payer(s), expected {expect}"
    got = set(row["payer_ids"])
    if got == expect:
        return True, ""
    missing = sorted(expect - got)
    extra = sorted(got - expect)
    parts = []
    if missing:
        parts.append(f"missing {len(missing)} ({', '.join(missing[:6])}"
                     + (", ..." if len(missing) > 6 else "") + ")")
    if extra:
        parts.append(f"unexpected {len(extra)} ({', '.join(extra[:6])}"
                     + (", ..." if len(extra) > 6 else "") + ")")
    return False, "payer set differs: " + "; ".join(parts)


def choose_run(rows: list[dict], *, run_id: int | None = None,
               expect_payers=None,
               allow_unverified: bool = False) -> tuple[int, str]:
    """Pick the conformance run to report on, or SystemExit saying why not.

    Pure over ``fetch_runs()`` output, so the rule is testable without a
    database -- and so all three consumers demonstrably share one rule.
    """
    by_id = {r["run_id"]: r for r in rows}

    if run_id is not None:
        row = by_id.get(int(run_id))
        if row is None:
            raise SystemExit(f"no conformance_run with run_id {run_id}")
        ok, why = run_usability(row)
        if not ok and not allow_unverified:
            raise SystemExit(
                f"FATAL: run {run_id} is not publishable: {why}.\n"
                "  A run that did not account for every instance it attempted "
                "reports a subset as if it were the whole.\n"
                "  Pass --allow-unverified to report on it anyway (and say so "
                "in the write-up).")
        pok, pwhy = _payers_match(row, expect_payers)
        if not pok:
            raise SystemExit(
                f"FATAL: run {run_id} does not match --expect-payers: {pwhy}.\n"
                f"  {describe_run(row)}")
        return row["run_id"], ("pinned: " + describe_run(row)
                               + (f" [UNVERIFIED: {why}]" if not ok else ""))

    usable, rejected = [], []
    for r in rows:
        ok, why = run_usability(r)
        if ok or allow_unverified:
            usable.append(r)
        else:
            rejected.append((r, why))
    if not usable:
        detail = "\n".join(f"  run {r['run_id']}: {why}" for r, why in rejected)
        raise SystemExit("FATAL: no publishable conformance run.\n" + detail
                         + "\n  Pass --conformance-run N --allow-unverified to "
                           "override, or re-run the validation.")

    if expect_payers is not None:
        matched = [r for r in usable if _payers_match(r, expect_payers)[0]]
        if not matched:
            detail = "\n".join(
                f"  {describe_run(r)} -- {_payers_match(r, expect_payers)[1]}"
                for r in usable)
            raise SystemExit("FATAL: no publishable conformance run matches "
                             f"--expect-payers.\n{detail}")
        pick = max(matched, key=lambda r: (r["recorded"], r["run_id"]))
        return pick["run_id"], "matched --expect-payers: " + describe_run(pick)

    widest = max(usable, key=lambda r: (r["payers"], r["recorded"], r["run_id"]))
    biggest = max(usable, key=lambda r: (r["recorded"], r["payers"], r["run_id"]))
    if widest["run_id"] != biggest["run_id"]:
        raise SystemExit(
            "FATAL: refusing to auto-select a conformance run -- the widest "
            "payer set and the largest instance count are different runs, so "
            "any automatic choice silently changes what the artifact "
            "measures.\n"
            f"  widest payer set : {describe_run(widest)}\n"
            f"  most instances   : {describe_run(biggest)}\n"
            "  Say which you mean: --conformance-run N, or --expect-payers "
            "<count|comma,separated,ids>.")
    return widest["run_id"], "auto-selected: " + describe_run(widest)


def select_run(conn, *, run_id: int | None = None, expect_payers=None,
               allow_unverified: bool = False) -> tuple[int, str]:
    """``choose_run`` against a live connection (psycopg or SQLAlchemy)."""
    return choose_run(fetch_runs(conn), run_id=run_id,
                      expect_payers=expect_payers,
                      allow_unverified=allow_unverified)


def latest_run(conn: psycopg.Connection) -> int | None:
    """Back-compat shim: the selected run, or None if nothing is publishable.

    No longer ``max(run_id) WHERE finished_at IS NOT NULL`` -- that predicate
    accepts a run that recorded a fraction of its instances, and would accept a
    single-payer run as the authority for a 61-payer artifact.
    """
    try:
        return select_run(conn)[0]
    except SystemExit:
        return None


# --------------------------------------------------------------------------
# The D3 aggregation unit, declared once.
#
# The framework declares "unweighted mean over (resource, LOB) cells".
# public.conformance_instance HAS NO LOB COLUMN: its grain is (run, payer,
# cell, instance) and `cell` is a bare resource name ('Organization',
# 'Network', ...), with LOB nowhere in the row. That unit is therefore NOT
# computable from Track B, and reporting it as though it were is the exact
# failure class this project exists to eliminate. The achievable unit is stated
# instead -- and it is the same unit in every artifact.
#
# Pooling (sum(clean)/sum(validated)) is not that unit. With Organization at
# 59% of rows and PractitionerRole at 143.8 issues/instance against Location's
# 2.1, a pooled rate is Organization's rate diluted by row count: the sampling
# design leaking into the headline. Pooled figures are still emitted, named
# `pooled_*` so they cannot be mistaken for the declared unit.
# --------------------------------------------------------------------------

D3_MIN_CELL = 30
D3_UNIT = ("unweighted mean over (payer, resource) cells; cells with fewer "
           f"than {D3_MIN_CELL} validated instances are suppressed")
D3_UNIT_CAVEAT = ("LOB is not recorded in public.conformance_instance, so the "
                  "framework's declared (resource, LOB) unit is not computable "
                  "from Track B; (payer, resource) is the finest grain the "
                  "table supports.")


def cell_mean_rate(cells) -> tuple[float | None, int, int]:
    """(unweighted mean clean rate, cells used, cells suppressed).

    ``cells`` is an iterable of (validated, clean) pairs, one per
    (payer, resource) cell.
    """
    pairs = [(int(n), int(c)) for n, c in cells]
    used = [(n, c) for n, c in pairs if n >= D3_MIN_CELL]
    n_sup = len(pairs) - len(used)
    if not used:
        return None, 0, n_sup
    return sum(c / n for n, c in used) / len(used), len(used), n_sup


def by_category(conn: psycopg.Connection, run_id: int) -> list[tuple]:
    """(category, severity, issue count, distinct instances) for a run."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {CATEGORY_SQL} AS category, severity, count(*), "
            "count(DISTINCT (payer_id, cell, instance)) "
            "FROM public.conformance_issue WHERE run_id = %(run)s "
            "GROUP BY 1, 2 ORDER BY 3 DESC",
            {**_PARAMS, "run": run_id},
        )
        return cur.fetchall()


def payer_scores(conn: psycopg.Connection, run_id: int) -> list[dict]:
    """Per-payer conformance: structural and binding, as separate columns.

    ``conformance`` counts only 'profile' violations (cardinality, fixed
    values, invariants); ``binding_conformance`` counts only 'binding-required'
    errors. Kept separate on purpose: they fail for different reasons and are
    fixed by different teams -- folding them together would hide which problem
    a payer actually has. Denominator is instances actually validated
    (conformance_instance), so a payer with no issues is distinguishable from
    one that was never sampled.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH cat AS (
              SELECT payer_id, cell, instance, ({CATEGORY_SQL}) AS category
              FROM public.conformance_issue
              WHERE run_id = %(run)s AND severity IN ('error','fatal')
            ), bad AS (
              SELECT DISTINCT payer_id, cell, instance FROM cat
              WHERE category = 'profile'
            ), bind AS (
              SELECT DISTINCT payer_id, cell, instance FROM cat
              WHERE category = 'binding-required'
            ), ext AS (
              SELECT DISTINCT payer_id, cell, instance FROM cat
              WHERE category = 'extension-unresolvable'
            )
            SELECT i.payer_id,
                   count(*) AS validated,
                   count(*) FILTER (WHERE b.instance IS NOT NULL) AS with_violation,
                   count(*) FILTER (WHERE d.instance IS NOT NULL) AS with_binding,
                   count(*) FILTER (WHERE e.instance IS NOT NULL) AS with_bad_ext,
                   count(DISTINCT i.cell) AS cells
            FROM public.conformance_instance i
            LEFT JOIN bad b ON b.payer_id = i.payer_id AND b.cell = i.cell
                           AND b.instance = i.instance
            LEFT JOIN bind d ON d.payer_id = i.payer_id AND d.cell = i.cell
                            AND d.instance = i.instance
            LEFT JOIN ext e ON e.payer_id = i.payer_id AND e.cell = i.cell
                           AND e.instance = i.instance
            WHERE i.run_id = %(run)s
            GROUP BY i.payer_id
            -- worst violation RATE first. Spelled out, not `ORDER BY 3 / n`:
            -- an ordinal stops being an ordinal the moment it is part of an
            -- expression (`3::numeric` is the literal three), which silently
            -- ordered by ascending sample size instead.
            ORDER BY count(*) FILTER (WHERE b.instance IS NOT NULL)::numeric
                     / nullif(count(*), 0) DESC NULLS LAST, 1
            """,
            {**_PARAMS, "run": run_id},
        )
        out = []
        for payer, n, bad_n, bind_n, ext_n, cells in cur.fetchall():
            out.append({
                "payer_id": payer, "validated": n, "with_violation": bad_n,
                "with_binding_violation": bind_n,
                "with_bad_extension": ext_n, "cells": cells,
                "violation_rate": round(100.0 * bad_n / n, 1) if n else None,
                "conformance": round(100.0 * (n - bad_n) / n, 1) if n else None,
                "binding_conformance": round(100.0 * (n - bind_n) / n, 1) if n else None,
            })
        return out


def cell_scores(conn: psycopg.Connection, run_id: int,
                category: str = "profile") -> list[tuple]:
    """(payer, cell, validated, with_violation) -- the gap grid, per category."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH bad AS (
              SELECT DISTINCT payer_id, cell, instance
              FROM public.conformance_issue
              WHERE run_id = %(run)s AND severity IN ('error','fatal')
                AND ({CATEGORY_SQL}) = %(cat)s
            )
            SELECT i.payer_id, i.cell, count(*),
                   count(*) FILTER (WHERE b.instance IS NOT NULL)
            FROM public.conformance_instance i
            LEFT JOIN bad b ON b.payer_id = i.payer_id AND b.cell = i.cell
                           AND b.instance = i.instance
            WHERE i.run_id = %(run)s
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
            {**_PARAMS, "run": run_id, "cat": category},
        )
        return cur.fetchall()


def top_violations(conn: psycopg.Connection, run_id: int, limit: int = 25,
                   category: str = "profile") -> list[tuple]:
    """Most common genuine violations of `category`, with payer breadth.

    Payer breadth matters more than raw count for the write-up: a violation 40
    payers share is an IG/tooling story, one payer's 5,000 hits is that payer's.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT coalesce(message_id, '(none)') AS mid,
                   count(*) AS hits,
                   count(DISTINCT payer_id) AS payers,
                   min(left(details, 130)) AS sample
            FROM public.conformance_issue
            WHERE run_id = %(run)s AND severity IN ('error','fatal')
              AND ({CATEGORY_SQL}) = %(cat)s
            GROUP BY 1 ORDER BY payers DESC, hits DESC LIMIT %(lim)s
            """,
            {**_PARAMS, "run": run_id, "lim": limit, "cat": category},
        )
        return cur.fetchall()


def binding_failures_by_valueset(conn: psycopg.Connection, run_id: int,
                                 limit: int = 20) -> list[tuple]:
    """(value_set_url, category, issues, payers) for binding failures.

    The value-set URL is embedded in the details text ("... in the value set
    'X' (url)"), not a structured column, so it is regexp-extracted here --
    good enough for grouping, and the raw details stay authoritative.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT coalesce(substring(details FROM '\\((http[^)| ]+)'),
                            '(unparsed)') AS vs,
                   ({CATEGORY_SQL}) AS category,
                   count(*) AS issues,
                   count(DISTINCT payer_id) AS payers
            FROM public.conformance_issue
            WHERE run_id = %(run)s
              AND ({CATEGORY_SQL}) IN ('binding-required', 'binding-extensible')
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT %(lim)s
            """,
            {**_PARAMS, "run": run_id, "lim": limit},
        )
        return cur.fetchall()


def unknown_systems(conn: psycopg.Connection, run_id: int,
                    limit: int = 25) -> list[tuple]:
    """(code_system_url, issues, payers) behind terminology-unavailable.

    Payer-proprietary code systems (healthsparq, optum, 1up, payer domains)
    are a finding in themselves: every code in one is uninterpretable to any
    consumer without that vendor's private documentation.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT coalesce(
                     substring(details FROM 'CodeSystem ''([^'']+)'''),
                     substring(details FROM 'Code System ''([^'']+)'''),
                     '(unparsed)') AS system,
                   count(*) AS issues,
                   count(DISTINCT payer_id) AS payers
            FROM public.conformance_issue
            WHERE run_id = %(run)s
              AND ({CATEGORY_SQL}) = 'terminology-unavailable'
            GROUP BY 1 ORDER BY 2 DESC LIMIT %(lim)s
            """,
            {**_PARAMS, "run": run_id, "lim": limit},
        )
        return cur.fetchall()

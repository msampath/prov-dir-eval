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


def latest_run(conn: psycopg.Connection) -> int | None:
    with conn.cursor() as cur:
        cur.execute("SELECT max(run_id) FROM public.conformance_run "
                    "WHERE finished_at IS NOT NULL")
        r = cur.fetchone()
        return r[0] if r else None


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

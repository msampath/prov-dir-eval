#!/usr/bin/env python
"""Exhaustively check terminology-binding membership for extracted code
columns against Plan-Net/US-Core/THO value sets -- ALL rows, not a sample.

Complements scripts/validate_conformance.py, which checks binding membership
too but only on a few-thousand-instance sample per cell (and cannot reach the
500M-row slim ``aetna_cvs.practitioner_role`` table at all, since its
``resource`` JSONB is a stub). This script instead scans the extracted code
columns (populated even on slim tables) directly in Postgres: two SQL scans
per (payer, check) -- one to classify every DISTINCT code seen against the
offline-expanded value set, one to roll that up into a row count -- so cost
scales with distinct-code cardinality and table size, not with a fixed
sample. See src/provdir/quality/bindings.py for the expansion engine, the
~15 (column, profile element) CHECKS, and the strengths/value-sets these
resolve to.

CODE-ONLY CAVEAT: the extracted columns hold code values only (no system),
so this cannot catch a right-code-under-wrong-system error -- that is what
the validator's sampled pass is for. Read these numbers next to it.

    python scripts/check_bindings.py --dry-run
    python scripts/check_bindings.py --payers advanced_health,florida_blue,devoted
    python scripts/check_bindings.py --run-id 1        # resume a prior run

NEVER run this unqualified against aetna_cvs without deliberately intending
the full sweep: practitioner_role alone is ~500M rows and each check costs 2
full-table scans (an unnest aggregate + a filtered count). Pilot on small
payers first (--payers advanced_health,florida_blue,devoted); the full sweep
including aetna_cvs is a separate, deliberate, multi-hour run.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from psycopg.types.json import Jsonb  # noqa: E402

from provdir.etl.loader import list_payer_schemas, pg_connection, schema_for  # noqa: E402
from provdir.quality import bindings  # noqa: E402

DDL = """
CREATE TABLE IF NOT EXISTS public.binding_check_run (
    run_id      bigserial PRIMARY KEY,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    packages    jsonb NOT NULL,
    notes       jsonb);
CREATE TABLE IF NOT EXISTS public.binding_check_code (
    run_id       bigint NOT NULL REFERENCES public.binding_check_run(run_id),
    payer_id     text NOT NULL,
    resource_type text NOT NULL,
    element      text NOT NULL,
    value_set    text NOT NULL,
    strength     text NOT NULL,
    code         text NOT NULL,
    n_rows       bigint NOT NULL,
    in_valueset  boolean NOT NULL,
    PRIMARY KEY (run_id, payer_id, resource_type, element, code));
CREATE TABLE IF NOT EXISTS public.binding_check_element (
    run_id       bigint NOT NULL REFERENCES public.binding_check_run(run_id),
    payer_id     text NOT NULL,
    resource_type text NOT NULL,
    element      text NOT NULL,
    value_set    text NOT NULL,
    strength     text NOT NULL,
    total_rows           bigint NOT NULL,
    rows_with_codes      bigint NOT NULL,
    rows_with_nonmember  bigint NOT NULL,
    -- ok: scanned and classified. unexpanded: expand() could not resolve the
    -- value set locally (counts are all 0 -- never real, never a false-clean
    -- signal). skipped-example: binding strength is 'example' (not
    -- conformance-bearing), scans skipped entirely to avoid the cost.
    status       text NOT NULL DEFAULT 'ok',
    PRIMARY KEY (run_id, payer_id, resource_type, element));
"""


def _existing_pairs(conn, run_id: int) -> set[tuple[str, str, str]]:
    """(payer_id, resource_type, element) triples already CONCLUSIVELY
    recorded for `run_id` -- a payer's checks commit together (see main()), so
    a triple's presence means that whole payer x check unit finished cleanly;
    a crash mid-payer leaves nothing for that payer, so resuming re-does it.

    'unexpanded' rows are deliberately NOT treated as done: they mean a value
    set could not be expanded (usually a missing/partial package), which is
    exactly the condition an operator fixes before resuming -- skipping them
    would freeze the failure into the run. Retried triples' stale rows are
    deleted before re-insert (see main()) so the plain INSERT cannot collide.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT payer_id, resource_type, element "
            "FROM public.binding_check_element WHERE run_id = %s "
            "AND status <> 'unexpanded'", (run_id,))
        return {(r[0], r[1], r[2]) for r in cur.fetchall()}


def _where(chk: bindings.Check) -> str:
    w = "payer_id = %(payer)s"
    if chk.cell_where:
        w += f" AND ({chk.cell_where})"
    return w


def _scan_distinct_codes(conn, schema: str, chk: bindings.Check) -> list[tuple[str, int]]:
    """[(code, n_rows)] over every distinct code value of chk.column, for one
    payer. Array columns are unnested first; the one scalar column
    (endpoint_resource.connection_type) is grouped directly."""
    col = chk.column
    params = {"payer": schema}
    with conn.cursor() as cur:
        if bindings.is_array_column(chk):
            cur.execute(
                f'SELECT code, count(*) FROM ('
                f'  SELECT unnest("{col}") AS code FROM "{schema}"."{chk.table.name}" '
                f'  WHERE {_where(chk)}'
                f') s GROUP BY 1',
                params,
            )
        else:
            cur.execute(
                f'SELECT "{col}" AS code, count(*) FROM "{schema}"."{chk.table.name}" '
                f'WHERE {_where(chk)} AND "{col}" IS NOT NULL GROUP BY 1',
                params,
            )
        return cur.fetchall()


def _scan_rollup(conn, schema: str, chk: bindings.Check,
                 bad_codes: list[str]) -> tuple[int, int, int]:
    """(total_rows, rows_with_codes, rows_with_nonmember) for one payer.

    rows_with_codes uses coalesce(cardinality(col),0) > 0 for array columns
    -- NOT `IS NOT NULL` -- because the extractor can populate an EMPTY array
    for "this element was absent", and IS NOT NULL would count that row as
    "has codes" when it has none.
    """
    col = chk.column
    params = {"payer": schema, "bad": bad_codes}
    with conn.cursor() as cur:
        if bindings.is_array_column(chk):
            cur.execute(
                f'SELECT count(*), '
                f'  count(*) FILTER (WHERE coalesce(cardinality("{col}"), 0) > 0), '
                f'  count(*) FILTER (WHERE "{col}" && %(bad)s::text[]) '
                f'FROM "{schema}"."{chk.table.name}" WHERE {_where(chk)}',
                params,
            )
        else:
            cur.execute(
                f'SELECT count(*), '
                f'  count(*) FILTER (WHERE "{col}" IS NOT NULL), '
                f'  count(*) FILTER (WHERE "{col}" = ANY(%(bad)s::text[])) '
                f'FROM "{schema}"."{chk.table.name}" WHERE {_where(chk)}',
                params,
            )
        return cur.fetchone()


def _insert_codes(conn, run_id: int, payer: str, chk: bindings.Check, vs_url: str,
                  strength: str, distinct: list[tuple[str, int]], codeset: set[str]) -> None:
    if not distinct:
        return
    with conn.cursor() as cur:
        with cur.copy(
            "COPY public.binding_check_code (run_id, payer_id, resource_type, element, "
            "value_set, strength, code, n_rows, in_valueset) FROM STDIN"
        ) as cp:
            for code, n in distinct:
                cp.write_row((run_id, payer, chk.resource_type, chk.element_path,
                             vs_url, strength, code, n, code in codeset))


def _insert_element(conn, run_id: int, payer: str, chk: bindings.Check, vs_url: str,
                    strength: str, total: int, with_codes: int, with_nonmember: int,
                    status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.binding_check_element "
            "(run_id, payer_id, resource_type, element, value_set, strength, "
            " total_rows, rows_with_codes, rows_with_nonmember, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (run_id, payer, chk.resource_type, chk.element_path, vs_url, strength,
             total, with_codes, with_nonmember, status),
        )


def _delete_stale_rows(conn, run_id: int, payer: str, chk: bindings.Check) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.binding_check_element WHERE run_id = %s "
            "AND payer_id = %s AND resource_type = %s AND element = %s",
            (run_id, payer, chk.resource_type, chk.element_path))
        cur.execute(
            "DELETE FROM public.binding_check_code WHERE run_id = %s "
            "AND payer_id = %s AND resource_type = %s AND element = %s",
            (run_id, payer, chk.resource_type, chk.element_path))


def run_check(conn, run_id: int, payer: str, schema: str, chk: bindings.Check,
             index: dict) -> str:
    """Run one (payer, check): resolve the binding, expand it, scan, record.
    Returns the status recorded ('ok' / 'unexpanded' / 'skipped-example')."""
    # A retried triple (resume re-runs 'unexpanded' rows -- see
    # _existing_pairs) still has its stale rows in both tables; clear them in
    # this same transaction so the plain INSERT/COPY below cannot PK-collide.
    _delete_stale_rows(conn, run_id, payer, chk)
    vs_url, strength = bindings.resolve_binding(index, chk)

    # 'example' bindings are not conformance-bearing (informational only) --
    # skip the scans entirely (this can be a 500M-row table) but still
    # inventory the element so a report distinguishes "not conformance
    # relevant" from "never ran".
    if strength == "example":
        _insert_element(conn, run_id, payer, chk, vs_url, strength, 0, 0, 0, "skipped-example")
        return "skipped-example"

    codeset = bindings.expand(index, vs_url)
    if codeset is None:
        # expand() could not resolve this value set locally -- record it as
        # unexpanded with all-zero counts (never a false "clean") rather than
        # scanning rows we cannot classify.
        _insert_element(conn, run_id, payer, chk, vs_url, strength, 0, 0, 0, "unexpanded")
        return "unexpanded"

    distinct = _scan_distinct_codes(conn, schema, chk)
    bad = sorted(code for code, _n in distinct if code not in codeset)
    _insert_codes(conn, run_id, payer, chk, vs_url, strength, distinct, codeset)

    total, with_codes, with_nonmember = _scan_rollup(conn, schema, chk, bad)
    _insert_element(conn, run_id, payer, chk, vs_url, strength,
                    total, with_codes, with_nonmember, "ok")
    return "ok"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payers", help="comma-separated payer schema keys "
                                     "(default: every payer with loaded resource tables)")
    ap.add_argument("--run-id", type=int,
                    help="resume this run_id: (payer,resource_type,element) triples "
                         "already recorded are skipped")
    ap.add_argument("--cache", default=str(bindings.DEFAULT_CACHE),
                    help="FHIR package cache root (default: %(default)s)")
    ap.add_argument("--dry-run", action="store_true",
                    help="resolve every binding + expand every value set and print the "
                         "plan; no DDL, no writes, no per-payer scans")
    args = ap.parse_args()

    cache = Path(args.cache)
    packages = bindings.ensure_packages(cache)
    print(f"packages: {[f'{n}#{v}' for n, v in packages]}", flush=True)
    index = bindings.load_index(cache, packages)
    print(f"index: {len(index):,} canonical resource(s) loaded", flush=True)

    if args.dry_run:
        for chk in bindings.CHECKS:
            vs_url, strength = bindings.resolve_binding(index, chk)
            codeset = bindings.expand(index, vs_url)
            size = str(len(codeset)) if codeset is not None else "UNEXPANDED"
            print(f"  {chk.resource_type:20s} {chk.element_path:35s} "
                 f"{strength:11s} size={size:>6s}  {vs_url}", flush=True)
        print(f"{len(bindings.CHECKS)} check(s) resolved; no DB writes (--dry-run)", flush=True)
        return 0

    conn = pg_connection()
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    payers = sorted(args.payers.split(",")) if args.payers else sorted(list_payer_schemas(conn))
    print(f"{len(payers)} payer(s): {payers}", flush=True)

    if args.run_id:
        run_id = args.run_id
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM public.binding_check_run WHERE run_id = %s", (run_id,))
            if cur.fetchone() is None:
                raise SystemExit(f"--run-id {run_id} does not exist in binding_check_run")
        existing = _existing_pairs(conn, run_id)
        print(f"resuming run_id={run_id}; {len(existing)} (payer,resource,element) "
             f"triple(s) already recorded", flush=True)
    else:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.binding_check_run (packages) VALUES (%s) RETURNING run_id",
                (Jsonb([f"{n}#{v}" for n, v in packages]),),
            )
            run_id = cur.fetchone()[0]
        conn.commit()
        existing = set()
        print(f"binding_check_run.run_id = {run_id}", flush=True)

    started = time.time()
    status_counts: dict[str, int] = {}
    for i, payer in enumerate(payers, 1):
        schema = schema_for(payer)
        with conn.cursor() as cur:
            # Scoped to this transaction only; re-set every payer since the
            # prior payer's commit ends it. Only the unnest/rollup aggregates
            # over the largest tables (practitioner_role) benefit, but it is
            # harmless to set unconditionally.
            cur.execute("SET LOCAL work_mem = '256MB'")
        n_done = 0
        for chk in bindings.CHECKS:
            key = (payer, chk.resource_type, chk.element_path)
            if key in existing:
                continue
            status = run_check(conn, run_id, payer, schema, chk, index)
            status_counts[status] = status_counts.get(status, 0) + 1
            n_done += 1
        conn.commit()
        el = time.time() - started
        print(f"  [{i}/{len(payers)}] {payer:24s} {n_done} check(s) done "
             f"({el / 60:.1f}m elapsed)", flush=True)

    with conn.cursor() as cur:
        cur.execute("UPDATE public.binding_check_run SET finished_at = now() "
                    "WHERE run_id = %s", (run_id,))
    conn.commit()
    print(f"done in {(time.time() - started) / 60:.1f}m; run_id={run_id}; "
         f"status counts: {status_counts}", flush=True)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

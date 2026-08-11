"""Phase 4 -- shared element-coverage engine (USCDI v3, MPF, Plan-Net extensions).

Three assessments (uscdi.py, mpf.py) share ONE measurement engine because they
are the same operation repeated: "of the rows a payer has, how many populate
element X". The engine does not know what USCDI or MPF *mean* -- it only knows
how to turn a list of :class:`CoverageCheck` into SQL, run one merged scan per
(payer, table), and route around slim tables. All domain content (which
elements, what they map to, disclaimers) lives in uscdi.py / mpf.py.

Query shape -- ONE merged seq scan per (payer, table), covering every check any
requested assessment defines against that table, not one scan per check:

    SELECT count(*) AS total,
           count(*) FILTER (WHERE <cell_where_i>)              AS d_i,
           count(*) FILTER (WHERE [cell_where_i AND] cond_i)   AS p_i, ...
    FROM <table> WHERE payer_id = %(payer)s

A table with 5 checks and no cell_where split costs the same ONE scan as a
table with 1 check -- the FILTER clauses are nearly free once Postgres is
already reading every row. Running 5 separate `SELECT count(*) FILTER...`
queries would instead cost 5 full scans.

Slim routing: a (payer, resource_type) pair registered in ``public.slim_table``
stores a stub in `resource` (see models.py's slim_table docstring for why). Its
``slim_safe`` checks (extracted columns, unaffected by stubbing) still run
through the merged SQL scan above; its JSONB-only checks (telecom, npi
identifiers, extensions -- none of which survive stubbing) are instead measured
from the on-disk validation sample via `_measure_from_sample`, method='sample',
with the sample's file count as the denominator. Zero sample files found =>
method='unmeasured', denominator 0 (never silently dropped).

Unmeasurable elements (no SQL and no sample can reach them at all, e.g. USCDI's
Author Organization) carry `condition_sql=None, json_predicate=None` and are
emitted as explicit gap rows (denominator 0, method='unmeasured') without
touching the database -- an absent row would read as "not attempted"; this
reads as "attempted, and there is no way to measure it here".
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import psycopg

from .. import OUTPUT_DIR
from ..config import Settings, get_settings, load_manifest
from ..etl.loader import list_payer_schemas, pg_connection, schema_for, set_search_path
from ..logging_setup import get_logger
from ..models import RESOURCE_TABLES, RESOURCE_TYPE_BY_TABLE

log = get_logger(__name__)

COVERAGE_DIR = OUTPUT_DIR / "quality"

# Default sample roots for slim-table JSONB measurement, in lookup order. The
# second is not a fallback in the "if broken, use this" sense -- F:/fhir_val is
# actively rewritten by validate_conformance.py runs (its `_batches/` directory
# comes and goes), so F:/fhir_val_keep (the preserved, never-rewritten copy) is
# always ALSO searched rather than trusted only when the first is missing.
DEFAULT_SAMPLE_ROOTS = ["F:/fhir_val", "F:/fhir_val_keep"]

DDL = """
CREATE TABLE IF NOT EXISTS public.element_coverage_run (
    run_id       bigserial PRIMARY KEY,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    assessments  text[] NOT NULL,
    notes        jsonb);
CREATE TABLE IF NOT EXISTS public.element_coverage (
    run_id       bigint NOT NULL REFERENCES public.element_coverage_run(run_id),
    assessment   text NOT NULL,
    payer_id     text NOT NULL,
    resource_type text NOT NULL,
    element      text NOT NULL,
    denominator  bigint NOT NULL,
    populated    bigint NOT NULL,
    method       text NOT NULL,
    required     boolean NOT NULL,
    mapping      text NOT NULL,
    detail       jsonb,
    measured_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, assessment, payer_id, resource_type, element));
CREATE INDEX IF NOT EXISTS ix_element_coverage_assessment_payer
    ON public.element_coverage (assessment, payer_id);
"""


@dataclass(frozen=True)
class CoverageCheck:
    element: str
    resource_type: str
    # Boolean SQL over the payer table's columns (extracted cols and/or the
    # `resource` JSONB), or None when the element cannot be measured at all
    # (emitted as an explicit unmeasured gap row instead of queried).
    condition_sql: Optional[str]
    slim_safe: bool
    # Pure-Python mirror of condition_sql over a parsed resource dict, used ONLY
    # for slim-table sample measurement. None => unmeasurable on slim tables
    # (the check still runs fine via SQL on every non-slim payer).
    json_predicate: Optional[Callable[[dict], bool]]
    required: bool = True
    mapping: str = "inferred"
    # Extra WHERE-clause fragment restricting which rows of the table this
    # check's denominator counts (e.g. Organization split into facilities vs
    # networks via `coalesce(is_network,0) = 0`). None => whole table.
    cell_where: Optional[str] = None
    note: str = ""


def _assert_engine_ready(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SHOW server_version_num")
        ver = int(cur.fetchone()[0])
        if ver < 120000:
            # jsonb_path_exists (used throughout uscdi.py/mpf.py's condition_sql)
            # was added in PG12; below that every JSONB check would fail at
            # execute() time, one payer into a multi-hour sweep.
            raise RuntimeError(
                f"element_coverage requires PostgreSQL >= 12 (jsonb_path_exists); "
                f"server_version_num={ver}"
            )
        # Checks intentionally run unbounded seq scans over payer tables as
        # large as aetna_cvs.practitioner_role (~500M rows); a default
        # statement_timeout would abort those mid-scan.
        cur.execute("SET statement_timeout = 0")


def _build_merged_query(table: str, items: list[tuple[str, CoverageCheck]]) -> str:
    """One SELECT with a `total` plus a (denominator, populated) FILTER pair per
    check, positionally ordered to match `items`. Exposed for unit testing the
    query shape without a database.
    """
    parts = ["count(*) AS total"]
    for i, (_assessment, c) in enumerate(items):
        cw = c.cell_where
        if cw:
            parts.append(f"count(*) FILTER (WHERE {cw}) AS d_{i}")
            parts.append(f"count(*) FILTER (WHERE ({cw}) AND ({c.condition_sql})) AS p_{i}")
        else:
            parts.append(f"count(*) AS d_{i}")
            parts.append(f"count(*) FILTER (WHERE {c.condition_sql}) AS p_{i}")
    return f"SELECT {', '.join(parts)} FROM {table} WHERE payer_id = %(payer)s"


def _run_sql_checks(
    conn: psycopg.Connection, table: str, payer_id: str,
    asof: Optional[datetime], items: list[tuple[str, CoverageCheck]],
) -> list[dict]:
    if not items:
        return []
    sql = _build_merged_query(table, items)
    with conn.cursor() as cur:
        cur.execute(sql, {"payer": payer_id, "asof": asof})
        row = cur.fetchone()
    total = int(row[0])
    out = []
    for i, (assessment, c) in enumerate(items):
        d, p = int(row[1 + 2 * i]), int(row[2 + 2 * i])
        out.append({
            "assessment": assessment, "resource_type": c.resource_type,
            "element": c.element, "denominator": d, "populated": p,
            "method": "sql", "required": c.required, "mapping": c.mapping,
            "detail": {"note": c.note, "table_rows": total, "cell_where": c.cell_where},
        })
    return out


def _measure_from_sample(
    sample_roots: list[str], payer: str, rtype: str, checks: list[CoverageCheck],
) -> dict[str, dict]:
    """Measure `checks` (all for the same resource_type) from on-disk sample
    files. Searches every root in `sample_roots` and unions the matches --
    rglob handles both the flat `<root>/<Cell>/<payer>__<rtype>__n.json` layout
    and the batched `<root>/_batches/<Cell>__NNN/<payer>__<rtype>__n.json`
    layout the validator leaves mid-run, since it recurses.
    """
    files: list[Path] = []
    seen_names: set[str] = set()
    for root in sample_roots:
        p = Path(root)
        if not p.is_dir():
            continue
        for f in p.rglob(f"{payer}__{rtype}__*.json"):
            if f.name not in seen_names:  # dedupe files present under both roots
                seen_names.add(f.name)
                files.append(f)

    total = len(files)
    populated = {c.element: 0 for c in checks}
    measurable = [c for c in checks if c.json_predicate is not None]
    for f in files:
        try:
            obj = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for c in measurable:
            try:
                if c.json_predicate(obj):
                    populated[c.element] += 1
            except Exception:  # noqa: BLE001 - one malformed sample must not sink the payer
                continue

    out: dict[str, dict] = {}
    for c in checks:
        if c.json_predicate is None or total == 0:
            out[c.element] = {"denominator": 0, "populated": 0, "method": "unmeasured"}
        else:
            out[c.element] = {"denominator": total, "populated": populated[c.element], "method": "sample"}
    return out


def payer_asof(conn: psycopg.Connection, payer_id: str) -> Optional[datetime]:
    """max(coalesce(last_seen_at, ingested_at)) across the 8 resource tables in
    the CURRENT search_path schema -- "when was this payer's data last
    confirmed present", used as the reference point for 30-day-freshness
    checks. `payer_id` is unused for the query itself (search_path already
    scopes it to one schema) but kept in the signature/log for readability at
    call sites.
    """
    parts = " UNION ALL ".join(
        f"SELECT max(coalesce(last_seen_at, ingested_at)) AS m FROM {t.name}"
        for t in RESOURCE_TABLES.values()
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT max(m) FROM ({parts}) s")
        row = cur.fetchone()
    return row[0] if row else None


def _measure_payer(
    conn: psycopg.Connection, payer_id: str,
    tagged_checks: list[tuple[str, CoverageCheck]],
    asof: Optional[datetime], sample_roots: list[str],
    slim_pairs: set[tuple[str, str]],
) -> list[dict]:
    """Measure every (assessment, check) pair for one payer. Groups checks by
    table so each table costs exactly one merged seq scan (or, for a slim
    table's non-slim-safe checks, one sample pass instead of SQL).
    """
    out: list[dict] = []
    unmeasurable = [(a, c) for a, c in tagged_checks if c.condition_sql is None]
    for assessment, c in unmeasurable:
        out.append({
            "assessment": assessment, "resource_type": c.resource_type,
            "element": c.element, "denominator": 0, "populated": 0,
            "method": "unmeasured", "required": c.required, "mapping": c.mapping,
            "detail": {"note": c.note},
        })

    by_table: dict[str, list[tuple[str, CoverageCheck]]] = {}
    for assessment, c in tagged_checks:
        if c.condition_sql is None:
            continue
        table = RESOURCE_TABLES[c.resource_type].name
        by_table.setdefault(table, []).append((assessment, c))

    for table, items in by_table.items():
        rtype = RESOURCE_TYPE_BY_TABLE[table]
        is_slim = (payer_id, rtype) in slim_pairs
        if is_slim:
            sql_items = [(a, c) for a, c in items if c.slim_safe]
            sample_items = [(a, c) for a, c in items if not c.slim_safe]
        else:
            sql_items, sample_items = items, []

        out.extend(_run_sql_checks(conn, table, payer_id, asof, sql_items))

        if sample_items:
            checks_only = [c for _, c in sample_items]
            measured = _measure_from_sample(sample_roots, payer_id, rtype, checks_only)
            for assessment, c in sample_items:
                m = measured[c.element]
                out.append({
                    "assessment": assessment, "resource_type": rtype,
                    "element": c.element, "denominator": m["denominator"],
                    "populated": m["populated"], "method": m["method"],
                    "required": c.required, "mapping": c.mapping,
                    "detail": {"note": c.note, "slim": True, "sample_roots": sample_roots},
                })
    return out


def _start_run(conn: psycopg.Connection, assessments: list[str], notes: dict) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.element_coverage_run (assessments, notes) "
            "VALUES (%s, %s) RETURNING run_id",
            (assessments, json.dumps(notes)),
        )
        return cur.fetchone()[0]


def _done_pairs(conn: psycopg.Connection, run_id: int) -> set[tuple[str, str]]:
    """(payer, assessment) pairs already recorded under `run_id`.

    Completion is tracked per PAIR, never per payer: a run started with
    --assessment uscdi and resumed with --assessment all must still run the
    mpf/plannet_ext checks for payers the uscdi pass already touched --
    payer-level tracking silently skipped them.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT payer_id, assessment FROM public.element_coverage "
            "WHERE run_id = %s",
            (run_id,),
        )
        return {(r[0], r[1]) for r in cur.fetchall()}


def _write_rows(conn: psycopg.Connection, run_id: int, payer_id: str, rows: list[dict]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO public.element_coverage "
            "(run_id, assessment, payer_id, resource_type, element, denominator, "
            " populated, method, required, mapping, detail) "
            "VALUES (%(run_id)s, %(assessment)s, %(payer_id)s, %(resource_type)s, "
            "%(element)s, %(denominator)s, %(populated)s, %(method)s, %(required)s, "
            "%(mapping)s, %(detail)s) "
            "ON CONFLICT (run_id, assessment, payer_id, resource_type, element) "
            "DO UPDATE SET denominator = EXCLUDED.denominator, populated = EXCLUDED.populated, "
            "method = EXCLUDED.method, detail = EXCLUDED.detail, measured_at = now()",
            [
                {
                    "run_id": run_id, "payer_id": payer_id,
                    "assessment": r["assessment"], "resource_type": r["resource_type"],
                    "element": r["element"], "denominator": r["denominator"],
                    "populated": r["populated"], "method": r["method"],
                    "required": r["required"], "mapping": r["mapping"],
                    "detail": json.dumps(r["detail"]),
                }
                for r in rows
            ],
        )


def _payers_from_db_rows(db_rows: list[tuple], asof_by_payer: dict) -> dict[str, dict]:
    """Artifact `payers` block from element_coverage rows read back off the DB.

    Pure so the resume-artifact invariant (every payer under the run appears,
    not just the ones this invocation measured) is unit-testable without a
    database. `db_rows` are (payer_id, resource_type, element, denominator,
    populated, method, required, mapping, detail) tuples; `asof_by_payer` is
    the run row's notes->'asof' object (payers measured before that map
    existed simply get asof None).
    """
    payers: dict[str, dict] = {}
    for payer, rtype, element, denom, pop, method, required, mapping, detail in db_rows:
        payers.setdefault(payer, {
            "asof": asof_by_payer.get(payer), "elements": [],
        })["elements"].append({
            "resource_type": rtype, "element": element,
            "denominator": denom, "populated": pop, "method": method,
            "required": required, "mapping": mapping, "detail": detail,
        })
    return payers


def run_element_coverage(
    assessments: dict[str, list[CoverageCheck]],
    keys: Optional[list[str]] = None,
    sample_root: str | list[str] = DEFAULT_SAMPLE_ROOTS,
    resume_run: Optional[int] = None,
    meta: Optional[dict[str, dict]] = None,
    assessment_payers: Optional[dict[str, set[str]]] = None,
    settings: Optional[Settings] = None,
) -> dict:
    """Measure every check in `assessments` (name -> checks) for each payer.

    `keys`: subset of payer keys to run (default: every KNOWN payer with
    loaded data). `sample_root`: root(s) searched for slim-table sample files.
    `resume_run`: an existing run_id to continue -- payers already fully
    written under it are skipped. Payer commits are atomic (all assessments for
    one payer, or none), so "payer present" always means "payer complete", never
    partial. `meta`: assessment -> arbitrary JSON-able dict (disclaimers,
    provenance, roster) merged verbatim into that assessment's output artifact.
    `assessment_payers`: assessment -> the payer keys eligible for it (e.g. MPF
    is only meaningful for MA-classified payers) -- an assessment absent from
    this dict runs for every payer in `keys`. This keeps MPF's narrower payer
    set and USCDI/plannet_ext's full payer set under ONE run_id (one classifier
    pass, one resumable sweep) instead of requiring separate runs.
    """
    settings = settings or get_settings()
    manifest = load_manifest()
    endpoints = manifest.subset(keys) if keys else manifest.known()
    sample_roots = [sample_root] if isinstance(sample_root, str) else list(sample_root)
    meta = meta or {}

    COVERAGE_DIR.mkdir(parents=True, exist_ok=True)
    conn = pg_connection()
    try:
        _assert_engine_ready(conn)
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        assessment_names = sorted(assessments)
        if resume_run:
            run_id = resume_run
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT assessments FROM public.element_coverage_run "
                    "WHERE run_id = %s", (run_id,)
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(f"--resume-run {run_id}: no such element_coverage_run")
                # Union this invocation's assessments into the run row: the
                # report gates its sections on this array, so a resume that
                # widened scope must widen the recorded scope too or the new
                # assessments' data would be written yet never reported.
                merged = sorted(set(row[0] or []) | set(assessment_names))
                cur.execute(
                    "UPDATE public.element_coverage_run SET assessments = %s "
                    "WHERE run_id = %s", (merged, run_id)
                )
            conn.commit()
        else:
            run_id = _start_run(conn, assessment_names, {
                "keys": keys, "sample_roots": sample_roots,
            })
            conn.commit()
        log.info("element_coverage: run_id=%s assessments=%s", run_id, assessment_names)

        skip = _done_pairs(conn, run_id) if resume_run else set()
        have_data = list_payer_schemas(conn)
        from ..etl.loader import slim_registry  # local import: avoid a hard cycle at module load
        # Snapshotted once per run: a pair registered slim WHILE this sweep is
        # in flight would still have its JSONB checks run as SQL against what
        # is by then a stubbed resource column (silent 0%). Acceptable because
        # a slim load takes hours to stub anything meaningful and holds an
        # exclusive run lock -- don't run one concurrently with a sweep.
        slim_pairs = slim_registry(conn)

        per_payer: dict[str, dict] = {}
        for ep in endpoints:
            schema = schema_for(ep.key)
            if schema not in have_data:
                log.info("element_coverage: %s has no loaded data; skipping", ep.key)
                continue
            tagged: list[tuple[str, CoverageCheck]] = []
            for assessment, checks in assessments.items():
                if (ep.key, assessment) in skip:
                    log.info("element_coverage: %s/%s already done under run %s; skipping",
                             ep.key, assessment, run_id)
                    continue
                eligible = assessment_payers.get(assessment) if assessment_payers else None
                if eligible is not None and ep.key not in eligible:
                    continue
                tagged.extend((assessment, c) for c in checks)
            if not tagged:
                log.info("element_coverage: %s not eligible for any requested assessment; skipping", ep.key)
                continue

            set_search_path(conn, schema)
            asof = payer_asof(conn, ep.key)
            rows = _measure_payer(conn, ep.key, tagged, asof, sample_roots, slim_pairs)
            _write_rows(conn, run_id, ep.key, rows)
            # Persist asof in the run row alongside the payer's rows (same
            # txn): element_coverage rows do not carry asof, and the artifact
            # rebuild below reads ALL of the run's payers from the DB -- a
            # payer measured by an earlier, crashed invocation must still get
            # its asof from somewhere durable, not this process's memory.
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.element_coverage_run "
                    "SET notes = jsonb_set(coalesce(notes, '{}'::jsonb), '{asof}', "
                    "coalesce(notes->'asof', '{}'::jsonb) || %s::jsonb) "
                    "WHERE run_id = %s",
                    (json.dumps({ep.key: asof.isoformat() if asof else None}), run_id),
                )
            conn.commit()

            per_payer[ep.key] = {
                "asof": asof.isoformat() if asof else None,
                "measurements": rows,
            }
            log.info("element_coverage: %-14s asof=%s checks=%d", ep.key, asof, len(rows))

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.element_coverage_run SET finished_at = now() WHERE run_id = %s",
                (run_id,),
            )
        conn.commit()

        # Artifacts are rebuilt from the DB, not from this invocation's
        # in-memory rows: on a resume, per_payer holds only the payers measured
        # THIS time, and writing just those would silently shrink the artifact
        # to the resumed tail. The DB holds every payer under the run.
        generated_at = datetime.now(tz=timezone.utc).isoformat()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT coalesce(notes->'asof', '{}'::jsonb) "
                "FROM public.element_coverage_run WHERE run_id = %s", (run_id,),
            )
            asof_by_payer = cur.fetchone()[0]
        for assessment in assessment_names:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payer_id, resource_type, element, denominator, populated, "
                    "method, required, mapping, detail "
                    "FROM public.element_coverage "
                    "WHERE run_id = %s AND assessment = %s "
                    "ORDER BY payer_id, resource_type, element",
                    (run_id, assessment),
                )
                db_rows = cur.fetchall()
            payers = _payers_from_db_rows(db_rows, asof_by_payer)
            payload = {
                "run_id": run_id, "assessment": assessment,
                "generated_at": generated_at,
                "meta": meta.get(assessment, {}),
                "payers": payers,
            }
            out_path = COVERAGE_DIR / f"{assessment}_coverage.json"
            out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
            log.info("element_coverage: wrote %s (%d payer(s))", out_path, len(payers))
    finally:
        conn.close()

    return {
        "run_id": run_id, "assessments": assessment_names,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "payers": per_payer,
    }

"""Phase 5.5 — ETL orchestration.

For each (endpoint, resource_type): extract (async, paginated/partitioned),
transform to rows, load (per-payer drop + bulk COPY) off the event loop, and
record a provenance row (with a >10% data-drop flag vs the prior run).

Endpoints run concurrently; DB writes happen in worker threads so blocking
psycopg calls don't stall the event loop. A semaphore bounds simultaneous
(endpoint, resource_type) units to keep memory and DB connections in check.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

from .. import OUTPUT_DIR
from ..config import Endpoint, Settings, get_settings, load_manifest
from ..http_client import FhirSession
from ..logging_setup import get_logger
from ..db import safe_ident
from ..models import RESOURCE_TABLES
from .extract import (
    MultiSink,
    ResourceSink,
    bare_fingerprint,
    extract_resource,
    id_chain_extract,
    id_read_extract,
    include_sweep,
)
from .loader import (
    count_rows,
    delete_checkpoint,
    ensure_payer_schema,
    insert_provenance,
    latest_prior_count,
    load_checkpoint,
    pg_connection,
    prepare_stage,
    schema_for,
    upsert_batch,
    upsert_checkpoint,
)
from .transform import TransformError, transform_resource

log = get_logger(__name__)

DATA_DROP_THRESHOLD = -10.0  # percent; flag drops worse than this


def classify_status(
    method: Optional[str],
    note: Optional[str],
    fetch_errors: int,
    transform_errors: int,
    extract_err: Optional[str],
    loaded: int,
    server_total: Optional[int],
) -> tuple[str, Optional[str]]:
    """Classify a run's provenance status. Returns (status, extra_note).

    A run that lands 0 rows against a server that reports data is an error,
    not "ok" — and 0 rows with no verifiable total is "empty-unverified", so
    silent extraction failures can't masquerade as clean runs.
    """
    if method in ("blocked", "partition-failed"):
        # The host refused the bare search (401/403/429). This is an access
        # failure, NOT "this resource isn't served" — and it must not read as
        # "ok" just because a previous run left rows in the table.
        return "error", str(note or "host refused the request")
    if method in ("needs-partition", "unsupported"):
        return "skipped", None
    if extract_err and loaded == 0:
        return "error", None
    if extract_err:
        return "partial", None
    if isinstance(server_total, int) and server_total > 0 and loaded == 0:
        return "error", f"0 rows landed vs server_total={server_total}"
    note_s = str(note or "")
    if "pagination stopped" in note_s or "budget" in note_s:
        return "partial", None
    if transform_errors or fetch_errors:
        return "partial", None
    if loaded == 0:
        if server_total == 0:
            return "ok", None
        return "empty-unverified", "0 rows landed; server total unknown"
    return "ok", None


def _open_for_resource(schema: str, table) -> "object":
    """Open a connection bound to the payer schema and prepare the upsert stage."""
    conn = pg_connection(schema)
    prepare_stage(conn, table)
    return conn


def _read_chain_ids(conn, source_table: str, payer_id: str,
                    source_filter: Optional[dict] = None) -> list[str]:
    """Ids from an already-loaded table, to drive id-chained reference harvesting.

    source_table is a fixed internal config value (never user input); the payer
    schema is on the connection's search_path.

    source_filter {column, value} narrows the source rows — e.g. chaining
    PractitionerRole on `network` must only use Organizations that ARE networks
    (``{column: is_network, value: 1}``), not every organization.
    """
    safe_ident(source_table)
    sql = f'SELECT id FROM "{source_table}" WHERE payer_id = %s'
    params: list = [payer_id]
    if source_filter:
        col = safe_ident(source_filter["column"])
        sql += f' AND "{col}" = %s'
        params.append(source_filter["value"])
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return [r[0] for r in cur.fetchall()]


def _read_ref_ids(conn, ref_sources: list, target_table: str) -> list[str]:
    """Distinct target ids referenced from other loaded tables, minus ids already
    in the target table (so id_read only fetches NEW resources; resumable).

    ref_sources: [[source_table, column], ...] — a column ending in ``_refs`` is
    a text[] (unnested); else a scalar ``Type/id`` string. All names are fixed
    internal config values (never user input); the payer schema is on search_path.
    """
    safe_ident(target_table)
    parts = []
    for src_table, col in ref_sources:
        safe_ident(src_table)
        safe_ident(col)
        if col.endswith("_refs"):
            parts.append(f'SELECT split_part(unnest({col}), \'/\', 2) AS id FROM "{src_table}" WHERE {col} IS NOT NULL')
        else:
            parts.append(f'SELECT split_part({col}, \'/\', 2) AS id FROM "{src_table}" WHERE {col} IS NOT NULL AND {col} <> \'\'')
    union = " UNION ".join(parts)
    sql = f'SELECT id FROM ({union}) s WHERE id <> \'\' EXCEPT SELECT id FROM "{target_table}"'
    with conn.cursor() as cur:
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]


def _validate_checkpoint(ckpt: Optional[dict], fingerprint: str, ttl_hours: float) -> Optional[dict]:
    """Return the checkpoint only if it is fresh (within TTL) and its fingerprint
    matches the current pull shape; else None (=> start fresh). A stale offset is
    dangerous: the upstream directory may have regenerated, shifting every offset."""
    if not ckpt:
        return None
    updated = ckpt.get("updated_at")
    if updated is not None:
        age_h = (datetime.now(tz=timezone.utc) - updated).total_seconds() / 3600.0
        if age_h > ttl_hours:
            log.info("checkpoint stale (%.1fh > %.1fh TTL); starting fresh", age_h, ttl_hours)
            return None
    if ckpt.get("params_fingerprint") != fingerprint:
        log.info("checkpoint fingerprint mismatch; starting fresh")
        return None
    return ckpt


def _write_checkpoint_guarded(conn, payer_id, resource_type, progress, page_size,
                              fingerprint, rows_committed) -> None:
    """Write/delete the bare-pagination checkpoint inside the batch's transaction,
    fenced by a SAVEPOINT so a checkpoint failure can never roll back the committed
    batch (nor escape and be misread by the extractor as an HTTP error)."""
    if not progress or not (progress.get("active") or progress.get("exhausted")):
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT provdir_ckpt")
        if progress.get("exhausted"):
            delete_checkpoint(conn, payer_id, resource_type)
        else:
            upsert_checkpoint(
                conn, payer_id, resource_type,
                resume_url=progress.get("page_url"),
                pages_done=progress.get("pages", 0),
                rows_added=(progress.get("added_base", 0) or 0) + rows_committed,
                page_size=page_size,
                params_fingerprint=fingerprint,
            )
        with conn.cursor() as cur:
            cur.execute("RELEASE SAVEPOINT provdir_ckpt")
    except Exception as exc:  # noqa: BLE001 - never cost the batch for a checkpoint
        try:
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT provdir_ckpt")
        except Exception:  # noqa: BLE001
            pass
        log.warning("checkpoint write failed for %s/%s (batch still committed): %s",
                    payer_id, resource_type, exc)


def _finalize(conn, ep, resource_type, table, started, stats, transform_errors, extract_err) -> dict:
    """Sync: count what landed, classify status, write provenance, commit."""
    loaded = count_rows(conn, table)
    prior = latest_prior_count(conn, ep.key, resource_type)
    pct_change = round(100.0 * (loaded - prior) / prior, 1) if prior else None
    method = stats.get("method")
    server_total = stats.get("server_total")
    coverage_pct = round(100.0 * loaded / server_total, 1) if isinstance(server_total, int) and server_total > 0 else None
    residual = max(0, server_total - loaded) if isinstance(server_total, int) else None

    status, status_note = classify_status(
        method, stats.get("note"), stats.get("fetch_errors") or 0,
        transform_errors, extract_err, loaded, server_total,
    )

    notes = {
        "method": method,
        "partitions": stats.get("partitions"),
        "note": stats.get("note"),
        "transform_errors": transform_errors,
        "fetch_errors": stats.get("fetch_errors") or 0,
        "extract_error": extract_err,
        # Coverage reconciliation: rows actually landed vs the server's total
        # (bare count where available, else an exhaustive-partition sum).
        "server_total": server_total,
        "server_total_source": stats.get("server_total_source"),
        "coverage_pct": coverage_pct,
        "residual_unreachable": residual,
    }
    if status_note:
        notes["status_note"] = status_note
    if stats.get("resumed_from_page") is not None:
        notes["resumed_from_page"] = stats.get("resumed_from_page")
        notes["resumed_offset"] = stats.get("resumed_offset")
    if coverage_pct is not None and coverage_pct > 100.0:
        # Landed more than the server claims to have => the server's total is
        # wrong (e.g. bcbs_mn reports total=50); don't trust it as a denominator.
        notes["server_total_misreported"] = True
    if pct_change is not None and pct_change <= DATA_DROP_THRESHOLD:
        notes["data_drop_flag"] = f"{pct_change}% vs prior {prior}"

    # A clean terminal state means there's nothing left to resume; drop any
    # checkpoint (no-op if absent). Retained on error/partial so --resume works.
    if status in ("ok", "empty-unverified", "skipped"):
        delete_checkpoint(conn, ep.key, resource_type)

    run_id = insert_provenance(
        conn,
        {
            "payer_id": ep.key,
            "source_base_url": ep.base_url,
            "resource_type": resource_type,
            "started_at": started,
            "finished_at": datetime.now(tz=timezone.utc),
            "status": status,
            "page_count": stats.get("pages", 0),
            "resource_count": loaded,
            "error_count": transform_errors,
            "prior_count": prior,
            "pct_change": pct_change,
            "notes": notes,
        },
    )
    conn.commit()
    return {
        "resource_type": resource_type,
        "loaded": loaded,
        "status": status,
        "method": method,
        "pct_change": pct_change,
        "coverage_pct": coverage_pct,
        "run_id": run_id,
        "errors": transform_errors,
    }


async def _extract_and_load(
    session: FhirSession,
    ep: Endpoint,
    resource_type: str,
    max_pages: Optional[int],
    sem: asyncio.Semaphore,
    upsert: bool = False,
    resume: bool = False,
) -> dict:
    async with sem:
        settings = get_settings()
        started = datetime.now(tz=timezone.utc)
        client = session.client_for(ep)
        table = RESOURCE_TABLES[resource_type]
        schema = schema_for(ep.key)
        conn = await asyncio.to_thread(_open_for_resource, schema, table)
        state = {"transform_errors": 0, "rows_committed": 0}

        # Bare-pagination checkpoint plumbing. `progress` is shared with sync_flush
        # (safe: bare pagination is single-task — see _paginate docstring). It stays
        # empty (=> no checkpoint writes) for adaptive/id_chain/id_read units.
        fingerprint = bare_fingerprint(ep, resource_type, settings.http_default_count)
        page_size = (ep.quirks.page_size_by_resource.get(resource_type)
                     or ep.quirks.page_size or settings.http_default_count)
        progress: dict = {}
        resume_ckpt = None
        if resume:
            raw_ckpt = await asyncio.to_thread(load_checkpoint, conn, ep.key, resource_type)
            await asyncio.to_thread(conn.commit)  # don't hold a read txn open for hours
            resume_ckpt = _validate_checkpoint(raw_ckpt, fingerprint, settings.checkpoint_ttl_hours)

        def sync_flush(resources: list[dict]) -> int:
            rows = []
            for r in resources:
                try:
                    rows.append(transform_resource(r, ep.key, ep.base_url))
                except TransformError:
                    state["transform_errors"] += 1
                except (ValueError, TypeError) as exc:
                    # Malformed payer data (e.g. position.latitude="", a null in
                    # address.line) raises before TransformError can wrap it.
                    # Losing one resource is fine; losing the 5000-row batch is not.
                    state["transform_errors"] += 1
                    log.debug("transform failed: %s: %s", type(exc).__name__, exc)
            try:
                n = upsert_batch(conn, table, rows, update=upsert)
                state["rows_committed"] += len(rows)
                # Checkpoint in the SAME transaction as the batch, so it can never
                # name data that isn't durable. SAVEPOINT-fenced: failure loses the
                # checkpoint, never the batch.
                _write_checkpoint_guarded(conn, ep.key, resource_type, progress,
                                          page_size, fingerprint, state["rows_committed"])
                conn.commit()  # commit per batch -> progress survives a reaped job
            except Exception:
                # Leave the connection usable: without this the aborted transaction
                # makes every later statement (incl. _finalize's count) fail with
                # InFailedSqlTransaction, masking the real error.
                conn.rollback()
                raise
            return n

        async def flush(resources: list[dict]) -> int:
            return await asyncio.to_thread(sync_flush, resources)

        sink = ResourceSink(flush, batch=5000)
        extract_err = None
        stats: dict = {"method": None}
        adaptive_cfg = (ep.quirks.adaptive or {}).get(resource_type)
        try:
            if adaptive_cfg and adaptive_cfg.get("mode") == "id_chain":
                # Filter-only reference resource: chain on ids from a loaded table.
                ids = await asyncio.to_thread(_read_chain_ids, conn, adaptive_cfg["source_table"],
                                              ep.key, adaptive_cfg.get("source_filter"))
                stats = await id_chain_extract(client, ep, resource_type, adaptive_cfg, sink, ids, max_pages)
            elif adaptive_cfg and adaptive_cfg.get("mode") == "id_read":
                # Un-searchable resource: harvest by id-read from references we hold.
                ids = await asyncio.to_thread(_read_ref_ids, conn, adaptive_cfg["ref_sources"], table.name)
                stats = await id_read_extract(client, ep, resource_type, adaptive_cfg, sink, ids, max_pages)
            else:
                stats = await extract_resource(client, ep, resource_type, sink, max_pages,
                                               resume_ckpt=resume_ckpt, progress=progress)
        except Exception as exc:  # noqa: BLE001
            extract_err = f"{type(exc).__name__}: {exc}"
            log.warning("etl extract error %s/%s: %s", ep.key, resource_type, extract_err)
        try:
            await sink.close()
        except Exception as exc:  # noqa: BLE001
            extract_err = extract_err or f"flush: {type(exc).__name__}: {exc}"

        result = await asyncio.to_thread(
            _finalize, conn, ep, resource_type, table, started, stats, state["transform_errors"], extract_err
        )
        await asyncio.to_thread(conn.close)
        log.info(
            "etl %-14s %-24s loaded=%-7d method=%-18s cov=%s%s",
            ep.key, resource_type, result["loaded"], str(result.get("method")),
            result.get("coverage_pct"),
            f" Δ={result['pct_change']:+}%" if result.get("pct_change") not in (None, 0) else "",
        )
        return result


# _include reference-param -> target resource type (for MultiSink routing).
_INCLUDE_TARGET = {
    "practitioner": "Practitioner", "location": "Location", "organization": "Organization",
    "service": "HealthcareService", "network": "Organization", "endpoint": "Endpoint",
    "coverage-area": "Location", "partof": "Location", "primary-organization": "Organization",
    "participating-organization": "Organization", "administered-by": "Organization",
    "owned-by": "Organization",
}


async def _harvest_one(session, ep: Endpoint, base_resource: str, cfg: dict, max_pages) -> dict:
    """Run one _include reference-graph sweep, writing to every harvested table."""
    include = list(cfg["include"])
    target_types = {base_resource}
    for inc in include:
        rt = _INCLUDE_TARGET.get(inc.split(":", 1)[1])
        if rt:
            target_types.add(rt)

    schema = schema_for(ep.key)
    ensure_payer_schema(schema)
    tables = {rt: RESOURCE_TABLES[rt] for rt in sorted(target_types)}
    conns = {rt: await asyncio.to_thread(_open_for_resource, schema, tbl) for rt, tbl in tables.items()}
    state = {"errors": 0}

    def _make_flush(rt, conn, table):
        def sync_flush(resources):
            rows = []
            for r in resources:
                try:
                    rows.append(transform_resource(r, ep.key, ep.base_url))
                except TransformError:
                    state["errors"] += 1
                except (ValueError, TypeError) as exc:
                    # Malformed payer data must cost one resource, not the batch
                    # (same guard as the ETL path).
                    state["errors"] += 1
                    log.debug("harvest transform failed: %s: %s", type(exc).__name__, exc)
            try:
                n = upsert_batch(conn, table, rows)
                conn.commit()
            except Exception:
                conn.rollback()  # keep the connection usable for the next batch
                raise
            return n
        async def flush(resources):
            return await asyncio.to_thread(sync_flush, resources)
        return flush

    sink = MultiSink({rt: _make_flush(rt, conns[rt], tables[rt]) for rt in tables})
    client = session.client_for(ep)
    started = datetime.now(tz=timezone.utc)
    extract_err = None
    stats = {"method": f"include:{base_resource}"}
    try:
        stats = await include_sweep(client, ep, base_resource, cfg, sink, max_pages)
    except Exception as exc:  # noqa: BLE001
        extract_err = f"{type(exc).__name__}: {exc}"
        log.warning("harvest error %s: %s", ep.key, extract_err)
    try:
        # A failure flushing the final partial batch must not skip every
        # provenance write and leak the connections after an expensive sweep.
        await sink.close()
    except Exception as exc:  # noqa: BLE001
        extract_err = "; ".join(filter(None, [extract_err, f"flush: {type(exc).__name__}: {exc}"]))
        log.warning("harvest flush error %s: %s", ep.key, exc)

    results = {}
    try:
        for rt, table in tables.items():
            res = await asyncio.to_thread(
                _finalize, conns[rt], ep, rt, table, started,
                # Pass include_sweep's own counters through instead of rebuilding
                # the dict, or pages/fetch_errors are silently dropped and a
                # partly-failed sweep is recorded "ok".
                {**stats, "server_total": None, "harvested_via": f"include:{base_resource}"},
                state["errors"] if rt == base_resource else 0, extract_err,
            )
            results[rt] = res
            log.info("harvest %-14s %-22s loaded=%d", ep.key, rt, res.get("loaded", 0))
    finally:
        for conn in conns.values():
            try:
                await asyncio.to_thread(conn.close)
            except Exception:  # noqa: BLE001
                pass
    return results


async def run_reference_harvest(
    keys: Optional[list[str]] = None,
    max_pages: Optional[int] = None,
    settings: Optional[Settings] = None,
) -> dict:
    """Reference-graph `_include` harvest for endpoints whose PractitionerRole
    adaptive config carries an `include` list. One sweep fills multiple tables."""
    settings = settings or get_settings()
    manifest = load_manifest()
    endpoints = manifest.subset(keys) if keys else manifest.mvp()
    out = {}
    async with FhirSession(settings) as session:
        for ep in endpoints:
            cfg = (ep.quirks.adaptive or {}).get("PractitionerRole")
            if not cfg or "include" not in cfg:
                log.info("harvest: %s has no PractitionerRole include config; skipping", ep.key)
                continue
            log.info("harvest: %s reference-graph sweep starting", ep.key)
            out[ep.key] = await _harvest_one(session, ep, "PractitionerRole", cfg, max_pages)
    return out


async def run_etl(
    keys: Optional[list[str]] = None,
    include_all_known: bool = False,
    max_pages: Optional[int] = None,
    resources: Optional[list[str]] = None,
    concurrency: int = 6,
    upsert: bool = False,
    resume: bool = False,
    settings: Optional[Settings] = None,
) -> dict:
    settings = settings or get_settings()
    manifest = load_manifest()

    if keys:
        endpoints = manifest.subset(keys)
    elif include_all_known:
        endpoints = manifest.known()
    else:
        endpoints = manifest.mvp()

    runnable = []
    skipped = []
    for ep in endpoints:
        reason = ep.skip_reason(settings)
        if reason and (reason.startswith("missing-credentials") or reason in {"blocked", "missing-token-url"}):
            skipped.append({"key": ep.key, "reason": reason})
        else:
            runnable.append(ep)

    # Create each payer's schema + tables once, before concurrent loads race.
    for ep in runnable:
        ensure_payer_schema(schema_for(ep.key))

    sem = asyncio.Semaphore(concurrency)
    tasks = []
    async with FhirSession(settings) as session:
        for ep in runnable:
            expected = ep.expected_resources(manifest.plannet_resources)
            rtypes = [r for r in expected if (not resources or r in resources)]
            for rtype in rtypes:
                tasks.append(_extract_and_load(session, ep, rtype, max_pages, sem, upsert, resume))
        # return_exceptions: one unit blowing up (a DB error outside the inner
        # guards, say) must not discard every other unit's work AND the summary.
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    for r in raw_results:
        if isinstance(r, BaseException):
            log.error("extract unit failed: %s: %s", type(r).__name__, r)
            results.append({"status": "error", "loaded": 0, "resource_type": None,
                            "payer": None, "note": f"{type(r).__name__}: {r}"})
        else:
            results.append(r)

    # Group results by payer.
    by_payer: dict[str, list[dict]] = {}
    for ep in runnable:
        by_payer[ep.key] = []
    idx = 0
    for ep in runnable:
        expected = ep.expected_resources(manifest.plannet_resources)
        rtypes = [r for r in expected if (not resources or r in resources)]
        for _ in rtypes:
            by_payer[ep.key].append(results[idx])
            idx += 1

    total_loaded = sum(r.get("loaded", 0) for r in results)
    summary = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "endpoints_run": len(runnable),
        "skipped": skipped,
        "total_rows_loaded": total_loaded,
        "max_pages": max_pages,
        "by_payer": {
            k: {
                "total_loaded": sum(r.get("loaded", 0) for r in v),
                "resources": v,
            }
            for k, v in by_payer.items()
        },
        "ok": all(r.get("status") != "error" for r in results),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / "etl_summary.json"
    payload = json.dumps(summary, indent=2, default=str)
    out.write_text(payload, encoding="utf-8")
    # etl_summary.json only ever reflects the LAST run; archive each run so
    # incremental pulls don't erase the record of earlier ones.
    runs_dir = OUTPUT_DIR / "etl_runs"
    runs_dir.mkdir(exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (runs_dir / f"etl_summary_{ts}.json").write_text(payload, encoding="utf-8")
    log.info(
        "etl complete: %d endpoints, %d rows loaded -> %s",
        len(runnable),
        total_loaded,
        out,
    )
    return summary

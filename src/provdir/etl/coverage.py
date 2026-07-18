"""Coverage scoreboard — the extraction campaign's progress meter.

For every (payer, resource type) with loaded data or a provenance record,
report: rows actually in the table, the server's total (and how we know it),
coverage %, the latest run status, and truth flags (misreported totals, our-cap
truncation). Prints an aligned table and writes output/coverage_report.json.

"Done" for the campaign = every row is either ~100% covered, verified-empty,
or documented as server-side unreachable.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .. import OUTPUT_DIR
from ..config import load_manifest
from ..logging_setup import get_logger
from ..models import RESOURCE_TABLES
from .loader import list_payer_schemas, pg_connection, schema_for, set_search_path

log = get_logger(__name__)


def _latest_provenance(conn) -> dict[tuple[str, str], dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT ON (payer_id, resource_type) "
            "  payer_id, resource_type, status, started_at, notes "
            "FROM public.provenance "
            "ORDER BY payer_id, resource_type, started_at DESC"
        )
        out = {}
        for payer, rtype, status, started, notes in cur.fetchall():
            out[(payer, rtype)] = {"status": status, "started_at": started, "notes": notes or {}}
    return out


def coverage_report() -> dict:
    manifest = load_manifest()
    conn = pg_connection()
    rows = []
    try:
        prov = _latest_provenance(conn)
        have_data = list_payer_schemas(conn)
        for ep in manifest.mvp():
            schema = schema_for(ep.key)
            if schema not in have_data:
                continue
            set_search_path(conn, schema)
            for rtype in ep.expected_resources(manifest.plannet_resources):
                table = RESOURCE_TABLES[rtype]
                with conn.cursor() as cur:
                    cur.execute(f'SELECT count(*) FROM "{table.name}" WHERE payer_id = %s', (ep.key,))
                    loaded = int(cur.fetchone()[0])
                p = prov.get((ep.key, rtype), {})
                notes = p.get("notes") or {}
                server_total = notes.get("server_total")
                coverage = (
                    round(100.0 * loaded / server_total, 1)
                    if isinstance(server_total, int) and server_total > 0
                    else None
                )
                flags = []
                if notes.get("server_total_misreported"):
                    flags.append("total-misreported")
                if "budget" in str(notes.get("note") or ""):
                    flags.append("our-cap-truncated")
                rows.append(
                    {
                        "payer": ep.key,
                        "resource_type": rtype,
                        "loaded": loaded,
                        "server_total": server_total if isinstance(server_total, int) else None,
                        "server_total_source": notes.get("server_total_source"),
                        "coverage_pct": coverage,
                        "status": p.get("status"),
                        "last_run": str(p.get("started_at") or ""),
                        "flags": flags,
                    }
                )
    finally:
        conn.close()

    print(f"{'payer':<14} {'resource':<24} {'loaded':>10} {'server_total':>12} {'cov%':>7}  {'status':<16} flags")
    for r in rows:
        st = f"{r['server_total']:,}" if r["server_total"] is not None else "?"
        cov = f"{r['coverage_pct']}" if r["coverage_pct"] is not None else "?"
        print(
            f"{r['payer']:<14} {r['resource_type']:<24} {r['loaded']:>10,} {st:>12} {cov:>7}"
            f"  {str(r['status']):<16} {','.join(r['flags'])}"
        )

    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "rows": rows,
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / "coverage_report.json"
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("coverage report: %d (payer, resource) rows -> %s", len(rows), out)
    return report

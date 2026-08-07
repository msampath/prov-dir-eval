#!/usr/bin/env python
"""Load paginated file-sweep captures into Postgres; report NEW vs UPDATED per file.

Batch/multi-file counterpart to bulk_ingest.py's --ingest-file mode: reuses the
same transform + upsert machinery (rows land identically, same columns, same
ON CONFLICT DO UPDATE), but walks every captured <payer>_<resource>.ndjson file
under --dir and reports, per (payer, resource), how many rows were net-new vs
how many existing rows were re-upserted (updated).

    python scripts/load_file_sweeps.py                  # load everything under I:\\file_export
    python scripts/load_file_sweeps.py --only humana     # one payer
    python scripts/load_file_sweeps.py --dry-run         # list units only, no DB writes

NEW vs UPDATED counting: `before`/`after` are the target table's row count
taken immediately before/after loading one file. `new = after - before` (net
rows added -- upsert never removes a row, so any increase is exactly the count
of previously-absent ids). `updated = max(0, loaded_rows - new)`, where
`loaded_rows` is the number of lines that transformed successfully: every
loaded row is either one of the `new` net-new ids or a re-upsert of an id that
already existed, so this split is exact EXCEPT when a single file upserts the
same id more than once -- that inflates `loaded_rows` by one without moving
`after` a second time, which shows up as an extra "updated". That is the one
acceptable inexactness called out in the task; it only overcounts `updated`,
never `new`, and file-sweep captures are sequential single-pass pulls so
intra-file duplicate ids should be rare.
"""
from __future__ import annotations

import argparse
import json
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import load_manifest                             # noqa: E402
from provdir.etl.loader import (                                      # noqa: E402
    count_rows, ensure_payer_schema, insert_provenance, pg_connection,
    prepare_stage, schema_for, upsert_batch,
)
from provdir.etl.transform import TransformError, transform_resource  # noqa: E402
from provdir.models import RESOURCE_TABLES                            # noqa: E402

BATCH = 5000
DEFAULT_DIR = r"I:\file_export"
SUMMARY_PATH = REPO / "output" / "orchestrator" / "file_load_summary.json"

# Dependency-friendly per-payer load order (chain sources before their dependents).
RES_ORDER = ["InsurancePlan", "Organization", "Location", "HealthcareService",
             "OrganizationAffiliation", "Practitioner", "PractitionerRole"]

# (payer, resource) units to skip outright: high-volume / still in flight via a
# separate capture path (see scripts/run_elevance_to_file.bat, aetna_cvs bulk).
EXCLUDE = {("aetna_cvs", "PractitionerRole"), ("elevance", "PractitionerRole")}


@dataclass
class UnitResult:
    payer: str
    resource: str
    file_rows: int = 0
    new: int = 0
    updated: int = 0
    errors: int = 0


def discover_units(root: Path, only: str | None) -> list[tuple[str, str, Path]]:
    """Return (payer, resource, path) triples, payers sorted, resources in RES_ORDER."""
    units: list[tuple[str, str, Path]] = []
    for payer_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        payer = payer_dir.name
        if only and payer != only:
            continue
        found: list[tuple[str, Path]] = []
        for f in payer_dir.glob(f"{payer}_*.ndjson"):
            resource = f.name[len(payer) + 1: -len(".ndjson")]
            found.append((resource, f))
        found.sort(key=lambda item: (
            RES_ORDER.index(item[0]) if item[0] in RES_ORDER else len(RES_ORDER), item[0]))
        for resource, f in found:
            units.append((payer, resource, f))
    return units


def load_file(payer: str, resource: str, path: Path, base_url: str, table, schema: str) -> UnitResult:
    started = datetime.now(tz=timezone.utc)
    conn = pg_connection(schema)
    result = UnitResult(payer=payer, resource=resource)
    try:
        prepare_stage(conn, table)
        before = count_rows(conn, table)
        batch: list[dict] = []
        file_rows = 0
        loaded_rows = 0
        errors = 0
        with open(path, "rb") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                file_rows += 1
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        raise TransformError("line is valid JSON but not an object")
                    row = transform_resource(obj, payer, base_url)
                except (TransformError, ValueError, TypeError):
                    errors += 1
                    continue
                batch.append(row)
                loaded_rows += 1
                if len(batch) >= BATCH:
                    upsert_batch(conn, table, batch, update=True)
                    conn.commit()
                    batch = []
                    if loaded_rows % 500000 < BATCH:
                        print(f"  {payer}/{resource}: ~{loaded_rows:,} rows loaded", flush=True)
            if batch:
                upsert_batch(conn, table, batch, update=True)
                conn.commit()

        after = count_rows(conn, table)
        new = after - before
        updated = max(0, loaded_rows - new)

        insert_provenance(conn, {
            "payer_id": payer, "source_base_url": base_url, "resource_type": resource,
            "started_at": started, "finished_at": datetime.now(tz=timezone.utc),
            "status": "ok", "page_count": 0, "resource_count": after,
            "error_count": errors, "prior_count": before,
            "pct_change": round(100.0 * new / before, 1) if before else None,
            "notes": {
                "method": "file-load", "file": str(path), "file_rows": file_rows,
                "loaded_rows": loaded_rows, "new": new, "updated": updated,
                "errors": errors, "before": before, "after": after,
            },
        })
        conn.commit()

        result.file_rows, result.new, result.updated, result.errors = file_rows, new, updated, errors
        print(f"LOADED {payer}/{resource}: file_rows={file_rows:,} new={new:,} "
              f"updated={updated:,} errors={errors:,}", flush=True)
    finally:
        conn.close()
    return result


def print_summary(results: list[UnitResult]) -> None:
    rows = sorted(results, key=lambda r: r.new, reverse=True)
    header = f"{'payer':<28}{'resource':<26}{'file_rows':>14}{'new':>12}{'updated':>12}{'errors':>10}"
    print("\n=== FILE LOAD SUMMARY ===", flush=True)
    print(header, flush=True)
    print("-" * len(header), flush=True)
    for r in rows:
        print(f"{r.payer:<28}{r.resource:<26}{r.file_rows:>14,}{r.new:>12,}"
              f"{r.updated:>12,}{r.errors:>10,}", flush=True)
    print("-" * len(header), flush=True)
    print(f"{'TOTAL':<28}{'':<26}{sum(r.file_rows for r in results):>14,}"
          f"{sum(r.new for r in results):>12,}{sum(r.updated for r in results):>12,}"
          f"{sum(r.errors for r in results):>10,}", flush=True)


def write_summary_json(results: list[UnitResult], failed: list[tuple[str, str]]) -> None:
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "failed": [{"payer": p, "resource": r} for p, r in failed],
        "units": [
            {"payer": r.payer, "resource": r.resource, "file_rows": r.file_rows,
             "new": r.new, "updated": r.updated, "errors": r.errors}
            for r in sorted(results, key=lambda r: r.new, reverse=True)
        ],
        "totals": {
            "file_rows": sum(r.file_rows for r in results),
            "new": sum(r.new for r in results),
            "updated": sum(r.updated for r in results),
            "errors": sum(r.errors for r in results),
        },
    }
    SUMMARY_PATH.write_text(json.dumps(payload, indent=2))
    print(f"summary written to {SUMMARY_PATH}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=DEFAULT_DIR, help="root of the file-sweep capture tree")
    ap.add_argument("--only", help="load just this payer key")
    ap.add_argument("--exclude", default="",
                    help="comma-separated payer keys to skip (e.g. still-in-flight sweeps)")
    ap.add_argument("--dry-run", action="store_true",
                    help="list what would load (payer/resource/file size); no DB writes")
    args = ap.parse_args()
    exclude_payers = {p.strip() for p in args.exclude.split(",") if p.strip()}

    root = Path(args.dir)
    if not root.is_dir():
        raise SystemExit(f"--dir not found: {root}")

    units = discover_units(root, args.only)
    if not units:
        print("no (payer, resource) ndjson files found", flush=True)
        return 0

    payers = sorted({p for p, _, _ in units})
    base_urls: dict[str, str] = {}
    skip_payers: set[str] = set()
    for payer in payers:
        try:
            base_urls[payer] = load_manifest().by_key(payer).base_url
        except KeyError:
            print(f"SKIP payer {payer}: not in endpoint manifest (base_url unknown)", flush=True)
            skip_payers.add(payer)

    results: list[UnitResult] = []
    failed: list[tuple[str, str]] = []
    schemas_ready: set[str] = set()
    for payer, resource, path in units:
        if payer in skip_payers:
            continue
        if payer in exclude_payers:
            continue
        if resource == "Endpoint":
            print(f"SKIP {payer}/{resource}: Endpoint resource excluded", flush=True)
            continue
        if (payer, resource) in EXCLUDE:
            print(f"SKIP {payer}/{resource}: in EXCLUDE set", flush=True)
            continue
        if resource not in RESOURCE_TABLES:
            print(f"SKIP {payer}/{resource}: unrecognized resource type", flush=True)
            continue
        if path.stat().st_size == 0:
            print(f"SKIP {payer}/{resource}: empty file (skipped-empty)", flush=True)
            continue

        base_url = base_urls[payer]
        if args.dry_run:
            print(f"WOULD LOAD {payer}/{resource}: {path} ({path.stat().st_size:,} bytes)",
                  flush=True)
            continue

        try:
            schema = schema_for(payer)
            if payer not in schemas_ready:
                ensure_payer_schema(schema)
                schemas_ready.add(payer)
            table = RESOURCE_TABLES[resource]
            results.append(load_file(payer, resource, path, base_url, table, schema))
        except Exception as exc:
            # One bad file (I/O error, DB hiccup, mid-file crash) must not abort
            # the rest of the sweep. Partial commits are safe: a re-run recomputes
            # before/after honestly and the DO UPDATE upsert is idempotent.
            failed.append((payer, resource))
            print(f"FAILED {payer}/{resource}: {type(exc).__name__}: {exc}", flush=True)
            traceback.print_exc()

    if args.dry_run:
        return 0

    if failed:
        print(f"\nFAILED units ({len(failed)}): "
              + ", ".join(f"{p}/{r}" for p, r in failed), flush=True)
    print_summary(results)
    write_summary_json(results, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

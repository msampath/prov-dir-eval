"""Unified `provdir` command-line interface.

Subcommands are added per phase:
    inventory     Phase 1  validate manifest + emit inventory_manifest.json
    smoke         Phase 2  GET /metadata across a subset
    db check      Phase 0  Postgres connectivity check
    conformance   Phase 3  CapabilityStatement conformance (added in Phase 3)
    etl           Phase 5  extract/load into Postgres (added in Phase 5)
    quality       Phase 6  conformance + referential integrity (added in Phase 6)
    score         Phase 7  composite data-quality scores (added in Phase 7)
    dashboard     Phase 7  build the static site (added in Phase 7)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Optional

from .logging_setup import setup_logging


def _split_keys(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    return [k.strip() for k in value.split(",") if k.strip()]


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="provdir", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("inventory", help="validate manifest + emit inventory_manifest.json")

    p_smoke = sub.add_parser("smoke", help="GET /metadata across a subset")
    p_smoke.add_argument("--subset", help="comma-separated endpoint keys")
    p_smoke.add_argument("--mvp", action="store_true", help="use the 7 fully-open MVP endpoints (default)")
    p_smoke.add_argument("--all-known", action="store_true", help="probe every KNOWN endpoint")

    p_db = sub.add_parser("db", help="database utilities")
    p_db.add_argument("db_command", nargs="?", default="check", choices=["check"])

    p_conf = sub.add_parser("conformance", help="Phase 3 CapabilityStatement conformance")
    p_conf.add_argument("--subset", help="comma-separated endpoint keys")
    p_conf.add_argument("--all-known", action="store_true")
    p_conf.add_argument("--no-probe", action="store_true", help="declared-only, skip live probes")

    p_fetch = sub.add_parser("fetch-ig", help="Phase 3 download the Plan-Net IG CapabilityStatement")
    p_fetch.add_argument("--version", default=None, help="IG version (default from settings)")

    p_etl = sub.add_parser("etl", help="Phase 5 extract + load into Postgres")
    p_etl.add_argument("--subset", help="comma-separated endpoint keys")
    p_etl.add_argument("--all-known", action="store_true")
    p_etl.add_argument("--max-pages", type=int, default=None, help="cap pages per resource (smoke ingest)")
    p_etl.add_argument("--resources", help="comma-separated resource types (default: all expected)")
    p_etl.add_argument("--upsert", action="store_true",
                       help="on (payer_id,id) conflict UPDATE the row (refresh changed records) instead of skipping")
    p_etl.add_argument("--resume", action="store_true",
                       help="resume bare pagination from the last committed checkpoint (offset servers; 72h TTL)")
    p_etl.add_argument("--allow-slim-upsert", action="store_true",
                       help="load payer/resource pairs registered in public.slim_table, "
                            "which are skipped by default. Those tables store a stub "
                            "`resource` with the real JSON archived on disk; loading "
                            "them here writes full resources back in, re-inflating the "
                            "table (aetna_cvs/PractitionerRole: ~500M rows toward "
                            "~1.3 TB) and mixing full with stub rows. Only use this "
                            "after confirming the storage exists.")

    sub.add_parser("coverage", help="per-(payer, resource) extraction coverage scoreboard")
    sub.add_parser("status-dashboard", help="build the collection-status dashboard (payer x resource coverage)")

    p_harvest = sub.add_parser("harvest", help="reference-graph _include sweep (roles+practitioners+locations+... in one pass)")
    p_harvest.add_argument("--subset", help="comma-separated endpoint keys")
    p_harvest.add_argument("--max-pages", type=int, default=None)

    p_qual = sub.add_parser("quality", help="Phase 6 conformance + referential integrity")
    p_qual.add_argument("--subset", help="comma-separated endpoint keys")

    p_elem = sub.add_parser("element-coverage",
                            help="Phase 4 USCDI v3 / MPF / Plan-Net-extensions element coverage")
    p_elem.add_argument("--assessment", default="all",
                        choices=["uscdi", "mpf", "plannet-ext", "all"])
    p_elem.add_argument("--subset", help="comma-separated endpoint keys")
    p_elem.add_argument("--sample-root", default="F:/fhir_val",
                        help="root searched for slim-table sample files "
                             "(F:/fhir_val_keep is always also searched)")
    p_elem.add_argument("--resume-run", type=int, default=None,
                        help="continue an existing element_coverage_run id, "
                             "skipping (payer, assessment) pairs already recorded")
    p_elem.add_argument("--auto-resume", action="store_true",
                        help="resume the newest UNFINISHED run if there is one, "
                             "else start fresh (for detached launchers)")

    sub.add_parser("score", help="Phase 7 compute composite data-quality scores")
    sub.add_parser("dashboard", help="Phase 7 build the static site")

    args = parser.parse_args(argv)

    import logging

    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO, run_name=args.command)

    if args.command == "inventory":
        from .inventory import write_inventory

        out = write_inventory()
        print(f"Wrote {out}")
        return 0

    if args.command == "smoke":
        from .smoke import run_smoke

        keys = _split_keys(args.subset)
        summary = asyncio.run(
            run_smoke(keys=keys, include_all_known=args.all_known)
        )
        return 0 if summary["failed"] == 0 else 1

    if args.command == "db":
        from .db import _cli as db_cli

        return db_cli([args.db_command])

    if args.command == "fetch-ig":
        from .conformance.ig import fetch_ig_capability_statement

        path = fetch_ig_capability_statement(version=args.version)
        print(f"Saved IG CapabilityStatement -> {path}")
        return 0

    if args.command == "conformance":
        from .conformance.runner import run_conformance

        keys = _split_keys(args.subset)
        summary = asyncio.run(
            run_conformance(keys=keys, include_all_known=args.all_known, probe=not args.no_probe)
        )
        return 0 if summary.get("endpoints") else 1

    if args.command == "etl":
        from .etl.pipeline import run_etl

        keys = _split_keys(args.subset)
        resources = _split_keys(args.resources)
        summary = asyncio.run(
            run_etl(
                keys=keys,
                include_all_known=args.all_known,
                max_pages=args.max_pages,
                resources=resources,
                upsert=args.upsert,
                resume=args.resume,
                allow_slim=args.allow_slim_upsert,
            )
        )
        return 0 if summary.get("ok") else 1

    if args.command == "coverage":
        from .etl.coverage import coverage_report

        coverage_report()
        return 0

    if args.command == "status-dashboard":
        from .quality.collection_status import run as run_status

        data = run_status()
        print(f"Collection dashboard: {data['payer_count']} payers, {data['grand_total']:,} rows")
        print("  -> output/collection_status.html (open via file://)")
        print("  -> output/collection_status_widget.html (inline fragment)")
        return 0

    if args.command == "harvest":
        from .etl.pipeline import run_reference_harvest

        keys = _split_keys(args.subset)
        asyncio.run(run_reference_harvest(keys=keys, max_pages=args.max_pages))
        return 0

    if args.command == "quality":
        from .quality.runner import run_quality

        keys = _split_keys(args.subset)
        run_quality(keys=keys)
        return 0

    if args.command == "element-coverage":
        from .config import load_manifest
        from .etl.loader import pg_connection
        from .quality.element_coverage import run_element_coverage
        from .quality.mpf import (
            MPF_CALLOUTS, MPF_CHECKS, MPF_SOURCE_PROVENANCE, PLANNET_EXT_ATTRIBUTION,
            PLANNET_EXT_CHECKS, classify_ma,
        )
        from .quality.uscdi import USCDI_CHECKS, USCDI_ELEMENT_META

        keys = _split_keys(args.subset)
        want = ({"uscdi", "mpf", "plannet_ext"} if args.assessment == "all"
               else {args.assessment.replace("-", "_")})

        assessments: dict = {}
        meta: dict = {}
        assessment_payers: dict = {}

        if "uscdi" in want:
            assessments["uscdi"] = USCDI_CHECKS
            meta["uscdi"] = {
                "disclaimer": (
                    "No official USCDI v3 -> Plan-Net element mapping exists (US "
                    "Core maps Care Team Member to CareTeam only, a resource "
                    "Plan-Net does not define). Every element below is an "
                    "inference; see each row's `mapping` label and "
                    "uscdi.py's module docstring."
                ),
                "facility_note": "USCDI v3 has no Facility class (that arrives in USCDI v4+).",
                "source": "USCDI v3.1 (June 2025); certification baseline Jan 1 2026 "
                          "(45 CFR 170.213, HTI-1).",
                "element_meta": USCDI_ELEMENT_META,
            }

        if "mpf" in want:
            manifest = load_manifest()
            conn = pg_connection()
            try:
                ma_roster = classify_ma(conn, manifest)
            finally:
                conn.close()
            print("=== MA classifier roster (payers with loaded schemas; "
                  "manifest payers without data are not listed) ===")
            for r in ma_roster:
                flag = "MA" if r["classified"] else "--"
                sigs = r["override"] or ", ".join(e["signal"] for e in r["evidence"]) or "(no signal)"
                print(f"  [{flag}] {r['payer']:24s} {sigs}")
            ma_keys = {r["payer"] for r in ma_roster if r["classified"]}
            assessments["mpf"] = MPF_CHECKS
            assessment_payers["mpf"] = ma_keys
            meta["mpf"] = {
                "source_provenance": MPF_SOURCE_PROVENANCE,
                "callouts": MPF_CALLOUTS,
                "ma_roster": ma_roster,
            }

        if "plannet_ext" in want:
            assessments["plannet_ext"] = PLANNET_EXT_CHECKS
            meta["plannet_ext"] = {"attribution": PLANNET_EXT_ATTRIBUTION}

        resume_run = args.resume_run
        if args.auto_resume and not resume_run:
            # A detached launcher would otherwise silently restart a multi-hour
            # sweep from the first payer after any interruption.
            from .etl.loader import pg_connection as _pg
            _c = _pg()
            try:
                with _c.cursor() as _cur:
                    _cur.execute("SELECT max(run_id) FROM public.element_coverage_run "
                                 "WHERE finished_at IS NULL")
                    _r = _cur.fetchone()
                resume_run = _r[0] if _r and _r[0] else None
            finally:
                _c.close()
            if resume_run:
                print(f"--auto-resume: picked up unfinished run {resume_run}")
            else:
                print("--auto-resume: no unfinished run; starting fresh")

        summary = run_element_coverage(
            assessments, keys=keys,
            sample_root=[args.sample_root, "F:/fhir_val_keep"],
            resume_run=resume_run, meta=meta,
            assessment_payers=assessment_payers or None,
        )
        print(f"element_coverage run {summary['run_id']}: {len(summary['payers'])} payer(s) measured")
        return 0

    if args.command == "score":
        from .quality.scoring import run_scoring

        run_scoring()
        return 0

    if args.command == "dashboard":
        from .quality.dashboard import build_dashboard

        out = build_dashboard()
        print(f"Built static dashboard -> {out}")
        return 0

    parser.error(f"unknown command {args.command!r}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

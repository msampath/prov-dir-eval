"""Phase 3 runner — per-endpoint conformance evaluation + roll-up.

Flow per endpoint:
  1. fetch live /metadata
  2. build the declared-capability matrix vs IG requirements
  3. actively probe applicable+declared resources
  4. fold probe results back into the matrix classifications
  5. score and write output/conformance/<key>.json
Then write output/conformance/summary.csv.
"""

from __future__ import annotations

import asyncio
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .. import OUTPUT_DIR
from ..config import Endpoint, Settings, get_settings, load_manifest
from ..http_client import FhirClient, FhirSession
from ..logging_setup import get_logger
from .checker import (
    DECLARED_BUT_FAILING,
    DECLARED_NOT_PROBED,
    NOT_APPLICABLE,
    SUPPORTED,
    build_resource_matrix,
    parse_live_capability,
    score_resource,
)
from .ig import IG_CAPABILITY_PATH, IgRequirements, fetch_ig_capability_statement, parse_capability_statement

log = get_logger(__name__)

CONF_DIR = OUTPUT_DIR / "conformance"


def _ensure_ig(settings: Settings) -> IgRequirements:
    if not IG_CAPABILITY_PATH.exists():
        log.info("IG CapabilityStatement not found locally; fetching v%s", settings.plannet_ig_version)
        fetch_ig_capability_statement(version=settings.plannet_ig_version)
    return parse_capability_statement()


async def _evaluate_endpoint(
    session: FhirSession,
    ep: Endpoint,
    ig: IgRequirements,
    expected_resources: list[str],
    probe: bool,
) -> dict:
    client: FhirClient = session.client_for(ep)
    report: dict = {
        "key": ep.key,
        "payer_name": ep.payer_name,
        "base_url": ep.base_url,
        "ig_version": ig.ig_version,
        "evaluated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    declared: dict = {}
    if ep.quirks.no_metadata:
        # Gateway doesn't expose a CapabilityStatement; confirm liveness and run
        # probe-only conformance against the expected resources.
        try:
            live = await client.liveness()
        except Exception as exc:  # noqa: BLE001
            report["reachable"] = False
            report["error"] = f"{type(exc).__name__}: {exc}"
            log.warning("conformance: %s liveness failed: %s", ep.key, report["error"])
            return report
        report["reachable"] = True
        report["metadata_available"] = False
        report["server"] = {"note": "no CapabilityStatement; probe-only", "liveness": live}
    else:
        try:
            metadata = await client.metadata()
        except Exception as exc:  # noqa: BLE001
            report["reachable"] = False
            report["error"] = f"{type(exc).__name__}: {exc}"
            log.warning("conformance: %s metadata fetch failed: %s", ep.key, report["error"])
            return report
        report["reachable"] = True
        report["metadata_available"] = True
        software = metadata.get("software") or {}
        report["server"] = {
            "fhir_version": metadata.get("fhirVersion"),
            "software_name": software.get("name"),
            "software_version": software.get("version"),
            "publisher": metadata.get("publisher"),
        }
        declared = parse_live_capability(metadata)
        report["declared_resource_types"] = sorted(declared.keys())

    metadata_available = report.get("metadata_available", True)
    resources: dict[str, dict] = {}
    for rtype, req in sorted(ig.resources.items()):
        expected = rtype in expected_resources
        rc = build_resource_matrix(req, declared.get(rtype), expected)
        if not metadata_available and expected:
            rc.notes.append("no CapabilityStatement; classification from live probes only")

        # Probe when declared, or (probe-only mode) for every expected resource.
        should_probe = probe and rc.applicable and (rc.declared or not metadata_available)
        if should_probe:
            from .probe import probe_resource

            if metadata_available:
                shall_for_probe = [p for p, s in rc.shall_search_params.items() if s == DECLARED_NOT_PROBED]
                includes, revincludes = rc.includes_declared, rc.revincludes_declared
            else:
                shall_for_probe = list(req.shall_search_params)
                includes, revincludes = req.supported_includes, req.supported_revincludes
            rc.probes = await probe_resource(client, rtype, shall_for_probe, includes, revincludes)
            _fold_probes(rc)

        resources[rtype] = {**rc.model_dump(), "score": score_resource(rc)}

    report["resources"] = resources
    report["summary"] = _endpoint_summary(resources)
    log.info(
        "conformance: %-14s declared %d/%d resources, SHALL-param coverage %.0f%%",
        ep.key,
        report["summary"]["resources_declared"],
        report["summary"]["resources_expected"],
        report["summary"]["shall_param_declared_pct"] or 0.0,
    )
    return report


def _fold_probes(rc) -> None:
    """Update classifications using live probe outcomes."""
    search = rc.probes.get("search")
    if search is not None:
        # search-type interaction reality check.
        if "search-type" in rc.interactions and rc.interactions["search-type"] != NOT_APPLICABLE:
            rc.interactions["search-type"] = SUPPORTED if search.get("ok") else DECLARED_BUT_FAILING
        if not search.get("ok"):
            rc.notes.append("basic search probe failed: " + str(search.get("note")))
        if search.get("requires_search_param"):
            rc.notes.append("server requires a search parameter (bare search rejected)")
        # Credit the one SHALL param the adaptive search exercised.
        param = search.get("param")
        if param and param in rc.shall_search_params:
            rc.shall_search_params[param] = SUPPORTED if search.get("ok") else DECLARED_BUT_FAILING


def _endpoint_summary(resources: dict[str, dict]) -> dict:
    expected = [r for r in resources.values() if r["applicable"]]
    declared = [r for r in expected if r["declared"]]

    def avg(key: str) -> Optional[float]:
        vals = [r["score"].get(key) for r in declared if r["score"].get(key) is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    probe_records = []
    for r in resources.values():
        for p in (r.get("probes") or {}).values():
            probe_records.append(bool(p.get("ok")))

    return {
        "resources_expected": len(expected),
        "resources_declared": len(declared),
        "resource_declared_pct": round(100.0 * len(declared) / len(expected), 1) if expected else None,
        "shall_param_declared_pct": avg("shall_param_declared_pct"),
        "required_interaction_pct": avg("required_interaction_pct"),
        "probes_run": len(probe_records),
        "probes_passed": sum(probe_records),
        "probe_pass_pct": round(100.0 * sum(probe_records) / len(probe_records), 1) if probe_records else None,
    }


async def run_conformance(
    keys: Optional[list[str]] = None,
    include_all_known: bool = False,
    probe: bool = True,
    settings: Optional[Settings] = None,
) -> dict:
    settings = settings or get_settings()
    manifest = load_manifest()
    ig = _ensure_ig(settings)

    if keys:
        endpoints = manifest.subset(keys)
    elif include_all_known:
        endpoints = manifest.known()
    else:
        endpoints = manifest.mvp()

    runnable, skipped = [], []
    for ep in endpoints:
        reason = ep.skip_reason(settings)
        if reason and (reason.startswith("missing-credentials") or reason in {"blocked", "missing-token-url"}):
            skipped.append({"key": ep.key, "reason": reason})
        else:
            runnable.append(ep)

    CONF_DIR.mkdir(parents=True, exist_ok=True)
    reports: list[dict] = []
    async with FhirSession(settings) as session:
        async def _one(ep: Endpoint) -> dict:
            expected = ep.expected_resources(manifest.plannet_resources)
            rep = await _evaluate_endpoint(session, ep, ig, expected, probe)
            (CONF_DIR / f"{ep.key}.json").write_text(json.dumps(rep, indent=2), encoding="utf-8")
            return rep

        reports = await asyncio.gather(*(_one(ep) for ep in runnable))

    summary = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "ig_version": ig.ig_version,
        "endpoints": [
            {
                "key": r["key"],
                "payer_name": r["payer_name"],
                "reachable": r.get("reachable"),
                **(r.get("summary") or {}),
                "error": r.get("error"),
            }
            for r in reports
        ],
        "skipped": skipped,
    }
    (CONF_DIR / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _write_summary_csv(summary["endpoints"])
    log.info(
        "conformance complete: %d evaluated, %d skipped -> %s",
        len(reports),
        len(skipped),
        CONF_DIR,
    )
    return summary


def _write_summary_csv(rows: list[dict]) -> Path:
    out = CONF_DIR / "summary.csv"
    cols = [
        "key",
        "payer_name",
        "reachable",
        "resources_expected",
        "resources_declared",
        "resource_declared_pct",
        "shall_param_declared_pct",
        "required_interaction_pct",
        "probes_run",
        "probes_passed",
        "probe_pass_pct",
        "error",
    ]
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return out

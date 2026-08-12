#!/usr/bin/env python
"""Run every paginate-to-file unit for ONE host, resumably.

One host == one scheduled task, so hosts are isolated (killing/stalling one host's
task cannot affect another), and same-host units run one-at-a-time by default (the
contention/token-pressure rule) unless that host is listed in HOST_CONCURRENCY. Units = each target payer on this host x each
resource type it serves (Endpoint excluded). Output -> I:\\file_export\\<payer>.
Each unit is driven by paginate_to_file.py, which is resumable and skips units
already marked .done, so re-running the task continues where it left off.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import load_manifest       # noqa: E402
from provdir.db import get_engine              # noqa: E402
from sqlalchemy import text                    # noqa: E402

# Sources with incomplete pulls (Endpoint-only and already-handled elevance/aetna
# excluded). Each is swept for every resource type it serves, minus Endpoint.
PAYERS = [
    "premera", "hap", "devoted", "christus", "amerihealth_laex",
    "capital_blue", "uhc_optum",
    "amerihealth_deex", "amerihealth_flex", "amerihealth_ncex", "amerihealth_scex",
    "bcbs_la", "hcsc", "mihin_bcbsm", "mihin_mdhhs",
    "vermont_blue_advantage", "bcbs_mn",
    "bcbs_az",  # re-pull to file: PractitionerRole suspected over-corrected low (99k)
]

# The reference-graph sources (humana, excellus, bcbs_ks, mvphealthcare, regence,
# medica, dean_healthsparq, health_advantage_ar, kaiser, uhc) were dropped from the
# file sweep: force-bare proved INSUFFICIENT for them (server caps bare pagination
# far below the real totals, e.g. regence Location 47k/5.5M), so their DB
# reference-graph / adaptive pulls remain authoritative. Empty set => no force-bare.
REFGRAPH: set[str] = set()
OUT_ROOT = r"I:\file_export"
# dependency-friendly order (chain sources before their dependents)
RES_ORDER = ["InsurancePlan", "Organization", "Location", "HealthcareService",
             "OrganizationAffiliation", "Practitioner", "PractitionerRole"]

# Per-host overrides of RES_ORDER, for when "chain sources first" is the wrong
# priority. Bare pagination to file has no cross-resource dependency (nothing
# here chains off another resource's ids -- that is the reference-graph path,
# not this one), so ordering is purely about which data is worth having first.
RES_ORDER_BY_HOST: dict[str, list[str]] = {
    # premera/Opala: PractitionerRole is the ONLY resource the old broken
    # pagination left genuinely unreachable (167 rows against a 95,404 server
    # total). Everything else we already hold a usable partial of, and Location
    # alone is 7,523 pages -- at the measured 4.0s/page it would otherwise eat
    # 8.4h before the actual gap even starts.
    "opala.tech": ["PractitionerRole", "Practitioner", "Organization",
                   "HealthcareService", "Location", "InsurancePlan",
                   "OrganizationAffiliation"],
}

# Hosts that tolerate concurrent unit walks, and how many. Default is 1: units
# on one host run SEQUENTIALLY, which is the project-wide rule for not
# hammering a shared datasource. Raising it is a per-host judgement about that
# server, never a global default.
HOST_CONCURRENCY: dict[str, int] = {
    # Opala serves premera's ~753k resources at a hard 50/page and ~4.0s/page,
    # so a serial walk is ~16.7h. The walks are independent cursor chains over
    # different resource types; running the five populated ones together
    # bounds the pull by its longest single chain (Location, ~8.4h) instead of
    # their sum. Five concurrent requests is a modest ask of a vendor API --
    # deliberately not one-per-unit.
    "opala.tech": 5,
}


def units_for_host(host: str) -> list[tuple[str, str]]:
    hostmap = {e.key: e.host for e in load_manifest().endpoints}
    payers = [p for p in PAYERS if hostmap.get(p) == host]
    order = RES_ORDER_BY_HOST.get(host, RES_ORDER)
    units: list[tuple[str, str]] = []
    with get_engine().connect() as c:
        for p in payers:
            served = {r[0] for r in c.execute(text(
                "select distinct resource_type from public.provenance "
                "where payer_id=:p and resource_type <> 'Endpoint'"), {"p": p})}
            for rt in order:
                if rt in served:
                    units.append((p, rt))
    return units


def run_unit(host: str, payer: str, rtype: str) -> None:
    print(f"[{host}] start {payer} {rtype}", flush=True)
    cmd = [sys.executable, str(REPO / "scripts" / "paginate_to_file.py"),
           "--payer", payer, "--resource", rtype,
           "--out-dir", f"{OUT_ROOT}\\{payer}"]
    if payer in REFGRAPH:
        cmd.append("--force-bare")
    subprocess.run(cmd, cwd=str(REPO))
    print(f"[{host}] done  {payer} {rtype}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--concurrency", type=int, default=None,
                    help="units to walk at once for this host. Defaults to "
                         "HOST_CONCURRENCY (1 unless the host is listed there). "
                         "Above 1 breaks the one-request-stream-per-datasource "
                         "rule on purpose -- only for servers known to tolerate it.")
    a = ap.parse_args()
    units = units_for_host(a.host)
    conc = a.concurrency if a.concurrency else HOST_CONCURRENCY.get(a.host, 1)
    conc = max(1, min(conc, len(units) or 1))
    print(f"host {a.host}: {len(units)} units, concurrency {conc}", flush=True)

    if conc == 1:
        for p, rt in units:
            run_unit(a.host, p, rt)
    else:
        # Units are independent cursor chains over different resource types, so
        # they parallelize cleanly. Each still writes its own ndjson + its own
        # checkpoint sidecar, so an interrupted concurrent run resumes exactly
        # like an interrupted serial one.
        with ThreadPoolExecutor(max_workers=conc) as pool:
            list(pool.map(lambda u: run_unit(a.host, u[0], u[1]), units))
    print(f"host {a.host}: COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

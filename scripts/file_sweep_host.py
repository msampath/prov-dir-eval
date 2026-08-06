#!/usr/bin/env python
"""Run every paginate-to-file unit for ONE host, sequentially and resumably.

One host == one scheduled task, so hosts are isolated (killing/stalling one host's
task cannot affect another), while same-host payers still run one-at-a-time (the
contention/token-pressure rule). Units = each target payer on this host x each
resource type it serves (Endpoint excluded). Output -> I:\\file_export\\<payer>.
Each unit is driven by paginate_to_file.py, which is resumable and skips units
already marked .done, so re-running the task continues where it left off.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import load_manifest       # noqa: E402
from provdir.db import get_engine              # noqa: E402
from sqlalchemy import text                    # noqa: E402

# Sources with incomplete pulls (Endpoint-only and already-handled elevance/aetna
# excluded). Each is swept for every resource type it serves, minus Endpoint.
PAYERS = [
    "premera", "hap", "devoted", "christus", "kaiser", "amerihealth_laex",
    "regence", "medica", "capital_blue", "humana", "uhc_optum", "uhc",
    "amerihealth_deex", "amerihealth_flex", "amerihealth_ncex", "amerihealth_scex",
    "bcbs_ks", "bcbs_la", "excellus", "hcsc", "mihin_bcbsm", "mihin_mdhhs",
    "mvphealthcare", "vermont_blue_advantage", "bcbs_mn",
    "bcbs_az",  # re-pull to file: PractitionerRole suspected over-corrected low (99k)
    "dean_healthsparq", "health_advantage_ar",  # reference-graph sources, force bare
]

# Reference-graph sources (id_chain / id_read / _include harvest in config). Per
# user: try plain BARE search to file for these instead of the reference-graph
# logic (validated on kaiser PractitionerRole: bare works, server_total 531,619).
REFGRAPH = {
    "humana", "excellus", "bcbs_ks", "mvphealthcare", "regence", "medica",
    "dean_healthsparq", "health_advantage_ar", "kaiser", "uhc",
}
OUT_ROOT = r"I:\file_export"
# dependency-friendly order (chain sources before their dependents)
RES_ORDER = ["InsurancePlan", "Organization", "Location", "HealthcareService",
             "OrganizationAffiliation", "Practitioner", "PractitionerRole"]


def units_for_host(host: str) -> list[tuple[str, str]]:
    hostmap = {e.key: e.host for e in load_manifest().endpoints}
    payers = [p for p in PAYERS if hostmap.get(p) == host]
    units: list[tuple[str, str]] = []
    with get_engine().connect() as c:
        for p in payers:
            served = {r[0] for r in c.execute(text(
                "select distinct resource_type from public.provenance "
                "where payer_id=:p and resource_type <> 'Endpoint'"), {"p": p})}
            for rt in RES_ORDER:
                if rt in served:
                    units.append((p, rt))
    return units


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    a = ap.parse_args()
    units = units_for_host(a.host)
    print(f"host {a.host}: {len(units)} units", flush=True)
    for p, rt in units:
        print(f"[{a.host}] start {p} {rt}", flush=True)
        cmd = [sys.executable, str(REPO / "scripts" / "paginate_to_file.py"),
               "--payer", p, "--resource", rt, "--out-dir", f"{OUT_ROOT}\\{p}"]
        if p in REFGRAPH:
            cmd.append("--force-bare")
        subprocess.run(cmd, cwd=str(REPO))
        print(f"[{a.host}] done  {p} {rt}", flush=True)
    print(f"host {a.host}: COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
"""Resume-loop the bare-pagination units that stalled on deep 503/timeout.

Groups the affected (payer, resource) units by host: hosts run in parallel, units
within a host run SEQUENTIALLY (one live pull per host — same rule the orchestrator
enforces), and each unit is driven by loop_resume.py (repeat --resume until it
completes or stops advancing). Detached from Claude via a scheduled task.
"""
from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
from provdir.config import load_manifest  # noqa: E402

# Bare-pagination units stalled on a deep 503/timeout (user-approved subset).
UNITS = [
    ("elevance", "PractitionerRole"), ("elevance", "Location"),
    ("kaiser", "Practitioner"), ("kaiser", "OrganizationAffiliation"),
    ("uhc_optum", "Location"), ("uhc_optum", "Organization"),
    ("uhc_optum", "Practitioner"), ("uhc_optum", "PractitionerRole"),
    ("uhc_optum", "OrganizationAffiliation"),
    ("amerihealth_laex", "OrganizationAffiliation"), ("amerihealth_laex", "PractitionerRole"),
    ("devoted", "PractitionerRole"),
    ("premera", "PractitionerRole"),
    ("mihin_bcbsm", "Location"), ("mihin_bcbsm", "Organization"),
    ("mihin_bcbsm", "OrganizationAffiliation"), ("mihin_bcbsm", "Practitioner"),
    ("mihin_bcbsm", "PractitionerRole"),
]


def main() -> int:
    host = {e.key: e.host for e in load_manifest().endpoints}
    groups: dict = {}
    for payer, res in UNITS:
        groups.setdefault(host[payer], []).append((payer, res))

    def run_host(h: str, units: list) -> None:
        for payer, res in units:
            print(f"[{h}] start {payer} {res}", flush=True)
            subprocess.run(
                [sys.executable, str(REPO / "scripts" / "loop_resume.py"),
                 "--payer", payer, "--resource", res, "--patience", "4", "--sleep", "30"],
                cwd=str(REPO),
            )
            print(f"[{h}] done  {payer} {res}", flush=True)

    print(f"resume sweep: {len(UNITS)} units across {len(groups)} hosts", flush=True)
    with ThreadPoolExecutor(max_workers=6) as ex:
        for f in [ex.submit(run_host, h, u) for h, u in groups.items()]:
            f.result()
    print("resume sweep complete", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

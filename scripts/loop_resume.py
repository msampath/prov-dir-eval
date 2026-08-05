#!/usr/bin/env python
"""Repeat `provdir etl --resume` for one (payer, resource) until it stops making
progress. For flaky deep-pagination servers (Elevance) that 503 at inconsistent
depths and have no bulk fallback: each run ratchets the resume checkpoint forward,
so re-running walks past the previous 503 over time.

Stops when: the checkpoint is deleted after having existed (clean exhaustion), or
`patience` consecutive iterations make no advancement (row count and checkpoint
page both unchanged) — which tolerates transient early-503s but ends at a real
ceiling. No external stall-watchdog: bare offset pagination terminates on its own
(next-link exhaustion or 503), so it cannot infinite-loop like a daterange sweep.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.db import get_engine                    # noqa: E402
from provdir.etl.loader import schema_for            # noqa: E402
from provdir.models import RESOURCE_TABLES           # noqa: E402
from sqlalchemy import text                          # noqa: E402


def state(payer: str, rtype: str):
    tbl = RESOURCE_TABLES[rtype].name
    sch = schema_for(payer)
    with get_engine().connect() as c:
        rows = c.execute(text(f'select count(*) from "{sch}"."{tbl}"')).scalar()
        ck = c.execute(
            text("select pages_done from public.extract_checkpoint "
                 "where payer_id=:p and resource_type=:r"),
            {"p": payer, "r": rtype},
        ).fetchone()
    return rows, (ck[0] if ck else None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payer", required=True)
    ap.add_argument("--resource", required=True)
    ap.add_argument("--max-iters", type=int, default=60)
    ap.add_argument("--patience", type=int, default=3)
    ap.add_argument("--sleep", type=int, default=30,
                    help="seconds between iterations (let a 503-ing server recover)")
    a = ap.parse_args()

    had_checkpoint = False
    no_progress = 0
    for i in range(1, a.max_iters + 1):
        rows0, pg0 = state(a.payer, a.resource)
        if pg0 is not None:
            had_checkpoint = True
        print(f"[iter {i}] before rows={rows0:,} pages_done={pg0}", flush=True)
        rc = subprocess.run(
            [sys.executable, "-m", "provdir.cli", "etl", "--subset", a.payer,
             "--resources", a.resource, "--upsert", "--resume"],
            cwd=str(REPO),
        ).returncode
        rows1, pg1 = state(a.payer, a.resource)
        print(f"[iter {i}] after  rows={rows1:,} pages_done={pg1} rc={rc}", flush=True)

        if had_checkpoint and pg1 is None:
            print("checkpoint cleared -> clean exhaustion; done.", flush=True)
            break
        advanced = (rows1 > rows0) or ((pg1 or 0) > (pg0 or 0))
        no_progress = 0 if advanced else no_progress + 1
        if no_progress >= a.patience:
            print(f"no advancement for {a.patience} iters -> ceiling; stopping.", flush=True)
            break
        time.sleep(a.sleep)

    print(f"loop done: final rows={state(a.payer, a.resource)[0]:,}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

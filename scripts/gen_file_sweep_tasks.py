#!/usr/bin/env python
"""Emit one launcher .bat per host for the file sweep and print the host->task
mapping. One task per host = isolation between hosts, sequential within a host.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import load_manifest       # noqa: E402
from provdir.db import get_engine              # noqa: E402
from sqlalchemy import text                    # noqa: E402
from file_sweep_host import PAYERS             # noqa: E402

BAT_DIR = REPO / "scripts" / "filesweep"
LOG_DIR = REPO / "output" / "orchestrator"


def main() -> int:
    hostmap = {e.key: e.host for e in load_manifest().endpoints}
    hosts: dict[str, list[str]] = {}
    for p in PAYERS:
        h = hostmap.get(p)
        if h:
            hosts.setdefault(h, []).append(p)
    # only hosts that actually have >=1 served non-Endpoint resource
    BAT_DIR.mkdir(parents=True, exist_ok=True)
    with get_engine().connect() as c:
        for host, payers in sorted(hosts.items()):
            served_any = False
            for p in payers:
                n = c.execute(text("select count(distinct resource_type) from public.provenance "
                                   "where payer_id=:p and resource_type<>'Endpoint'"), {"p": p}).scalar()
                if n:
                    served_any = True
            if not served_any:
                continue
            label = re.sub(r"[^a-z0-9]+", "_", host.lower()).strip("_")
            bat = BAT_DIR / f"{label}.bat"
            log = LOG_DIR / f"file_sweep_{label}.log"
            bat.write_text(
                "@echo off\r\n"
                f"cd /d {REPO}\r\n"
                f'.venv\\Scripts\\python.exe scripts\\file_sweep_host.py --host "{host}" >> "{log}" 2>&1\r\n'
            )
            print(f"provdir_fs_{label}|{host}|{bat}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

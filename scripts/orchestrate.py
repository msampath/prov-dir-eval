#!/usr/bin/env python
"""Deterministic monthly re-pull orchestrator (no LLM in the loop).

Re-runs `provdir etl` for every known, runnable (payer, resource) unit, one
subprocess per unit, grouped into LANES so that payers sharing a datasource/host
run sequentially while independent datasources run in parallel. A per-lane
watchdog polls Postgres write activity and kills any unit that lands no new rows
for a stall window, then re-queues it once with --resume.

    python scripts/orchestrate.py --dry-run        # print the lane/unit plan
    python scripts/orchestrate.py --no-skip        # re-run everything (kickoff)
    python scripts/orchestrate.py                   # skip units already ok this month
    python scripts/orchestrate.py --status          # print the latest run's live state
    python scripts/orchestrate.py --only elevance   # one payer (debug)

Everything here is deterministic and self-contained; it only shells out to
`provdir.cli etl ... --upsert --resume` and reads Postgres. See the plan at
.claude/plans/indexed-riding-gadget.md for the design rationale.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.config import get_settings, load_manifest          # noqa: E402
from provdir.etl.loader import pg_connection, schema_for        # noqa: E402
from provdir.models import RESOURCE_TABLES                      # noqa: E402

# --- tunable policy constants ---------------------------------------------
POLL_INTERVAL_S = 300          # 5-min watchdog tick (user spec)
GRACE_S = 1200                 # 20-min initial grace before stall-counting
ZERO_POLLS_TO_KILL = 2         # consecutive zero-delta polls past grace => kill (=> 10 min)
MAX_LANES = 8                  # concurrent lanes
HOST_BUSY_RECHECK_S = 30       # re-scan cadence when an external process holds a lane's host

# Longer grace for units with a long quiet phase before rows land: id_read/id_chain
# reference-graph queries (uhc/uhc_optum), daterange count-bisection (humana),
# and very deep/slow pagination (kaiser). Keyed by payer or (payer, resource).
GRACE_OVERRIDES: dict = {
    "humana": 2400,
    "uhc": 2400,
    "uhc_optum": 2400,
    "kaiser": 2400,
}

# (payer, resource) units to never run. Aetna PractitionerRole is evidence-frozen
# pending Aetna's reply on its implausible ~380M count; navitus is an out-of-scope
# PBM pharmacy directory. "*" excludes the whole payer.
EXCLUSIONS: dict = {
    "aetna_cvs": {"PractitionerRole"},
    "navitus": {"*"},
}

# Resource order within a payer: sources before the resources that chain off them.
RESOURCE_ORDER = [
    "InsurancePlan", "Organization", "Location", "HealthcareService",
    "OrganizationAffiliation", "Practitioner", "PractitionerRole", "Endpoint",
]


def lane_of(host: str) -> str:
    """Map an endpoint host to its lane id. Same-host payers share a lane
    automatically; a few vendor families share one backend across subdomains and
    are collapsed here so they serialize too."""
    h = host.lower()
    if h.endswith("innovaccer.com"):
        return "innovaccer"
    if h.endswith("healthsparq.com"):
        return "healthsparq"
    if re.match(r"^api\.[a-z0-9]+fhir\.com$", h):
        return "oneup"        # 1upHealth family: distinct subdomains, one backend
    return h                  # everything else serializes by exact host


@dataclass
class Unit:
    payer: str
    resource: str

    @property
    def key(self) -> str:
        return f"{self.payer}:{self.resource}"

    @property
    def schema(self) -> str:
        return schema_for(self.payer)

    @property
    def table(self) -> str:
        return RESOURCE_TABLES[self.resource].name


@dataclass
class _Shared:
    """Cross-thread state: child PIDs (for external-process exclusion), the live
    per-lane status map, and file writers — all under one lock."""
    run_dir: Path
    lock: threading.Lock = field(default_factory=threading.Lock)
    child_pids: set = field(default_factory=set)
    live: dict = field(default_factory=dict)   # lane -> status dict

    def add_pid(self, pid: int) -> None:
        with self.lock:
            self.child_pids.add(pid)

    def drop_pid(self, pid: int) -> None:
        with self.lock:
            self.child_pids.discard(pid)

    def snapshot_pids(self) -> set:
        with self.lock:
            return set(self.child_pids)

    def journal(self, record: dict) -> None:
        record = {"ts": datetime.now(tz=timezone.utc).isoformat(), **record}
        line = json.dumps(record, default=str)
        with self.lock:
            with (self.run_dir / "journal.jsonl").open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    def set_live(self, lane: str, status: dict) -> None:
        with self.lock:
            self.live[lane] = {**status, "updated": datetime.now(tz=timezone.utc).isoformat()}
            (self.run_dir / "state.json").write_text(
                json.dumps(self.live, indent=2, default=str), encoding="utf-8"
            )


# --- Postgres helpers (short queries, autocommit so each sees fresh data) --
def _poller_conn():
    conn = pg_connection()
    conn.autocommit = True
    return conn


def _scalar(conn, sql: str, params: tuple = ()):  # -> int
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return (row[0] if row and row[0] is not None else 0)
    except Exception:  # noqa: BLE001 - table may not exist yet; treat as 0
        return 0


def table_count(conn, unit: Unit) -> int:
    return _scalar(conn, f'SELECT count(*) FROM "{unit.schema}"."{unit.table}"')


def write_activity(conn, unit: Unit) -> int:
    """Cumulative insert+update tuple count for the unit's table (O(1) — the
    watchdog's progress signal; a delta of 0 across polls means no writes)."""
    return _scalar(
        conn,
        "SELECT COALESCE(n_tup_ins,0)+COALESCE(n_tup_upd,0) "
        "FROM pg_stat_user_tables WHERE schemaname=%s AND relname=%s",
        (unit.schema, unit.table),
    )


# --- running-process detection (Windows) ----------------------------------
_CMD_SUBSET = re.compile(r"--subset\s+(\S+)")
_CMD_RES = re.compile(r"--resources\s+(\S+)")


def scan_running_units(exclude_pids: set) -> list:
    """External `provdir.cli etl` processes as [{pid, payer, resource}], excluding
    our own children. Best-effort; returns [] if the scan fails."""
    ps = (
        "Get-CimInstance Win32_Process -Filter \"Name='python.exe' OR Name='py.exe'\" | "
        "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
    )
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
        if not out:
            return []
        data = json.loads(out)
        if isinstance(data, dict):
            data = [data]
    except Exception:  # noqa: BLE001
        return []
    found = []
    for p in data:
        pid = p.get("ProcessId")
        cmd = p.get("CommandLine") or ""
        if pid in exclude_pids or "provdir.cli" not in cmd or " etl" not in cmd:
            continue
        ms, mr = _CMD_SUBSET.search(cmd), _CMD_RES.search(cmd)
        if ms and mr:
            found.append({"pid": pid, "payer": ms.group(1), "resource": mr.group(1)})
    return found


# --- plan construction ----------------------------------------------------
def month_start_utc() -> datetime:
    now = datetime.now(tz=timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def latest_provenance() -> dict:
    """{(payer, resource): (status, finished_at)} from the newest row each."""
    out = {}
    conn = _poller_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (payer_id, resource_type) "
                "payer_id, resource_type, status, finished_at "
                "FROM public.provenance "
                "ORDER BY payer_id, resource_type, finished_at DESC NULLS LAST"
            )
            for payer, rtype, status, finished in cur.fetchall():
                out[(payer, rtype)] = (status, finished)
    finally:
        conn.close()
    return out


def build_plan(args) -> tuple:
    """Returns (lanes: {lane_id: [Unit]}, skipped: [dict], lane_hosts: {lane_id: set})."""
    settings = get_settings()
    manifest = load_manifest()
    prov = latest_provenance()
    cutoff = (datetime.fromisoformat(args.cutoff) if args.cutoff else month_start_utc())
    running = scan_running_units(set())
    running_units = {(u["payer"], u["resource"]) for u in running}
    running_hosts = set()

    endpoints = manifest.known()
    by_key = {e.key: e for e in endpoints}
    for u in running:
        ep = by_key.get(u["payer"])
        if ep:
            running_hosts.add(lane_of(ep.host))

    lanes: dict = {}
    lane_hosts: dict = {}
    skipped: list = []

    for ep in endpoints:
        if args.only and ep.key != args.only:
            continue
        reason = ep.skip_reason(settings)
        if reason and (reason.startswith("missing-credentials")
                       or reason in {"blocked", "missing-token-url", "unconfirmed-auth"}):
            skipped.append({"payer": ep.key, "resource": "*", "outcome": "skipped-unrunnable",
                            "reason": reason})
            continue
        excl = EXCLUSIONS.get(ep.key, set())
        if "*" in excl:
            skipped.append({"payer": ep.key, "resource": "*", "outcome": "skipped-excluded"})
            continue
        lane = lane_of(ep.host)
        expected = ep.expected_resources(manifest.plannet_resources)
        ordered = [r for r in RESOURCE_ORDER if r in expected]
        for r in ordered:
            if r in excl:
                skipped.append({"payer": ep.key, "resource": r, "outcome": "skipped-excluded"})
                continue
            if (ep.key, r) in running_units:
                skipped.append({"payer": ep.key, "resource": r, "outcome": "skipped-running"})
                continue
            if not args.no_skip:
                pv = prov.get((ep.key, r))
                if pv and pv[0] == "ok" and pv[1] is not None and pv[1] >= cutoff:
                    skipped.append({"payer": ep.key, "resource": r,
                                    "outcome": "skipped-recent", "finished_at": pv[1]})
                    continue
            lanes.setdefault(lane, []).append(Unit(ep.key, r))
            lane_hosts.setdefault(lane, set()).add(lane)

    return lanes, skipped, running_hosts


# --- unit execution + watchdog --------------------------------------------
def grace_for(unit: Unit) -> int:
    return (GRACE_OVERRIDES.get((unit.payer, unit.resource))
            or GRACE_OVERRIDES.get(unit.payer) or GRACE_S)


def run_unit(unit: Unit, lane: str, attempt: int, shared: _Shared, conn) -> str:
    log_path = shared.run_dir / "logs" / f"{unit.payer}__{unit.resource}{'' if attempt == 1 else f'.retry{attempt-1}'}.log"
    cmd = [sys.executable, "-m", "provdir.cli", "etl",
           "--subset", unit.payer, "--resources", unit.resource, "--upsert", "--resume"]
    rows_before = table_count(conn, unit)
    started = datetime.now(tz=timezone.utc)
    t0 = time.monotonic()
    with log_path.open("w", encoding="utf-8") as logf:
        proc = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT, cwd=str(REPO))
        shared.add_pid(proc.pid)
        last = write_activity(conn, unit)
        zero = 0
        next_poll = t0 + POLL_INTERVAL_S
        killed = False
        try:
            while proc.poll() is None:
                time.sleep(2)
                now = time.monotonic()
                if now < next_poll:
                    continue
                cur = write_activity(conn, unit)
                delta = cur - last
                last = cur
                age = now - t0
                if age > grace_for(unit):
                    zero = zero + 1 if delta <= 0 else 0
                    if zero >= ZERO_POLLS_TO_KILL:
                        proc.kill()
                        killed = True
                shared.set_live(lane, {"unit": unit.key, "pid": proc.pid, "attempt": attempt,
                                       "age_s": round(age), "delta": delta,
                                       "rows": table_count(conn, unit)})
                next_poll += POLL_INTERVAL_S
            rc = proc.wait()
        finally:
            shared.drop_pid(proc.pid)
    rows_after = table_count(conn, unit)
    dur = round(time.monotonic() - t0)
    outcome = "stall-killed" if killed else ("completed" if rc == 0 else "error")
    shared.journal({"payer": unit.payer, "resource": unit.resource, "lane": lane,
                    "attempt": attempt, "outcome": outcome, "exit_code": rc,
                    "rows_before": rows_before, "rows_after": rows_after,
                    "rows_added": rows_after - rows_before, "duration_s": dur,
                    "started_at": started, "log": str(log_path)})
    return outcome


def wait_for_host_clear(lane: str, shared: _Shared, manifest) -> None:
    """Block while an EXTERNAL provdir process holds this lane's host (e.g. the
    Elevance PractitionerRole pull still running from a previous session)."""
    by_key = {e.key: e for e in manifest.known()}
    announced = False
    while True:
        ext = scan_running_units(shared.snapshot_pids())
        busy = any(lane_of(by_key[u["payer"]].host) == lane
                   for u in ext if u["payer"] in by_key)
        if not busy:
            return
        if not announced:
            shared.journal({"lane": lane, "outcome": "waiting-external-host"})
            announced = True
        time.sleep(HOST_BUSY_RECHECK_S)


def run_lane(lane: str, units: list, shared: _Shared, manifest) -> None:
    conn = _poller_conn()
    retried: set = set()
    queue = list(units)
    try:
        while queue:
            unit = queue.pop(0)
            attempt = 2 if unit.key in retried else 1
            wait_for_host_clear(lane, shared, manifest)
            outcome = run_unit(unit, lane, attempt, shared, conn)
            if outcome == "stall-killed" and unit.key not in retried:
                retried.add(unit.key)
                queue.append(unit)   # one auto-resume retry at the end of the lane
                shared.journal({"payer": unit.payer, "resource": unit.resource,
                                "lane": lane, "outcome": "retry-queued"})
        shared.set_live(lane, {"unit": None, "state": "done"})
    finally:
        conn.close()


# --- entrypoints ----------------------------------------------------------
def cmd_dry_run(lanes: dict, skipped: list, running_hosts: set) -> None:
    total = sum(len(v) for v in lanes.values())
    print(f"\nPLAN: {total} units across {len(lanes)} lanes "
          f"(max {MAX_LANES} concurrent)\n")
    for lane in sorted(lanes, key=lambda k: (-len(lanes[k]), k)):
        units = lanes[lane]
        payers = sorted({u.payer for u in units})
        busy = "  [WAITS: external process on this host]" if lane in running_hosts else ""
        print(f"  lane {lane}  ({len(units)} units, {len(payers)} payers){busy}")
        for p in payers:
            rs = [u.resource for u in units if u.payer == p]
            print(f"      {p}: {', '.join(rs)}")
    by_outcome: dict = {}
    for s in skipped:
        by_outcome.setdefault(s["outcome"], []).append(s)
    print("\nSKIPPED:")
    for outcome, items in sorted(by_outcome.items()):
        print(f"  {outcome}: {len(items)}")
        for it in items[:12]:
            r = it.get("resource", "*")
            extra = f" ({it['reason']})" if it.get("reason") else ""
            print(f"      {it['payer']}:{r}{extra}")
        if len(items) > 12:
            print(f"      ... and {len(items) - 12} more")
    print()


def cmd_status(base: Path) -> None:
    runs = sorted(base.glob("run_*"), reverse=True)
    if not runs:
        print("no orchestrator runs found")
        return
    run_dir = runs[0]
    print(f"latest run: {run_dir.name}")
    state = run_dir / "state.json"
    if state.exists():
        live = json.loads(state.read_text(encoding="utf-8"))
        print("\nLIVE LANES:")
        for lane, st in sorted(live.items()):
            print(f"  {lane}: {json.dumps(st, default=str)}")
    jpath = run_dir / "journal.jsonl"
    if jpath.exists():
        lines = jpath.read_text(encoding="utf-8").splitlines()
        done = [json.loads(x) for x in lines]
        counts: dict = {}
        for d in done:
            counts[d.get("outcome", "?")] = counts.get(d.get("outcome", "?"), 0) + 1
        print("\nJOURNAL TOTALS:")
        for o, n in sorted(counts.items()):
            print(f"  {o}: {n}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Deterministic monthly re-pull orchestrator")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    ap.add_argument("--status", action="store_true", help="print the latest run's state and exit")
    ap.add_argument("--no-skip", action="store_true",
                    help="re-run every unit even if it completed ok this month (kickoff)")
    ap.add_argument("--cutoff", default=None,
                    help="ISO datetime; skip units ok since this (default: 1st of this month UTC)")
    ap.add_argument("--only", default=None, help="restrict to a single payer key (debug)")
    args = ap.parse_args()

    base = REPO / "output" / "orchestrator"
    base.mkdir(parents=True, exist_ok=True)

    if args.status:
        cmd_status(base)
        return 0

    lanes, skipped, running_hosts = build_plan(args)

    if args.dry_run:
        cmd_dry_run(lanes, skipped, running_hosts)
        return 0

    if not lanes:
        print("nothing to run (everything skipped). Use --dry-run to see why.")
        return 0

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = base / f"run_{ts}"
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    shared = _Shared(run_dir=run_dir)
    for s in skipped:
        shared.journal(s)

    manifest = load_manifest()
    total = sum(len(v) for v in lanes.values())
    print(f"orchestrator: {total} units across {len(lanes)} lanes -> {run_dir}")
    shared.journal({"outcome": "run-start", "units": total, "lanes": len(lanes),
                    "no_skip": args.no_skip})

    order = sorted(lanes, key=lambda k: (-len(lanes[k]), k))
    with ThreadPoolExecutor(max_workers=MAX_LANES) as pool:
        futs = [pool.submit(run_lane, lane, lanes[lane], shared, manifest) for lane in order]
        for f in futs:
            f.result()

    shared.journal({"outcome": "run-complete"})
    print(f"orchestrator complete -> {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

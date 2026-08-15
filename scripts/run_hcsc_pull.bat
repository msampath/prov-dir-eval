@echo off
REM Full HCSC provider directory pull to NDJSON on I:.
REM
REM ~27M records at a hard 50/page and ~2.7s/page, so this is a multi-day run.
REM Resumable at two levels: finished buckets are skipped, and an interrupted
REM walk resumes from its stored cursor rather than page 1. Records are
REM deduplicated by id, so re-running never writes anything twice.
REM
REM The scheduled task repeats every 2 hours so a crashed run restarts itself.
REM Two things stop that becoming a problem: MultipleInstancesPolicy=IgnoreNew
REM plus an OS lock on the output directory prevent a second concurrent puller,
REM and the _complete.json guard below stops relaunching once the pull is done.
REM Without that guard every restart would re-read ~120GB of NDJSON to rebuild
REM the dedup set before finding nothing left to do.
REM
REM 8 workers. At ~2.7s per page that is roughly 3 requests/sec against a public
REM API serving a 15M-member payer. Lower it if HCSC asks. Humana's Akamai front
REM end IP-blocked us at higher concurrency, so this is deliberately short of it.
cd /d F:\github\prov-dir-eval
if exist I:\file_export\hcsc\_complete.json (
  echo [%DATE% %TIME%] pull already complete, nothing to do >> output\orchestrator\hcsc_pull.log
  exit /b 0
)
echo [%DATE% %TIME%] starting/resuming HCSC pull >> output\orchestrator\hcsc_pull.log
.venv\Scripts\python.exe scripts\pull_hcsc.py --workers 8 >> output\orchestrator\hcsc_pull.log 2>&1

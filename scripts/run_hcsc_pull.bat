@echo off
REM Full HCSC provider directory pull to NDJSON on I:.
REM
REM ~27M records at a hard 50/page and ~2.7s/page, so this is a multi-day run.
REM It is resumable: each bucket is checkpointed in _progress.json and records
REM are deduplicated by id, so re-running skips finished buckets and re-walking
REM an unfinished one writes nothing twice.
REM
REM 8 workers. The measured page latency is ~2.7s, so 8 concurrent walks is
REM about 3 requests/sec against a public API that serves a 15M-member payer.
REM Lower it if HCSC asks. Humana's Akamai front-end IP-blocked us at higher
REM concurrency, so this is deliberately short of that.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\pull_hcsc.py --workers 8 >> output\orchestrator\hcsc_pull.log 2>&1

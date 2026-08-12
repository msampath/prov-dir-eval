@echo off
REM Premera full re-pull after Opala's pagination fix (2026-08-11).
REM File sweep, not a direct DB load: ~15k sequential requests at the server's
REM 50-record cap, and writing to disk first keeps the network walk independent
REM of DB contention (the same shape that got premera's other resources in).
REM Resumable -- paginate_to_file keeps a per-unit checkpoint sidecar and skips
REM units already carrying a .done marker.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host opala.tech >> output\orchestrator\premera_pull.log 2>&1

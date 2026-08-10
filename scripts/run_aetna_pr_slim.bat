@echo off
REM Resume the structured-only Aetna PractitionerRole load onto M: (bulk_ssd).
REM Setup (truncate+move+drop+wipe) already committed; --resume skips done files
REM (public.slim_load_done), VACUUMs, loads the rest, rebuilds indexes once.
REM --workers N shards the remaining files across N child processes: the
REM transform is CPU-bound at ~6k rows/s per core, so a single process leaves
REM this 36-core box ~97%% idle and the load takes days instead of hours.
REM For a full fresh restart instead, run: bulk_replace_load.py --slim
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\bulk_replace_load.py --slim --resume --workers 12 >> output\orchestrator\aetna_pr_slim.log 2>&1

@echo off
REM Resume the structured-only Aetna PractitionerRole load onto M: (bulk_ssd).
REM Setup (truncate+move+drop+wipe) + file 1 already committed; --resume skips
REM done files (public.slim_load_done), VACUUMs, loads the rest, rebuilds indexes.
REM For a full fresh restart instead, run: bulk_replace_load.py --slim
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\bulk_replace_load.py --slim --resume >> output\orchestrator\aetna_pr_slim.log 2>&1

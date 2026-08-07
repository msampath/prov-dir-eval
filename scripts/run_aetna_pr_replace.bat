@echo off
REM Replace-load Aetna PractitionerRole from the I:\aetna_export capture (~500M rows).
REM Truncates the crashed 59.8M partial, drops indexes, COPYs, rebuilds indexes on
REM hot_idx (M:). Table heap on bulk_heap (E:). NOT resumable mid-load; re-run on crash.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\bulk_replace_load.py >> output\orchestrator\aetna_pr_replace.log 2>&1

@echo off
REM Load the HCSC capture into Postgres. Pure upsert (update=True, no truncate),
REM so re-running is always safe: it re-reads each ndjson and writes only what
REM is missing or changed.
REM
REM Runs as a scheduled task rather than a shell job because it takes hours
REM (154GB of PractitionerRole alone) and a shell job dies with the session that
REM started it. No trigger: on demand only, and the loader has no cross-process
REM lock, so a second concurrent copy would fight the first over the same tables.
REM
REM Accepted cost of a restart: the loader has no per-file resume, so an
REM interrupted run re-reads every file from the top. Upsert makes that correct,
REM just slow.
cd /d F:\github\prov-dir-eval
echo [%DATE% %TIME%] starting/resuming HCSC load >> output\orchestrator\hcsc_load.log
.venv\Scripts\python.exe scripts\load_file_sweeps.py --only hcsc >> output\orchestrator\hcsc_load.log 2>&1
echo [%DATE% %TIME%] load exited with %ERRORLEVEL% >> output\orchestrator\hcsc_load.log

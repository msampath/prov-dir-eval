@echo off
REM Detached full pull of Florida Blue via the orchestrator's single-payer mode.
REM FL Blue has no bulk $export (404), so this is paginated search with the
REM registered app's X-IBM auth for attribution. Its own process (untethered from
REM Claude), fixed distinct-growth watchdog, cannot affect other payers.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\orchestrate.py --only florida_blue --no-skip >> output\orchestrator\florida_blue.log 2>&1

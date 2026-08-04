@echo off
REM Isolated detached restart of capital_blue PractitionerRole via the orchestrator's
REM single-unit mode, so it gets the FIXED watchdog (distinct-row-growth stall
REM detection). Prior run looped ~20h re-covering _lastUpdated windows with no
REM distinct growth past 2.24M; the fixed watchdog kills that at ~35 min.
REM Its own orchestrator process => killing/stalling it cannot affect other payers.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\orchestrate.py --only capital_blue --resource PractitionerRole --no-skip >> output\orchestrator\capital_blue_pr.log 2>&1

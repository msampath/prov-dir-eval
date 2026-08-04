@echo off
REM Detached launcher for the monthly re-pull orchestrator.
REM Run via a Windows Scheduled Task so the run is not tied to any console
REM session (survives Claude Code teardown, terminal close, etc.).
REM Resume mode (no --no-skip): finishes units not already ok this month;
REM per-unit --resume picks up bare pulls from their checkpoints.
cd /d F:\github\prov-dir-eval
if not exist output\orchestrator mkdir output\orchestrator
.venv\Scripts\python.exe scripts\orchestrate.py >> output\orchestrator\detached.log 2>&1

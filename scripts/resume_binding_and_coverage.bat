@echo off
REM Resume launcher for the binding + element-coverage sweeps.
REM Separate file from run_binding_and_coverage.bat ON PURPOSE: cmd.exe re-reads
REM a batch file line-by-line by byte offset while executing it, so editing the
REM currently-running launcher can make it jump to the wrong line. Use this one
REM to restart after any interruption -- --auto-resume picks up the newest
REM unfinished run instead of restarting a multi-hour sweep from payer 1.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\check_bindings.py --auto-resume >> output\orchestrator\check_bindings.log 2>&1
.venv\Scripts\python.exe -m provdir.cli element-coverage --assessment all --auto-resume >> output\orchestrator\element_coverage.log 2>&1

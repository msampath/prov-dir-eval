@echo off
REM Full-population binding membership sweep, then the element-coverage sweep
REM (USCDI v3 + MPF + Plan-Net extensions). Sequential ON PURPOSE: both are
REM seq-scan-heavy over the same payer tables; concurrent runs would thrash.
REM Both are resumable (check_bindings --run-id N / element-coverage --resume-run N).
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\check_bindings.py >> output\orchestrator\check_bindings.log 2>&1
.venv\Scripts\python.exe -m provdir.cli element-coverage --assessment all >> output\orchestrator\element_coverage.log 2>&1

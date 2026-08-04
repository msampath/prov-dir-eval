@echo off
REM One-off detached pull for Aetna PractitionerRole (excluded from the monthly
REM orchestrator because its _summary=count is implausible/unstable ~380M).
REM Run standalone since the aetna lane in the orchestrator has already finished,
REM so apif1.aetna.com is free of contention.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe -m provdir.cli etl --subset aetna_cvs --resources PractitionerRole --upsert --resume >> output\orchestrator\aetna_pracrole.log 2>&1

@echo off
REM Detached full pull of Aetna PractitionerRole via FHIR Bulk Data $export.
REM Replaces the paginated 50/page pull (which the server caps painfully); bulk
REM returns the full resource set as ndjson in one async job.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\bulk_ingest.py --payer aetna_cvs --type PractitionerRole >> output\orchestrator\aetna_pr_bulk.log 2>&1

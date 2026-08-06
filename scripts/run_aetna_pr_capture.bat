@echo off
REM PHASE 1: capture Aetna PractitionerRole $export to a local ndjson on I: as fast
REM as the link allows (raw bytes, no DB) to beat Aetna's export-file purge. Then
REM PHASE 2 (run_aetna_pr_load.bat) ingests the local file into Postgres/E: offline.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\bulk_ingest.py --payer aetna_cvs --type PractitionerRole --stage-dir I:\aetna_export >> output\orchestrator\aetna_pr_capture.log 2>&1

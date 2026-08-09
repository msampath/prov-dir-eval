@echo off
REM Structured-only load of Aetna PractitionerRole (~500M) from I:\aetna_export
REM into aetna_cvs.practitioner_role on the M: SSD (bulk_ssd), stub resource, raw
REM stays in the capture files. Per-file resumable (public.slim_load_done).
REM Resume after a crash: bulk_replace_load.py --slim --resume
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\bulk_replace_load.py --slim >> output\orchestrator\aetna_pr_slim.log 2>&1

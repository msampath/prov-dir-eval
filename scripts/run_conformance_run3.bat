@echo off
REM Conformance run 3: full 118,718-instance sample vs Plan-Net 1.1.0 with THO
REM (fixes v2-0360 resolution), shared package cache, chunk timeouts, oversize
REM isolation. Supersedes run 2 for both structural and binding reporting.
REM --reuse-outputs makes a re-run resume: chunks whose _out/<name>.json already
REM exists are skipped, so an interrupted run re-validates only what is missing
REM (chunk names are deterministic; the end-of-run DB load reads every output).
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\validate_conformance.py --sample-dir F:/fhir_val --workers 14 --reuse-outputs >> output\orchestrator\conformance_run3.log 2>&1

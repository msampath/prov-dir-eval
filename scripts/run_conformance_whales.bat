@echo off
REM Retry pass for the oversize chunks that blew the 4h budget.
REM
REM --reuse-outputs is what makes this cheap: chunks that already produced an
REM output are skipped, so only the ~10 failed whale chunks actually re-run.
REM The reuse signature covers (ig, tho, tx) and NOT the timeout, which is
REM correct -- a longer budget changes how long we wait, never what the
REM validator concludes.
REM
REM 12h per oversize chunk, because the evidence says 4h was not the wall:
REM bcbs_az network Organizations that DID finish took 220-300 min each,
REM one file per container, so the biggest sit just past 4h individually.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\validate_conformance.py --sample-dir F:/fhir_val --workers 14 --reuse-outputs --oversize-timeout 43200 >> output\orchestrator\conformance_whales.log 2>&1

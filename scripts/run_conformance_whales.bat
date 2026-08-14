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
REM --chunk-timeout is raised too, and that is the fix the first retry missed.
REM --oversize-timeout only covers chunks holding a single >1MB file, so the
REM 11 bcbs_az Network whales got the 12h budget while InsurancePlan__000,
REM Network__000, Organization__001 and PractitionerRole__001 (ordinary 4,000
REM file chunks) stayed on the 1h default and died at 60.6m for the third run
REM running. Six hours is well past any observed honest completion; a chunk
REM that still times out there is a finding, not a budget problem.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\validate_conformance.py --sample-dir F:/fhir_val --workers 14 --reuse-outputs --oversize-timeout 43200 --chunk-timeout 21600 >> output\orchestrator\conformance_whales.log 2>&1

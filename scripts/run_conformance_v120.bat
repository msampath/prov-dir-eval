@echo off
REM Conformance run against Plan-Net 1.2.0, including HCSC.
REM
REM Run 6 (1.1.0, 118,951 instances) is kept for the version delta: 1.2.0 makes
REM 10 elements must-support that were not, seven of them Practitioner address
REM and telecom, so the difference between the two runs is a finding rather than
REM bookkeeping.
REM
REM Timeouts carry the lessons from run 6: --oversize-timeout only covers chunks
REM holding a single >1MB file, so --chunk-timeout has to be raised too or the
REM ordinary 4,000-file chunks die at the 1h default, as they did three runs
REM running.
REM
REM --restore-from '' is REQUIRED. It defaults to F:/fhir_val_keep and runs
REM unconditionally, which would silently refill the sample with the pre-HCSC
REM copy and quietly drop the 2,087 HCSC instances just added.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\validate_conformance.py ^
  --sample-dir F:/fhir_val ^
  --ig-version 1.2.0 ^
  --restore-from "" ^
  --workers 14 ^
  --chunk-timeout 21600 ^
  --oversize-timeout 43200 >> output\orchestrator\conformance_v120.log 2>&1

@echo off
REM Elevance paginate-to-file: bypass the DB-upsert path (whose latency stretches
REM the page-to-page gap and forces mid-walk OAuth-token refreshes that 503 and
REM truncate the pull). Sequential (one resource at a time) to keep token pressure
REM minimal. PractitionerRole -> I: (largest); everything else -> M: SSD.
cd /d F:\github\prov-dir-eval
set PY=.venv\Scripts\python.exe
set LOG=output\orchestrator\elevance_to_file.log
%PY% scripts\paginate_to_file.py --payer elevance --resource PractitionerRole        --out-dir I:\elevance_export >> %LOG% 2>&1
%PY% scripts\paginate_to_file.py --payer elevance --resource Practitioner            --out-dir M:\elevance_export >> %LOG% 2>&1
%PY% scripts\paginate_to_file.py --payer elevance --resource HealthcareService       --out-dir M:\elevance_export >> %LOG% 2>&1
%PY% scripts\paginate_to_file.py --payer elevance --resource OrganizationAffiliation --out-dir M:\elevance_export >> %LOG% 2>&1
%PY% scripts\paginate_to_file.py --payer elevance --resource Organization            --out-dir M:\elevance_export >> %LOG% 2>&1
%PY% scripts\paginate_to_file.py --payer elevance --resource Location                --out-dir M:\elevance_export >> %LOG% 2>&1
%PY% scripts\paginate_to_file.py --payer elevance --resource Endpoint                --out-dir M:\elevance_export >> %LOG% 2>&1
echo ELEVANCE-TO-FILE COMPLETE >> %LOG% 2>&1

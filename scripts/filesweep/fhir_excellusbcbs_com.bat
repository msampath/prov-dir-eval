@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "fhir.excellusbcbs.com" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_fhir_excellusbcbs_com.log" 2>&1

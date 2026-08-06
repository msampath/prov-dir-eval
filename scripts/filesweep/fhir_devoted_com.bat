@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "fhir.devoted.com" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_fhir_devoted_com.log" 2>&1

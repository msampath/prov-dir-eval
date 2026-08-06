@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "provider-directory-r4.api.hap.org" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_provider_directory_r4_api_hap_org.log" 2>&1

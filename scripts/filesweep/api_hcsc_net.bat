@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "api.hcsc.net" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_api_hcsc_net.log" 2>&1

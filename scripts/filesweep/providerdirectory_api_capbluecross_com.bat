@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "providerdirectory-api.capbluecross.com" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_providerdirectory_api_capbluecross_com.log" 2>&1

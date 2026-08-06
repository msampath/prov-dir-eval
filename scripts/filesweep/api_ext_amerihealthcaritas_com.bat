@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "api-ext.amerihealthcaritas.com" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_api_ext_amerihealthcaritas_com.log" 2>&1

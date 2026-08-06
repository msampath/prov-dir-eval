@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "gateway.api.lablue.com" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_gateway_api_lablue_com.log" 2>&1

@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "healthadvantage.healthsparq.com" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_healthadvantage_healthsparq_com.log" 2>&1

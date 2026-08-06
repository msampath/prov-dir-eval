@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "flex.optum.com" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_flex_optum_com.log" 2>&1

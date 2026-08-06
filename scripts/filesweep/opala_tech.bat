@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "opala.tech" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_opala_tech.log" 2>&1

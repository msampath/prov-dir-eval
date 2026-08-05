@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\resume_sweep.py >> output\orchestrator\resume_sweep.log 2>&1

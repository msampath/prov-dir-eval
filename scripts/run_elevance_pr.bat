@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\orchestrate.py --only elevance --resource PractitionerRole --no-skip >> output\orchestrator\elevance_pr.log 2>&1

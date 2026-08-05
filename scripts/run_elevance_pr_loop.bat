@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\loop_resume.py --payer elevance --resource PractitionerRole --patience 6 --sleep 30 >> output\orchestrator\elevance_pr_loop.log 2>&1

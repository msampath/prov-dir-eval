@echo off
REM One premera unit, detached. %1 = resource type.
REM A scheduled task, not a shell background job: these survive the agent
REM session and a Claude Code restart, which nohup'd children may not.
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\paginate_to_file.py --payer premera --resource %1 --out-dir "I:\file_export\premera" >> output\orchestrator\premera_%1.log 2>&1

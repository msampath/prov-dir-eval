@echo off
cd /d F:\github\prov-dir-eval
.venv\Scripts\python.exe scripts\file_sweep_host.py --host "kpx-service-bus.kp.org" >> "F:\github\prov-dir-eval\output\orchestrator\file_sweep_kpx_service_bus_kp_org.log" 2>&1

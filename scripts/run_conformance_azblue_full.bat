@echo off
REM ===========================================================================
REM Track B - full-population D3 conformance over bcbs_az_new (~3,133,000
REM instances). Measured at 45-96 hours; the point of this file is that the run
REM survives being interrupted anywhere inside that window.
REM
REM Register it BOTH ways (see the schtasks lines at the bottom of this file):
REM   * a boot trigger, so a reboot resumes the run instead of ending it;
REM   * /RU with a password so it runs whether the user is logged on or not.
REM     All 40 existing provdir_* tasks are "Interactive only", which means a
REM     logoff or a locked console with no session ends them.
REM
REM Safe to fire repeatedly. Three independent guards:
REM   1. _out\_complete.json - written only by a run that finished AND verified
REM      its instance count. Present => nothing to do, exit 0.
REM   2. lock_sample_dir()   - an exclusive OS lock on the sample dir. A second
REM      copy started while the first is running exits FATAL without touching
REM      anything (two runs cannot share a sample dir: each re-chunks it).
REM   3. --reuse-outputs     - chunks that already produced a COMPLETE output
REM      are skipped. Truncated outputs are quarantined and re-validated, which
REM      is the fix that makes resume safe at all; before it, a chunk killed
REM      mid-write was silently accepted forever and the run reported success
REM      while short by up to a whole chunk.
REM
REM --restore-from "" is REQUIRED. The default (F:/fhir_val_keep) runs
REM unconditionally and would merge 121,038 stale mixed-payer files from the
REM 60-payer sample into these cell dirs.
REM
REM Disk: the sample lives on M: (SATA SSD; _batches and _out are hardcoded
REM siblings of --sample-dir), plus ~150 GB of worker package caches on F:.
REM The old "never M:" rule predated the tablespace migration: during Track B
REM the DB does not touch M:, and E: is a USB HDD - the worst possible host
REM for 3.13M small files read by 44 concurrent containers.
REM ===========================================================================
setlocal
set SAMPLE=M:/fhir_val_azblue
set SAMPLE_WIN=M:\fhir_val_azblue
set LOG=F:\github\prov-dir-eval\output\orchestrator\conformance_azblue_full.log

cd /d F:\github\prov-dir-eval
if not exist output\orchestrator mkdir output\orchestrator

REM Written only by a run that finished AND verified its instance count. To
REM deliberately start a fresh run, delete it: del %SAMPLE_WIN%\_out\_complete.json
if exist "%SAMPLE_WIN%\_out\_complete.json" (
  echo [%DATE% %TIME%] run already complete; nothing to resume >> "%LOG%"
  exit /b 0
)

REM On a boot trigger this starts before Docker Desktop has a daemon. Without
REM the wait every chunk fails instantly and the resume burns the whole queue.
REM 60 tries x 30s = 30 minutes, then give up loudly rather than run blind.
set /a TRIES=0
:waitdocker
docker info >nul 2>&1
if not errorlevel 1 goto dockerup
set /a TRIES+=1
if %TRIES% GEQ 60 (
  echo [%DATE% %TIME%] FATAL: docker daemon not up after 30m; not starting >> "%LOG%"
  exit /b 3
)
timeout /t 30 /nobreak >nul
goto waitdocker
:dockerup

echo [%DATE% %TIME%] starting/resuming full-population D3 run >> "%LOG%"

REM --workers 44 against the 23-core / 46-thread cap: 44 concurrent containers
REM plus this process and the Docker daemon. Chunk sizes and per-chunk timeouts
REM are derived per cell from CELL_RATE_S - do NOT pass a global --chunk, which
REM would force one size across cells whose per-instance cost differs 8x.
REM
REM Memory. The Docker VM is ~101 GB and 44 containers share it, so the honest
REM per-container budget is 101/44 = 2.29 GB. -Xmx alone does NOT enforce that:
REM it bounds the JVM heap, while metaspace, code cache and direct buffers sit
REM outside it - --docker-memory is the actual ceiling. 44 x 2g = 88 GB, which
REM leaves ~13 GB for the daemon and the host.
REM
REM The 36 Network whales (25 MB Organizations carrying up to 66,235 addresses)
REM cannot run in that budget, so they get their own tier. There are only 36 of
REM them and oversize isolation puts one per chunk, so at most a handful are in
REM flight at once.
REM
REM --xmx 1500m is the one number here that has NOT been measured, and the run
REM --cell-rate: MEASURED steady-state rates from 1,000-instance benchmarks on
REM real bcbs_az_new data (2026-08-26), padded ~20%% for 44-way contention:
REM   Organization     0.63-0.71 s/inst measured -> 0.85   (was 2.58, cold-JIT)
REM   PractitionerRole 8.66 s/inst mean, BIMODAL (median 0.20s, p90 22s,
REM                    max 388s - innovaccer-coding-heavy records) -> 10
REM   Location         0.08 s/inst, uniform -> 0.15        (was 1.66 default)
REM   Practitioner / HealthcareService: unmeasured, small simple records -> 0.5
REM Network stays at its default: all 36 are >1MB whales, one per oversize
REM chunk with their own timeout, so the rate never sizes anything.
REM
REM checks it for us: the warm-up chunk runs alone and synchronously before any
REM fan-out, and the run exits FATAL if it fails. So a too-small heap surfaces
REM in the first ~20 minutes, not 40 hours in. If it does: raise --xmx and drop
REM --workers to keep (workers x docker-memory) under ~90 GB.
.venv\Scripts\python.exe scripts\validate_conformance.py ^
  --sample-dir %SAMPLE% ^
  --ig-version 1.2.0 ^
  --cell-rate Organization=0.85,PractitionerRole=10,Location=0.15,Practitioner=0.5,HealthcareService=0.5 ^
  --restore-from "" ^
  --reuse-outputs ^
  --workers 28 ^
  --docker-cpus 2 ^
  --xmx 1500m ^
  --docker-memory 2g ^
  --oversize-xmx 8g ^
  --oversize-docker-memory 12g ^
  --chunk-timeout 43200 ^
  --oversize-timeout 43200 >> "%LOG%" 2>&1

set RC=%ERRORLEVEL%
echo [%DATE% %TIME%] exit %RC% >> "%LOG%"
REM 0 = complete and verified, 1 = finished with failed chunks (re-run to
REM retry them), 2 = the instance count does NOT add up - do not publish.
exit /b %RC%

REM ---------------------------------------------------------------------------
REM Registration (run these in an elevated shell; do not run them from here):
REM
REM  schtasks /Create /TN provdir_conformance_azblue_full /SC ONSTART /DELAY 0005:00 ^
REM    /TR "F:\github\prov-dir-eval\scripts\run_conformance_azblue_full.bat" ^
REM    /RU %USERDOMAIN%\%USERNAME% /RP * /RL HIGHEST /F
REM
REM  schtasks /Change /TN provdir_conformance_azblue_full /ENABLE
REM
REM /RU + /RP is what makes it "run whether the user is logged on or not";
REM without them schtasks registers the task Interactive-only, which is exactly
REM why none of the existing 40 provdir_* tasks would survive a reboot.
REM
REM CAVEAT worth knowing before you rely on the boot trigger: Docker Desktop
REM starts in a USER session. An ONSTART task on a box that reboots to a login
REM screen and is never signed into will sit in the docker-wait loop for 30
REM minutes and exit 3 (logged, not silent). Two ways to close that gap:
REM   * add a companion logon trigger, which covers "rebooted and signed in":
REM       schtasks /Create /TN provdir_conformance_azblue_full_logon /SC ONLOGON ^
REM         /DELAY 0003:00 /TR "F:\github\prov-dir-eval\scripts\run_conformance_azblue_full.bat" ^
REM         /RU %USERDOMAIN%\%USERNAME% /RP * /RL HIGHEST /F
REM     Both triggers firing is harmless - the sample-dir lock makes the second
REM     one exit immediately.
REM   * or set Docker Desktop to start on boot, so the ONSTART trigger alone is
REM     enough.
REM ---------------------------------------------------------------------------

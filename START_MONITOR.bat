@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if errorlevel 1 goto missing_python

py -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)"
if errorlevel 1 goto unsupported_python

if not exist ".venv\Scripts\python.exe" (
    echo Creating the local Python environment...
    py -m venv .venv
    if errorlevel 1 goto setup_failed
)

if not exist ".venv\.requirements_installed" (
    echo Installing required packages. This first run needs internet access...
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt
    if errorlevel 1 goto setup_failed
    type nul > ".venv\.requirements_installed"
)

echo Starting the BTC 5m/15m monitor. Press Ctrl+C to stop.
".venv\Scripts\python.exe" main.py
if errorlevel 1 goto run_failed
exit /b 0

:missing_python
echo Python 3.11 or newer is required. Install Python from python.org, then run this file again.
pause
exit /b 1

:unsupported_python
echo The default Python version is older than 3.11. Install Python 3.11 or newer, then try again.
pause
exit /b 1

:setup_failed
echo Setup failed. Check the error above and confirm this computer has internet access.
pause
exit /b 1

:run_failed
echo The monitor stopped with an error.
pause
exit /b 1

@echo off
setlocal
REM Robust launcher for the BSE Orders Streamlit app (no parentheses in echo within blocks).

cd /d "%~dp0"

REM 1) Create venv if needed
if not exist ".venv\Scripts\python.exe" (
  echo Creating virtual environment...
  py -m venv .venv 2>nul || python -m venv .venv
)

set "VPY=.venv\Scripts\python.exe"
if not exist "%VPY%" (
  echo ERROR: venv python not found at "%CD%\%VPY%".
  echo Install Python 3.9+ and try again.
  exit /b 1
)

REM 2) Install/upgrade deps using the venv's python
echo Upgrading pip...
"%VPY%" -m pip install --upgrade pip

if exist requirements.txt (
  echo Installing dependencies from requirements.txt ...
  "%VPY%" -m pip install -r requirements.txt
) else (
  echo Installing dependencies: streamlit, requests, pandas
  "%VPY%" -m pip install streamlit requests pandas
)

echo(
echo Launching app at http://localhost:8501 ...
echo Press Ctrl+C in this window to stop.
"%VPY%" -m streamlit run "%CD%\bse_orders_app.py" --server.headless true

@echo off
setlocal enabledelayedexpansion

cd %~dp0

:start
echo Starting Excel monitor...

REM Try to gracefully stop any existing Python processes
tasklist /FI "IMAGENAME eq python.exe" 2>NUL | find /I /N "python.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo Stopping existing Python processes...
    taskkill /IM python.exe /F >NUL 2>&1
    timeout /t 5 /nobreak >NUL
)

REM Start the monitor
python -u run_excel_monitor.py

REM Check if we need to restart
if errorlevel 1 (
    echo Monitor exited with error. Restarting in 10 seconds...
    timeout /t 10 /nobreak >NUL
    goto start
) else (
    echo Monitor exited normally.
    exit /b 0
) 
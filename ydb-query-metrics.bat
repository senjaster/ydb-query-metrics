@echo off
REM Development entry point for ydb-query-metrics
REM This script:
REM 1. Creates a virtual environment if it doesn't exist
REM 2. Installs dependencies if needed
REM 3. Runs the package in development mode
REM 4. Passes all arguments to the CLI

REM Exit on error
setlocal enabledelayedexpansion

REM Get the directory where this script is located
set "SCRIPT_DIR=%~dp0"
REM Remove trailing backslash
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "VENV_DIR=%SCRIPT_DIR%\venv"

REM Check if virtual environment exists, create if it doesn't
if not exist "%VENV_DIR%" (
    echo Creating virtual environment...
    python -m venv "%VENV_DIR%"
    
    echo Virtual environment setup complete.
)

REM Activate the virtual environment
call "%VENV_DIR%\Scripts\activate.bat"

REM Check if the package is installed in development mode
pip list | findstr "ydb-query-metrics" > nul
if errorlevel 1 (
    echo Installing package in development mode...
    pip install -e "%SCRIPT_DIR%"
)

REM Run the CLI with all arguments
ydb-query-metrics %*
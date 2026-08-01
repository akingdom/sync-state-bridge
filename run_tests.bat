@echo off
REM run_tests.bat – Run the sync-state-bridge test suite on Windows

echo =========================================
echo   sync-state-bridge Test Suite Runner
echo =========================================
echo.

REM Check if pytest is installed
python -c "import pytest" 2>nul
if errorlevel 1 (
    echo Error: pytest not found. Please install it with:
    echo   pip install pytest pytest-asyncio pytest-cov
    exit /b 1
)

echo Running all tests with verbose output...
python -m pytest tests/ -v --tb=short --maxfail=5 --asyncio-mode=auto --color=yes

echo.
echo =========================================
echo   All tests completed.
echo =========================================
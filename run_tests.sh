#!/usr/bin/env bash
# run_tests.sh – Run the sync-state-bridge test suite

set -e

echo "========================================="
echo "  sync-state-bridge Test Suite Runner"
echo "========================================="
echo

# Ensure we're in the project root
cd "$(dirname "$0")"

# Check if pytest is installed
if ! python -c "import pytest" 2>/dev/null; then
    echo "Error: pytest not found. Please install it with:"
    echo "  pip install pytest pytest-asyncio pytest-cov"
    exit 1
fi

echo "Running all tests with verbose output..."
python -m pytest tests/ \
    -v \
    --tb=short \
    --maxfail=5 \
    --asyncio-mode=auto \
    --color=yes

# Uncomment the next lines for coverage report
# echo
# echo "Running with coverage..."
# python -m pytest tests/ \
#     --cov=sync_state \
#     --cov-report=term \
#     --cov-report=html:coverage_html

echo
echo "========================================="
echo "  All tests completed."
echo "========================================="
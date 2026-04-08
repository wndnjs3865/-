#!/usr/bin/env bash
# QuantPulse v3 — Health Check Script
# Returns 0 if system is running, 1 otherwise.

set -e

cd "$(dirname "$0")/.."

# Check if the main process is running
if pgrep -f "python main.py" > /dev/null 2>&1; then
    echo "OK: QuantPulse v3 is running"
    exit 0
fi

echo "FAIL: QuantPulse v3 is not running"
exit 1

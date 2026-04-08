#!/usr/bin/env bash
# QuantPulse v3 — Cloud Startup Script
# Usage: ./scripts/start.sh
# Supports: Railway, Replit, Docker, VPS

set -e

cd "$(dirname "$0")/.."

echo "════════════════════════════════════════"
echo " QuantPulse v3 — Starting System"
echo "════════════════════════════════════════"

# Load .env if present
if [ -f .env ]; then
    echo "[BOOT] Loading .env"
    set -a; source .env; set +a
fi

# Ensure data directory exists
mkdir -p data

# Install deps if needed (skip in Docker)
if [ ! -f "/.dockerenv" ] && [ -f "requirements.txt" ]; then
    echo "[BOOT] Checking dependencies..."
    pip install -q -r requirements.txt 2>/dev/null || true
fi

# Run with auto-restart on crash
MAX_RESTARTS=10
RESTART_COUNT=0

while [ $RESTART_COUNT -lt $MAX_RESTARTS ]; do
    echo "[BOOT] Starting QuantPulse v3 (attempt $((RESTART_COUNT + 1))/$MAX_RESTARTS)..."
    python main.py
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "[BOOT] Clean shutdown."
        exit 0
    fi

    RESTART_COUNT=$((RESTART_COUNT + 1))
    echo "[BOOT] Process exited with code $EXIT_CODE. Restarting in 5s..."
    sleep 5
done

echo "[BOOT] Max restarts ($MAX_RESTARTS) reached. Exiting."
exit 1

#!/usr/bin/env bash
# QuantPulse v3 — Cloud Startup Script
# Usage: ./scripts/start.sh
# Supports: Railway, Replit, Docker, VPS

set -e

cd "$(dirname "$0")/.."

echo "========================================"
echo " QuantPulse v3 — Starting System"
echo "========================================"

# Load .env if present
if [ -f .env ]; then
    echo "[BOOT] Loading .env"
    set -a; source .env; set +a
fi

# Ensure data directory exists
mkdir -p data

# Install deps if needed
if [ ! -d ".venv" ] && [ -f "requirements.txt" ]; then
    echo "[BOOT] Installing dependencies..."
    pip install -q -r requirements.txt
fi

# Run
echo "[BOOT] Starting QuantPulse v3..."
exec python main.py

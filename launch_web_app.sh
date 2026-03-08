#!/bin/bash

set -euo pipefail

PROJECT_ROOT="/Users/chenmayao/Desktop/code/fund-tracker"
LOG_DIR="$PROJECT_ROOT/data/logs"
BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"
BACKEND_URL="http://127.0.0.1:8000/api/summary"
FRONTEND_URL="http://127.0.0.1:5173"

mkdir -p "$LOG_DIR"

wait_for_url() {
    local url="$1"
    local name="$2"
    local retries="${3:-60}"
    local delay="${4:-1}"

    for ((i=1; i<=retries; i++)); do
        if curl -fsS "$url" > /dev/null 2>&1; then
            echo "$name is ready: $url"
            return 0
        fi
        sleep "$delay"
    done

    echo "$name failed to become ready: $url" >&2
    return 1
}

echo "Starting Fund Tracker Web Services..."

if [ ! -d "$PROJECT_ROOT/.venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$PROJECT_ROOT/.venv"
fi

echo "Installing/Updating Python dependencies..."
"$PROJECT_ROOT/.venv/bin/pip" install -q -r "$PROJECT_ROOT/requirements.txt"

cd "$PROJECT_ROOT/web-ui"
if [ ! -d "node_modules" ]; then
    echo "Installing frontend dependencies (this may take a minute)..."
    npm install --no-audit --no-fund --cache /tmp/npm-cache-fund-tracker
fi

if lsof -ti :8000 >/dev/null 2>&1; then
    echo "Backend already running on port 8000, reusing it."
else
    echo "Starting Backend (FastAPI)..."
    cd "$PROJECT_ROOT"
    export PYTHONPATH="$PROJECT_ROOT"
    nohup "$PROJECT_ROOT/.venv/bin/python" src/fund_tracker/web_api.py > "$BACKEND_LOG" 2>&1 &
    BACKEND_PID=$!
    echo "$BACKEND_PID" > "$LOG_DIR/backend.pid"
fi

if lsof -ti :5173 >/dev/null 2>&1; then
    echo "Frontend already running on port 5173, reusing it."
else
    echo "Starting Frontend (Vite)..."
    cd "$PROJECT_ROOT/web-ui"
    nohup npm run dev -- --host 127.0.0.1 --strictPort > "$FRONTEND_LOG" 2>&1 &
    FRONTEND_PID=$!
    echo "$FRONTEND_PID" > "$LOG_DIR/frontend.pid"
fi

echo "Waiting for services to become ready..."
wait_for_url "$BACKEND_URL" "Backend" 60 1
wait_for_url "$FRONTEND_URL" "Frontend" 60 1

open "$FRONTEND_URL"

echo "Fund Tracker is running!"
echo "Backend logs: $BACKEND_LOG"
echo "Frontend logs: $FRONTEND_LOG"
echo "Use stop_web_app.sh to stop the services."

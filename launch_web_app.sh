#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
LOG_DIR="$PROJECT_ROOT/data/logs"
VENV_DIR="$PROJECT_ROOT/.venv"
BACKEND_PID_FILE="$LOG_DIR/backend.pid"
BACKEND_PORT_FILE="$LOG_DIR/backend.port"
BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_PID_FILE="$LOG_DIR/frontend.pid"
FRONTEND_PORT_FILE="$LOG_DIR/frontend.port"
FRONTEND_API_BASE_FILE="$LOG_DIR/frontend.api_base"
FRONTEND_LOG="$LOG_DIR/frontend.log"
PYTHON_REQUIREMENTS_STAMP="$VENV_DIR/.requirements.sha1"
NODE_REQUIREMENTS_STAMP="$PROJECT_ROOT/web-ui/node_modules/.package-lock.sha1"

mkdir -p "$LOG_DIR"

wait_for_url() {
    local url="$1"
    local name="$2"
    local retries="${3:-60}"
    local delay="${4:-1}"

    for ((i=1; i<=retries; i++)); do
        if curl --noproxy '*' -fsS "$url" > /dev/null 2>&1; then
            echo "$name is ready: $url"
            return 0
        fi
        sleep "$delay"
    done

    echo "$name failed to become ready: $url" >&2
    return 1
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

find_supported_python() {
    local candidate
    for candidate in "${PYTHON_BIN:-}" python3 python3.11 python3.10 python3.9; do
        if [ -z "${candidate:-}" ] || ! command_exists "$candidate"; then
            continue
        fi
        if "$candidate" -c 'import sys; raise SystemExit(0 if (3, 9) <= sys.version_info[:2] < (3, 12) else 1)'; then
            echo "$candidate"
            return 0
        fi
    done

    echo "No supported Python interpreter found. Need Python 3.9-3.11." >&2
    return 1
}

ensure_venv() {
    local recreate=0
    local selected_python

    if [ ! -x "$VENV_DIR/bin/python" ]; then
        recreate=1
    elif ! "$VENV_DIR/bin/python" -c 'import sys; raise SystemExit(0 if (3, 9) <= sys.version_info[:2] < (3, 12) else 1)'; then
        local backup_dir="$PROJECT_ROOT/.venv.backup.$(date +%Y%m%d%H%M%S)"
        echo "Existing .venv uses an unsupported Python version; moving it to $backup_dir"
        mv "$VENV_DIR" "$backup_dir"
        recreate=1
    fi

    if [ "$recreate" -eq 1 ]; then
        selected_python="$(find_supported_python)"
        echo "Creating virtual environment with $selected_python ..."
        "$selected_python" -m venv "$VENV_DIR"
    fi

    VENV_PYTHON="$VENV_DIR/bin/python"
    VENV_PIP="$VENV_DIR/bin/pip"
}

requirements_hash() {
    shasum "$PROJECT_ROOT/requirements.txt" | awk '{print $1}'
}

node_lock_hash() {
    shasum "$PROJECT_ROOT/web-ui/package-lock.json" | awk '{print $1}'
}

python_runtime_ready() {
    "$VENV_PYTHON" - <<'PY' >/dev/null 2>&1
import akshare  # noqa: F401
import fastapi  # noqa: F401
import pydantic  # noqa: F401
import uvicorn  # noqa: F401
import yaml  # noqa: F401
PY
}

ensure_python_dependencies() {
    local current_hash expected_hash
    expected_hash="$(requirements_hash)"
    current_hash="$(cat "$PYTHON_REQUIREMENTS_STAMP" 2>/dev/null || true)"

    if [ "$current_hash" = "$expected_hash" ] && python_runtime_ready; then
        return 0
    fi

    echo "Installing/Updating Python dependencies..."
    "$VENV_PIP" install -q -r "$PROJECT_ROOT/requirements.txt"
    printf '%s\n' "$expected_hash" > "$PYTHON_REQUIREMENTS_STAMP"
}

ensure_node_dependencies() {
    local current_hash expected_hash
    expected_hash="$(node_lock_hash)"
    current_hash="$(cat "$NODE_REQUIREMENTS_STAMP" 2>/dev/null || true)"

    cd "$PROJECT_ROOT/web-ui"
    if [ -d "node_modules" ] && [ "$current_hash" = "$expected_hash" ]; then
        return 0
    fi

    echo "Installing frontend dependencies (this may take a minute)..."
    npm install --no-audit --no-fund --cache /tmp/npm-cache-fund-tracker
    mkdir -p "$(dirname "$NODE_REQUIREMENTS_STAMP")"
    printf '%s\n' "$expected_hash" > "$NODE_REQUIREMENTS_STAMP"
}

pid_is_running() {
    local pid="$1"
    [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1
}

read_file_if_exists() {
    local path="$1"
    if [ -f "$path" ]; then
        cat "$path"
    fi
    return 0
}

backend_health_ok() {
    local port="$1"
    curl --noproxy '*' -fsS "http://127.0.0.1:$port/api/health" 2>/dev/null | grep -q '"service":"fund-tracker"'
}

frontend_health_ok() {
    local port="$1"
    curl --noproxy '*' -fsS "http://127.0.0.1:$port" 2>/dev/null | grep -q 'Fund Tracker Web UI'
}

port_listener_pid() {
    local port="$1"
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1
}

stop_listener_on_port() {
    local port="$1"
    local pid

    pid="$(port_listener_pid "$port")"
    if [ -z "${pid:-}" ]; then
        return 0
    fi

    kill "$pid" >/dev/null 2>&1 || true
    for _ in {1..10}; do
        if ! lsof -tiTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    kill -9 "$pid" >/dev/null 2>&1 || true
}

cleanup_known_backend_ports() {
    local keep_port="${1:-}"
    local port

    for port in 8000 18000 28000; do
        if [ -n "$keep_port" ] && [ "$port" = "$keep_port" ]; then
            continue
        fi
        if backend_health_ok "$port"; then
            echo "Stopping stale backend on port $port ..."
            stop_listener_on_port "$port"
        fi
    done
}

cleanup_known_frontend_ports() {
    local keep_port="${1:-}"
    local port

    for port in 5173 15173 25173; do
        if [ -n "$keep_port" ] && [ "$port" = "$keep_port" ]; then
            continue
        fi
        if frontend_health_ok "$port"; then
            echo "Stopping stale frontend on port $port ..."
            stop_listener_on_port "$port"
        fi
    done
}

pick_available_port() {
    local port
    for port in "$@"; do
        if ! lsof -ti :"$port" >/dev/null 2>&1; then
            echo "$port"
            return 0
        fi
    done

    port="$1"
    while lsof -ti :"$port" >/dev/null 2>&1; do
        port=$((port + 1))
    done
    echo "$port"
}

cleanup_stale_file() {
    local path="$1"
    if [ -f "$path" ]; then
        rm -f "$path"
    fi
    return 0
}

start_backend() {
    local existing_pid existing_port
    existing_pid="$(read_file_if_exists "$BACKEND_PID_FILE")"
    existing_port="$(read_file_if_exists "$BACKEND_PORT_FILE")"

    if pid_is_running "$existing_pid" && [ -n "$existing_port" ] && backend_health_ok "$existing_port"; then
        cleanup_known_backend_ports "$existing_port"
        BACKEND_PORT="$existing_port"
        echo "Backend already running on port $BACKEND_PORT, reusing it."
        return 0
    fi

    cleanup_stale_file "$BACKEND_PID_FILE"
    cleanup_stale_file "$BACKEND_PORT_FILE"
    cleanup_known_backend_ports

    BACKEND_PORT="$(pick_available_port 8000 18000 28000)"
    echo "Starting Backend (FastAPI) on port $BACKEND_PORT ..."
    cd "$PROJECT_ROOT"
    export PYTHONPATH="$PROJECT_ROOT"
    FUND_TRACKER_API_PORT="$BACKEND_PORT" nohup "$VENV_PYTHON" src/fund_tracker/web_api.py > "$BACKEND_LOG" 2>&1 &
    BACKEND_PID=$!
    printf '%s\n' "$BACKEND_PID" > "$BACKEND_PID_FILE"
    printf '%s\n' "$BACKEND_PORT" > "$BACKEND_PORT_FILE"
}

start_frontend() {
    local api_base existing_pid existing_port existing_api_base
    api_base="http://127.0.0.1:$BACKEND_PORT/api"
    existing_pid="$(read_file_if_exists "$FRONTEND_PID_FILE")"
    existing_port="$(read_file_if_exists "$FRONTEND_PORT_FILE")"
    existing_api_base="$(read_file_if_exists "$FRONTEND_API_BASE_FILE")"

    if pid_is_running "$existing_pid" && [ -n "$existing_port" ] && [ "$existing_api_base" = "$api_base" ] && frontend_health_ok "$existing_port"; then
        cleanup_known_frontend_ports "$existing_port"
        FRONTEND_PORT="$existing_port"
        echo "Frontend already running on port $FRONTEND_PORT, reusing it."
        return 0
    fi

    if pid_is_running "$existing_pid"; then
        kill "$existing_pid" >/dev/null 2>&1 || true
        sleep 1
    fi

    cleanup_stale_file "$FRONTEND_PID_FILE"
    cleanup_stale_file "$FRONTEND_PORT_FILE"
    cleanup_stale_file "$FRONTEND_API_BASE_FILE"
    cleanup_known_frontend_ports

    FRONTEND_PORT="$(pick_available_port 5173 15173 25173)"
    echo "Starting Frontend (Vite) on port $FRONTEND_PORT ..."
    cd "$PROJECT_ROOT/web-ui"
    VITE_API_BASE="$api_base" nohup npm run dev -- --host 127.0.0.1 --port "$FRONTEND_PORT" --strictPort > "$FRONTEND_LOG" 2>&1 &
    FRONTEND_PID=$!
    printf '%s\n' "$FRONTEND_PID" > "$FRONTEND_PID_FILE"
    printf '%s\n' "$FRONTEND_PORT" > "$FRONTEND_PORT_FILE"
    printf '%s\n' "$api_base" > "$FRONTEND_API_BASE_FILE"
}

echo "Starting Fund Tracker Web Services..."

if ! command_exists npm; then
    echo "npm is required but was not found in PATH." >&2
    exit 1
fi

ensure_venv
ensure_python_dependencies
ensure_node_dependencies
start_backend
start_frontend

BACKEND_URL="http://127.0.0.1:$BACKEND_PORT/api/health"
FRONTEND_URL="http://127.0.0.1:$FRONTEND_PORT"

echo "Waiting for services to become ready..."
wait_for_url "$BACKEND_URL" "Backend" 60 1
wait_for_url "$FRONTEND_URL" "Frontend" 60 1

echo "Frontend URL: $FRONTEND_URL"
echo "Backend URL: http://127.0.0.1:$BACKEND_PORT/api"

if command_exists open; then
    if ! open "$FRONTEND_URL" >/dev/null 2>&1; then
        echo "Could not auto-open browser. Open this URL manually: $FRONTEND_URL"
    fi
fi

echo "Fund Tracker is running!"
echo "Backend logs: $BACKEND_LOG"
echo "Frontend logs: $FRONTEND_LOG"
echo "Use stop_web_app.sh to stop the services."

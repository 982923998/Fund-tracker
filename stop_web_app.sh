#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
LOG_DIR="$PROJECT_ROOT/data/logs"
BACKEND_PID_FILE="$LOG_DIR/backend.pid"
BACKEND_PORT_FILE="$LOG_DIR/backend.port"
FRONTEND_PID_FILE="$LOG_DIR/frontend.pid"
FRONTEND_PORT_FILE="$LOG_DIR/frontend.port"
FRONTEND_API_BASE_FILE="$LOG_DIR/frontend.api_base"

pid_is_running() {
    local pid="${1:-}"
    [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1
}

cleanup_file() {
    local path="$1"
    if [ -f "$path" ]; then
        rm -f "$path"
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

stop_listener_on_port() {
    local port="$1"
    local pid

    pid="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1)"
    if [ -z "${pid:-}" ]; then
        return 0
    fi

    kill "$pid" 2>/dev/null || true
    for _ in {1..10}; do
        if ! lsof -tiTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    kill -9 "$pid" 2>/dev/null || true
}

cleanup_known_ports() {
    local port

    for port in 8000 18000 28000; do
        if backend_health_ok "$port"; then
            echo "Stopping Backend on port $port ..."
            stop_listener_on_port "$port"
        fi
    done

    for port in 5173 15173 25173; do
        if frontend_health_ok "$port"; then
            echo "Stopping Frontend on port $port ..."
            stop_listener_on_port "$port"
        fi
    done
}

echo "Stopping Fund Tracker Web Services..."

# Stop Backend
if [ -f "$BACKEND_PID_FILE" ]; then
    PID=$(cat "$BACKEND_PID_FILE")
    echo "Stopping Backend (PID: $PID)..."
    if pid_is_running "$PID"; then
        kill "$PID" 2>/dev/null || true
    fi
fi

# Stop Frontend (including child processes)
if [ -f "$FRONTEND_PID_FILE" ]; then
    PID=$(cat "$FRONTEND_PID_FILE")
    echo "Stopping Frontend (PID: $PID)..."
    # Kill the entire process group
    PGID=$(ps -o pgid= -p "$PID" 2>/dev/null | grep -o '[0-9]*' || true)
    if [ -n "$PGID" ]; then
        kill -TERM "-$PGID" 2>/dev/null || true
    else
        kill "$PID" 2>/dev/null || true
    fi
fi

cleanup_file "$BACKEND_PID_FILE"
cleanup_file "$BACKEND_PORT_FILE"
cleanup_file "$FRONTEND_PID_FILE"
cleanup_file "$FRONTEND_PORT_FILE"
cleanup_file "$FRONTEND_API_BASE_FILE"
cleanup_known_ports

echo "Stopped."

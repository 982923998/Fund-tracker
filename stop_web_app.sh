#!/bin/bash

PROJECT_ROOT="/Users/chenmayao/Desktop/code/fund-tracker"
LOG_DIR="$PROJECT_ROOT/data/logs"

echo "Stopping Fund Tracker Web Services..."

# Stop Backend
if [ -f "$LOG_DIR/backend.pid" ]; then
    PID=$(cat "$LOG_DIR/backend.pid")
    echo "Stopping Backend (PID: $PID)..."
    kill $PID 2>/dev/null
    rm "$LOG_DIR/backend.pid"
fi

# Stop Frontend (including child processes)
if [ -f "$LOG_DIR/frontend.pid" ]; then
    PID=$(cat "$LOG_DIR/frontend.pid")
    echo "Stopping Frontend (PID: $PID)..."
    # Kill the entire process group
    PGID=$(ps -o pgid= -p $PID | grep -o '[0-9]*')
    if [ ! -z "$PGID" ]; then
        kill -TERM -$PGID 2>/dev/null
    else
        kill $PID 2>/dev/null
    fi
    rm "$LOG_DIR/frontend.pid"
fi

# Also clean up any lingering processes on known ports
echo "Cleaning up lingering processes on ports 8000 and 5173..."
lsof -ti :8000 | xargs kill -9 2>/dev/null
lsof -ti :5173 | xargs kill -9 2>/dev/null

echo "Stopped."

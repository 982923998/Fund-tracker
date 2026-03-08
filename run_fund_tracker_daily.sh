#!/bin/bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$REPO_ROOT/data/fund_tracker_daily.log"
CONFIG_FILE="$REPO_ROOT/config/fund_tracker.yaml"
ENV_FILE="$REPO_ROOT/.env"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
fi

mkdir -p "$(dirname "$LOG_FILE")"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

if [[ -f "$CONFIG_FILE" ]]; then
  "$PYTHON_BIN" "$REPO_ROOT/fund_tracker_cli.py" --config "$CONFIG_FILE" daily-run >>"$LOG_FILE" 2>&1
else
  "$PYTHON_BIN" "$REPO_ROOT/fund_tracker_cli.py" daily-run >>"$LOG_FILE" 2>&1
fi

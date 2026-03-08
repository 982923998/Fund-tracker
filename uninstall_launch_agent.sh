#!/bin/bash

set -euo pipefail

LABEL="com.chenmayao.fund-tracker"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"
USER_ID="$(id -u)"
RUNTIME_ROOT="$HOME/Library/Application Support/FundTrackerRuntime"

launchctl bootout "gui/$USER_ID" "$PLIST_PATH" >/dev/null 2>&1 || true
rm -f "$PLIST_PATH"

echo "已卸载 LaunchAgent：$PLIST_PATH"
echo "运行副本仍保留：$RUNTIME_ROOT"

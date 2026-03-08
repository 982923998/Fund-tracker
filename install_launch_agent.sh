#!/bin/bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="$PROJECT_ROOT/config/fund_tracker.yaml"
LABEL="com.chenmayao.fund-tracker"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"
RUNTIME_ROOT="$HOME/Library/Application Support/FundTrackerRuntime"
RUN_SCRIPT="$RUNTIME_ROOT/run_fund_tracker_daily.sh"
STDOUT_LOG="$RUNTIME_ROOT/data/launchd_stdout.log"
STDERR_LOG="$RUNTIME_ROOT/data/launchd_stderr.log"
USER_ID="$(id -u)"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "缺少配置文件：$CONFIG_FILE" >&2
  exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$PROJECT_ROOT/data" "$RUNTIME_ROOT"

RUN_TIME="$(awk -F'\"' '/daily_run_time/ {print $2}' "$CONFIG_FILE" | head -1)"
if [[ -z "$RUN_TIME" ]]; then
  echo "无法从配置中读取 daily_run_time" >&2
  exit 1
fi

HOUR="${RUN_TIME%:*}"
MINUTE="${RUN_TIME#*:}"

mkdir -p "$RUNTIME_ROOT/data"

rsync -a --delete \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude '__pycache__' \
  --exclude 'data' \
  "$PROJECT_ROOT/" "$RUNTIME_ROOT/"

if [[ -f "$PROJECT_ROOT/data/fund_tracker.db" && ! -f "$RUNTIME_ROOT/data/fund_tracker.db" ]]; then
  cp "$PROJECT_ROOT/data/fund_tracker.db" "$RUNTIME_ROOT/data/fund_tracker.db"
fi

if [[ -d "$PROJECT_ROOT/data/fund_tracker_snapshots" && ! -d "$RUNTIME_ROOT/data/fund_tracker_snapshots" ]]; then
  mkdir -p "$RUNTIME_ROOT/data/fund_tracker_snapshots"
  cp -R "$PROJECT_ROOT/data/fund_tracker_snapshots/." "$RUNTIME_ROOT/data/fund_tracker_snapshots/"
fi

if [[ ! -d "$RUNTIME_ROOT/.venv" ]]; then
  python3 -m venv "$RUNTIME_ROOT/.venv"
fi

"$RUNTIME_ROOT/.venv/bin/pip" install -q --upgrade pip
"$RUNTIME_ROOT/.venv/bin/pip" install -q -r "$RUNTIME_ROOT/requirements.txt"

"$RUNTIME_ROOT/.venv/bin/python" - <<PY
from pathlib import Path
import yaml

config_path = Path("$PROJECT_ROOT/config/fund_tracker.yaml")
runtime_root = Path("$RUNTIME_ROOT")
runtime_data = runtime_root / "data"
db_path = str((runtime_data / "fund_tracker.db").resolve())
snapshot_dir = str((runtime_data / "fund_tracker_snapshots").resolve())

with open(config_path, "r", encoding="utf-8") as handle:
    config = yaml.safe_load(handle) or {}

storage = config.setdefault("storage", {})
storage["db_path"] = db_path
storage["snapshot_dir"] = snapshot_dir

with open(config_path, "w", encoding="utf-8") as handle:
    yaml.safe_dump(config, handle, allow_unicode=True, sort_keys=False)

runtime_config_path = runtime_root / "config" / "fund_tracker.yaml"
runtime_config_path.parent.mkdir(parents=True, exist_ok=True)
with open(runtime_config_path, "w", encoding="utf-8") as handle:
    yaml.safe_dump(config, handle, allow_unicode=True, sort_keys=False)
PY

chmod +x "$RUNTIME_ROOT/run_fund_tracker_daily.sh"

cat >"$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>

  <key>ProgramArguments</key>
  <array>
    <string>$RUN_SCRIPT</string>
  </array>

  <key>WorkingDirectory</key>
  <string>$RUNTIME_ROOT</string>

  <key>RunAtLoad</key>
  <false/>

  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>$HOUR</integer>
    <key>Minute</key>
    <integer>$MINUTE</integer>
  </dict>

  <key>StandardOutPath</key>
  <string>$STDOUT_LOG</string>

  <key>StandardErrorPath</key>
  <string>$STDERR_LOG</string>
</dict>
</plist>
PLIST

launchctl bootout "gui/$USER_ID" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$USER_ID" "$PLIST_PATH"
launchctl enable "gui/$USER_ID/$LABEL"

echo "已安装 LaunchAgent：$PLIST_PATH"
echo "运行副本目录：$RUNTIME_ROOT"
echo "计划执行时间：每天 $RUN_TIME"
echo "查看状态：launchctl print gui/$USER_ID/$LABEL"

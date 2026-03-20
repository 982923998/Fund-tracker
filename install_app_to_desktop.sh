#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
APP_NAME="Launch-Fund-Tracker.app"
APP_DIR="$HOME/Desktop/AI"
APP_PATH="$APP_DIR/$APP_NAME"
LAUNCH_COMMAND="$PROJECT_ROOT/launch_fund_tracker.command"
REFERENCE_APP="$HOME/Desktop/AI/Launch-Daily-Insights.app"

upsert_plist_string() {
    local key="$1"
    local value="$2"
    if /usr/libexec/PlistBuddy -c "Print :$key" "$APP_PATH/Contents/Info.plist" >/dev/null 2>&1; then
        /usr/libexec/PlistBuddy -c "Set :$key $value" "$APP_PATH/Contents/Info.plist"
    else
        /usr/libexec/PlistBuddy -c "Add :$key string $value" "$APP_PATH/Contents/Info.plist"
    fi
}

upsert_plist_bool() {
    local key="$1"
    local value="$2"
    if /usr/libexec/PlistBuddy -c "Print :$key" "$APP_PATH/Contents/Info.plist" >/dev/null 2>&1; then
        /usr/libexec/PlistBuddy -c "Set :$key $value" "$APP_PATH/Contents/Info.plist"
    else
        /usr/libexec/PlistBuddy -c "Add :$key bool $value" "$APP_PATH/Contents/Info.plist"
    fi
}

echo "Creating macOS app bundle at $APP_PATH ..."

mkdir -p "$APP_DIR"
rm -rf "$APP_PATH"

osacompile -o "$APP_PATH" <<EOF
on run
	do shell script "open " & quoted form of POSIX path of "$LAUNCH_COMMAND"
end run
EOF

upsert_plist_string "CFBundleIdentifier" "com.chenmayao.launch-fund-tracker"
upsert_plist_string "CFBundleName" "Launch-Fund-Tracker"
upsert_plist_bool "OSAAppletShowStartupScreen" "false"

if [ -f "$REFERENCE_APP/Contents/Resources/applet.icns" ]; then
    cp "$REFERENCE_APP/Contents/Resources/applet.icns" "$APP_PATH/Contents/Resources/applet.icns"
fi

echo "App bundle updated: $APP_PATH"
echo "Launch command: $LAUNCH_COMMAND"

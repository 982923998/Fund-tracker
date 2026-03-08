#!/bin/bash

APP_NAME="FundTrackerLauncher.app"
DESKTOP_DIR="$HOME/Desktop"
PROJECT_ROOT="/Users/chenmayao/Desktop/code/fund-tracker"

echo "Creating Terminal-aware macOS App Bundle on Desktop..."

# 1. Create directory structure
mkdir -p "$DESKTOP_DIR/$APP_NAME/Contents/MacOS"

# 2. Create the executable script that opens Terminal
cat <<EOF > "$DESKTOP_DIR/$APP_NAME/Contents/MacOS/Launcher"
#!/bin/bash
osascript -e 'tell application "Terminal" to do script "cd \"$PROJECT_ROOT\" && ./launch_web_app.sh"'
EOF

chmod +x "$DESKTOP_DIR/$APP_NAME/Contents/MacOS/Launcher"

# 3. Create Info.plist
cat <<EOF > "$DESKTOP_DIR/$APP_NAME/Contents/Info.plist"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>Launcher</string>
    <key>CFBundleIdentifier</key>
    <string>com.fundtracker.launcher</string>
    <key>CFBundleName</key>
    <string>FundTracker</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>1.1</string>
    <key>LSUIElement</key>
    <true/>
</dict>
</plist>
EOF

echo "App Bundle updated: $DESKTOP_DIR/$APP_NAME"

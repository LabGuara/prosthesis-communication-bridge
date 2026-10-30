#!/bin/bash

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UPDATE_SCRIPT="$SCRIPT_DIR/update-binary.py"
COMMANDS_DIR="$SCRIPT_DIR/bin"
COMMANDS_INSTALL_DIR="/usr/local/bin"

echo "Stopping and removing prosthesis-logger service..."

# Stop the service
echo "Stopping service..."
sudo systemctl stop prosthesis-communication-bridge

# Disable the service (prevent it from starting on boot)
echo "Disabling service..."
sudo systemctl disable prosthesis-communication-bridge

# Remove the service file
echo "Removing service file..."
sudo rm -f /etc/systemd/system/prosthesis-communication-bridge.service
sudo systemctl daemon-reload

# Remove the operator commands installed by init.sh
echo "Removing operator commands..."
if [ -d "$COMMANDS_DIR" ]; then
    for command_path in "$COMMANDS_DIR"/*; do
        command_name="$(basename "$command_path")"
        sudo rm -f "$COMMANDS_INSTALL_DIR/$command_name"
        echo "  $COMMANDS_INSTALL_DIR/$command_name"
    done
fi

# Remove the cron job
echo "Removing cron job..."
(sudo crontab -u root -l 2>/dev/null || echo "") | grep -v "$UPDATE_SCRIPT" | sudo crontab -u root -

echo ""
echo "Cleanup complete!"
echo "The service has been stopped and disabled."
echo "The cron job has been removed."
echo "The watch-prosthesis and stop-prosthesis commands have been removed."

#!/bin/bash

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UPDATE_SCRIPT="$SCRIPT_DIR/update-binary.py"

echo "Stopping and removing prosthesis-logger service..."

# Stop the service
echo "Stopping service..."
sudo systemctl stop prosthesis-logger-wrapper

# Disable the service (prevent it from starting on boot)
echo "Disabling service..."
sudo systemctl disable prosthesis-logger-wrapper

# Remove the service file
echo "Removing service file..."
sudo rm -f /etc/systemd/system/prosthesis-logger-wrapper.service
sudo systemctl daemon-reload

# Remove the cron job
echo "Removing cron job..."
(sudo crontab -u root -l 2>/dev/null || echo "") | grep -v "$UPDATE_SCRIPT" | sudo crontab -u root -

echo ""
echo "Cleanup complete!"
echo "The service has been stopped and disabled."
echo "The cron job has been removed."

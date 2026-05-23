#!/bin/bash

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_FILE="$SCRIPT_DIR/prothesis-logger-wrapper.service"
UPDATE_SCRIPT="$SCRIPT_DIR/update-binary.py"
MAIN_PROTHESIS_EXECUTABLE="prothesis-mock.sh"
ENVIRONMENT_FILE="/etc/prothesis-logger-wrapper.env"

echo "Setting up prothesis-logger..."

# Ensure the deploy directory and files are readable by prothesis user
echo "Setting permissions..."
sudo chmod 755 "$SCRIPT_DIR"
sudo chmod 644 "$UPDATE_SCRIPT"
if [ -f "$SCRIPT_DIR/.env" ]; then
    sudo chmod 644 "$SCRIPT_DIR/.env"
fi

chmod +x "$SCRIPT_DIR/$MAIN_PROTHESIS_EXECUTABLE"

cp "$SCRIPT_DIR/.env.example" "$ENVIRONMENT_FILE"

# Copy and enable service
echo "Setting up systemd service..."
sudo cp "$SERVICE_FILE" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable prothesis-logger-wrapper
echo "Service enabled (will start on boot)"

# Start the service now
echo "Starting service..."
sudo systemctl start prothesis-logger-wrapper
echo "Service started"

# Set up cron job for the prothesis user
echo "Setting up cron job..."
CRON_SCHEDULE="* * * * * python3 $UPDATE_SCRIPT >> /var/log/prothesis-logger-update.log 2>&1"

# Remove old cron job if it exists and add new one (updates if already exists)
(sudo crontab -u prothesis -l 2>/dev/null || echo "") | grep -v "$UPDATE_SCRIPT" | sudo crontab -u prothesis -
(sudo crontab -u prothesis -l 2>/dev/null; echo "$CRON_SCHEDULE") | sudo crontab -u prothesis -
echo "Cron job added/updated"

# Create log directory
echo "Creating log directory..."
sudo mkdir -p /var/log
sudo touch /var/log/prothesis-logger-update.log
sudo chown prothesis:prothesis /var/log/prothesis-logger-update.log

echo ""
echo "Setup complete!"
echo ""
echo "Verify with:"
echo "  sudo systemctl status prothesis-logger-wrapper"
echo "  sudo crontab -u prothesis -l"
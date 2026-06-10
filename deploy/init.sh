#!/bin/bash

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVICE_FILE="$SCRIPT_DIR/prothesis-logger-wrapper.service"
UPDATE_SCRIPT="$SCRIPT_DIR/update-binary.py"
ENVIRONMENT_FILE="/etc/prothesis-logger-wrapper.env"

echo "Setting up prothesis-logger..."

# Ensure the deploy directory and files are readable by the service
echo "Setting permissions..."
sudo chmod 755 "$SCRIPT_DIR"
sudo chmod 644 "$UPDATE_SCRIPT"
sudo chmod 644 "$SCRIPT_DIR/.env.example"
if [ -f "$SCRIPT_DIR/.env" ]; then
    sudo chmod 644 "$SCRIPT_DIR/.env"
fi

cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
sudo cp "$ROOT_DIR/.env" "$ENVIRONMENT_FILE"

# Copy and enable service
echo "Setting up systemd service..."
sudo cp "$SERVICE_FILE" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable prothesis-logger-wrapper
echo "Service enabled (will start on boot)"

# Install the wrapper binary before starting the service.
echo "Installing wrapper binary..."
sudo python3 "$UPDATE_SCRIPT"

# Start the service now
echo "Starting service..."
sudo systemctl start prothesis-logger-wrapper
echo "Service started"

echo ""
echo "Setup complete!"
echo ""
echo "Verify with:"
echo "  sudo systemctl status prothesis-logger-wrapper"
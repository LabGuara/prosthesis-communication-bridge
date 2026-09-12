#!/bin/bash

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVICE_FILE="$SCRIPT_DIR/prosthesis-logger-wrapper.service"
UPDATE_SCRIPT="$SCRIPT_DIR/update-binary.py"
ENVIRONMENT_FILE="/etc/prosthesis-logger-wrapper.env"
COMMANDS_DIR="$SCRIPT_DIR/bin"
COMMANDS_INSTALL_DIR="/usr/local/bin"

echo "Setting up prosthesis-logger..."

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

# Install the operator commands on PATH: watch-prosthesis follows the log,
# stop-prosthesis stops the prosthesis without going through the broker.
echo "Installing operator commands..."
for command_path in "$COMMANDS_DIR"/*; do
    command_name="$(basename "$command_path")"
    sudo install -m 755 "$command_path" "$COMMANDS_INSTALL_DIR/$command_name"
    echo "  $COMMANDS_INSTALL_DIR/$command_name"
done

# Copy and enable service
echo "Setting up systemd service..."
sudo cp "$SERVICE_FILE" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable prosthesis-logger-wrapper
echo "Service enabled (will start on boot)"

# Install the wrapper binary before starting the service.
echo "Installing wrapper binary..."
sudo python3 "$UPDATE_SCRIPT"

# Start the service now
echo "Starting service..."
sudo systemctl start prosthesis-logger-wrapper
echo "Service started"

echo ""
echo "Setup complete!"
echo ""
echo "Verify with:"
echo "  sudo systemctl status prosthesis-logger-wrapper"
echo ""
echo "Follow the logs with (press 's' there to stop the prosthesis):"
echo "  watch-prosthesis"
echo ""
echo "Or stop the prosthesis from any terminal with:"
echo "  stop-prosthesis"
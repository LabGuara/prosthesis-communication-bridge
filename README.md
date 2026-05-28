# prothesis-logger-wrapper

This binary wraps the prothesis executable, forwards its stdout/stderr, and optionally publishes each log line to MQTT.

## Deployment

The first deployment path is GitHub Actions plus a Raspberry Pi update script.

- The workflow in [.github/workflows/release.yml](.github/workflows/release.yml) builds a Linux `aarch64` release on every push to `main` and publishes a GitHub Release asset.
- The Pi-side updater in [deploy/pi/update-wrapper.py](deploy/pi/update-wrapper.py) downloads the latest release asset, installs it under `/opt/prothesis-logger-wrapper`, updates a `current` symlink, and restarts systemd.
- The systemd unit template in [deploy/pi/prothesis-logger-wrapper.service](deploy/pi/prothesis-logger-wrapper.service) is a starting point. Edit `ExecStart` so it launches the actual prothesis executable and arguments you want the wrapper to supervise.

## Raspberry Pi setup

1. Copy the systemd unit template to `/etc/systemd/system/prothesis-logger-wrapper.service`.
2. Create `/etc/prothesis-logger-wrapper.env` with the MQTT settings you want on the Pi.
3. Install the wrapper with `sudo REPO=your-org/prothesis-logger-wrapper python3 deploy/pi/update-wrapper.py`.
4. Enable the service with `sudo systemctl enable --now prothesis-logger-wrapper`.

## Environment variables

The wrapper reads these variables from the environment or from a local `.env` file:

- `MQTT_HOST`
- `MQTT_PORT`
- `MQTT_LOGS_TOPIC`
- `MQTT_COMMAND_LOGS_PARSED_TOPIC`
- `MQTT_QOS`
- `MQTT_CLIENT_ID`
- `MQTT_USERNAME`
- `MQTT_PASSWORD`
- `MQTT_TLS`

The Pi updater also accepts `GITHUB_TOKEN` for private repositories. Use a token that can read release metadata and download release assets from the repo.

## Notes

- The workflow currently targets Raspberry Pi OS 64-bit (`aarch64-unknown-linux-gnu`). If your Pi is running 32-bit OS, the target should be changed before relying on the release artifacts.
- The updater keeps each release under a versioned directory and flips `/opt/prothesis-logger-wrapper/current` to the newest one.
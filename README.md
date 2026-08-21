# prosthesis-logger-wrapper

This binary wraps the prosthesis executable, forwards its stdout/stderr, and optionally publishes each log line to MQTT.

## Deployment

The first deployment path is GitHub Actions plus a Raspberry Pi update script.

- The workflow in [.github/workflows/release.yml](.github/workflows/release.yml) builds a Linux `aarch64` release on every push to `main` and publishes a GitHub Release asset.
- The Pi-side updater in [deploy/pi/update-wrapper.py](deploy/pi/update-wrapper.py) downloads the latest release asset, installs it under `/opt/prosthesis-logger-wrapper`, updates a `current` symlink, and restarts systemd.
- The systemd unit template in [deploy/pi/prosthesis-logger-wrapper.service](deploy/pi/prosthesis-logger-wrapper.service) is a starting point. Edit `ExecStart` so it launches the actual prosthesis executable and arguments you want the wrapper to supervise.

## Raspberry Pi setup

1. Copy the systemd unit template to `/etc/systemd/system/prosthesis-logger-wrapper.service`.
2. Create `/etc/prosthesis-logger-wrapper.env` with the MQTT settings you want on the Pi.
3. Install the wrapper with `sudo REPO=your-org/prosthesis-logger-wrapper python3 deploy/pi/update-wrapper.py`.
4. Enable the service with `sudo systemctl enable --now prosthesis-logger-wrapper`.

## Environment variables

The wrapper reads these variables from the environment or from a local `.env` file:

- `MQTT_HOST`
- `MQTT_PORT`
- `MQTT_LOGS_TOPIC`
- `MQTT_COMMAND_LOGS_PARSED_TOPIC`
- `MQTT_POWER_FEEDBACK_TOPIC`
- `MQTT_QOS`
- `MQTT_CLIENT_ID`
- `MQTT_USERNAME`
- `MQTT_PASSWORD`
- `MQTT_TLS`

The Pi updater also accepts `GITHUB_TOKEN` for private repositories. Use a token that can read release metadata and download release assets from the repo.

## Power commands

Commands `16` (power on) and `17` (power off) on `MQTT_COMMAND_LOGS_PARSED_TOPIC` start and stop the `RoboticLegControl` executable instead of being forwarded over TCP. The wrapper keeps the power state in memory (seeded from the running processes at startup), so repeating a command that is already satisfied is a no-op.

Every power command answers on `MQTT_POWER_FEEDBACK_TOPIC`:

```json
{
  "timestamp": "2026-08-17T12:00:00+00:00",
  "command": "power_on",
  "status": "success",
  "state": "on",
  "message": "started RoboticLegControl (sudo pid 1234)"
}
```

`status` is `failure` only when the prosthesis did not reach the requested state — failing to start while off, or failing to stop while on. Commands that were already satisfied report `success`.

## Notes

- The workflow currently targets Raspberry Pi OS 64-bit (`aarch64-unknown-linux-gnu`). If your Pi is running 32-bit OS, the target should be changed before relying on the release artifacts.
- The updater keeps each release under a versioned directory and flips `/opt/prosthesis-logger-wrapper/current` to the newest one.
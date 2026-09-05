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

## Startup check

`RoboticLegControl` reports its input buffer on stdout as `Data bytes available <n>, out = 0.`. The wrapper captures that stdout, forwards every line to its own stdout unchanged, and watches the buffer on every start (power on and gait changes alike):

- A healthy prosthesis fills the buffer up, reads it out when it reaches **exactly 52** bytes and drops back to 0, over and over. Every value on the way there is normal.
- A bad start never reaches 52. It may well go past it — a faulty actuator has been seen cycling `24, 24, 24, 60` and climbing into the 70s — so the check is an exact match on 52, not "at least 52".

The watch only starts once the executable reports the buffer for the first time, so the CAN bus and maxon initialization before that does not count against it. From that first report the buffer has one second to reach 52, and the start is only called bad once at least 20 reports back that verdict up: the buffer reaches 52 only every few reports, so a control loop printing slower than usual gets more time rather than being failed on the clock alone. A start that reports nothing at all within ten seconds counts as bad too.

A bad start is killed and retried, up to 3 attempts. The power feedback message reports `success` on the first healthy start (naming the attempts that failed before it, if any) and `failure` with the reason of each attempt when all of them failed, leaving the prosthesis stopped.

## Notes

- The workflow currently targets Raspberry Pi OS 64-bit (`aarch64-unknown-linux-gnu`). If your Pi is running 32-bit OS, the target should be changed before relying on the release artifacts.
- The updater keeps each release under a versioned directory and flips `/opt/prosthesis-logger-wrapper/current` to the newest one.
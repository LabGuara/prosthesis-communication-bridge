# prosthesis-logger-wrapper

This binary wraps the prosthesis executable, forwards its stdout/stderr, and optionally publishes each log line to MQTT.

## Deployment

The first deployment path is GitHub Actions plus a Raspberry Pi update script.

- The workflow in [.github/workflows/release.yml](.github/workflows/release.yml) builds a Linux `aarch64` release on every push to `main` and publishes a GitHub Release asset.
- The Pi-side updater in [deploy/update-binary.py](deploy/update-binary.py) downloads the latest release asset, installs it under `/opt/prosthesis-logger-wrapper`, updates a `current` symlink, and restarts systemd.
- The systemd unit template in [deploy/prosthesis-logger-wrapper.service](deploy/prosthesis-logger-wrapper.service) is a starting point. Edit `ExecStart` so it launches the actual prosthesis executable and arguments you want the wrapper to supervise.

## Raspberry Pi setup

1. Copy the systemd unit template to `/etc/systemd/system/prosthesis-logger-wrapper.service`.
2. Create `/etc/prosthesis-logger-wrapper.env` with the MQTT settings you want on the Pi.
3. Install the wrapper with `sudo REPO=your-org/prosthesis-logger-wrapper python3 deploy/update-binary.py`.
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

## Commands

Commands arrive on `MQTT_COMMAND_LOGS_PARSED_TOPIC` as a bare decimal number, and the value says what it is:

| Value | Command | Handling |
| --- | --- | --- |
| `16` | power on | starts the `RoboticLegControl` executable |
| `17` | power off | stops it |
| `1`-`9` | gait mode | restarts it with `--gait=<value>` |
| `3000`-`15000` | actuator current | restarts it with `--I=<value>` |

The wrapper keeps the power state in memory (seeded from the running processes at startup), so repeating a power command that is already satisfied is a no-op. It also remembers the gait mode and the actuator current, so each restart keeps whatever the other one was last set to; until a command says otherwise they are `1` and `3000`, the control executable's own defaults. Anything outside these ranges is logged and ignored rather than started: a restart on a value nobody can interpret is worse than no restart.

Every command answers on `MQTT_POWER_FEEDBACK_TOPIC`:

```json
{
  "timestamp": "2026-08-17T12:00:00+00:00",
  "command": "set_actuator_current",
  "status": "success",
  "state": "on",
  "gait": 1,
  "current": 5000,
  "message": "stopped RoboticLegControl (pids 1234, 1235); started RoboticLegControl with gait 1 and current 5000 (sudo pid 1287), buffer reaching 52 bytes"
}
```

`command` is `power_on`, `power_off`, `set_gait` or `set_actuator_current`. `gait` and `current` are the settings the prosthesis is running with after the command. `status` is `failure` only when the prosthesis did not reach the requested state — failing to start while off, or failing to stop while on. Commands that were already satisfied report `success`.

## Startup check

`RoboticLegControl` reports its input buffer on stdout as `Data bytes available <n>, out = 0.`. The wrapper captures that stdout, forwards every line to its own stdout unchanged, and watches the buffer on every start (power on and gait changes alike):

- A healthy prosthesis fills the buffer up, reads it out when it reaches **exactly 52** bytes and drops back to 0, over and over. Every value on the way there is normal.
- A bad start never reaches 52. It may well go past it — a faulty actuator has been seen cycling `24, 24, 24, 60` and climbing into the 70s — so the check is an exact match on 52, not "at least 52".

The watch only starts once the executable reports the buffer for the first time, so the CAN bus and maxon initialization before that does not count against it. From that first report the buffer has one second to reach 52, and the start is only called bad once at least 20 reports back that verdict up: the buffer reaches 52 only every few reports, so a control loop printing slower than usual gets more time rather than being failed on the clock alone. A start that reports nothing at all within ten seconds counts as bad too.

There is one failure the check refuses to retry. When the actuators are switched off, the maxon drives never answer on the CAN bus and the executable gives up on them with `[maxon] no <n> NAO respondeu. O modelo vai rodar sem esse eixo.`, carrying on without the axis. The buffer can still reach 52 in that state, so a startup that only watched the buffer would call a prosthesis that cannot move healthy. As soon as such a line shows up the process is killed and the command answers `failure` right away: no amount of starting again switches the actuators on, so the remaining attempts are not spent on it.

Any other bad start is killed and retried, up to 3 attempts. The power feedback message reports `success` on the first healthy start (naming the attempts that failed before it, if any) and `failure` with the reason of each attempt when all of them failed, leaving the prosthesis stopped.

## Stopping from the Pi

The prosthesis can be stopped without going through the broker, for when the leg is doing something it should not and reaching for the server is not an option. [deploy/init.sh](deploy/init.sh) installs two commands into `/usr/local/bin`, and [deploy/cleanup.sh](deploy/cleanup.sh) removes them again:

- `watch-prosthesis` follows the service log — everything the wrapper prints plus the stdout of the control executable. Arguments are passed on to `journalctl`, so `watch-prosthesis -n 200` works.
- `stop-prosthesis` stops the prosthesis now. It sends `SIGUSR1` to the wrapper process, which kills the control executable and answers on `MQTT_POWER_FEEDBACK_TOPIC` like any other power command. The wrapper stays up, so the server can power the prosthesis on again afterwards. It signals the unit's `MainPID` directly rather than using `systemctl kill`, which would deliver the signal to every process in the unit: `SIGUSR1` terminates `sudo` and the control executable outright, so they would be cut down where they stand instead of being shut down by the wrapper.

Note that a keyboard shortcut inside `watch-prosthesis` cannot do this: `journalctl` is only reading the log, and Ctrl+C or Ctrl+Z there reaches `journalctl`, never the service. Run `stop-prosthesis` from another terminal instead.

The wrapper handles three signals, and every one of them answers on the feedback topic:

| Signal | Sent by | What happens |
| --- | --- | --- |
| `SIGUSR1` | `stop-prosthesis` | Kills the control executable; the wrapper keeps running. |
| `SIGINT` | Ctrl+C, when run in a terminal instead of under systemd | Kills the control executable, then exits. |
| `SIGTERM` | `systemctl stop prosthesis-logger-wrapper` | Kills the control executable, then exits. |

A stop signal also cancels a power-on that is still retrying, so the prosthesis is not brought back up by a start that was already on its way.

## Notes

- The workflow currently targets Raspberry Pi OS 64-bit (`aarch64-unknown-linux-musl`). If your Pi is running 32-bit OS, the target should be changed before relying on the release artifacts.
- The updater keeps each release under a versioned directory and flips `/opt/prosthesis-logger-wrapper/current` to the newest one.
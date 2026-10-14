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

Commands arrive on `MQTT_COMMAND_LOGS_PARSED_TOPIC` in the same JSON format the app publishes and the orchestrator validates:

```json
{
  "timestamp": "2026-08-17T12:00:00+00:00",
  "command_type": 2,
  "value": 1
}
```

`command_type` alone says what the command is; `value` is only checked against the range that type accepts:

| `command_type` | Command | `value` | Handling |
| --- | --- | --- | --- |
| `1` | gait mode | `1`-`9` | restarts `RoboticLegControl` with `--gait=<value>` |
| `2` | power | `1` on, `2` off | starts or stops `RoboticLegControl` |
| `3` | actuator current | `3000`-`15000` | restarts `RoboticLegControl` with `--I=<value>` |

The wrapper keeps the power state in memory (seeded from the running processes at startup), so repeating a power command that is already satisfied is a no-op. It also remembers the gait mode and the actuator current, so each restart keeps whatever the other one was last set to; until a command says otherwise they are `1` and `3000`, the control executable's own defaults. A payload that is not this JSON, an unknown `command_type`, or a value outside the range of its type is logged and ignored rather than started: a restart on a value nobody can interpret is worse than no restart.

Every command answers on `MQTT_POWER_FEEDBACK_TOPIC`:

```json
{
  "timestamp": "2026-08-17T12:00:00+00:00",
  "command": "set_actuator_current",
  "status": "success",
  "state": "on",
  "gait": 1,
  "current": 5000,
  "message": "stopped RoboticLegControl (pids 1234, 1235); started RoboticLegControl with gait 1 and current 5000 (sudo pid 1287), buffer steadily cycling 52 -> 0 for 3s"
}
```

`command` is `power_on`, `power_off`, `set_gait` or `set_actuator_current`. `gait` and `current` are the settings the prosthesis is running with after the command. `status` is `failure` only when the prosthesis did not reach the requested state — failing to start while off, or failing to stop while on. Commands that were already satisfied report `success`.

## Startup check

`RoboticLegControl` reports its input buffer on stdout as `Data bytes available <n>, out = 0.`. The wrapper captures that stdout, forwards every line to its own stdout unchanged, and watches the buffer on every start (power on and gait changes alike):

- A working prosthesis runs one cycle, over and over: the buffer **fills up to exactly 52, and the very next report is 0** (`21, 52, 0, 0, 23, 52, 0, 0, ...`).
- Anything else is not working. Out of sync, the buffer is never read out: it goes past 52 (`23, 23, 45, 75`) and never returns to 0, while one column of it keeps climbing through every value — 52 included, and again and again as it wraps around. That is why the check looks at the cycle, not at whether 52 shows up.

The rule: **the 52 → 0 cycle has to keep going for three seconds, never a second without completing, and a single report that breaks it fails the start on the spot** — a buffer past 52, or a 52 followed by anything but 0. Out of sync, that happens within the first few reports, so a bad start is caught in milliseconds; a good one gets its success feedback after three seconds of clean cycles.

The watch begins at the first report, so the CAN bus and maxon initialization before it does not count against the prosthesis. A start that reports nothing at all within ten seconds counts as bad too.

There is one failure the check refuses to retry. When the actuators are switched off, the maxon drives never answer on the CAN bus and the executable gives up on them with `[maxon] no <n> NAO respondeu. O modelo vai rodar sem esse eixo.`, carrying on without the axis. The buffer can still reach 52 in that state, so a startup that only watched the buffer would call a prosthesis that cannot move healthy. As soon as such a line shows up the process is killed and the command answers `failure` right away: no amount of starting again switches the actuators on, so the remaining attempts are not spent on it.

Any other bad start is killed and **retried for as long as it takes**, a second apart, until the prosthesis comes up stable. Killing means `SIGTERM` first, so the control executable gets to write its `.mat` file, and `SIGKILL` three seconds later if it is still there — a process that sits through `SIGTERM` must not be able to keep the prosthesis running. Every attempt logs what the cleanup did, including the case where it found nothing to kill. The feedback message only reports `success` once the prosthesis is stable, naming how many attempts failed before it. It reports `failure` only when the actuators are off, or when a newer command comes in while the start is still retrying — a power off, a different gait, a stop signal — in which case the start lets go of the prosthesis and the newer command runs instead.

Commands run on a worker thread of their own, one at a time in the order they arrived. That keeps the MQTT connection alive during a start that retries for minutes, which is what lets a power off get through to stop it.

## Stopping from the Pi

The prosthesis can be stopped without going through the broker, for when the leg is doing something it should not and reaching for the server is not an option. [deploy/init.sh](deploy/init.sh) installs two commands into `/usr/local/bin`, and [deploy/cleanup.sh](deploy/cleanup.sh) removes them again:

- `watch-prosthesis` follows the service log — everything the wrapper prints plus the stdout of the control executable — and keeps a stop key ready while it runs: **`s` stops the prosthesis**, `q` leaves the log view without touching it. Arguments are passed on to `journalctl`, so `watch-prosthesis -n 200` works; `--plain` drops the stop key and only follows the log, which is what you want when piping the output somewhere.
- `stop-prosthesis` stops the prosthesis now. It sends `SIGUSR1` to the wrapper process, which kills the control executable and answers on `MQTT_POWER_FEEDBACK_TOPIC` like any other power command. The wrapper stays up, so the server can power the prosthesis on again afterwards. It signals the unit's `MainPID` directly rather than using `systemctl kill`, which would deliver the signal to every process in the unit: `SIGUSR1` terminates `sudo` and the control executable outright, so they would be cut down where they stand instead of being shut down by the wrapper.

The `s` key works because `watch-prosthesis` is a program in its own right: it runs `journalctl` in the background and reads the keyboard itself, so a key press becomes a signal to the service. Terminal shortcuts cannot do the same — Ctrl+C and Ctrl+Z reach whatever is in the foreground of that terminal, which is the log viewer, never the service. It asks for the sudo password up front, if one is needed at all, so the stop key does not stop to ask at the worst possible moment.

The wrapper handles three signals, and every one of them answers on the feedback topic:

| Signal | Sent by | What happens |
| --- | --- | --- |
| `SIGUSR1` | `stop-prosthesis` | Kills the control executable (`SIGTERM`, then `SIGKILL` after 3s); the wrapper keeps running. |
| `SIGINT` | Ctrl+C, when run in a terminal instead of under systemd | Kills the control executable, then exits. |
| `SIGTERM` | `systemctl stop prosthesis-logger-wrapper` | Kills the control executable, then exits. |

A stop signal also cancels a start that is still retrying, so the prosthesis is not brought back up by a start that was already on its way.

## Notes

- The workflow currently targets Raspberry Pi OS 64-bit (`aarch64-unknown-linux-musl`). If your Pi is running 32-bit OS, the target should be changed before relying on the release artifacts.
- The updater keeps each release under a versioned directory and flips `/opt/prosthesis-logger-wrapper/current` to the newest one.
# Power command feedback — bridge → orchestrator handoff

The communication bridge (this repo, running on the Raspberry Pi) now answers every power
command with a feedback message on a **new MQTT topic**. This document is the contract the
orchestrator needs to implement against.

Nothing on the command side changed: the orchestrator keeps publishing `16` / `17` exactly
as it does today. What is new is that a reply always comes back.

## At a glance

| | |
| --- | --- |
| Feedback topic | `prothesis/actions/feedback` (env `MQTT_POWER_FEEDBACK_TOPIC`) |
| Direction | bridge → orchestrator |
| QoS | same as the bridge's `MQTT_QOS` (default `1`) |
| Retain | `false` |
| Broker | unchanged — same host, credentials and TLS settings as the logs/commands topics |
| Triggered by | every `16` / `17` received on the commands topic, without exception |

## Command side (unchanged, for reference)

Commands arrive on `prothesis/actions/parsed` (env `MQTT_COMMAND_LOGS_PARSED_TOPIC`) as the
ASCII decimal representation of a `u8`:

| Value | Meaning | Handling |
| --- | --- | --- |
| `1`–`9` | gait commands | forwarded over TCP to `192.168.0.101:25000`. **No feedback is published.** |
| `16` | power on | starts `RoboticLegControl`, publishes feedback |
| `17` | power off | stops `RoboticLegControl`, publishes feedback |

## Feedback payload

A JSON object. All five fields are always present.

| Field | Type | Values |
| --- | --- | --- |
| `timestamp` | string | RFC 3339 UTC, e.g. `2026-08-17T12:00:00.123456789+00:00` (nanosecond precision, `+00:00` offset — not `Z`) |
| `command` | string | `"power_on"` or `"power_off"` — echoes which command this answers |
| `status` | string | `"success"` or `"failure"` |
| `state` | string | `"on"` or `"off"` — the bridge's power state **after** the command |
| `message` | string | human-readable detail. Free text, **not a stable API**: log it, show it to an operator, never branch on it |

### JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ProsthesisPowerFeedback",
  "type": "object",
  "required": ["timestamp", "command", "status", "state", "message"],
  "properties": {
    "timestamp": { "type": "string", "format": "date-time" },
    "command": { "type": "string", "enum": ["power_on", "power_off"] },
    "status": { "type": "string", "enum": ["success", "failure"] },
    "state": { "type": "string", "enum": ["on", "off"] },
    "message": { "type": "string" }
  }
}
```

## Status semantics — the part that matters

`status` describes **the resulting state, not the work performed**. A command that was
already satisfied is a success, because the prosthesis is in the state the orchestrator
asked for.

| Scenario | `status` | `state` | Process action |
| --- | --- | --- | --- |
| `power_on`, prosthesis already on | `success` | `on` | none — command is a no-op |
| `power_on`, was off, started fine | `success` | `on` | control process spawned |
| `power_on`, was off, start failed | `failure` | `off` | spawn failed or the process died immediately |
| `power_off`, prosthesis already off | `success` | `off` | none — command is a no-op |
| `power_off`, was on, stopped fine | `success` | `off` | process(es) killed and confirmed gone |
| `power_off`, was on, stop failed | `failure` | `on` | process survived the kill, or the check could not run |

The invariant the orchestrator can rely on:

> `status == "success"` **if and only if** `state` equals the state the command asked for.

That makes `state` sufficient on its own to drive the UI — `status` is there so error
handling reads naturally.

### Sample payloads

```json
{"timestamp":"2026-08-17T12:00:00.421870000+00:00","command":"power_on","status":"success","state":"on","message":"started RoboticLegControl (sudo pid 4312)"}
{"timestamp":"2026-08-17T12:00:07.118240000+00:00","command":"power_on","status":"success","state":"on","message":"RoboticLegControl is already running"}
{"timestamp":"2026-08-17T12:01:11.903115000+00:00","command":"power_on","status":"failure","state":"off","message":"RoboticLegControl exited right after start with status ExitStatus(unix_wait_status(256))"}
{"timestamp":"2026-08-17T12:04:30.552004000+00:00","command":"power_off","status":"success","state":"off","message":"stopped RoboticLegControl (pids 4312, 4313)"}
{"timestamp":"2026-08-17T12:04:35.007881000+00:00","command":"power_off","status":"success","state":"off","message":"RoboticLegControl is already stopped"}
{"timestamp":"2026-08-17T12:06:02.774390000+00:00","command":"power_off","status":"failure","state":"on","message":"RoboticLegControl is still running after kill (pids 4312): kill for pid 4312 exited with status ExitStatus(unix_wait_status(256))"}
```

### Messages the bridge can send

Useful for log parsing and test fixtures. Again: display only, do not branch on these.

**Success**

- `RoboticLegControl is already running`
- `started RoboticLegControl (sudo pid <pid>)`
- `RoboticLegControl is already stopped`
- `no running RoboticLegControl process found, already stopped`
- `stopped RoboticLegControl (pids <pid>, <pid>)`

**Failure**

- `failed to start RoboticLegControl: <os error>`
- `RoboticLegControl exited right after start with status <status>`
- `failed to check whether RoboticLegControl (sudo pid <pid>) stayed alive: <os error>`
- `failed to run pgrep to locate RoboticLegControl: <os error>`
- `RoboticLegControl is still running after kill (pids <pids>)[: <kill errors>]`
- `could not confirm RoboticLegControl was stopped: <reason>`

## Timing and delivery

- The bridge verifies the outcome before answering: it waits **500 ms** after spawning to
  confirm the process stayed alive, and **500 ms** after killing to confirm the processes
  are gone. Feedback therefore lands roughly **0.5–1.5 s** after the command.
- Commands are handled sequentially on the bridge's single MQTT event loop thread, so
  feedback order always matches command order.
- The bridge is **blocked** while a power command runs. Other commands sent in that window
  are processed right after, not dropped.
- If the bridge is offline, no feedback is published and nothing is queued — the
  orchestrator must fall back to a timeout. **5 s** is a comfortable ceiling.

## What to implement in the orchestrator

1. **Subscribe** to `MQTT_POWER_FEEDBACK_TOPIC` at QoS 1, on the same broker/credentials
   already used for logs and commands.
2. **Parse** the JSON. Ignore unknown fields (room for future additions); treat a missing
   required field or an unknown enum value as malformed and log it rather than crashing.
3. **Track** the prosthesis power state from `state`, and let feedback be the only writer of
   that state — never optimistically flip it when publishing a command.
4. **Await** feedback after publishing `16` / `17`: keep the control pending, resolve it on
   the matching `command`, and time out after ~5 s with a "no response from prosthesis"
   error.
5. **Serialize** power commands — do not issue a new one while another is pending. Feedback
   carries no request id, so matching relies on one command being in flight at a time.
6. **Surface** `message` verbatim on failure. It is the only diagnostic the operator gets
   without SSH access to the Pi.

## Deployment note

`MQTT_POWER_FEEDBACK_TOPIC` must be the same string on both sides. The bridge defaults to
`prothesis/actions/feedback` if the variable is unset. On the Pi it belongs in
`/etc/prothesis-logger-wrapper.env`; the orchestrator should read it from its own
configuration rather than hardcoding it.

## Known gaps — not in this version

Flagging these so the orchestrator design accounts for them rather than assuming they exist.

- **No state query.** The bridge only reports in response to a command. On orchestrator
  startup the prosthesis state is unknown until the first command completes. (The bridge
  does seed its own state at startup by checking for a running control process, so a
  "power on" right after a bridge restart correctly reports `already running`.)
- **No device identifier.** The payload and topic both assume a single prosthesis. Multiple
  devices would need a per-device topic or an added field.
- **No correlation id.** Matching is by `command` field plus one-in-flight discipline.
- **No feedback for gait commands** (`1`–`9`). Those are still fire-and-forget over TCP.

## Bridge-side changes (reference)

| File | Change |
| --- | --- |
| `src/config.rs` | new `power_feedback_topic` field, env `MQTT_POWER_FEEDBACK_TOPIC`, default `prothesis/actions/feedback` |
| `src/models/prosthesis_state.rs` | new — `ProsthesisState::{On, Off}` |
| `src/models/power_feedback.rs` | new — `PowerCommand`, `PowerFeedback`, JSON payload builder |
| `src/mqtt.rs` | in-memory power state seeded from `pgrep`; `start`/`stop` return `Result<String, String>`; feedback published after every power command |
| `.env.example`, `README.md` | document the new variable and the feedback payload |

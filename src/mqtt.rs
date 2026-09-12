use crate::models::log_level::LogLevel;
use crate::models::log::Log;
use crate::models::power_feedback::{PowerCommand, PowerFeedback};
use crate::models::prosthesis_state::ProsthesisState;
use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Power command values received on the commands topic.
const POWER_ON: u32 = 16;
const POWER_OFF: u32 = 17;

/// Gait modes, applied by restarting the control executable with the new mode.
const MIN_GAIT_MODE: u32 = 1;
const MAX_GAIT_MODE: u32 = 9;

/// Actuator currents, applied the same way as a gait mode. The control
/// executable validates the range itself and falls back to its default when a
/// value is outside it, so the bridge only forwards what it knows is usable.
const MIN_ACTUATOR_CURRENT: u32 = 3000;
const MAX_ACTUATOR_CURRENT: u32 = 15000;

/// What the control executable is started with until a gait or actuator
/// current command selects something else. The current matches the default the
/// control executable itself falls back to.
const DEFAULT_GAIT_MODE: u8 = 1;
const DEFAULT_ACTUATOR_CURRENT: u16 = 3000;

/// The MATLAB-generated control executable started/stopped by the power
/// commands. In `ps` its process name shows up truncated as `RoboticLegContr`.
const ROBOTIC_LEG_CONTROL_BIN: &str =
    "/home/labguara/Resultados/MATLAB_ws/R2025b/RoboticLegControl2025b.elf";

/// While it runs, the control executable reports its input buffer on stdout as
/// `Data bytes available <n>, out = 0.`. On a healthy prosthesis the buffer
/// fills up, is read out once it reaches 52 bytes and drops back to 0, over and
/// over. Every value on the way there is normal; a startup that never reaches
/// 52, stalling at some other number instead, is a bad one and has to be
/// restarted.
const HEALTHY_DATA_BYTES: u64 = 52;

/// How long to wait for the control executable to report the buffer at all.
/// Everything it prints before that is initialization (CAN bus, maxon drives),
/// which the startup check has no opinion about.
const STARTUP_STREAM_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the buffer is watched for `HEALTHY_DATA_BYTES` once the control
/// executable started reporting it. This also doubles as the delay before
/// checking that the freshly spawned process is still alive.
const STARTUP_WATCH_WINDOW: Duration = Duration::from_secs(1);

/// How many buffer reports have to be seen before a startup may be called bad,
/// however long they took. The buffer only reaches `HEALTHY_DATA_BYTES` once
/// every few reports, so condemning a startup on a couple of them would fail a
/// healthy prosthesis that simply prints slower than usual.
const STARTUP_MIN_REPORTS: u32 = 20;

/// How many times the control executable is started before giving up on a
/// healthy startup.
const STARTUP_ATTEMPTS: u32 = 3;

/// How long to keep waiting for killed processes to actually go away, and how
/// often to look in the meantime. `sudo kill` only asks the control executable
/// to terminate: it shuts down on its own time and `sudo` only exits once it
/// has, which regularly takes longer than a single short delay.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long `SIGTERM` is given before the same processes are sent `SIGKILL`.
/// Asking is better for the control executable, which writes its `.mat` file on
/// the way out, but a prosthesis that has to stop has to stop: asking is not
/// allowed to be the last word.
const KILL_ESCALATION_DELAY: Duration = Duration::from_secs(3);

/// How long a signalled shutdown waits before exiting, so the MQTT event loop
/// gets to put the last feedback message on the wire first.
const SHUTDOWN_FLUSH_GRACE: Duration = Duration::from_secs(1);

/// Set while a stop signal is being handled. A start that is already in
/// progress watches this and gives up, instead of bringing the prosthesis back
/// up right after the signal killed it.
static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Why a start attempt failed, and whether starting again could do better.
enum StartFailure {
    /// The control loop came up, just not healthily. Another attempt may work.
    Retryable(String),
    /// Another attempt would fail in exactly the same way, because what the
    /// control loop is missing is not something a restart provides.
    Fatal(String),
}

impl StartFailure {
    fn message(&self) -> &str {
        match self {
            StartFailure::Retryable(message) | StartFailure::Fatal(message) => message,
        }
    }

    fn is_fatal(&self) -> bool {
        matches!(self, StartFailure::Fatal(_))
    }
}

/// What the startup check picks out of the control executable's stdout.
enum StartupReport {
    /// The input buffer, from a `Data bytes available <n>, out = 0.` line.
    DataBytes(u64),
    /// A maxon drive that never answered, named by its axis. The control
    /// executable carries on without the axis, so this has to be caught: the
    /// buffer may well reach `HEALTHY_DATA_BYTES` on a prosthesis that cannot
    /// move.
    UnresponsiveDrive(String),
}

/// What the control executable is launched with. The gait and actuator current
/// commands are exactly this: a new value here, followed by a restart.
#[derive(Clone, Copy, Debug)]
struct LaunchSettings {
    gait: u8,
    current: u16,
}

impl LaunchSettings {
    /// The command line arguments these settings amount to.
    fn as_args(self) -> [String; 2] {
        [format!("--gait={}", self.gait), format!("--I={}", self.current)]
    }
}

/// What the bridge holds about the prosthesis. Both the MQTT command handler
/// and the stop signal handler drive the control executable, so this lives
/// behind a mutex: whoever holds it is the one allowed to start or stop it.
#[derive(Clone, Copy, Debug)]
struct ControlState {
    prosthesis: ProsthesisState,
    gait: u8,
    current: u16,
}

impl ControlState {
    /// What a start should launch the control executable with, as the gait and
    /// actuator current commands have left it.
    fn launch(&self) -> LaunchSettings {
        LaunchSettings { gait: self.gait, current: self.current }
    }
}

type SharedControlState = Arc<Mutex<ControlState>>;

/// Take the control state lock, ignoring poisoning. A handler that panicked
/// mid-command leaves the state behind poisoned, and refusing every command
/// from then on is worse for a prosthesis than working with it.
fn lock_control_state(control_state: &Mutex<ControlState>) -> std::sync::MutexGuard<'_, ControlState> {
    control_state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct MqttPublisher {
    sender: mpsc::Sender<String>,
}

impl MqttPublisher {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(MqttConfig::from_env())
    }

    pub fn new(config: MqttConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let username = config.username.clone();
        let password = config.password.clone();
        let use_tls = config.tls;

        eprintln!("[MQTT] Attempting to establish connection to {}:{}", config.host, config.port);
        
        if let Some(ref user) = username {
            eprintln!("[MQTT] Using credentials - username: {}", user);
        }
        
        eprintln!("[MQTT] TLS: {}", use_tls);

        let mut mqtt_options = MqttOptions::new(&config.client_id, &config.host, config.port);
        mqtt_options.set_keep_alive(Duration::from_secs(30));

        if use_tls {
            eprintln!("[MQTT] Configuring TLS");
            mqtt_options.set_transport(Transport::Tls(rumqttc::TlsConfiguration::default()));
        }

        if let Some(user) = username {
            mqtt_options.set_credentials(user, password.unwrap_or_default());
        }

        eprintln!("[MQTT] Creating client...");

        let (client, connection) = Client::new(mqtt_options, 100);
        let (sender, receiver) = mpsc::channel::<String>();
        
        let connected = Arc::new(Mutex::new(false));
        let connected_for_wait = connected.clone();

        let logs_topic = config.logs_topic;
        let commands_topic = config.commands_topic;
        let power_feedback_topic = config.power_feedback_topic;
        let qos = match config.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        };

        // Seed the power state from the processes actually running, so a
        // restart of this bridge does not think the prosthesis is off while
        // the control loop is still up.
        let control_state: SharedControlState = Arc::new(Mutex::new(ControlState {
            prosthesis: match robotic_leg_control_pids() {
                Ok(pids) if !pids.is_empty() => ProsthesisState::On,
                _ => ProsthesisState::Off,
            },
            gait: DEFAULT_GAIT_MODE,
            current: DEFAULT_ACTUATOR_CURRENT,
        }));

        {
            let state = lock_control_state(&control_state);
            eprintln!(
                "[CMD] Initial prosthesis state: {} (gait {}, current {})",
                state.prosthesis.as_str(),
                state.gait,
                state.current
            );
        }

        eprintln!("[SIG] Spawning stop signal handler...");

        thread::spawn({
            let client = client.clone();
            let power_feedback_topic = power_feedback_topic.clone();
            let control_state = control_state.clone();

            move || listen_for_stop_signals(client, power_feedback_topic, qos, control_state)
        });

        eprintln!("[MQTT] Spawning event loop...");

        // Spawn the event loop in a separate thread
        thread::spawn(move || {
            eprintln!("[MQTT] Event loop: connection handler started");

            let mut connection = connection;

            loop {
                // Handle all pending messages from the queue
                while let Ok(message) = receiver.try_recv() {
                    let log = Log {
                        timestamp: chrono::Utc::now(),
                        level: LogLevel::Info,
                        sensor_name: "prosthesis-logger".to_string(),
                        message,
                    };

                    let json_str = log.generate_payload();

                    match client.publish(&logs_topic, qos, false, json_str) {
                        Ok(_) => {},
                        Err(err) => eprintln!("[MQTT] Publish error: {err}"),
                    }
                }

                // Poll for connection events with timeout
                match connection.recv_timeout(Duration::from_secs(2)) {
                    Ok(Ok(rumqttc::Event::Incoming(rumqttc::Incoming::ConnAck(_)))) => {
                        eprintln!("[MQTT] Connected!");
                        let mut state = connected_for_wait.lock().unwrap();
                        *state = true;

                        // Subscribe to commands topic when connected
                        match client.subscribe(&commands_topic, qos) {
                            Ok(_) => eprintln!("[MQTT] Subscribed to commands topic: {}", commands_topic),
                            Err(e) => eprintln!("[MQTT] Subscribe error: {:?}", e),
                        }
                    }
                    Ok(Ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(p)))) => {
                        if p.topic == commands_topic {
                            let payload_str = String::from_utf8_lossy(&p.payload);
                            match payload_str.trim().parse::<u32>() {
                                Ok(POWER_ON) => {
                                    eprintln!("[CMD] Received power ON command. Starting RoboticLegControl...");
                                    run_control_command(
                                        &client,
                                        &power_feedback_topic,
                                        qos,
                                        &control_state,
                                        PowerCommand::On,
                                        |state| {
                                            let launch = state.launch();

                                            start_robotic_leg_control(
                                                &mut state.prosthesis,
                                                launch,
                                            )
                                        },
                                    );
                                }
                                Ok(POWER_OFF) => {
                                    eprintln!("[CMD] Received power OFF command. Stopping RoboticLegControl...");
                                    run_control_command(
                                        &client,
                                        &power_feedback_topic,
                                        qos,
                                        &control_state,
                                        PowerCommand::Off,
                                        |state| stop_robotic_leg_control(&mut state.prosthesis),
                                    );
                                }
                                Ok(value) if (MIN_GAIT_MODE..=MAX_GAIT_MODE).contains(&value) => {
                                    eprintln!("[CMD] Received gait command: {}. Restarting RoboticLegControl...", value);
                                    run_control_command(
                                        &client,
                                        &power_feedback_topic,
                                        qos,
                                        &control_state,
                                        PowerCommand::SetGait,
                                        |state| {
                                            state.gait = value as u8;
                                            let launch = state.launch();

                                            restart_robotic_leg_control(
                                                &mut state.prosthesis,
                                                launch,
                                            )
                                        },
                                    );
                                }
                                Ok(value)
                                    if (MIN_ACTUATOR_CURRENT..=MAX_ACTUATOR_CURRENT)
                                        .contains(&value) =>
                                {
                                    eprintln!("[CMD] Received actuator current command: {}. Restarting RoboticLegControl...", value);
                                    run_control_command(
                                        &client,
                                        &power_feedback_topic,
                                        qos,
                                        &control_state,
                                        PowerCommand::SetActuatorCurrent,
                                        |state| {
                                            state.current = value as u16;
                                            let launch = state.launch();

                                            restart_robotic_leg_control(
                                                &mut state.prosthesis,
                                                launch,
                                            )
                                        },
                                    );
                                }
                                // Restarting the prosthesis on a value nobody can make sense
                                // of is worse than doing nothing, so unknown commands are
                                // only logged.
                                Ok(value) => eprintln!(
                                    "[CMD] Ignoring unknown command {}: expected {}/{} (power), {}-{} (gait) or {}-{} (actuator current)",
                                    value,
                                    POWER_ON,
                                    POWER_OFF,
                                    MIN_GAIT_MODE,
                                    MAX_GAIT_MODE,
                                    MIN_ACTUATOR_CURRENT,
                                    MAX_ACTUATOR_CURRENT,
                                ),
                                Err(e) => eprintln!("[MQTT] Failed to parse command payload {:?} as a number: {:?}", payload_str, e),
                            }
                        }
                    }
                    Ok(Ok(_event)) => {}
                    Ok(Err(e)) => {
                        eprintln!("[MQTT] Connection event error: {:?}", e);
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        eprintln!("[MQTT] Connection lost!");
                        break;
                    }
                }
            }
        });

        // Wait for connection acknowledgement
        eprintln!("[MQTT] Waiting for broker acknowledgement...");
        
        for attempt in 1..=60 {
            thread::sleep(Duration::from_millis(500));
            let is_connected = {
                let state = connected.lock().unwrap();
                *state
            };
            
            if is_connected {
                eprintln!("[MQTT] Successfully connected to broker!");
                return Ok(Self { sender });
            }
            
            if attempt % 6 == 0 {
                eprintln!("[MQTT] Still waiting... ({} seconds)", attempt / 2);
            }
        }

        eprintln!("[MQTT] WARNING: No connection ACK received");
        eprintln!("[MQTT] Proceeding anyway - will queue messages");
        
        Ok(Self { sender })
    }

    pub fn publish(&self, message: &str) {
        if let Err(err) = self.sender.send(message.to_string()) {
            eprintln!("MQTT queue error: {err}");
        }
    }
}

/// Run one command against the control state and answer on the feedback topic
/// with whatever it did. Holding the lock for the whole command is what keeps
/// two of them from driving the control executable at the same time.
fn run_control_command(
    client: &Client,
    topic: &str,
    qos: QoS,
    control_state: &Mutex<ControlState>,
    command: PowerCommand,
    run: impl FnOnce(&mut ControlState) -> Result<String, String>,
) {
    let mut state = lock_control_state(control_state);
    let result = run(&mut state);

    publish_power_feedback(
        client,
        topic,
        qos,
        command,
        state.prosthesis,
        state.launch(),
        result,
    );
}

/// Stop the prosthesis because someone at the Pi asked for it rather than the
/// server, and answer on the feedback topic all the same. `trigger` names what
/// asked for it and `shutdown` says whether the bridge exits afterwards.
///
/// Only the unix signal listener calls this, but it is kept out of the `cfg`
/// gate so it is type-checked on every host the bridge is developed on.
#[cfg_attr(not(unix), allow(dead_code))]
fn stop_prosthesis_out_of_band(
    client: &Client,
    power_feedback_topic: &str,
    qos: QoS,
    control_state: &Mutex<ControlState>,
    trigger: &str,
    shutdown: bool,
) {
    eprintln!("[SIG] {} received. Stopping RoboticLegControl...", trigger);

    // Kill first and take the state lock afterwards. A power or gait command
    // can hold that lock for a while, and the whole point of this is that the
    // prosthesis stops now; the flag is what keeps such a command from
    // starting it up again behind our back.
    STOP_REQUESTED.store(true, Ordering::SeqCst);

    let mut killed = ProsthesisState::On;
    let result = stop_robotic_leg_control(&mut killed)
        .map(|message| format!("stop requested by {}: {}", trigger, message))
        .map_err(|message| format!("stop requested by {}: {}", trigger, message));

    {
        let mut state = lock_control_state(control_state);

        if killed == ProsthesisState::Off {
            state.prosthesis = ProsthesisState::Off;
        }

        publish_power_feedback(
            client,
            power_feedback_topic,
            qos,
            PowerCommand::Off,
            state.prosthesis,
            state.launch(),
            result,
        );
    }

    STOP_REQUESTED.store(false, Ordering::SeqCst);

    if shutdown {
        // Let the event loop put the feedback on the wire before the process
        // goes away under it.
        eprintln!("[SIG] Shutting down on {}", trigger);
        thread::sleep(SHUTDOWN_FLUSH_GRACE);
        std::process::exit(0);
    }
}

/// Stop the prosthesis whenever a stop signal comes in, so an operator at the
/// Pi can take it down without going through the broker. `SIGUSR1` leaves the
/// bridge running, ready to power the prosthesis on again over MQTT; `SIGINT`
/// (Ctrl+C on a terminal) and `SIGTERM` (`systemctl stop`) stop it and exit.
#[cfg(unix)]
fn listen_for_stop_signals(
    client: Client,
    power_feedback_topic: String,
    qos: QoS,
    control_state: SharedControlState,
) {
    use signal_hook::consts::{SIGINT, SIGTERM, SIGUSR1};
    use signal_hook::iterator::Signals;

    let mut signals = match Signals::new([SIGUSR1, SIGINT, SIGTERM]) {
        Ok(signals) => signals,
        Err(e) => {
            eprintln!("[SIG] Could not listen for stop signals: {:?}", e);
            return;
        }
    };

    eprintln!(
        "[SIG] Listening for SIGUSR1 (stop the prosthesis), SIGINT and SIGTERM (stop it and exit)"
    );

    for signal in &mut signals {
        let trigger = match signal {
            SIGUSR1 => "SIGUSR1",
            SIGINT => "SIGINT",
            SIGTERM => "SIGTERM",
            other => {
                eprintln!("[SIG] Ignoring unexpected signal {}", other);
                continue;
            }
        };

        stop_prosthesis_out_of_band(
            &client,
            &power_feedback_topic,
            qos,
            &control_state,
            trigger,
            signal != SIGUSR1,
        );
    }
}

/// Signals are a unix thing; on every other host the bridge simply does not
/// listen for them. The control executable only runs on the Pi anyway.
#[cfg(not(unix))]
fn listen_for_stop_signals(
    _client: Client,
    _power_feedback_topic: String,
    _qos: QoS,
    _control_state: SharedControlState,
) {
    eprintln!("[SIG] Stop signals are only handled on unix; not listening.");
}

/// Publish the outcome of a power, gait or actuator current command on the
/// feedback topic. `Ok` carries the success message, `Err` the failure reason;
/// `state` and `launch` are the power state and the launch settings the bridge
/// holds after running the command.
fn publish_power_feedback(
    client: &Client,
    topic: &str,
    qos: QoS,
    command: PowerCommand,
    state: ProsthesisState,
    launch: LaunchSettings,
    result: Result<String, String>,
) {
    let feedback = match result {
        Ok(message) => {
            eprintln!("[CMD] {} succeeded: {}", command.as_str(), message);
            PowerFeedback::success(command, state, launch.gait, launch.current, message)
        }
        Err(message) => {
            eprintln!("[CMD] {} failed: {}", command.as_str(), message);
            PowerFeedback::failure(command, state, launch.gait, launch.current, message)
        }
    };

    match client.publish(topic, qos, false, feedback.generate_payload()) {
        Ok(_) => {}
        Err(e) => eprintln!("[MQTT] Failed to publish power feedback: {:?}", e),
    }
}

/// PIDs of the running RoboticLegControl process(es), found by command line.
/// The `sudo` parent matches the pattern too, so this usually returns both it
/// and the executable itself.
fn robotic_leg_control_pids() -> Result<Vec<String>, String> {
    let output = std::process::Command::new("pgrep")
        .arg("-f")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .output()
        .map_err(|e| format!("failed to run pgrep to locate RoboticLegControl: {:?}", e))?;

    Ok(std::str::from_utf8(&output.stdout)
        .unwrap_or("")
        .split_whitespace()
        .map(|pid| pid.to_string())
        .collect())
}

/// Launch the RoboticLegControl executable (`sudo <bin> --gait=<n> --I=<n>`) as
/// a detached child and make sure it actually came up: a start that does not
/// report a healthy buffer size is killed and tried again, up to
/// `STARTUP_ATTEMPTS` times. We intentionally do not wait on the control loop
/// itself: it is long-running and is later stopped by
/// `stop_robotic_leg_control`. On success `state` is set to `On`; when every
/// attempt failed it is set to `Off`, since the last attempt is cleaned up
/// before giving up.
fn start_robotic_leg_control(
    state: &mut ProsthesisState,
    launch: LaunchSettings,
) -> Result<String, String> {
    if *state == ProsthesisState::On {
        return Ok("RoboticLegControl is already running".to_string());
    }

    let mut failures: Vec<String> = Vec::new();

    for attempt in 1..=STARTUP_ATTEMPTS {
        // A stop signal beats a start that is on its way up: the signal
        // handler already killed whatever was running, so starting another one
        // here would undo exactly what was asked for.
        if STOP_REQUESTED.load(Ordering::SeqCst) {
            *state = ProsthesisState::Off;

            return Err(format!(
                "start aborted before attempt {}: a stop signal came in",
                attempt
            ));
        }

        match spawn_and_verify_robotic_leg_control(launch) {
            Ok(message) => {
                *state = ProsthesisState::On;

                return Ok(if failures.is_empty() {
                    message
                } else {
                    format!("{} (after {})", message, failures.join("; "))
                });
            }
            Err(failure) => {
                eprintln!(
                    "[CMD] RoboticLegControl startup attempt {}/{} failed: {}",
                    attempt,
                    STARTUP_ATTEMPTS,
                    failure.message()
                );
                failures.push(format!("failed attempt {}: {}", attempt, failure.message()));

                // Whatever is left of the failed attempt has to go before the
                // next one, otherwise two control loops end up fighting over
                // the hardware. This is also what takes the process down when
                // there is not going to be a next one.
                // `stop_robotic_leg_control` returns early when it is told the
                // prosthesis is off, so give it a local `On`.
                let mut leftover = ProsthesisState::On;

                match stop_robotic_leg_control(&mut leftover) {
                    // Logged even when it found nothing to do: a cleanup that
                    // quietly killed nothing is exactly the case worth seeing
                    // in the journal.
                    Ok(stop_message) => eprintln!(
                        "[CMD] Cleaned up after failed startup attempt {}: {}",
                        attempt, stop_message
                    ),
                    Err(stop_error) => {
                        eprintln!(
                            "[CMD] Could not clean up after failed startup attempt {}: {}",
                            attempt, stop_error
                        );
                        failures
                            .push(format!("failed attempt {} cleanup: {}", attempt, stop_error));
                    }
                }

                if failure.is_fatal() {
                    *state = ProsthesisState::Off;

                    return Err(format!(
                        "RoboticLegControl cannot start and starting again would not help: {}",
                        failures.join("; ")
                    ));
                }
            }
        }
    }

    *state = ProsthesisState::Off;

    Err(format!(
        "RoboticLegControl did not start up correctly in {} attempts: {}",
        STARTUP_ATTEMPTS,
        failures.join("; ")
    ))
}

/// Start the control executable once and watch its stdout for what it reports
/// about the hardware. Its output keeps being forwarded to our own stdout for
/// as long as it runs; only the beginning of it is inspected. Fails when the
/// process dies right away, when a maxon drive does not answer, or when the
/// buffer never reaches `HEALTHY_DATA_BYTES`.
fn spawn_and_verify_robotic_leg_control(launch: LaunchSettings) -> Result<String, StartFailure> {
    let mut child = std::process::Command::new("sudo")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .args(launch.as_args())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            StartFailure::Retryable(format!("failed to start RoboticLegControl: {:?}", e))
        })?;

    let pid = child.id();

    let stdout = child.stdout.take().ok_or_else(|| {
        StartFailure::Retryable(format!(
            "RoboticLegControl (sudo pid {}) was started without a stdout pipe",
            pid
        ))
    })?;

    let (reports, reports_receiver) = mpsc::channel::<StartupReport>();
    thread::spawn(move || forward_robotic_leg_control_stdout(stdout, reports));

    eprintln!(
        "[CMD] Watching the startup of RoboticLegControl (sudo pid {}): its buffer has to reach {} bytes",
        pid, HEALTHY_DATA_BYTES
    );

    let startup = watch_startup(&reports_receiver);

    // `spawn` only reports that the process was launched: `sudo` can still exit
    // right away (missing binary, missing permission). The watch window already
    // gave it a moment, so it is enough to look once, and a process that died
    // explains a failed watch better than the watch itself does.
    match child.try_wait() {
        Ok(Some(status)) => {
            return Err(StartFailure::Retryable(format!(
                "RoboticLegControl exited right after start with status {:?}",
                status
            )));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(StartFailure::Retryable(format!(
                "failed to check whether RoboticLegControl (sudo pid {}) stayed alive: {:?}",
                pid, e
            )));
        }
    }

    if let Err(failure) = startup {
        eprintln!("[CMD] {}", failure.message());

        // Take the process down by the pid we already have. The sweep in
        // `start_robotic_leg_control` runs afterwards and is what confirms it
        // is gone, but the takedown itself must not depend on finding a
        // process we already know the pid of.
        for error in signal_control_pids(&[pid.to_string()], "TERM") {
            eprintln!("[CMD] {}", error);
        }

        return Err(failure);
    }

    // Nothing waits on the control loop from here on, so hand the child to a
    // thread that reaps it whenever it eventually exits.
    thread::spawn(move || {
        let _ = child.wait();
    });

    Ok(format!(
        "started RoboticLegControl with gait {} and current {} (sudo pid {}), buffer reaching {} bytes",
        launch.gait, launch.current, pid, HEALTHY_DATA_BYTES
    ))
}

/// Judge a startup from what the control executable reports: healthy as soon as
/// the buffer reaches `HEALTHY_DATA_BYTES`, hopeless the moment a maxon drive
/// turns out not to answer, and bad once the buffer has been reporting other
/// sizes for `STARTUP_WATCH_WINDOW` without getting there. The clock only
/// starts on the first report, so a slow initialization does not count against
/// the prosthesis; not reporting at all within `STARTUP_STREAM_TIMEOUT` is a
/// bad startup of its own.
fn watch_startup(reports: &mpsc::Receiver<StartupReport>) -> Result<(), StartFailure> {
    let stream_deadline = Instant::now() + STARTUP_STREAM_TIMEOUT;
    let mut window_deadline: Option<Instant> = None;
    let mut data_reports: u32 = 0;
    let mut highest_seen: u64 = 0;

    loop {
        // The startup is only called bad once the watch window ran out *and*
        // enough reports back that verdict up. Until then the overall timeout
        // is what limits the wait, so a control loop that prints slowly gets
        // its reports counted instead of being failed on the clock alone.
        let deadline = match window_deadline {
            Some(window) if data_reports >= STARTUP_MIN_REPORTS => window.min(stream_deadline),
            _ => stream_deadline,
        };

        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            break;
        }

        match reports.recv_timeout(remaining) {
            Ok(StartupReport::DataBytes(HEALTHY_DATA_BYTES)) => return Ok(()),
            Ok(StartupReport::DataBytes(size)) => {
                // The buffer is being reported now, so this is where the watch
                // window actually begins.
                window_deadline.get_or_insert_with(|| Instant::now() + STARTUP_WATCH_WINDOW);

                data_reports += 1;
                highest_seen = highest_seen.max(size);
            }
            Ok(StartupReport::UnresponsiveDrive(axis)) => {
                // Actuators that are switched off are not something the bridge
                // fixes by starting the control loop again, so say so and stop
                // here instead of burning the remaining attempts on it.
                return Err(StartFailure::Fatal(format!(
                    "maxon drive on axis {} did not answer, so the prosthesis would run without that axis: its actuators are most likely switched off",
                    axis
                )));
            }
            // Either a deadline ran out or the process closed its stdout. Both
            // are decided by what was seen so far.
            Err(_) => break,
        }
    }

    if data_reports == 0 {
        return Err(StartFailure::Retryable(format!(
            "RoboticLegControl did not report its buffer at all within {}s",
            STARTUP_STREAM_TIMEOUT.as_secs()
        )));
    }

    Err(StartFailure::Retryable(format!(
        "RoboticLegControl buffer never reached {} bytes in {} reports (highest was {})",
        HEALTHY_DATA_BYTES, data_reports, highest_seen
    )))
}

/// Forward the control executable's stdout to our own, line by line, reporting
/// what the startup check cares about on `reports` along the way. The thread
/// lives as long as the process does: once the startup check drops its receiver
/// the sends fail silently and only the forwarding goes on.
fn forward_robotic_leg_control_stdout(
    stdout: std::process::ChildStdout,
    reports: mpsc::Sender<StartupReport>,
) {
    for line in BufReader::new(stdout).lines() {
        match line {
            Ok(line) => {
                println!("{}", line);

                if let Some(size) = parse_data_bytes_available(&line) {
                    let _ = reports.send(StartupReport::DataBytes(size));
                } else if let Some(axis) = parse_unresponsive_drive(&line) {
                    let _ = reports.send(StartupReport::UnresponsiveDrive(axis));
                }
            }
            Err(e) => {
                eprintln!("[CMD] Failed to read RoboticLegControl stdout: {:?}", e);
                break;
            }
        }
    }
}

/// Read the `<n>` out of a `Data bytes available <n>, out = 0.` line. Every
/// other line the control executable prints (its initialization text, for one)
/// yields `None`. The report is looked up anywhere in the line, since the
/// control executable does not always start a line for it.
fn parse_data_bytes_available(line: &str) -> Option<u64> {
    const REPORT_PREFIX: &str = "Data bytes available";

    let start = line.find(REPORT_PREFIX)? + REPORT_PREFIX.len();
    let rest = &line[start..];
    let size = rest.split(',').next().unwrap_or(rest);

    size.trim().parse().ok()
}

/// Read the axis out of a `[maxon] no <n> NAO respondeu.` line, which is how
/// the control executable says a maxon drive never answered on the CAN bus.
/// The axis is only for the message: a line saying a drive did not answer is
/// caught whether or not the axis can be read out of it.
fn parse_unresponsive_drive(line: &str) -> Option<String> {
    const DRIVE_PREFIX: &str = "[maxon] no ";

    if !line.contains("NAO respondeu") && !line.contains("nao respondeu") {
        return None;
    }

    let axis = line
        .find(DRIVE_PREFIX)
        .and_then(|at| line[at + DRIVE_PREFIX.len()..].split_whitespace().next())
        .unwrap_or("?");

    Some(axis.to_string())
}

/// Find the running RoboticLegControl process(es) by command line and kill
/// them. The executable is started via `sudo`, so it runs as root and must be
/// killed with `sudo kill`. On success `state` is set to `Off`; on failure it
/// is left untouched.
fn stop_robotic_leg_control(state: &mut ProsthesisState) -> Result<String, String> {
    if *state == ProsthesisState::Off {
        return Ok("RoboticLegControl is already stopped".to_string());
    }

    let pids = robotic_leg_control_pids()?;

    if pids.is_empty() {
        *state = ProsthesisState::Off;
        return Ok("no running RoboticLegControl process found, already stopped".to_string());
    }

    // A failed `kill` call is only a problem if the process survived it, so the
    // end state is what decides success or failure here, not these errors.
    let mut kill_errors = signal_control_pids(&pids, "TERM");

    // Termination is not instant, so keep looking rather than judging on a
    // single glance. Processes that sit through `SIGTERM` get `SIGKILL`: the
    // point of this function is that the prosthesis ends up stopped.
    let survivors = match wait_for_control_processes_to_exit(KILL_ESCALATION_DELAY)? {
        None => Vec::new(),
        Some(survivors) => {
            eprintln!(
                "[CMD] RoboticLegControl sat through SIGTERM for {}s, sending SIGKILL (pids {})",
                KILL_ESCALATION_DELAY.as_secs(),
                survivors.join(", ")
            );

            kill_errors.extend(signal_control_pids(&survivors, "KILL"));

            wait_for_control_processes_to_exit(STOP_TIMEOUT)?.unwrap_or_default()
        }
    };

    if survivors.is_empty() {
        *state = ProsthesisState::Off;
        return Ok(format!("stopped RoboticLegControl (pids {})", pids.join(", ")));
    }

    Err(format!(
        "RoboticLegControl is still running after SIGTERM and SIGKILL (pids {}){}",
        survivors.join(", "),
        if kill_errors.is_empty() {
            String::new()
        } else {
            format!(": {}", kill_errors.join("; "))
        }
    ))
}

/// Send a signal to each pid with `sudo kill`, since the control executable
/// runs as root. The returned strings are what went wrong, if anything;
/// whether the processes actually went away is decided by looking for them
/// afterwards, not by these exit statuses.
fn signal_control_pids(pids: &[String], signal: &str) -> Vec<String> {
    let mut errors: Vec<String> = Vec::new();

    for pid in pids {
        eprintln!(
            "[CMD] Sending SIG{} to RoboticLegControl process (pid {})",
            signal, pid
        );

        match std::process::Command::new("sudo")
            .arg("kill")
            .arg(format!("-{}", signal))
            .arg(pid)
            .status()
        {
            Ok(status) if status.success() => {}
            Ok(status) => errors.push(format!(
                "kill -{} for pid {} exited with status {:?}",
                signal, pid, status
            )),
            Err(e) => errors.push(format!(
                "failed to send SIG{} to pid {}: {:?}",
                signal, pid, e
            )),
        }
    }

    errors
}

/// Wait for every RoboticLegControl process to go away. `Ok(None)` means they
/// are gone, `Ok(Some(pids))` the ones still standing when `timeout` ran out.
fn wait_for_control_processes_to_exit(
    timeout: Duration,
) -> Result<Option<Vec<String>>, String> {
    let deadline = Instant::now() + timeout;

    loop {
        thread::sleep(STOP_POLL_INTERVAL);

        let remaining = robotic_leg_control_pids()
            .map_err(|e| format!("could not confirm RoboticLegControl was stopped: {}", e))?;

        if remaining.is_empty() {
            return Ok(None);
        }

        if Instant::now() >= deadline {
            return Ok(Some(remaining));
        }
    }
}

/// Apply a gait or actuator current command: stop the control executable and
/// start it again with the requested settings. A failure on the way stops the
/// restart, leaving `state` at whatever the last successful step set it to.
fn restart_robotic_leg_control(
    state: &mut ProsthesisState,
    launch: LaunchSettings,
) -> Result<String, String> {
    let stop_message = stop_robotic_leg_control(state)?;
    let start_message = start_robotic_leg_control(state, launch)?;
    Ok(format!("{}; {}", stop_message, start_message))
}

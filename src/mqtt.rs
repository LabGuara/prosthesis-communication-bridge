use crate::models::power_feedback::{FeedbackOrigin, PowerCommand, PowerFeedback};
use crate::models::prosthesis_state::ProsthesisState;
use crate::models::sensor_batch::SensorBatch;
use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const COMMAND_TYPE_GAIT_MODE: u64 = 1;
const COMMAND_TYPE_POWER: u64 = 2;
const COMMAND_TYPE_ACTUATOR_CURRENT: u64 = 3;

const POWER_ON: u64 = 1;
const POWER_OFF: u64 = 2;

const MIN_GAIT_MODE: u64 = 1;
const MAX_GAIT_MODE: u64 = 9;

const MIN_ACTUATOR_CURRENT: u64 = 3000;
const MAX_ACTUATOR_CURRENT: u64 = 15000;

const DEFAULT_GAIT_MODE: u8 = 1;
const DEFAULT_ACTUATOR_CURRENT: u16 = 3000;

const ROBOTIC_LEG_CONTROL_BIN: &str =
    "/home/labguara/Resultados/MATLAB_ws/R2025b/RoboticLegControl2025b.elf";

const HEALTHY_DATA_BYTES: u64 = 52;

const STARTUP_STREAM_TIMEOUT: Duration = Duration::from_secs(10);

const STARTUP_WATCH_WINDOW: Duration = Duration::from_secs(1);

const STARTUP_STABILIZATION: Duration = Duration::from_secs(3);

const RESTART_DELAY: Duration = Duration::from_secs(1);

const SUPERSEDE_POLL_INTERVAL: Duration = Duration::from_millis(200);

const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(250);

const KILL_ESCALATION_DELAY: Duration = Duration::from_secs(3);

const SHUTDOWN_FLUSH_GRACE: Duration = Duration::from_secs(1);

static COMMAND_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Id (a UUID v4) of the run the prosthesis is in: one start of RoboticLegControl that passed its
/// startup check. `None` while the prosthesis is off or a start is still being checked. Sensor
/// readings are only forwarded while there is a run, so the orchestrator can file everything it
/// receives under the run the last feedback told it about.
static CURRENT_RUN_ID: Mutex<Option<String>> = Mutex::new(None);

fn lock_run_id() -> std::sync::MutexGuard<'static, Option<String>> {
    CURRENT_RUN_ID
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn new_run_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Makes `run_id` the current run, which turns on forwarding of sensor readings.
fn begin_run(run_id: String) {
    eprintln!("[RUN] Started run {}", run_id);
    *lock_run_id() = Some(run_id);
}

/// Called whenever RoboticLegControl is known to be stopped. Readings stop being forwarded.
fn end_run() {
    if let Some(run_id) = lock_run_id().take() {
        eprintln!("[RUN] Ended run {}", run_id);
    }
}

fn current_run_id() -> Option<String> {
    lock_run_id().clone()
}

/// Decides the run a feedback reports, given the run that was going before the command (or stop)
/// and the state it left the prosthesis in:
/// - on, with a run going: that run (it kept running, e.g. a power on while already on);
/// - on, with no run: a start that went through, so a new run. The returned flag says it still has
///   to be begun, which happens only after its feedback is published, so the feedback reaches the
///   broker before any reading of the new run;
/// - off: the run the command ended, or `None` if the prosthesis was already off.
fn feedback_run(run_before: Option<String>, state: ProsthesisState) -> (Option<String>, bool) {
    match (state, current_run_id()) {
        (ProsthesisState::On, Some(run_id)) => (Some(run_id), false),
        (ProsthesisState::On, None) => (Some(new_run_id()), true),
        (ProsthesisState::Off, _) => (run_before, false),
    }
}

fn next_command_generation() -> u64 {
    COMMAND_GENERATION.fetch_add(1, Ordering::SeqCst) + 1
}

fn superseded(generation: u64) -> bool {
    COMMAND_GENERATION.load(Ordering::SeqCst) != generation
}

enum StartFailure {
    Retryable(String),
    Fatal(String),
    Superseded,
}

impl StartFailure {
    fn message(&self) -> &str {
        match self {
            StartFailure::Retryable(message) | StartFailure::Fatal(message) => message,
            StartFailure::Superseded => "a newer command came in before it was done",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ControlCommand {
    PowerOn,
    PowerOff,
    SetGait(u8),
    SetActuatorCurrent(u16),
}

/// Parses a command and the id the orchestrator stored it under, which the feedback echoes.
/// The id is optional, so a command published without it is still carried out.
fn parse_control_command(payload: &str) -> Result<(ControlCommand, Option<u64>), String> {
    let message: serde_json::Value = serde_json::from_str(payload.trim())
        .map_err(|e| format!("not valid JSON: {}", e))?;

    let command_type = read_unsigned_field(&message, "command_type")?;
    let value = read_unsigned_field(&message, "value")?;

    let command_id = match message.get("command_id") {
        None | Some(serde_json::Value::Null) => None,
        Some(id) => Some(
            id.as_u64()
                .ok_or_else(|| "non-integer field: command_id".to_string())?,
        ),
    };

    let command = match command_type {
        COMMAND_TYPE_GAIT_MODE => {
            if (MIN_GAIT_MODE..=MAX_GAIT_MODE).contains(&value) {
                Ok(ControlCommand::SetGait(value as u8))
            } else {
                Err(format!(
                    "gait mode out of range ({}-{}): {}",
                    MIN_GAIT_MODE, MAX_GAIT_MODE, value
                ))
            }
        }
        COMMAND_TYPE_POWER => match value {
            POWER_ON => Ok(ControlCommand::PowerOn),
            POWER_OFF => Ok(ControlCommand::PowerOff),
            other => Err(format!(
                "unknown power value (expected {} or {}): {}",
                POWER_ON, POWER_OFF, other
            )),
        },
        COMMAND_TYPE_ACTUATOR_CURRENT => {
            if (MIN_ACTUATOR_CURRENT..=MAX_ACTUATOR_CURRENT).contains(&value) {
                Ok(ControlCommand::SetActuatorCurrent(value as u16))
            } else {
                Err(format!(
                    "actuator current out of range ({}-{}): {}",
                    MIN_ACTUATOR_CURRENT, MAX_ACTUATOR_CURRENT, value
                ))
            }
        }
        other => Err(format!("unknown command type: {}", other)),
    }?;

    Ok((command, command_id))
}

fn read_unsigned_field(message: &serde_json::Value, key: &str) -> Result<u64, String> {
    message
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("missing or non-integer field: {}", key))
}

enum StartupReport {
    DataBytes(u64),
    UnresponsiveDrive(String),
}

#[derive(Clone, Copy, Debug)]
struct LaunchSettings {
    gait: u8,
    current: u16,
}

impl LaunchSettings {
    fn as_args(self) -> [String; 2] {
        [format!("--gait={}", self.gait), format!("--I={}", self.current)]
    }
}

#[derive(Clone, Copy, Debug)]
struct ControlState {
    prosthesis: ProsthesisState,
    gait: u8,
    current: u16,
}

impl ControlState {
    fn launch(&self) -> LaunchSettings {
        LaunchSettings { gait: self.gait, current: self.current }
    }
}

type SharedControlState = Arc<Mutex<ControlState>>;

fn lock_control_state(control_state: &Mutex<ControlState>) -> std::sync::MutexGuard<'_, ControlState> {
    control_state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct MqttPublisher {
    sender: mpsc::Sender<SensorBatch>,
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
        let (sender, receiver) = mpsc::channel::<SensorBatch>();
        
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

        let control_state: SharedControlState = Arc::new(Mutex::new(ControlState {
            prosthesis: match robotic_leg_control_pids() {
                Ok(pids) if !pids.is_empty() => ProsthesisState::On,
                _ => ProsthesisState::Off,
            },
            gait: DEFAULT_GAIT_MODE,
            current: DEFAULT_ACTUATOR_CURRENT,
        }));

        if lock_control_state(&control_state).prosthesis == ProsthesisState::On {
            // A RoboticLegControl this wrapper did not start (it outlived a restart of the
            // wrapper): it gets a run so its readings keep being forwarded.
            begin_run(new_run_id());
        }

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

        let (commands, commands_receiver) = mpsc::channel::<(u64, ControlCommand, Option<u64>)>();

        eprintln!("[CMD] Spawning command worker...");

        thread::spawn({
            let client = client.clone();
            let power_feedback_topic = power_feedback_topic.clone();
            let control_state = control_state.clone();

            move || {
                run_control_commands(
                    commands_receiver,
                    client,
                    power_feedback_topic,
                    qos,
                    control_state,
                )
            }
        });

        eprintln!("[MQTT] Spawning event loop...");

        thread::spawn(move || {
            eprintln!("[MQTT] Event loop: connection handler started");

            let mut connection = connection;

            loop {
                while let Ok(batch) = receiver.try_recv() {
                    let json_str = batch.generate_payload();

                    match client.publish(&logs_topic, qos, false, json_str) {
                        Ok(_) => {},
                        Err(err) => eprintln!("[MQTT] Publish error: {err}"),
                    }
                }

                match connection.recv_timeout(Duration::from_secs(2)) {
                    Ok(Ok(rumqttc::Event::Incoming(rumqttc::Incoming::ConnAck(_)))) => {
                        eprintln!("[MQTT] Connected!");
                        let mut state = connected_for_wait.lock().unwrap();
                        *state = true;

                        match client.subscribe(&commands_topic, qos) {
                            Ok(_) => eprintln!("[MQTT] Subscribed to commands topic: {}", commands_topic),
                            Err(e) => eprintln!("[MQTT] Subscribe error: {:?}", e),
                        }
                    }
                    Ok(Ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(p)))) => {
                        if p.topic == commands_topic {
                            let payload_str = String::from_utf8_lossy(&p.payload);

                            let command = match parse_control_command(&payload_str) {
                                Ok(command) => Some(command),
                                Err(reason) => {
                                    eprintln!("[CMD] Ignoring command {:?}: {}", payload_str, reason);
                                    None
                                }
                            };

                            if let Some((command, command_id)) = command {
                                let generation = next_command_generation();

                                eprintln!(
                                    "[CMD] Received {:?} (command #{}, id {:?})",
                                    command, generation, command_id
                                );

                                if let Err(e) = commands.send((generation, command, command_id)) {
                                    eprintln!("[CMD] Command worker is gone, dropping {:?}: {:?}", command, e);
                                }
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

    /// Queues a batch of readings, stamped with the time it arrived. Batches that arrive while
    /// there is no run (the prosthesis is off, or a start is still being checked or failed) are
    /// dropped.
    pub fn publish(&self, message: &str) {
        if current_run_id().is_none() {
            return;
        }

        let batch = SensorBatch::new(chrono::Utc::now(), message);

        if let Err(err) = self.sender.send(batch) {
            eprintln!("MQTT queue error: {err}");
        }
    }
}

fn run_control_commands(
    commands: mpsc::Receiver<(u64, ControlCommand, Option<u64>)>,
    client: Client,
    power_feedback_topic: String,
    qos: QoS,
    control_state: SharedControlState,
) {
    for (generation, command, command_id) in commands {
        let feedback_command = match command {
            ControlCommand::PowerOn => PowerCommand::On,
            ControlCommand::PowerOff => PowerCommand::Off,
            ControlCommand::SetGait(_) => PowerCommand::SetGait,
            ControlCommand::SetActuatorCurrent(_) => PowerCommand::SetActuatorCurrent,
        };

        eprintln!("[CMD] Running {:?} (command #{})", command, generation);

        run_control_command(
            &client,
            &power_feedback_topic,
            qos,
            &control_state,
            feedback_command,
            command_id,
            |state| match command {
                ControlCommand::PowerOn => {
                    let launch = state.launch();
                    start_robotic_leg_control(&mut state.prosthesis, launch, generation)
                }
                ControlCommand::PowerOff => stop_robotic_leg_control(&mut state.prosthesis),
                ControlCommand::SetGait(gait) => {
                    state.gait = gait;
                    let launch = state.launch();
                    restart_robotic_leg_control(&mut state.prosthesis, launch, generation)
                }
                ControlCommand::SetActuatorCurrent(current) => {
                    state.current = current;
                    let launch = state.launch();
                    restart_robotic_leg_control(&mut state.prosthesis, launch, generation)
                }
            },
        );
    }

    eprintln!("[CMD] Command queue closed, command worker stopping");
}

fn run_control_command(
    client: &Client,
    topic: &str,
    qos: QoS,
    control_state: &Mutex<ControlState>,
    command: PowerCommand,
    command_id: Option<u64>,
    run: impl FnOnce(&mut ControlState) -> Result<String, String>,
) {
    let mut state = lock_control_state(control_state);
    let run_before = current_run_id();
    let result = run(&mut state);
    let (run_id, started) = feedback_run(run_before, state.prosthesis);

    publish_power_feedback(
        client,
        topic,
        qos,
        command,
        command_id,
        FeedbackOrigin::Remote,
        state.prosthesis,
        state.launch(),
        run_id.clone(),
        result,
    );

    if started {
        if let Some(run_id) = run_id {
            begin_run(run_id);
        }
    }
}

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

    next_command_generation();

    let run_before = current_run_id();
    let mut killed = ProsthesisState::On;
    let result = stop_robotic_leg_control(&mut killed)
        .map(|message| format!("stop requested by {}: {}", trigger, message))
        .map_err(|message| format!("stop requested by {}: {}", trigger, message));

    {
        let mut state = lock_control_state(control_state);

        if killed == ProsthesisState::Off {
            state.prosthesis = ProsthesisState::Off;
        }

        // A stop never starts a run, so there is nothing to begin afterwards.
        let (run_id, _) = feedback_run(run_before, state.prosthesis);

        publish_power_feedback(
            client,
            power_feedback_topic,
            qos,
            PowerCommand::Off,
            None,
            FeedbackOrigin::Internal,
            state.prosthesis,
            state.launch(),
            run_id,
            result,
        );
    }

    if shutdown {
        eprintln!("[SIG] Shutting down on {}", trigger);
        thread::sleep(SHUTDOWN_FLUSH_GRACE);
        std::process::exit(0);
    }
}

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

#[cfg(not(unix))]
fn listen_for_stop_signals(
    _client: Client,
    _power_feedback_topic: String,
    _qos: QoS,
    _control_state: SharedControlState,
) {
    eprintln!("[SIG] Stop signals are only handled on unix; not listening.");
}

#[allow(clippy::too_many_arguments)]
fn publish_power_feedback(
    client: &Client,
    topic: &str,
    qos: QoS,
    command: PowerCommand,
    command_id: Option<u64>,
    origin: FeedbackOrigin,
    state: ProsthesisState,
    launch: LaunchSettings,
    run_id: Option<String>,
    result: Result<String, String>,
) {
    let (success, message) = match result {
        Ok(message) => {
            eprintln!("[CMD] {} succeeded: {}", command.as_str(), message);
            (true, message)
        }
        Err(message) => {
            eprintln!("[CMD] {} failed: {}", command.as_str(), message);
            (false, message)
        }
    };

    let feedback = PowerFeedback {
        timestamp: chrono::Utc::now(),
        command,
        command_id,
        origin,
        success,
        state,
        gait: launch.gait,
        current: launch.current,
        run_id,
        message,
    };

    match client.publish(topic, qos, false, feedback.generate_payload()) {
        Ok(_) => {}
        Err(e) => eprintln!("[MQTT] Failed to publish power feedback: {:?}", e),
    }
}

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

fn start_robotic_leg_control(
    state: &mut ProsthesisState,
    launch: LaunchSettings,
    generation: u64,
) -> Result<String, String> {
    if *state == ProsthesisState::On {
        return Ok("RoboticLegControl is already running".to_string());
    }

    let mut attempt: u32 = 0;

    loop {
        attempt += 1;

        if superseded(generation) {
            *state = ProsthesisState::Off;
            end_run();

            return Err(format!(
                "start abandoned before attempt {}: a newer command came in",
                attempt
            ));
        }

        let failure = match spawn_and_verify_robotic_leg_control(launch, generation) {
            Ok(message) => {
                *state = ProsthesisState::On;

                return Ok(message);
            }
            Err(failure) => failure,
        };

        eprintln!(
            "[CMD] RoboticLegControl startup attempt {} failed: {}",
            attempt,
            failure.message()
        );

        let mut leftover = ProsthesisState::On;

        match stop_robotic_leg_control(&mut leftover) {
            Ok(stop_message) => eprintln!(
                "[CMD] Cleaned up after failed startup attempt {}: {}",
                attempt, stop_message
            ),
            Err(stop_error) => eprintln!(
                "[CMD] Could not clean up after failed startup attempt {}: {}",
                attempt, stop_error
            ),
        }

        match failure {
            StartFailure::Fatal(_) => {
                *state = ProsthesisState::Off;
                end_run();

                return Err(format!("RoboticLegControl cannot start"));
            }
            StartFailure::Superseded => {
                *state = ProsthesisState::Off;
                end_run();
                return Err(format!("Startup retried"));
            }
            StartFailure::Retryable(_) => {}
        }

        eprintln!(
            "[CMD] Restarting RoboticLegControl in {}ms (attempt {} next)",
            RESTART_DELAY.as_millis(),
            attempt + 1
        );

        sleep_unless_superseded(RESTART_DELAY, generation);
    }
}

fn sleep_unless_superseded(duration: Duration, generation: u64) {
    let deadline = Instant::now() + duration;

    while !superseded(generation) {
        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            return;
        }

        thread::sleep(remaining.min(SUPERSEDE_POLL_INTERVAL));
    }
}

fn spawn_and_verify_robotic_leg_control(
    launch: LaunchSettings,
    generation: u64,
) -> Result<String, StartFailure> {
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
        "[CMD] Watching the startup of RoboticLegControl (sudo pid {}): its buffer has to keep cycling {} -> 0 for {}s",
        pid,
        HEALTHY_DATA_BYTES,
        STARTUP_STABILIZATION.as_secs()
    );

    let startup = watch_startup(&reports_receiver, generation);

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

        for error in signal_control_pids(&[pid.to_string()], "TERM") {
            eprintln!("[CMD] {}", error);
        }

        return Err(failure);
    }

    thread::spawn(move || {
        let _ = child.wait();
    });

    Ok(format!(
        "started RoboticLegControl with gait {} and current {} (sudo pid {})",
        launch.gait,
        launch.current,
        pid
    ))
}

fn watch_startup(
    reports: &mpsc::Receiver<StartupReport>,
    generation: u64,
) -> Result<(), StartFailure> {
    let mut deadline = Instant::now() + STARTUP_STREAM_TIMEOUT;
    let mut stable_since: Option<Instant> = None;
    let mut previous: Option<u64> = None;
    let mut data_reports: u32 = 0;
    let mut cycles: u32 = 0;

    loop {
        if superseded(generation) {
            return Err(StartFailure::Superseded);
        }

        let now = Instant::now();

        if now >= deadline {
            break;
        }

        if stable_since.is_some_and(|since| now.duration_since(since) >= STARTUP_STABILIZATION) {
            return Ok(());
        }

        let wait = deadline
            .saturating_duration_since(now)
            .min(SUPERSEDE_POLL_INTERVAL);

        match reports.recv_timeout(wait) {
            Ok(StartupReport::DataBytes(size)) => {
                data_reports += 1;

                if data_reports == 1 {
                    deadline = Instant::now() + STARTUP_WATCH_WINDOW;
                }

                if size > HEALTHY_DATA_BYTES {
                    return Err(StartFailure::Retryable(format!("RoboticLegControl buffer desynchronized")));
                }

                if previous == Some(HEALTHY_DATA_BYTES) {
                    if size != 0 {
                        return Err(StartFailure::Retryable(format!("RoboticLegControl buffer desynchronized")));
                    }

                    cycles += 1;
                    stable_since.get_or_insert_with(Instant::now);
                    deadline = Instant::now() + STARTUP_WATCH_WINDOW;
                }

                previous = Some(size);
            }
            Ok(StartupReport::UnresponsiveDrive(axis)) => {
                return Err(StartFailure::Fatal(format!(
                    "maxon drive on axis {} did not answer: its actuators are most likely switched off",
                    axis
                )));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    if data_reports == 0 {
        return Err(StartFailure::Retryable(format!(
            "RoboticLegControl did not report its buffer at all within {}s",
            STARTUP_STREAM_TIMEOUT.as_secs()
        )));
    }

    if cycles == 0 {
        return Err(StartFailure::Retryable(format!(
            "RoboticLegControl buffer never completed a {} -> 0 cycle in {} reports",
            HEALTHY_DATA_BYTES, data_reports
        )));
    }

    Err(StartFailure::Retryable(format!(
        "RoboticLegControl buffer completed {} {} -> 0 cycles, then went {}ms without one",
        cycles,
        HEALTHY_DATA_BYTES,
        STARTUP_WATCH_WINDOW.as_millis()
    )))
}

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

fn parse_data_bytes_available(line: &str) -> Option<u64> {
    const REPORT_PREFIX: &str = "Data bytes available";

    let start = line.find(REPORT_PREFIX)? + REPORT_PREFIX.len();
    let rest = &line[start..];
    let size = rest.split(',').next().unwrap_or(rest);

    size.trim().parse().ok()
}

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

fn stop_robotic_leg_control(state: &mut ProsthesisState) -> Result<String, String> {
    if *state == ProsthesisState::Off {
        end_run();
        return Ok("RoboticLegControl is already stopped".to_string());
    }

    let pids = robotic_leg_control_pids()?;

    if pids.is_empty() {
        *state = ProsthesisState::Off;
        end_run();
        return Ok("no running RoboticLegControl process found, already stopped".to_string());
    }

    let mut kill_errors = signal_control_pids(&pids, "TERM");

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
        end_run();
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

fn restart_robotic_leg_control(
    state: &mut ProsthesisState,
    launch: LaunchSettings,
    generation: u64,
) -> Result<String, String> {
    let stop_message = stop_robotic_leg_control(state)?;
    let start_message = start_robotic_leg_control(state, launch, generation)?;
    Ok(format!("{}; {}", stop_message, start_message))
}

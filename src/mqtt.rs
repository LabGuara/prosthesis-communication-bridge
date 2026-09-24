use crate::models::log_level::LogLevel;
use crate::models::log::Log;
use crate::models::power_feedback::{PowerCommand, PowerFeedback};
use crate::models::prosthesis_state::ProsthesisState;
use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const POWER_ON: u32 = 16;
const POWER_OFF: u32 = 17;

const MIN_GAIT_MODE: u32 = 1;
const MAX_GAIT_MODE: u32 = 9;

const MIN_ACTUATOR_CURRENT: u32 = 3000;
const MAX_ACTUATOR_CURRENT: u32 = 15000;

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

        let (commands, commands_receiver) = mpsc::channel::<(u64, ControlCommand)>();

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

                            let command = match payload_str.trim().parse::<u32>() {
                                Ok(POWER_ON) => Some(ControlCommand::PowerOn),
                                Ok(POWER_OFF) => Some(ControlCommand::PowerOff),
                                Ok(value) if (MIN_GAIT_MODE..=MAX_GAIT_MODE).contains(&value) => {
                                    Some(ControlCommand::SetGait(value as u8))
                                }
                                Ok(value)
                                    if (MIN_ACTUATOR_CURRENT..=MAX_ACTUATOR_CURRENT)
                                        .contains(&value) =>
                                {
                                    Some(ControlCommand::SetActuatorCurrent(value as u16))
                                }
                                Ok(value) => {
                                    eprintln!("[CMD] Ignoring unknown command {}", value);
                                    None
                                }
                                Err(_) => {
                                    eprintln!("[MQTT] Failed to parse command payload {:?} as a number", payload_str);
                                    None
                                }
                            };

                            if let Some(command) = command {
                                let generation = next_command_generation();

                                eprintln!("[CMD] Received {:?} (command #{})", command, generation);

                                if let Err(e) = commands.send((generation, command)) {
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

    pub fn publish(&self, message: &str) {
        if let Err(err) = self.sender.send(message.to_string()) {
            eprintln!("MQTT queue error: {err}");
        }
    }
}

fn run_control_commands(
    commands: mpsc::Receiver<(u64, ControlCommand)>,
    client: Client,
    power_feedback_topic: String,
    qos: QoS,
    control_state: SharedControlState,
) {
    for (generation, command) in commands {
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

    let mut failed_attempts: u32 = 0;
    let mut last_failure = String::new();
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;

        if superseded(generation) {
            *state = ProsthesisState::Off;

            return Err(format!(
                "start abandoned before attempt {}: a newer command came in",
                attempt
            ));
        }

        let failure = match spawn_and_verify_robotic_leg_control(launch, generation) {
            Ok(message) => {
                *state = ProsthesisState::On;

                return Ok(if failed_attempts == 0 {
                    message
                } else {
                    format!(
                        "{} (after {} failed attempts; the last one: {})",
                        message, failed_attempts, last_failure
                    )
                });
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
            StartFailure::Fatal(message) => {
                *state = ProsthesisState::Off;

                return Err(format!(
                    "RoboticLegControl cannot start and starting again would not help: {}",
                    message
                ));
            }
            StartFailure::Superseded => {
                *state = ProsthesisState::Off;

                return Err(format!(
                    "start abandoned during attempt {}: a newer command came in",
                    attempt
                ));
            }
            StartFailure::Retryable(message) => {
                failed_attempts += 1;
                last_failure = message;
            }
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
        return Ok("RoboticLegControl is already stopped".to_string());
    }

    let pids = robotic_leg_control_pids()?;

    if pids.is_empty() {
        *state = ProsthesisState::Off;
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

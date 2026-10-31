use crate::models::power_feedback::{FeedbackOrigin, PowerCommand, PowerFeedback};
use crate::models::prosthesis_state::ProsthesisState;
use crate::models::sensor_batch::SensorBatch;
use crate::config::MqttConfig;
use crate::prosthesis::{LaunchSettings, Prosthesis};
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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

const SHUTDOWN_FLUSH_GRACE: Duration = Duration::from_secs(1);

const EVENT_LOOP_WAIT: Duration = Duration::from_secs(2);

static COMMAND_GENERATION: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
struct Run {
    id: String,
    collect_id: Option<String>,
}

static CURRENT_RUN: Mutex<Option<Run>> = Mutex::new(None);

fn lock_run() -> std::sync::MutexGuard<'static, Option<Run>> {
    CURRENT_RUN
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn new_run(collect_id: Option<String>) -> Run {
    Run { id: uuid::Uuid::new_v4().to_string(), collect_id }
}

fn begin_run(run: Run) {
    eprintln!("[RUN] Started run {} (collect {:?})", run.id, run.collect_id);
    *lock_run() = Some(run);
}

fn end_run() {
    if let Some(run) = lock_run().take() {
        eprintln!("[RUN] Ended run {}", run.id);
    }
}

fn current_run() -> Option<Run> {
    lock_run().clone()
}

fn feedback_run(
    run_before: Option<Run>,
    state: ProsthesisState,
    collect_id: Option<String>,
) -> (Option<Run>, bool) {
    match (state, current_run()) {
        (ProsthesisState::On, Some(run)) => (Some(run), false),
        (ProsthesisState::On, None) => (Some(new_run(collect_id)), true),
        (ProsthesisState::Off, _) => (run_before, false),
    }
}

fn next_command_generation() -> u64 {
    COMMAND_GENERATION.fetch_add(1, Ordering::SeqCst) + 1
}

fn superseded(generation: u64) -> bool {
    COMMAND_GENERATION.load(Ordering::SeqCst) != generation
}

#[derive(Clone, Copy, Debug)]
enum ControlCommand {
    PowerOn,
    PowerOff,
    SetGait(u8),
    SetActuatorCurrent(u16),
}

fn parse_control_command(payload: &str) -> Result<(ControlCommand, Option<u64>, Option<String>), String> {
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

    Ok((command, command_id, parse_collect_id(&message)))
}

fn parse_collect_id(message: &serde_json::Value) -> Option<String> {
    match message.get("collect_id") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => match value.as_str().and_then(|id| uuid::Uuid::parse_str(id.trim()).ok()) {
            Some(id) => Some(id.to_string()),
            None => {
                eprintln!("[CMD] Ignoring malformed collect_id {}; the command is kept", value);
                None
            }
        },
    }
}

fn read_unsigned_field(message: &serde_json::Value, key: &str) -> Result<u64, String> {
    message
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("missing or non-integer field: {}", key))
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

fn start_prosthesis(
    prosthesis: &dyn Prosthesis,
    state: &mut ProsthesisState,
    launch: LaunchSettings,
    generation: u64,
) -> Result<String, String> {
    if *state == ProsthesisState::On {
        return Ok(format!("{} is already running", prosthesis.name()));
    }

    let result = prosthesis.start(launch, &|| superseded(generation));

    if result.is_ok() {
        *state = ProsthesisState::On;
    } else {
        *state = ProsthesisState::Off;
        end_run();
    }

    result
}

fn stop_prosthesis(prosthesis: &dyn Prosthesis, state: &mut ProsthesisState) -> Result<String, String> {
    if *state == ProsthesisState::Off {
        end_run();
        return Ok(format!("{} is already stopped", prosthesis.name()));
    }

    let result = prosthesis.stop();

    if result.is_ok() {
        *state = ProsthesisState::Off;
        end_run();
    }

    result
}

fn restart_prosthesis(
    prosthesis: &dyn Prosthesis,
    state: &mut ProsthesisState,
    launch: LaunchSettings,
    generation: u64,
) -> Result<String, String> {
    let stop_message = stop_prosthesis(prosthesis, state)?;
    let start_message = start_prosthesis(prosthesis, state, launch, generation)?;
    Ok(format!("{}; {}", stop_message, start_message))
}

pub struct MqttPublisher {
    sender: mpsc::Sender<SensorBatch>,
}

impl MqttPublisher {
    pub fn from_env(prosthesis: Arc<dyn Prosthesis>) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(MqttConfig::from_env(), prosthesis)
    }

    pub fn new(
        config: MqttConfig,
        prosthesis: Arc<dyn Prosthesis>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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
            prosthesis: if prosthesis.is_running() {
                ProsthesisState::On
            } else {
                ProsthesisState::Off
            },
            gait: DEFAULT_GAIT_MODE,
            current: DEFAULT_ACTUATOR_CURRENT,
        }));

        if lock_control_state(&control_state).prosthesis == ProsthesisState::On {
            begin_run(new_run(None));
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
            let prosthesis = prosthesis.clone();

            move || {
                listen_for_stop_signals(client, power_feedback_topic, qos, control_state, prosthesis)
            }
        });

        let (commands, commands_receiver) =
            mpsc::channel::<(u64, ControlCommand, Option<u64>, Option<String>)>();

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
                    prosthesis,
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

                match connection.recv_timeout(EVENT_LOOP_WAIT) {
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

                            if let Some((command, command_id, collect_id)) = command {
                                let generation = next_command_generation();

                                eprintln!(
                                    "[CMD] Received {:?} (command #{}, id {:?}, collect {:?})",
                                    command, generation, command_id, collect_id
                                );

                                if let Err(e) = commands.send((generation, command, command_id, collect_id)) {
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
        if current_run().is_none() {
            return;
        }

        let batch = SensorBatch::new(chrono::Utc::now(), message);

        if let Err(err) = self.sender.send(batch) {
            eprintln!("MQTT queue error: {err}");
        }
    }
}

fn run_control_commands(
    commands: mpsc::Receiver<(u64, ControlCommand, Option<u64>, Option<String>)>,
    client: Client,
    power_feedback_topic: String,
    qos: QoS,
    control_state: SharedControlState,
    prosthesis: Arc<dyn Prosthesis>,
) {
    let prosthesis = prosthesis.as_ref();

    for (generation, command, command_id, collect_id) in commands {
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
            collect_id,
            |state| match command {
                ControlCommand::PowerOn => {
                    let launch = state.launch();
                    start_prosthesis(prosthesis, &mut state.prosthesis, launch, generation)
                }
                ControlCommand::PowerOff => stop_prosthesis(prosthesis, &mut state.prosthesis),
                ControlCommand::SetGait(gait) => {
                    state.gait = gait;
                    let launch = state.launch();
                    restart_prosthesis(prosthesis, &mut state.prosthesis, launch, generation)
                }
                ControlCommand::SetActuatorCurrent(current) => {
                    state.current = current;
                    let launch = state.launch();
                    restart_prosthesis(prosthesis, &mut state.prosthesis, launch, generation)
                }
            },
        );
    }

    eprintln!("[CMD] Command queue closed, command worker stopping");
}

#[allow(clippy::too_many_arguments)]
fn run_control_command(
    client: &Client,
    topic: &str,
    qos: QoS,
    control_state: &Mutex<ControlState>,
    command: PowerCommand,
    command_id: Option<u64>,
    collect_id: Option<String>,
    run: impl FnOnce(&mut ControlState) -> Result<String, String>,
) {
    let mut state = lock_control_state(control_state);
    let run_before = current_run();
    let result = run(&mut state);
    let (run_after, started) = feedback_run(run_before, state.prosthesis, collect_id);

    publish_power_feedback(
        client,
        topic,
        qos,
        command,
        command_id,
        FeedbackOrigin::Remote,
        state.prosthesis,
        state.launch(),
        run_after.clone(),
        result,
    );

    if started {
        if let Some(run_after) = run_after {
            begin_run(run_after);
        }
    }
}

#[cfg_attr(not(unix), allow(dead_code))]
fn stop_prosthesis_out_of_band(
    client: &Client,
    power_feedback_topic: &str,
    qos: QoS,
    control_state: &Mutex<ControlState>,
    prosthesis: &dyn Prosthesis,
    trigger: &str,
    shutdown: bool,
) {
    eprintln!("[SIG] {} received. Stopping {}...", trigger, prosthesis.name());

    next_command_generation();

    let run_before = current_run();
    let mut killed = ProsthesisState::On;
    let result = stop_prosthesis(prosthesis, &mut killed)
        .map(|message| format!("stop requested by {}: {}", trigger, message))
        .map_err(|message| format!("stop requested by {}: {}", trigger, message));

    {
        let mut state = lock_control_state(control_state);

        if killed == ProsthesisState::Off {
            state.prosthesis = ProsthesisState::Off;
        }

        let (run, _) = feedback_run(run_before, state.prosthesis, None);

        publish_power_feedback(
            client,
            power_feedback_topic,
            qos,
            PowerCommand::Off,
            None,
            FeedbackOrigin::Internal,
            state.prosthesis,
            state.launch(),
            run,
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
    prosthesis: Arc<dyn Prosthesis>,
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
            prosthesis.as_ref(),
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
    _prosthesis: Arc<dyn Prosthesis>,
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
    run: Option<Run>,
    result: Result<String, String>,
) {
    let (run_id, collect_id) = match run {
        Some(run) => (Some(run.id), run.collect_id),
        None => (None, None),
    };

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
        collect_id,
        message,
    };

    match client.publish(topic, qos, false, feedback.generate_payload()) {
        Ok(_) => {}
        Err(e) => eprintln!("[MQTT] Failed to publish power feedback: {:?}", e),
    }
}

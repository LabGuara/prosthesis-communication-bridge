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

/// How long to wait for the control executable to report the buffer at all.
/// Everything it prints before that is initialization (CAN bus, maxon drives),
/// which the startup check has no opinion about.
const STARTUP_STREAM_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the buffer may go without completing a 52 → 0 cycle, counted from
/// the first report and then again from every cycle. A working prosthesis
/// completes one every few reports, a few dozen milliseconds apart.
const STARTUP_WATCH_WINDOW: Duration = Duration::from_secs(1);

/// How long the 52 → 0 cycle has to keep going, without a single report
/// breaking it, before a start counts as healthy and gets its success feedback.
const STARTUP_STABILIZATION: Duration = Duration::from_secs(3);

/// Pause between a failed start and the next one, so the drives get a moment
/// before being brought up again.
const RESTART_DELAY: Duration = Duration::from_secs(1);

/// How often a start in progress checks whether a newer command came in.
const SUPERSEDE_POLL_INTERVAL: Duration = Duration::from_millis(200);

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

/// Bumped for every command, whether it came over MQTT or as a stop signal. A
/// start remembers the value it began under and gives up as soon as it changes:
/// whatever came in since is what the operator wants now. A start retries
/// until the prosthesis works, so without this nothing could ever get past one.
static COMMAND_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Supersede whatever is still in progress, and hand out the generation the
/// new command runs under.
fn next_command_generation() -> u64 {
    COMMAND_GENERATION.fetch_add(1, Ordering::SeqCst) + 1
}

/// Whether a newer command came in since `generation` was handed out.
fn superseded(generation: u64) -> bool {
    COMMAND_GENERATION.load(Ordering::SeqCst) != generation
}

/// Why a start attempt failed, and what that means for the next one.
enum StartFailure {
    /// The control loop came up, just not healthily. Another attempt may work.
    Retryable(String),
    /// Another attempt would fail in exactly the same way, because what the
    /// control loop is missing is not something a restart provides.
    Fatal(String),
    /// A newer command came in, so there is no point in another attempt.
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

        // Commands run on a thread of their own. A start retries until the
        // prosthesis works, which can take any amount of time, and the MQTT
        // connection only makes progress while the event loop below is polled:
        // running commands there would stall it, and a power off could never
        // arrive to stop a start that keeps failing.
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
                                // Restarting the prosthesis on a value nobody can make sense
                                // of is worse than doing nothing, so unknown commands are
                                // only logged.
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
                                // Supersede whatever is still in progress first, so a
                                // start that keeps retrying lets go and the worker gets
                                // to this command.
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

/// Run the commands queued by the MQTT event loop, one at a time and in the
/// order they came in, answering each on the feedback topic. `generation` is
/// what lets a newer command cut short a start that is still retrying.
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

    // Kill first and take the state lock afterwards. A start that keeps
    // retrying holds that lock for as long as it takes, and the whole point of
    // this is that the prosthesis stops now. Superseding it is what makes it
    // let go instead of starting the prosthesis up again behind our back.
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
/// a detached child and keep at it until it comes up stable: a start whose
/// buffer does not keep reaching `HEALTHY_DATA_BYTES` is killed and tried
/// again, for as long as it takes. Only two things end that early: the
/// actuators turning out to be off, which no restart fixes, and a newer command
/// coming in (`generation`), which is what the operator wants now instead.
///
/// We intentionally do not wait on the control loop itself: it is long-running
/// and is later stopped by `stop_robotic_leg_control`. On success `state` is
/// set to `On`; when the start ends without success it is set to `Off`, since
/// every failed attempt is cleaned up.
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

        // A newer command beats a start that is on its way up. For a stop
        // signal in particular, whatever was running is already killed, so
        // starting another one here would undo exactly what was asked for.
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

        // Whatever is left of the failed attempt has to go before the next
        // one, otherwise two control loops end up fighting over the hardware.
        // This is also what takes the process down when there is not going to
        // be a next one. `stop_robotic_leg_control` returns early when it is
        // told the prosthesis is off, so give it a local `On`.
        let mut leftover = ProsthesisState::On;

        match stop_robotic_leg_control(&mut leftover) {
            // Logged even when it found nothing to do: a cleanup that quietly
            // killed nothing is exactly the case worth seeing in the journal.
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

/// Sleep for `duration`, waking early if a newer command comes in, so a start
/// that is between attempts lets go of the prosthesis without delay.
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

/// Start the control executable once and watch its stdout for what it reports
/// about the hardware. Its output keeps being forwarded to our own stdout for
/// as long as it runs; only the beginning of it is inspected. Fails when the
/// process dies right away, when a maxon drive does not answer, or when the
/// buffer never reaches `HEALTHY_DATA_BYTES`.
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
        "started RoboticLegControl with gait {} and current {} (sudo pid {})",
        launch.gait,
        launch.current,
        pid
    ))
}

/// Judge a startup by the cycle that says the prosthesis is working: the buffer
/// fills up to exactly `HEALTHY_DATA_BYTES`, and the very next report is 0.
/// That cycle has to keep going for `STARTUP_STABILIZATION`, never going
/// `STARTUP_WATCH_WINDOW` without completing, before the start counts as
/// healthy. A single report that breaks it fails the start on the spot: a
/// buffer past 52 (it overflowed instead of being read out), or a 52 followed
/// by anything but 0 (it reached 52 and was still not read out).
///
/// Also fails the moment a maxon drive turns out not to answer, and gives up as
/// soon as a newer command than `generation` comes in. The clock only starts on
/// the first report, so the CAN and maxon initialization before it does not
/// count against the prosthesis; reporting nothing at all within
/// `STARTUP_STREAM_TIMEOUT` is a bad startup of its own.
fn watch_startup(
    reports: &mpsc::Receiver<StartupReport>,
    generation: u64,
) -> Result<(), StartFailure> {
    // Until the buffer is reported at all, the overall timeout is what limits
    // the wait. From then on a cycle has to complete before each deadline,
    // which moves forward every time one does.
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

        // Checked before stability, so a gap right at the end still fails.
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
                    // The buffer is being reported now, so this is where the
                    // watch window starts.
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
                // Actuators that are switched off are not something the bridge
                // fixes by starting the control loop again, so say so and stop
                // here instead of restarting it forever.
                return Err(StartFailure::Fatal(format!(
                    "maxon drive on axis {} did not answer: its actuators are most likely switched off",
                    axis
                )));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            // The process closed its stdout, which it only does on the way
            // out. Decided by what was seen so far.
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
    generation: u64,
) -> Result<String, String> {
    let stop_message = stop_robotic_leg_control(state)?;
    let start_message = start_robotic_leg_control(state, launch, generation)?;
    Ok(format!("{}; {}", stop_message, start_message))
}

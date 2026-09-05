use crate::models::log_level::LogLevel;
use crate::models::log::Log;
use crate::models::power_feedback::{PowerCommand, PowerFeedback};
use crate::models::prosthesis_state::ProsthesisState;
use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Power command values received on the commands topic. Every other value is a
/// gait command, applied by restarting the control executable with the new gait
/// mode.
const POWER_ON: u8 = 16;
const POWER_OFF: u8 = 17;

/// Gait mode the control executable is started with until a gait command
/// selects another one.
const DEFAULT_GAIT_MODE: u8 = 1;

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

        eprintln!("[MQTT] Spawning event loop...");
        
        // Spawn the event loop in a separate thread
        thread::spawn(move || {
            eprintln!("[MQTT] Event loop: connection handler started");
            
            let mut connection = connection;

            // Seed the power state from the processes actually running, so a
            // restart of this bridge does not think the prosthesis is off while
            // the control loop is still up.
            let mut prosthesis_state = match robotic_leg_control_pids() {
                Ok(pids) if !pids.is_empty() => ProsthesisState::On,
                _ => ProsthesisState::Off,
            };
            let mut gait_mode = DEFAULT_GAIT_MODE;
            eprintln!(
                "[CMD] Initial prosthesis state: {} (gait {})",
                prosthesis_state.as_str(),
                gait_mode
            );

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
                            match payload_str.trim().parse::<u8>() {
                                Ok(POWER_ON) => {
                                    eprintln!("[CMD] Received power ON command. Starting RoboticLegControl...");
                                    let result =
                                        start_robotic_leg_control(&mut prosthesis_state, gait_mode);
                                    publish_power_feedback(
                                        &client,
                                        &power_feedback_topic,
                                        qos,
                                        PowerCommand::On,
                                        prosthesis_state,
                                        gait_mode,
                                        result,
                                    );
                                }
                                Ok(POWER_OFF) => {
                                    eprintln!("[CMD] Received power OFF command. Stopping RoboticLegControl...");
                                    let result = stop_robotic_leg_control(&mut prosthesis_state);
                                    publish_power_feedback(
                                        &client,
                                        &power_feedback_topic,
                                        qos,
                                        PowerCommand::Off,
                                        prosthesis_state,
                                        gait_mode,
                                        result,
                                    );
                                }
                                Ok(value) => {
                                    eprintln!("[CMD] Received gait command: {}. Restarting RoboticLegControl...", value);
                                    gait_mode = value;
                                    let result =
                                        restart_robotic_leg_control(&mut prosthesis_state, gait_mode);
                                    publish_power_feedback(
                                        &client,
                                        &power_feedback_topic,
                                        qos,
                                        PowerCommand::SetGait,
                                        prosthesis_state,
                                        gait_mode,
                                        result,
                                    );
                                }
                                Err(e) => eprintln!("[MQTT] Failed to parse command payload {:?} as u8: {:?}", payload_str, e),
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

/// Publish the outcome of a power or gait command on the feedback topic. `Ok`
/// carries the success message, `Err` the failure reason; `state` and `gait`
/// are the power state and gait mode the bridge holds after running the
/// command.
fn publish_power_feedback(
    client: &Client,
    topic: &str,
    qos: QoS,
    command: PowerCommand,
    state: ProsthesisState,
    gait: u8,
    result: Result<String, String>,
) {
    let feedback = match result {
        Ok(message) => {
            eprintln!("[CMD] {} succeeded: {}", command.as_str(), message);
            PowerFeedback::success(command, state, gait, message)
        }
        Err(message) => {
            eprintln!("[CMD] {} failed: {}", command.as_str(), message);
            PowerFeedback::failure(command, state, gait, message)
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

/// Launch the RoboticLegControl executable (`sudo <bin> --gait=<gait_mode>`) as
/// a detached child and make sure it actually came up: a start that does not
/// report a healthy buffer size is killed and tried again, up to
/// `STARTUP_ATTEMPTS` times. We intentionally do not wait on the control loop
/// itself: it is long-running and is later stopped by
/// `stop_robotic_leg_control`. On success `state` is set to `On`; when every
/// attempt failed it is set to `Off`, since the last attempt is cleaned up
/// before giving up.
fn start_robotic_leg_control(
    state: &mut ProsthesisState,
    gait_mode: u8,
) -> Result<String, String> {
    if *state == ProsthesisState::On {
        return Ok("RoboticLegControl is already running".to_string());
    }

    let mut failures: Vec<String> = Vec::new();

    for attempt in 1..=STARTUP_ATTEMPTS {
        match spawn_and_verify_robotic_leg_control(gait_mode) {
            Ok(message) => {
                *state = ProsthesisState::On;

                return Ok(if failures.is_empty() {
                    message
                } else {
                    format!("{} (after {})", message, failures.join("; "))
                });
            }
            Err(e) => {
                eprintln!(
                    "[CMD] RoboticLegControl startup attempt {}/{} failed: {}",
                    attempt, STARTUP_ATTEMPTS, e
                );
                failures.push(format!("failed attempt {}: {}", attempt, e));

                // Whatever is left of the failed attempt has to go before the
                // next one, otherwise two control loops end up fighting over
                // the hardware. `stop_robotic_leg_control` returns early when
                // it is told the prosthesis is off, so give it a local `On`.
                let mut leftover = ProsthesisState::On;

                if let Err(stop_error) = stop_robotic_leg_control(&mut leftover) {
                    eprintln!(
                        "[CMD] Could not clean up after failed startup attempt {}: {}",
                        attempt, stop_error
                    );
                    failures.push(format!("failed attempt {} cleanup: {}", attempt, stop_error));
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

/// Start the control executable once and watch its stdout for the buffer size
/// it reports. Its output keeps being forwarded to our own stdout for as long
/// as it runs; only the beginning of it is inspected. Fails when the process
/// dies right away or when the buffer never reaches `HEALTHY_DATA_BYTES`.
fn spawn_and_verify_robotic_leg_control(gait_mode: u8) -> Result<String, String> {
    let mut child = std::process::Command::new("sudo")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .arg(format!("--gait={}", gait_mode))
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to start RoboticLegControl: {:?}", e))?;

    let pid = child.id();

    let stdout = child.stdout.take().ok_or_else(|| {
        format!(
            "RoboticLegControl (sudo pid {}) was started without a stdout pipe",
            pid
        )
    })?;

    let (data_bytes, data_bytes_receiver) = mpsc::channel::<u64>();
    thread::spawn(move || forward_robotic_leg_control_stdout(stdout, data_bytes));

    let startup = watch_startup_data_bytes(&data_bytes_receiver);

    // `spawn` only reports that the process was launched: `sudo` can still exit
    // right away (missing binary, missing permission). The watch window already
    // gave it a moment, so it is enough to look once, and a process that died
    // explains a failed watch better than the watch itself does.
    match child.try_wait() {
        Ok(Some(status)) => {
            return Err(format!(
                "RoboticLegControl exited right after start with status {:?}",
                status
            ));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(format!(
                "failed to check whether RoboticLegControl (sudo pid {}) stayed alive: {:?}",
                pid, e
            ));
        }
    }

    startup?;

    // Nothing waits on the control loop from here on, so hand the child to a
    // thread that reaps it whenever it eventually exits.
    thread::spawn(move || {
        let _ = child.wait();
    });

    Ok(format!(
        "started RoboticLegControl with gait {} (sudo pid {}), buffer reaching {} bytes",
        gait_mode, pid, HEALTHY_DATA_BYTES
    ))
}

/// Judge a startup from the reported buffer sizes: healthy as soon as the
/// buffer reaches `HEALTHY_DATA_BYTES`, bad once it has been reporting other
/// sizes for `STARTUP_WATCH_WINDOW` without getting there. The clock only
/// starts on the first report, so a slow initialization does not count against
/// the prosthesis; not reporting at all within `STARTUP_STREAM_TIMEOUT` is a
/// bad startup of its own.
fn watch_startup_data_bytes(data_bytes: &mpsc::Receiver<u64>) -> Result<(), String> {
    let stream_deadline = Instant::now() + STARTUP_STREAM_TIMEOUT;
    let mut window_deadline: Option<Instant> = None;
    let mut reports: u32 = 0;
    let mut highest_seen: u64 = 0;

    loop {
        // The startup is only called bad once the watch window ran out *and*
        // enough reports back that verdict up. Until then the overall timeout
        // is what limits the wait, so a control loop that prints slowly gets
        // its reports counted instead of being failed on the clock alone.
        let deadline = match window_deadline {
            Some(window) if reports >= STARTUP_MIN_REPORTS => window.min(stream_deadline),
            _ => stream_deadline,
        };

        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            break;
        }

        match data_bytes.recv_timeout(remaining) {
            Ok(HEALTHY_DATA_BYTES) => return Ok(()),
            Ok(size) => {
                // The buffer is being reported now, so this is where the watch
                // window actually begins.
                window_deadline.get_or_insert_with(|| Instant::now() + STARTUP_WATCH_WINDOW);

                reports += 1;
                highest_seen = highest_seen.max(size);
            }
            // Either a deadline ran out or the process closed its stdout. Both
            // are decided by what was seen so far.
            Err(_) => break,
        }
    }

    if reports == 0 {
        return Err(format!(
            "RoboticLegControl did not report its buffer at all within {}s",
            STARTUP_STREAM_TIMEOUT.as_secs()
        ));
    }

    Err(format!(
        "RoboticLegControl buffer never reached {} bytes in {} reports (highest was {})",
        HEALTHY_DATA_BYTES, reports, highest_seen
    ))
}

/// Forward the control executable's stdout to our own, line by line, reporting
/// every buffer size it prints on `data_bytes` along the way. The thread lives
/// as long as the process does: once the startup check drops its receiver the
/// sends fail silently and only the forwarding goes on.
fn forward_robotic_leg_control_stdout(
    stdout: std::process::ChildStdout,
    data_bytes: mpsc::Sender<u64>,
) {
    for line in BufReader::new(stdout).lines() {
        match line {
            Ok(line) => {
                println!("{}", line);

                if let Some(size) = parse_data_bytes_available(&line) {
                    let _ = data_bytes.send(size);
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

    let mut kill_errors: Vec<String> = Vec::new();

    for pid in &pids {
        eprintln!("[CMD] Killing RoboticLegControl process (pid {})", pid);
        match std::process::Command::new("sudo")
            .arg("kill")
            .arg(pid)
            .status()
        {
            Ok(status) if status.success() => {
                eprintln!("[CMD] Killed RoboticLegControl process (pid {})", pid)
            }
            Ok(status) => kill_errors.push(format!(
                "kill for pid {} exited with status {:?}",
                pid, status
            )),
            Err(e) => kill_errors.push(format!("failed to kill pid {}: {:?}", pid, e)),
        }
    }

    // A failed `kill` call is only a problem if the process survived it, so the
    // end state is what decides success or failure here. Termination is not
    // instant, so keep checking until the processes are gone or the timeout
    // runs out rather than judging on a single look.
    let deadline = Instant::now() + STOP_TIMEOUT;

    let survivors = loop {
        thread::sleep(STOP_POLL_INTERVAL);

        match robotic_leg_control_pids() {
            Ok(remaining) if remaining.is_empty() => {
                *state = ProsthesisState::Off;
                return Ok(format!("stopped RoboticLegControl (pids {})", pids.join(", ")));
            }
            Ok(remaining) if Instant::now() >= deadline => break remaining,
            Ok(_) => {}
            Err(e) => {
                return Err(format!(
                    "could not confirm RoboticLegControl was stopped: {}",
                    e
                ));
            }
        }
    };

    Err(format!(
        "RoboticLegControl is still running {}s after kill (pids {}){}",
        STOP_TIMEOUT.as_secs(),
        survivors.join(", "),
        if kill_errors.is_empty() {
            String::new()
        } else {
            format!(": {}", kill_errors.join("; "))
        }
    ))
}

/// Apply a gait command: stop the control executable and start it again with
/// the requested gait mode. A failure on the way stops the restart, leaving
/// `state` at whatever the last successful step set it to.
fn restart_robotic_leg_control(
    state: &mut ProsthesisState,
    gait_mode: u8,
) -> Result<String, String> {
    let stop_message = stop_robotic_leg_control(state)?;
    let start_message = start_robotic_leg_control(state, gait_mode)?;
    Ok(format!("{}; {}", stop_message, start_message))
}

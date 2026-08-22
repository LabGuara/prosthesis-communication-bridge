use crate::models::log_level::LogLevel;
use crate::models::log::Log;
use crate::models::power_feedback::{PowerCommand, PowerFeedback};
use crate::models::prosthesis_state::ProsthesisState;
use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
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

/// How long to wait before checking that a freshly spawned control process is
/// still alive.
const POWER_CHECK_DELAY: Duration = Duration::from_millis(500);

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
/// a detached child. We intentionally do not wait on it: it is a long-running
/// control loop that is later stopped by `stop_robotic_leg_control`. On success
/// `state` is set to `On`; on failure it is left untouched.
fn start_robotic_leg_control(
    state: &mut ProsthesisState,
    gait_mode: u8,
) -> Result<String, String> {
    if *state == ProsthesisState::On {
        return Ok("RoboticLegControl is already running".to_string());
    }

    let mut child = std::process::Command::new("sudo")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .arg(format!("--gait={}", gait_mode))
        .spawn()
        .map_err(|e| format!("failed to start RoboticLegControl: {:?}", e))?;

    let pid = child.id();

    // `spawn` only reports that the process was launched: `sudo` can still exit
    // right away (missing binary, missing permission). Give it a moment and
    // make sure it is still alive before reporting success.
    thread::sleep(POWER_CHECK_DELAY);

    match child.try_wait() {
        Ok(Some(status)) => Err(format!(
            "RoboticLegControl exited right after start with status {:?}",
            status
        )),
        Ok(None) => {
            *state = ProsthesisState::On;
            Ok(format!(
                "started RoboticLegControl with gait {} (sudo pid {})",
                gait_mode, pid
            ))
        }
        Err(e) => Err(format!(
            "failed to check whether RoboticLegControl (sudo pid {}) stayed alive: {:?}",
            pid, e
        )),
    }
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

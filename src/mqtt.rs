use crate::models::log_level::LogLevel;
use crate::models::log::Log;
use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Power command values received on the commands topic. Gait commands use
/// values 1-9 and are forwarded via TCP; these power values are handled
/// locally instead.
const POWER_ON: u8 = 16;
const POWER_OFF: u8 = 17;

/// The MATLAB-generated control executable started/stopped by the power
/// commands. In `ps` its process name shows up truncated as `RoboticLegContr`.
const ROBOTIC_LEG_CONTROL_BIN: &str =
    "/home/labguara/Resultados/MATLAB_ws/R2025b/RoboticLegControl2025b.elf";

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
            
            loop {
                // Handle all pending messages from the queue
                while let Ok(message) = receiver.try_recv() {
                    let log = Log {
                        timestamp: chrono::Utc::now(),
                        level: LogLevel::Info,
                        sensor_name: "prothesis-logger".to_string(),
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
                                    start_robotic_leg_control();
                                }
                                Ok(POWER_OFF) => {
                                    eprintln!("[CMD] Received power OFF command. Stopping RoboticLegControl...");
                                    stop_robotic_leg_control();
                                }
                                Ok(value) => {
                                    eprintln!("[MQTT] Received command: {}. Forwarding via TCP...", value);
                                    match std::net::TcpStream::connect("192.168.0.101:25000") {
                                        Ok(mut stream) => {
                                            use std::io::Write;
                                            match stream.write_all(&[value]) {
                                                Ok(_) => eprintln!("[TCP] Successfully sent {} to 192.168.0.101:25000", value),
                                                Err(e) => eprintln!("[TCP] Failed to write data: {:?}", e),
                                            }
                                        }
                                        Err(e) => eprintln!("[TCP] Connection failed to 192.168.0.101:25000: {:?}", e),
                                    }
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

/// Launch the RoboticLegControl executable (`sudo <bin>`) as a detached child.
/// We intentionally do not wait on it: it is a long-running control loop that
/// is later stopped by `stop_robotic_leg_control`.
fn start_robotic_leg_control() {
    match std::process::Command::new("sudo")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .spawn()
    {
        Ok(child) => eprintln!(
            "[CMD] Started RoboticLegControl (sudo pid {})",
            child.id()
        ),
        Err(e) => eprintln!("[CMD] Failed to start RoboticLegControl: {:?}", e),
    }
}

/// Find the running RoboticLegControl process(es) by command line and kill
/// them. The executable is started via `sudo`, so it runs as root and must be
/// killed with `sudo kill`.
fn stop_robotic_leg_control() {
    let output = match std::process::Command::new("pgrep")
        .arg("-f")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .output()
    {
        Ok(output) => output,
        Err(e) => {
            eprintln!("[CMD] Failed to run pgrep to locate RoboticLegControl: {:?}", e);
            return;
        }
    };

    let pids: Vec<&str> = std::str::from_utf8(&output.stdout)
        .unwrap_or("")
        .split_whitespace()
        .collect();

    if pids.is_empty() {
        eprintln!("[CMD] No running RoboticLegControl process found to stop");
        return;
    }

    for pid in pids {
        eprintln!("[CMD] Killing RoboticLegControl process (pid {})", pid);
        match std::process::Command::new("sudo")
            .arg("kill")
            .arg(pid)
            .status()
        {
            Ok(status) if status.success() => {
                eprintln!("[CMD] Killed RoboticLegControl process (pid {})", pid)
            }
            Ok(status) => eprintln!(
                "[CMD] kill for pid {} exited with status {:?}",
                pid, status
            ),
            Err(e) => eprintln!("[CMD] Failed to kill pid {}: {:?}", pid, e),
        }
    }
}

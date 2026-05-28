use crate::models::log_level::LogLevel;
use crate::models::log::Log;
use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, Transport, RecvTimeoutError};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub struct MqttPublisher {
    sender: Sender<String>,
}

impl MqttPublisher {
    pub fn from_env(command_sender: Sender<String>) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(MqttConfig::from_env(), command_sender)
    }

    pub fn new(
        config: MqttConfig,
        command_sender: Sender<String>,
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
                            match serde_json::from_slice::<serde_json::Value>(&p.payload) {
                                Ok(value) => {
                                    match value.get("message").and_then(|message| message.as_str()) {
                                        Some(message) => {
                                            if let Err(err) = command_sender.send(message.to_string()) {
                                                eprintln!("[MQTT] Failed to forward command to stdin: {err}");
                                            }
                                        }
                                        None => {
                                            eprintln!("[MQTT] Incoming command payload missing string 'message' field");
                                        }
                                    }
                                }
                                Err(err) => {
                                    eprintln!("[MQTT] Failed to parse command payload as JSON: {err}");
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

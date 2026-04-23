use crate::config::MqttConfig;
use rumqttc::{Client, MqttOptions, QoS, RecvTimeoutError};
use serde_json::json;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

pub struct MqttPublisher {
    sender: Sender<String>,
}

impl MqttPublisher {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(MqttConfig::from_env())
    }

    pub fn new(config: MqttConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let mut mqtt_options = MqttOptions::new(&config.client_id, &config.host, config.port);
        mqtt_options.set_keep_alive(Duration::from_secs(30));

        let (client, mut connection) = Client::new(mqtt_options, 100);
        let (sender, receiver) = mpsc::channel::<String>();

        let topic = config.topic;
        let qos = match config.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        };

        thread::spawn(move || {
            loop {
                while let Ok(message) = receiver.try_recv() {
                    let payload = json!({
                        "level": "normal",
                        "sensor_type": "action",
                        "sensor_name": "action-01",
                        "message": message.trim()
                    });

                    let json_str = payload.to_string();
                    if let Err(err) = client.publish(&topic, qos, false, json_str) {
                        eprintln!("MQTT publish error: {err}");
                    }
                }

                match connection.recv_timeout(Duration::from_millis(100)) {
                    Ok(_) => {}
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        eprintln!("MQTT connection error: disconnected");
                        break;
                    }
                }
            }
        });

        Ok(Self { sender })
    }

    pub fn publish(&self, message: &str) {
        if let Err(err) = self.sender.send(message.to_string()) {
            eprintln!("MQTT queue error: {err}");
        }
    }
}

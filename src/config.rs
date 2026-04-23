use std::env;

#[derive(Clone, Debug)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub topic: String,
    pub qos: i32,
    pub client_id: String,
}

impl MqttConfig {
    pub fn from_env() -> Self {
        dotenv::dotenv().ok();
        
        Self {
            host: env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("MQTT_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1883),
            topic: env::var("MQTT_RAW_TOPIC").unwrap_or_else(|_| "prothesis/logs/raw".to_string()),
            qos: env::var("MQTT_QOS")
                .ok()
                .and_then(|q| q.parse().ok())
                .unwrap_or(1),
            client_id: {
                let base = env::var("MQTT_CLIENT_ID").unwrap_or_else(|_| "prothesis-logger-wrapper".to_string());
                format!("{}-{}", base, std::process::id())
            },
        }
    }
}
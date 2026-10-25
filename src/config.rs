use std::env;

#[derive(Clone, Debug)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub logs_topic: String,
    pub commands_topic: String,
    pub power_feedback_topic: String,
    pub qos: i32,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: bool,
}

impl MqttConfig {
    pub fn from_env() -> Self {
        dotenv::dotenv().ok();
        
        Self {
            host: env::var("MQTT_HOST")
                .unwrap_or_else(|_| "localhost".to_string()),

            port: env::var("MQTT_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1883),

            logs_topic: env::var("MQTT_LOGS_TOPIC")
                .unwrap_or_else(|_| "prosthesis/logs".to_string()),

            commands_topic: env::var("MQTT_COMMAND_LOGS_VALIDATED_TOPIC")
                .unwrap_or_else(|_| "prosthesis/actions/validated".to_string()),

            power_feedback_topic: env::var("MQTT_POWER_FEEDBACK_TOPIC")
                .unwrap_or_else(|_| "prosthesis/actions/feedback".to_string()),

            qos: env::var("MQTT_QOS")
                .ok()
                .and_then(|q| q.parse().ok())
                .unwrap_or(1),

            client_id: {
                let base = env::var("MQTT_CLIENT_ID").unwrap_or_else(|_| "prosthesis-logger-wrapper".to_string());
                format!("{}-{}", base, std::process::id())
            },

            username: env::var("MQTT_USERNAME").ok(),
            
            password: env::var("MQTT_PASSWORD").ok(),
            
            tls: env::var("MQTT_TLS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(false),
        }
    }
}
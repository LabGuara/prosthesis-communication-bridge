use chrono::{DateTime, Utc};
use serde_json::json;

use crate::models::log_level::LogLevel;


#[derive(Clone, Debug)]
pub struct Log {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub sensor_name: String,
    pub message: String,
}

impl Log {
    pub fn generate_payload(&self) -> String {
        json!({
            "level": self.level.as_i32(),
            "timestamp": self.timestamp.to_rfc3339(),
            "sensor_name": self.sensor_name,
            "message": self.message,
        })
        .to_string()
    }
}

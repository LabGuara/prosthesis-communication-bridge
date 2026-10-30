use chrono::{DateTime, Utc};
use serde_json::{json, Value};

#[derive(Clone, Debug)]
pub struct SensorBatch {
    pub timestamp: DateTime<Utc>,
    pub data: Value,
}

impl SensorBatch {
    pub fn new(timestamp: DateTime<Utc>, payload: &str) -> Self {
        let data = serde_json::from_str(payload).unwrap_or_else(|_| Value::String(payload.to_string()));
        Self { timestamp, data }
    }

    pub fn generate_payload(&self) -> String {
        json!({
            "timestamp": self.timestamp.to_rfc3339(),
            "data": self.data,
        })
        .to_string()
    }
}

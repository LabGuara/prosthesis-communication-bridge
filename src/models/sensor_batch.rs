use chrono::{DateTime, Utc};
use serde_json::{json, Value};

/// A batch of readings from RoboticLegControl, as forwarded to the orchestrator.
#[derive(Clone, Debug)]
pub struct SensorBatch {
    /// When the bridge received the batch.
    pub timestamp: DateTime<Utc>,
    /// The readings as the control program sent them: JSON when they parse, text otherwise.
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

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::models::prosthesis_state::ProsthesisState;

/// The power command a feedback message refers to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PowerCommand {
    On,
    Off,
}

impl PowerCommand {
    pub fn as_str(self) -> &'static str {
        match self {
            PowerCommand::On => "power_on",
            PowerCommand::Off => "power_off",
        }
    }
}

/// Result of a power command, published back to the main server so it knows
/// whether the prosthesis actually reached the requested state.
#[derive(Clone, Debug)]
pub struct PowerFeedback {
    pub timestamp: DateTime<Utc>,
    pub command: PowerCommand,
    pub success: bool,
    pub state: ProsthesisState,
    pub message: String,
}

impl PowerFeedback {
    pub fn success(command: PowerCommand, state: ProsthesisState, message: String) -> Self {
        Self { timestamp: Utc::now(), command, success: true, state, message }
    }

    pub fn failure(command: PowerCommand, state: ProsthesisState, message: String) -> Self {
        Self { timestamp: Utc::now(), command, success: false, state, message }
    }

    pub fn generate_payload(&self) -> String {
        json!({
            "timestamp": self.timestamp.to_rfc3339(),
            "command": self.command.as_str(),
            "status": if self.success { "success" } else { "failure" },
            "state": self.state.as_str(),
            "message": self.message,
        })
        .to_string()
    }
}

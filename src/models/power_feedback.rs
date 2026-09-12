use chrono::{DateTime, Utc};
use serde_json::json;

use crate::models::prosthesis_state::ProsthesisState;

/// The command a feedback message refers to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PowerCommand {
    On,
    Off,
    SetGait,
    SetActuatorCurrent,
}

impl PowerCommand {
    pub fn as_str(self) -> &'static str {
        match self {
            PowerCommand::On => "power_on",
            PowerCommand::Off => "power_off",
            PowerCommand::SetGait => "set_gait",
            PowerCommand::SetActuatorCurrent => "set_actuator_current",
        }
    }
}

/// Result of a power or gait command, published back to the main server so it
/// knows whether the prosthesis actually reached the requested state.
#[derive(Clone, Debug)]
pub struct PowerFeedback {
    pub timestamp: DateTime<Utc>,
    pub command: PowerCommand,
    pub success: bool,
    pub state: ProsthesisState,
    pub gait: u8,
    pub current: u16,
    pub message: String,
}

impl PowerFeedback {
    pub fn success(
        command: PowerCommand,
        state: ProsthesisState,
        gait: u8,
        current: u16,
        message: String,
    ) -> Self {
        Self { timestamp: Utc::now(), command, success: true, state, gait, current, message }
    }

    pub fn failure(
        command: PowerCommand,
        state: ProsthesisState,
        gait: u8,
        current: u16,
        message: String,
    ) -> Self {
        Self { timestamp: Utc::now(), command, success: false, state, gait, current, message }
    }

    pub fn generate_payload(&self) -> String {
        json!({
            "timestamp": self.timestamp.to_rfc3339(),
            "command": self.command.as_str(),
            "status": if self.success { "success" } else { "failure" },
            "state": self.state.as_str(),
            "gait": self.gait,
            "current": self.current,
            "message": self.message,
        })
        .to_string()
    }
}

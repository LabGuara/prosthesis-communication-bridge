use chrono::{DateTime, Utc};
use serde_json::json;

use crate::models::prosthesis_state::ProsthesisState;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedbackOrigin {
    Remote,
    Internal,
}

impl FeedbackOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            FeedbackOrigin::Remote => "remote",
            FeedbackOrigin::Internal => "internal",
        }
    }
}

#[derive(Clone, Debug)]
pub struct PowerFeedback {
    pub timestamp: DateTime<Utc>,
    pub command: PowerCommand,
    pub command_id: Option<u64>,
    pub origin: FeedbackOrigin,
    pub success: bool,
    pub state: ProsthesisState,
    pub gait: u8,
    pub current: u16,
    pub run_id: Option<String>,
    pub collect_id: Option<String>,
    pub message: String,
}

impl PowerFeedback {
    pub fn generate_payload(&self) -> String {
        let running = self.state == ProsthesisState::On;

        json!({
            "timestamp": self.timestamp.to_rfc3339(),
            "command_id": self.command_id,
            "origin": self.origin.as_str(),
            "run_id": self.run_id,
            "collect_id": self.collect_id,
            "command": self.command.as_str(),
            "status": if self.success { "success" } else { "failure" },
            "state": self.state.as_str(),
            "gait": if running { Some(self.gait) } else { None },
            "current": if running { Some(self.current) } else { None },
            "message": self.message,
        })
        .to_string()
    }
}

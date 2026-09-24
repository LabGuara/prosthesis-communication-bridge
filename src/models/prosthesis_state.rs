#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProsthesisState {
    On,
    Off,
}

impl ProsthesisState {
    pub fn as_str(self) -> &'static str {
        match self {
            ProsthesisState::On => "on",
            ProsthesisState::Off => "off",
        }
    }
}

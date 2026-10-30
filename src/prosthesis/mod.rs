mod mock;
mod real;

pub use mock::MockProsthesis;
pub use real::RealProsthesis;

#[derive(Clone, Copy, Debug)]
pub struct LaunchSettings {
    pub gait: u8,
    pub current: u16,
}

pub trait Prosthesis: Send + Sync {
    fn name(&self) -> &'static str;

    fn is_running(&self) -> bool;

    fn start(&self, launch: LaunchSettings, superseded: &dyn Fn() -> bool) -> Result<String, String>;

    fn stop(&self) -> Result<String, String>;
}

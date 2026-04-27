#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum LogLevel {
    Info = 1,
    Warning = 2,
    Error = 3,
}

impl LogLevel {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

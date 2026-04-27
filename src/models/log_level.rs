#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum LogLevel {
    Debug = 1,
    Info = 2,
    Warning = 3,
    Error = 4,
}

impl LogLevel {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

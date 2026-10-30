//! The real prosthesis: RoboticLegControl, started and stopped as a process on the Pi.

use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use super::{LaunchSettings, Prosthesis};

const ROBOTIC_LEG_CONTROL_BIN: &str =
    "/home/labguara/Resultados/MATLAB_ws/R2025b/RoboticLegControl2025b.elf";

const HEALTHY_DATA_BYTES: u64 = 52;

const STARTUP_STREAM_TIMEOUT: Duration = Duration::from_secs(10);

const STARTUP_WATCH_WINDOW: Duration = Duration::from_secs(1);

const STARTUP_STABILIZATION: Duration = Duration::from_secs(3);

const RESTART_DELAY: Duration = Duration::from_secs(1);

const SUPERSEDE_POLL_INTERVAL: Duration = Duration::from_millis(200);

const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(250);

const KILL_ESCALATION_DELAY: Duration = Duration::from_secs(3);

pub struct RealProsthesis;

impl Prosthesis for RealProsthesis {
    fn name(&self) -> &'static str {
        "RoboticLegControl"
    }

    fn is_running(&self) -> bool {
        matches!(robotic_leg_control_pids(), Ok(pids) if !pids.is_empty())
    }

    fn start(&self, launch: LaunchSettings, superseded: &dyn Fn() -> bool) -> Result<String, String> {
        start_robotic_leg_control(launch, superseded)
    }

    fn stop(&self) -> Result<String, String> {
        stop_robotic_leg_control()
    }
}

enum StartFailure {
    Retryable(String),
    Fatal(String),
    Superseded,
}

impl StartFailure {
    fn message(&self) -> &str {
        match self {
            StartFailure::Retryable(message) | StartFailure::Fatal(message) => message,
            StartFailure::Superseded => "a newer command came in before it was done",
        }
    }
}

enum StartupReport {
    DataBytes(u64),
    UnresponsiveDrive(String),
}

impl LaunchSettings {
    fn as_args(self) -> [String; 2] {
        [format!("--gait={}", self.gait), format!("--I={}", self.current)]
    }
}

fn robotic_leg_control_pids() -> Result<Vec<String>, String> {
    let output = std::process::Command::new("pgrep")
        .arg("-f")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .output()
        .map_err(|e| format!("failed to run pgrep to locate RoboticLegControl: {:?}", e))?;

    Ok(std::str::from_utf8(&output.stdout)
        .unwrap_or("")
        .split_whitespace()
        .map(|pid| pid.to_string())
        .collect())
}

fn start_robotic_leg_control(
    launch: LaunchSettings,
    superseded: &dyn Fn() -> bool,
) -> Result<String, String> {
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;

        if superseded() {
            return Err(format!(
                "start abandoned before attempt {}: a newer command came in",
                attempt
            ));
        }

        let failure = match spawn_and_verify_robotic_leg_control(launch, superseded) {
            Ok(message) => return Ok(message),
            Err(failure) => failure,
        };

        eprintln!(
            "[CMD] RoboticLegControl startup attempt {} failed: {}",
            attempt,
            failure.message()
        );

        match stop_robotic_leg_control() {
            Ok(stop_message) => eprintln!(
                "[CMD] Cleaned up after failed startup attempt {}: {}",
                attempt, stop_message
            ),
            Err(stop_error) => eprintln!(
                "[CMD] Could not clean up after failed startup attempt {}: {}",
                attempt, stop_error
            ),
        }

        match failure {
            StartFailure::Fatal(_) => return Err(format!("RoboticLegControl cannot start")),
            StartFailure::Superseded => return Err(format!("Startup retried")),
            StartFailure::Retryable(_) => {}
        }

        eprintln!(
            "[CMD] Restarting RoboticLegControl in {}ms (attempt {} next)",
            RESTART_DELAY.as_millis(),
            attempt + 1
        );

        sleep_unless_superseded(RESTART_DELAY, superseded);
    }
}

fn sleep_unless_superseded(duration: Duration, superseded: &dyn Fn() -> bool) {
    let deadline = Instant::now() + duration;

    while !superseded() {
        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            return;
        }

        thread::sleep(remaining.min(SUPERSEDE_POLL_INTERVAL));
    }
}

fn spawn_and_verify_robotic_leg_control(
    launch: LaunchSettings,
    superseded: &dyn Fn() -> bool,
) -> Result<String, StartFailure> {
    let mut child = std::process::Command::new("sudo")
        .arg(ROBOTIC_LEG_CONTROL_BIN)
        .args(launch.as_args())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            StartFailure::Retryable(format!("failed to start RoboticLegControl: {:?}", e))
        })?;

    let pid = child.id();

    let stdout = child.stdout.take().ok_or_else(|| {
        StartFailure::Retryable(format!(
            "RoboticLegControl (sudo pid {}) was started without a stdout pipe",
            pid
        ))
    })?;

    let (reports, reports_receiver) = mpsc::channel::<StartupReport>();
    thread::spawn(move || forward_robotic_leg_control_stdout(stdout, reports));

    eprintln!(
        "[CMD] Watching the startup of RoboticLegControl (sudo pid {}): its buffer has to keep cycling {} -> 0 for {}s",
        pid,
        HEALTHY_DATA_BYTES,
        STARTUP_STABILIZATION.as_secs()
    );

    let startup = watch_startup(&reports_receiver, superseded);

    match child.try_wait() {
        Ok(Some(status)) => {
            return Err(StartFailure::Retryable(format!(
                "RoboticLegControl exited right after start with status {:?}",
                status
            )));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(StartFailure::Retryable(format!(
                "failed to check whether RoboticLegControl (sudo pid {}) stayed alive: {:?}",
                pid, e
            )));
        }
    }

    if let Err(failure) = startup {
        eprintln!("[CMD] {}", failure.message());

        for error in signal_control_pids(&[pid.to_string()], "TERM") {
            eprintln!("[CMD] {}", error);
        }

        return Err(failure);
    }

    thread::spawn(move || {
        let _ = child.wait();
    });

    Ok(format!(
        "started RoboticLegControl with gait {} and current {} (sudo pid {})",
        launch.gait,
        launch.current,
        pid
    ))
}

fn watch_startup(
    reports: &mpsc::Receiver<StartupReport>,
    superseded: &dyn Fn() -> bool,
) -> Result<(), StartFailure> {
    let mut deadline = Instant::now() + STARTUP_STREAM_TIMEOUT;
    let mut stable_since: Option<Instant> = None;
    let mut previous: Option<u64> = None;
    let mut data_reports: u32 = 0;
    let mut cycles: u32 = 0;

    loop {
        if superseded() {
            return Err(StartFailure::Superseded);
        }

        let now = Instant::now();

        if now >= deadline {
            break;
        }

        if stable_since.is_some_and(|since| now.duration_since(since) >= STARTUP_STABILIZATION) {
            return Ok(());
        }

        let wait = deadline
            .saturating_duration_since(now)
            .min(SUPERSEDE_POLL_INTERVAL);

        match reports.recv_timeout(wait) {
            Ok(StartupReport::DataBytes(size)) => {
                data_reports += 1;

                if data_reports == 1 {
                    deadline = Instant::now() + STARTUP_WATCH_WINDOW;
                }

                if size > HEALTHY_DATA_BYTES {
                    return Err(StartFailure::Retryable(format!("RoboticLegControl buffer desynchronized")));
                }

                if previous == Some(HEALTHY_DATA_BYTES) {
                    if size != 0 {
                        return Err(StartFailure::Retryable(format!("RoboticLegControl buffer desynchronized")));
                    }

                    cycles += 1;
                    stable_since.get_or_insert_with(Instant::now);
                    deadline = Instant::now() + STARTUP_WATCH_WINDOW;
                }

                previous = Some(size);
            }
            Ok(StartupReport::UnresponsiveDrive(axis)) => {
                return Err(StartFailure::Fatal(format!(
                    "maxon drive on axis {} did not answer: its actuators are most likely switched off",
                    axis
                )));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    if data_reports == 0 {
        return Err(StartFailure::Retryable(format!(
            "RoboticLegControl did not report its buffer at all within {}s",
            STARTUP_STREAM_TIMEOUT.as_secs()
        )));
    }

    if cycles == 0 {
        return Err(StartFailure::Retryable(format!(
            "RoboticLegControl buffer never completed a {} -> 0 cycle in {} reports",
            HEALTHY_DATA_BYTES, data_reports
        )));
    }

    Err(StartFailure::Retryable(format!(
        "RoboticLegControl buffer completed {} {} -> 0 cycles, then went {}ms without one",
        cycles,
        HEALTHY_DATA_BYTES,
        STARTUP_WATCH_WINDOW.as_millis()
    )))
}

fn forward_robotic_leg_control_stdout(
    stdout: std::process::ChildStdout,
    reports: mpsc::Sender<StartupReport>,
) {
    for line in BufReader::new(stdout).lines() {
        match line {
            Ok(line) => {
                println!("{}", line);

                if let Some(size) = parse_data_bytes_available(&line) {
                    let _ = reports.send(StartupReport::DataBytes(size));
                } else if let Some(axis) = parse_unresponsive_drive(&line) {
                    let _ = reports.send(StartupReport::UnresponsiveDrive(axis));
                }
            }
            Err(e) => {
                eprintln!("[CMD] Failed to read RoboticLegControl stdout: {:?}", e);
                break;
            }
        }
    }
}

fn parse_data_bytes_available(line: &str) -> Option<u64> {
    const REPORT_PREFIX: &str = "Data bytes available";

    let start = line.find(REPORT_PREFIX)? + REPORT_PREFIX.len();
    let rest = &line[start..];
    let size = rest.split(',').next().unwrap_or(rest);

    size.trim().parse().ok()
}

fn parse_unresponsive_drive(line: &str) -> Option<String> {
    const DRIVE_PREFIX: &str = "[maxon] no ";

    if !line.contains("NAO respondeu") && !line.contains("nao respondeu") {
        return None;
    }

    let axis = line
        .find(DRIVE_PREFIX)
        .and_then(|at| line[at + DRIVE_PREFIX.len()..].split_whitespace().next())
        .unwrap_or("?");

    Some(axis.to_string())
}

fn stop_robotic_leg_control() -> Result<String, String> {
    let pids = robotic_leg_control_pids()?;

    if pids.is_empty() {
        return Ok("no running RoboticLegControl process found, already stopped".to_string());
    }

    let mut kill_errors = signal_control_pids(&pids, "TERM");

    let survivors = match wait_for_control_processes_to_exit(KILL_ESCALATION_DELAY)? {
        None => Vec::new(),
        Some(survivors) => {
            eprintln!(
                "[CMD] RoboticLegControl sat through SIGTERM for {}s, sending SIGKILL (pids {})",
                KILL_ESCALATION_DELAY.as_secs(),
                survivors.join(", ")
            );

            kill_errors.extend(signal_control_pids(&survivors, "KILL"));

            wait_for_control_processes_to_exit(STOP_TIMEOUT)?.unwrap_or_default()
        }
    };

    if survivors.is_empty() {
        return Ok(format!("stopped RoboticLegControl (pids {})", pids.join(", ")));
    }

    Err(format!(
        "RoboticLegControl is still running after SIGTERM and SIGKILL (pids {}){}",
        survivors.join(", "),
        if kill_errors.is_empty() {
            String::new()
        } else {
            format!(": {}", kill_errors.join("; "))
        }
    ))
}

fn signal_control_pids(pids: &[String], signal: &str) -> Vec<String> {
    let mut errors: Vec<String> = Vec::new();

    for pid in pids {
        eprintln!(
            "[CMD] Sending SIG{} to RoboticLegControl process (pid {})",
            signal, pid
        );

        match std::process::Command::new("sudo")
            .arg("kill")
            .arg(format!("-{}", signal))
            .arg(pid)
            .status()
        {
            Ok(status) if status.success() => {}
            Ok(status) => errors.push(format!(
                "kill -{} for pid {} exited with status {:?}",
                signal, pid, status
            )),
            Err(e) => errors.push(format!(
                "failed to send SIG{} to pid {}: {:?}",
                signal, pid, e
            )),
        }
    }

    errors
}

fn wait_for_control_processes_to_exit(
    timeout: Duration,
) -> Result<Option<Vec<String>>, String> {
    let deadline = Instant::now() + timeout;

    loop {
        thread::sleep(STOP_POLL_INTERVAL);

        let remaining = robotic_leg_control_pids()
            .map_err(|e| format!("could not confirm RoboticLegControl was stopped: {}", e))?;

        if remaining.is_empty() {
            return Ok(None);
        }

        if Instant::now() >= deadline {
            return Ok(Some(remaining));
        }
    }
}

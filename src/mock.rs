//! Mock mode (`--mock`): a stand-in for the prosthesis, for running the bridge on a computer that
//! has none.
//!
//! Commands are answered as if RoboticLegControl had started or stopped, always successfully (see
//! `Prosthesis::Mock` in mqtt.rs). While the simulated program runs, this module
//! plays its part in the data path: it sends batches of made-up readings to the bridge's UDP port,
//! formatted like `send_sensor_data.m` does, so they reach MQTT through the same listener and the
//! same run bookkeeping as real ones.
//!
//! The readings follow the gait mode the program was started with (a gait cycle for walking,
//! stairs and slopes; slow sway for standing and sitting; repeated transfers for sit-to-stand and
//! stand-to-sit), with a little noise and stride-to-stride variation. The limited currents are the
//! raw ones clamped to the actuator current the program was started with, so a current command
//! shows up in the data. The shapes are only meant to look plausible on a chart, not to be
//! biomechanically exact.

use std::f64::consts::TAU;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use crate::mqtt::EVENT_LOOP_WAIT;

/// Readings per batch, as in `send_sensor_data.m`.
const BATCH_SIZE: usize = 7;

/// `send_sensor_data.m` sends its whole buffer: the batch, padded with spaces up to this size.
const DATAGRAM_SIZE: usize = 4000;

/// Step of the simulated control loop, in seconds (80 Hz).
const SAMPLE_PERIOD: f64 = 0.0125;

/// How long a start takes. Like the startup check of RoboticLegControl, which takes three seconds,
/// it lets the bridge publish every reading of the previous run before the feedback that begins
/// the next one: a reading can wait up to `EVENT_LOOP_WAIT` to be published. What the program
/// sends meanwhile is dropped, its run not having begun, so the first reading forwarded is not at
/// t = 0, as with the real prosthesis.
const STARTUP_DURATION: Duration = EVENT_LOOP_WAIT.saturating_add(Duration::from_millis(500));

/// How long a stop takes. Readings already on their way reach the bridge within it, while their
/// run is still going.
const STOP_DURATION: Duration = Duration::from_millis(100);

/// Further behind schedule than this (e.g. after the computer slept), the readings carry on from
/// now instead of catching up in a burst.
const MAX_LAG: Duration = Duration::from_secs(1);

/// What the simulated RoboticLegControl was started with. `id` tells one start from the next, so
/// each start's readings begin at t = 0, as the clock of a freshly started program does.
#[derive(Clone, Copy, Debug)]
struct Program {
    id: u64,
    gait: u8,
    current: u16,
}

#[derive(Default)]
struct Programs {
    running: Option<Program>,
    starts: u64,
}

pub struct MockProsthesis {
    programs: Mutex<Programs>,
}

impl MockProsthesis {
    /// Creates the simulated prosthesis, switched off, and the thread that sends its readings to
    /// `127.0.0.1:<udp_port>` while it runs.
    pub fn spawn(udp_port: u16) -> Arc<Self> {
        let mock = Arc::new(Self {
            programs: Mutex::new(Programs::default()),
        });

        thread::spawn({
            let mock = mock.clone();
            move || stream_readings(&mock, udp_port)
        });

        mock
    }

    /// Starts the simulated RoboticLegControl, whose readings begin right away at t = 0, and
    /// returns once the start is through: `false` if the program was stopped meanwhile, which
    /// only a stop signal can do, since commands run one at a time.
    pub fn start(&self, gait: u8, current: u16) -> bool {
        let id = {
            let mut programs = self.lock();
            programs.starts += 1;
            programs.running = Some(Program {
                id: programs.starts,
                gait,
                current,
            });
            programs.starts
        };

        eprintln!(
            "[MOCK] Simulated RoboticLegControl started (gait {}, current {}), up in {}ms",
            gait,
            current,
            STARTUP_DURATION.as_millis()
        );

        thread::sleep(STARTUP_DURATION);

        self.lock().running.is_some_and(|program| program.id == id)
    }

    /// Stops the simulated RoboticLegControl. Returns whether it was running.
    pub fn stop(&self) -> bool {
        let was_running = self.lock().running.take().is_some();

        if was_running {
            eprintln!("[MOCK] Simulated RoboticLegControl stopped");
            thread::sleep(STOP_DURATION);
        }

        was_running
    }

    fn lock(&self) -> MutexGuard<'_, Programs> {
        self.programs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn stream_readings(mock: &MockProsthesis, udp_port: u16) {
    let socket = match UdpSocket::bind(("127.0.0.1", 0)) {
        Ok(socket) => socket,
        Err(e) => {
            eprintln!("[MOCK] Could not open a UDP socket, no readings will be sent: {e}");
            return;
        }
    };

    let batch_period = Duration::from_secs_f64(SAMPLE_PERIOD * BATCH_SIZE as f64);
    let mut simulator: Option<Simulator> = None;
    let mut next_batch = Instant::now();

    loop {
        next_batch += batch_period;
        let now = Instant::now();

        if next_batch > now {
            thread::sleep(next_batch - now);
        } else if now - next_batch > MAX_LAG {
            next_batch = now;
        }

        // Held while the batch is sent, so once `stop` returns the stopped program sends nothing.
        let programs = mock.lock();

        let Some(program) = programs.running else {
            simulator = None;
            continue;
        };

        if simulator.as_ref().map(|s| s.program_id) != Some(program.id) {
            simulator = Some(Simulator::new(program));
        }

        if let Some(simulator) = simulator.as_mut() {
            let datagram = simulator.next_datagram();

            if let Err(e) = socket.send_to(datagram.as_bytes(), ("127.0.0.1", udp_port)) {
                eprintln!("[MOCK] Failed to send readings: {e}");
            }
        }
    }
}

/// One sample, with the channels `send_sensor_data.m` sends.
#[derive(Clone, Copy, Debug)]
struct Reading {
    /// Microstate of the gait state machine.
    state: f64,
    /// Clock of the control program, in seconds since it started.
    t: f64,
    /// Knee angle (°), current (mA) and current after the limit (mA).
    knee: f64,
    knee_current: f64,
    knee_current_limited: f64,
    /// Foot (ankle) angle (°), current (mA) and current after the limit (mA).
    ankle: f64,
    ankle_current: f64,
    ankle_current_limited: f64,
}

impl Reading {
    /// The same JSON object, with the same two decimals, as the `sprintf` of `send_sensor_data.m`.
    fn to_json(self) -> String {
        format!(
            "{{\"s\":{:.2},\"t\":{:.2},\"pJ\":{:.2},\"cJ\":{:.2},\"cJl\":{:.2},\"pP\":{:.2},\"cP\":{:.2},\"cPl\":{:.2}}}",
            self.state,
            self.t,
            self.knee,
            self.knee_current,
            self.knee_current_limited,
            self.ankle,
            self.ankle_current,
            self.ankle_current_limited,
        )
    }
}

/// The readings of one start of the simulated program.
struct Simulator {
    program_id: u64,
    activity: Activity,
    current_limit: f64,
    step: u64,
    /// Where the current cycle is, from 0 to 1.
    phase: f64,
    /// Length of the current cycle (s) and how pronounced it is: both vary a little from one
    /// cycle to the next, as strides do.
    cycle_period: f64,
    cycle_gain: f64,
    rng: Rng,
}

impl Simulator {
    fn new(program: Program) -> Self {
        let activity = Activity::from_gait(program.gait);

        Self {
            program_id: program.id,
            activity,
            current_limit: f64::from(program.current),
            step: 0,
            phase: 0.0,
            cycle_period: activity.cycle_period(),
            cycle_gain: 1.0,
            rng: Rng::new(),
        }
    }

    /// A batch as the control program sends it: a JSON array of `BATCH_SIZE` readings, padded
    /// with spaces to `DATAGRAM_SIZE` bytes.
    fn next_datagram(&mut self) -> String {
        let readings: Vec<String> = (0..BATCH_SIZE)
            .map(|_| self.next_reading().to_json())
            .collect();

        format!("{:<width$}", format!("[{}]", readings.join(",")), width = DATAGRAM_SIZE)
    }

    fn next_reading(&mut self) -> Reading {
        let pose = self.activity.pose(self.phase, self.cycle_gain);
        let knee_current = pose.knee_current + self.rng.noise(40.0);
        let ankle_current = pose.ankle_current + self.rng.noise(40.0);

        let reading = Reading {
            state: pose.state,
            t: self.step as f64 * SAMPLE_PERIOD,
            knee: pose.knee + self.rng.noise(0.2),
            knee_current,
            knee_current_limited: knee_current.clamp(-self.current_limit, self.current_limit),
            ankle: pose.ankle + self.rng.noise(0.2),
            ankle_current,
            ankle_current_limited: ankle_current.clamp(-self.current_limit, self.current_limit),
        };

        self.step += 1;
        self.phase += SAMPLE_PERIOD / self.cycle_period;

        if self.phase >= 1.0 {
            self.phase -= 1.0;
            self.cycle_period = self.activity.cycle_period() * (1.0 + self.rng.noise(0.04));
            self.cycle_gain = 1.0 + self.rng.noise(0.06);
        }

        reading
    }
}

/// The gait modes, numbered as in the app and in RoboticLegControl (`--gait=<n>`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Activity {
    Walking,
    StairAscent,
    Standing,
    StairDescent,
    UpslopeWalking,
    DownslopeWalking,
    Sitting,
    SitToStand,
    StandToSit,
}

impl Activity {
    fn from_gait(gait: u8) -> Self {
        match gait {
            2 => Activity::StairAscent,
            3 => Activity::Standing,
            4 => Activity::StairDescent,
            5 => Activity::UpslopeWalking,
            6 => Activity::DownslopeWalking,
            7 => Activity::Sitting,
            8 => Activity::SitToStand,
            9 => Activity::StandToSit,
            _ => Activity::Walking,
        }
    }

    /// Average length of one cycle, in seconds.
    fn cycle_period(self) -> f64 {
        match self {
            Activity::Standing => 8.0,
            Activity::Sitting => 10.0,
            Activity::SitToStand | Activity::StandToSit => 7.0,
            _ => self.stride().period,
        }
    }

    fn pose(self, phase: f64, gain: f64) -> Pose {
        match self {
            Activity::Standing => standing(phase, gain),
            Activity::Sitting => sitting(phase, gain),
            Activity::SitToStand => transfer(phase, gain, 6500.0, -3500.0),
            Activity::StandToSit => transfer((phase + 0.5) % 1.0, gain, 4000.0, -6000.0),
            _ => self.stride().pose(phase, gain),
        }
    }

    fn stride(self) -> &'static Stride {
        match self {
            Activity::StairAscent => &STAIR_ASCENT,
            Activity::StairDescent => &STAIR_DESCENT,
            Activity::UpslopeWalking => &UPSLOPE_WALKING,
            Activity::DownslopeWalking => &DOWNSLOPE_WALKING,
            _ => &WALKING,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Pose {
    state: f64,
    knee: f64,
    ankle: f64,
    knee_current: f64,
    ankle_current: f64,
}

/// A bell on the cycle: `amp` at phase `at`, `width` cycles wide (its standard deviation). It
/// wraps around, so the end of one cycle flows into the next.
#[derive(Clone, Copy, Debug)]
struct Bump {
    amp: f64,
    at: f64,
    width: f64,
}

const fn bump(amp: f64, at: f64, width: f64) -> Bump {
    Bump { amp, at, width }
}

impl Bump {
    fn at_phase(self, phase: f64) -> f64 {
        let distance = (phase - self.at + 0.5).rem_euclid(1.0) - 0.5;
        self.amp * (-0.5 * (distance / self.width).powi(2)).exp()
    }
}

fn bumps(bumps: &[Bump], phase: f64) -> f64 {
    bumps.iter().map(|b| b.at_phase(phase)).sum()
}

/// One gait cycle (a stride), starting at heel strike.
struct Stride {
    period: f64,
    /// The microstates in order: each lasts until the phase it is paired with.
    states: &'static [(f64, f64)],
    knee: (f64, &'static [Bump]),
    ankle: (f64, &'static [Bump]),
    knee_current: &'static [Bump],
    ankle_current: &'static [Bump],
}

impl Stride {
    fn pose(&self, phase: f64, gain: f64) -> Pose {
        let state = self
            .states
            .iter()
            .find(|(until, _)| phase < *until)
            .or(self.states.last())
            .map_or(1.0, |(_, state)| *state);

        Pose {
            state,
            knee: self.knee.0 + gain * bumps(self.knee.1, phase),
            ankle: self.ankle.0 + gain * bumps(self.ankle.1, phase),
            knee_current: gain * bumps(self.knee_current, phase),
            ankle_current: gain * bumps(self.ankle_current, phase),
        }
    }
}

/// Microstates of level and sloped walking: early, mid and late stance, pre-swing, swing.
const WALKING_STATES: &[(f64, f64)] = &[(0.15, 1.0), (0.40, 2.0), (0.50, 2.5), (0.62, 3.0), (1.0, 4.0)];

/// Microstates of stair ascent and descent.
const STAIR_STATES: &[(f64, f64)] = &[(0.20, 1.0), (0.45, 2.0), (0.60, 3.0), (1.0, 4.0)];

const WALKING: Stride = Stride {
    period: 1.1,
    states: WALKING_STATES,
    knee: (3.0, &[bump(15.0, 0.13, 0.05), bump(58.0, 0.72, 0.085)]),
    ankle: (2.0, &[bump(-6.0, 0.04, 0.03), bump(10.0, 0.42, 0.11), bump(-18.0, 0.62, 0.045)]),
    knee_current: &[bump(4200.0, 0.12, 0.05), bump(900.0, 0.66, 0.05), bump(-2600.0, 0.88, 0.04)],
    ankle_current: &[bump(1200.0, 0.08, 0.03), bump(-4500.0, 0.55, 0.06), bump(700.0, 0.78, 0.06)],
};

const STAIR_ASCENT: Stride = Stride {
    period: 1.5,
    states: STAIR_STATES,
    knee: (6.0, &[bump(55.0, 0.0, 0.12), bump(75.0, 0.78, 0.12)]),
    ankle: (0.0, &[bump(8.0, 0.10, 0.08), bump(-12.0, 0.55, 0.06), bump(10.0, 0.82, 0.08)]),
    knee_current: &[bump(6500.0, 0.18, 0.09), bump(-1500.0, 0.75, 0.06)],
    ankle_current: &[bump(-3800.0, 0.50, 0.07), bump(900.0, 0.85, 0.06)],
};

const STAIR_DESCENT: Stride = Stride {
    period: 1.4,
    states: STAIR_STATES,
    knee: (8.0, &[bump(85.0, 0.62, 0.15)]),
    ankle: (0.0, &[bump(-18.0, 0.02, 0.05), bump(18.0, 0.45, 0.12), bump(-15.0, 0.90, 0.06)]),
    knee_current: &[bump(-5500.0, 0.40, 0.12), bump(800.0, 0.80, 0.06)],
    ankle_current: &[bump(2500.0, 0.08, 0.05), bump(-1800.0, 0.45, 0.10)],
};

const UPSLOPE_WALKING: Stride = Stride {
    period: 1.2,
    states: WALKING_STATES,
    knee: (6.0, &[bump(22.0, 0.12, 0.06), bump(62.0, 0.72, 0.09)]),
    ankle: (6.0, &[bump(12.0, 0.45, 0.12), bump(-16.0, 0.63, 0.045)]),
    knee_current: &[bump(5200.0, 0.15, 0.06), bump(-2400.0, 0.88, 0.04)],
    ankle_current: &[bump(-5000.0, 0.55, 0.06), bump(900.0, 0.78, 0.06)],
};

const DOWNSLOPE_WALKING: Stride = Stride {
    period: 1.15,
    states: WALKING_STATES,
    knee: (5.0, &[bump(25.0, 0.18, 0.07), bump(60.0, 0.72, 0.085)]),
    ankle: (-4.0, &[bump(-10.0, 0.05, 0.03), bump(8.0, 0.42, 0.10), bump(-12.0, 0.62, 0.045)]),
    knee_current: &[bump(-3800.0, 0.20, 0.07), bump(-2600.0, 0.88, 0.04)],
    ankle_current: &[bump(1800.0, 0.08, 0.04), bump(-3000.0, 0.55, 0.06)],
};

/// Standing still, shifting weight now and then.
fn standing(phase: f64, gain: f64) -> Pose {
    let angle = TAU * phase;

    Pose {
        state: if phase < 0.6 { 1.0 } else { 2.0 },
        knee: 4.0 + gain * 2.5 * angle.sin(),
        ankle: 1.0 + gain * 2.0 * (angle + 1.2).sin(),
        knee_current: 900.0 + gain * 500.0 * (angle + 0.4).sin(),
        ankle_current: -500.0 + gain * 350.0 * (angle + 1.6).sin(),
    }
}

/// Seated, the leg swaying a little.
fn sitting(phase: f64, gain: f64) -> Pose {
    let angle = TAU * phase;

    Pose {
        state: if phase < 0.7 { 1.0 } else { 2.0 },
        knee: 88.0 + gain * 3.0 * angle.sin(),
        ankle: 4.0 + gain * 2.0 * (angle + 0.5).sin(),
        knee_current: gain * 150.0 * angle.cos(),
        ankle_current: gain * 80.0 * (angle + 0.5).cos(),
    }
}

/// Sitting, standing up, standing, sitting down again, over and over. The sit-to-stand and
/// stand-to-sit modes are fixed impedances without a state machine, so the microstate stays 1.
fn transfer(phase: f64, gain: f64, rise_current: f64, lower_current: f64) -> Pose {
    let standing = smoothstep(0.30, 0.50, phase) - smoothstep(0.80, 1.0, phase);

    Pose {
        state: 1.0,
        knee: 90.0 - 85.0 * standing,
        ankle: 2.0 + gain * (bump(14.0, 0.36, 0.05).at_phase(phase) + bump(8.0, 0.92, 0.04).at_phase(phase)),
        knee_current: 600.0 * standing
            + gain * (bump(rise_current, 0.40, 0.05).at_phase(phase) + bump(lower_current, 0.90, 0.04).at_phase(phase)),
        ankle_current: -300.0 * standing
            + gain * (bump(-2200.0, 0.42, 0.05).at_phase(phase) + bump(900.0, 0.90, 0.04).at_phase(phase)),
    }
}

fn smoothstep(from: f64, to: f64, x: f64) -> f64 {
    let x = ((x - from) / (to - from)).clamp(0.0, 1.0);
    x * x * (3.0 - 2.0 * x)
}

/// xorshift64*: enough to make the readings look alive, without a dependency for it.
struct Rng(u64);

impl Rng {
    fn new() -> Self {
        Self(uuid::Uuid::new_v4().as_u128() as u64 | 1)
    }

    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Between -amplitude and amplitude, most of it near 0.
    fn noise(&mut self, amplitude: f64) -> f64 {
        (self.unit() + self.unit() - 1.0) * amplitude
    }
}

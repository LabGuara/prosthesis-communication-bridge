use std::f64::consts::TAU;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use super::{LaunchSettings, Prosthesis};

const BATCH_SIZE: usize = 7;

const DATAGRAM_SIZE: usize = 4000;

const SAMPLE_PERIOD: f64 = 0.0125;

const STARTUP_DURATION: Duration = Duration::from_secs(3);

const STOP_DURATION: Duration = Duration::from_millis(100);

const MAX_LAG: Duration = Duration::from_secs(1);

const GRF_POSITIONS: [f64; 4] = [0.0, 0.35, 0.65, 1.0];

const GRF_SCALE: f64 = 50.0;

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

    fn lock(&self) -> MutexGuard<'_, Programs> {
        self.programs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Prosthesis for MockProsthesis {
    fn name(&self) -> &'static str {
        "the simulated prosthesis"
    }

    fn is_running(&self) -> bool {
        false
    }

    fn start(&self, launch: LaunchSettings, _superseded: &dyn Fn() -> bool) -> Result<String, String> {
        let id = {
            let mut programs = self.lock();
            programs.starts += 1;
            programs.running = Some(Program {
                id: programs.starts,
                gait: launch.gait,
                current: launch.current,
            });
            programs.starts
        };

        eprintln!(
            "[MOCK] Simulated RoboticLegControl started (gait {}, current {}), up in {}ms",
            launch.gait,
            launch.current,
            STARTUP_DURATION.as_millis()
        );

        thread::sleep(STARTUP_DURATION);

        if !self.lock().running.is_some_and(|program| program.id == id) {
            return Err("the simulated prosthesis was stopped while starting".to_string());
        }

        Ok(format!(
            "started the simulated prosthesis with gait {} and current {}",
            launch.gait, launch.current
        ))
    }

    fn stop(&self) -> Result<String, String> {
        let was_running = self.lock().running.take().is_some();

        if !was_running {
            return Ok("the simulated prosthesis is already stopped".to_string());
        }

        eprintln!("[MOCK] Simulated RoboticLegControl stopped");
        thread::sleep(STOP_DURATION);

        Ok("stopped the simulated prosthesis".to_string())
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

#[derive(Clone, Copy, Debug)]
struct Reading {
    state: f64,
    t: f64,
    knee: f64,
    knee_current: f64,
    knee_current_limited: f64,
    ankle: f64,
    ankle_current: f64,
    ankle_current_limited: f64,
    grf: [f64; 4],
}

impl Reading {
    fn to_json(self) -> String {
        format!(
            "{{\"s\":{:.2},\"t\":{:.2},\"pJ\":{:.2},\"cJ\":{:.2},\"cJl\":{:.2},\"pP\":{:.2},\"cP\":{:.2},\"cPl\":{:.2},\"g1\":{:.2},\"g2\":{:.2},\"g3\":{:.2},\"g4\":{:.2}}}",
            self.state,
            self.t,
            self.knee,
            self.knee_current,
            self.knee_current_limited,
            self.ankle,
            self.ankle_current,
            self.ankle_current_limited,
            self.grf[0],
            self.grf[1],
            self.grf[2],
            self.grf[3],
        )
    }
}

struct Simulator {
    program_id: u64,
    activity: Activity,
    current_limit: f64,
    step: u64,
    phase: f64,
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
            grf: pose.grf().map(|grf| grf + self.rng.noise(0.8)),
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
    load: f64,
    center_of_pressure: f64,
}

impl Pose {
    fn grf(&self) -> [f64; 4] {
        GRF_POSITIONS.map(|at| GRF_SCALE * self.load * (1.0 - (self.center_of_pressure - at).abs()).max(0.0))
    }
}

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

struct Stride {
    period: f64,
    states: &'static [(f64, f64)],
    knee: (f64, &'static [Bump]),
    ankle: (f64, &'static [Bump]),
    knee_current: &'static [Bump],
    ankle_current: &'static [Bump],
    stance: f64,
    load: &'static [Bump],
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
            load: gain * bumps(self.load, phase),
            center_of_pressure: smoothstep(0.0, self.stance, phase),
        }
    }
}

const WALKING_STATES: &[(f64, f64)] = &[(0.15, 1.0), (0.40, 2.0), (0.50, 2.5), (0.62, 3.0), (1.0, 4.0)];

const STAIR_STATES: &[(f64, f64)] = &[(0.20, 1.0), (0.45, 2.0), (0.60, 3.0), (1.0, 4.0)];

const WALKING: Stride = Stride {
    period: 1.1,
    states: WALKING_STATES,
    knee: (3.0, &[bump(15.0, 0.13, 0.05), bump(58.0, 0.72, 0.085)]),
    ankle: (2.0, &[bump(-6.0, 0.04, 0.03), bump(10.0, 0.42, 0.11), bump(-18.0, 0.62, 0.045)]),
    knee_current: &[bump(4200.0, 0.12, 0.05), bump(900.0, 0.66, 0.05), bump(-2600.0, 0.88, 0.04)],
    ankle_current: &[bump(1200.0, 0.08, 0.03), bump(-4500.0, 0.55, 0.06), bump(700.0, 0.78, 0.06)],
    stance: 0.6,
    load: &[bump(1.0, 0.13, 0.07), bump(0.75, 0.30, 0.10), bump(1.05, 0.47, 0.06)],
};

const STAIR_ASCENT: Stride = Stride {
    period: 1.5,
    states: STAIR_STATES,
    knee: (6.0, &[bump(55.0, 0.0, 0.12), bump(75.0, 0.78, 0.12)]),
    ankle: (0.0, &[bump(8.0, 0.10, 0.08), bump(-12.0, 0.55, 0.06), bump(10.0, 0.82, 0.08)]),
    knee_current: &[bump(6500.0, 0.18, 0.09), bump(-1500.0, 0.75, 0.06)],
    ankle_current: &[bump(-3800.0, 0.50, 0.07), bump(900.0, 0.85, 0.06)],
    stance: 0.6,
    load: &[bump(0.9, 0.15, 0.08), bump(1.15, 0.38, 0.10)],
};

const STAIR_DESCENT: Stride = Stride {
    period: 1.4,
    states: STAIR_STATES,
    knee: (8.0, &[bump(85.0, 0.62, 0.15)]),
    ankle: (0.0, &[bump(-18.0, 0.02, 0.05), bump(18.0, 0.45, 0.12), bump(-15.0, 0.90, 0.06)]),
    knee_current: &[bump(-5500.0, 0.40, 0.12), bump(800.0, 0.80, 0.06)],
    ankle_current: &[bump(2500.0, 0.08, 0.05), bump(-1800.0, 0.45, 0.10)],
    stance: 0.55,
    load: &[bump(1.25, 0.10, 0.06), bump(0.8, 0.35, 0.10)],
};

const UPSLOPE_WALKING: Stride = Stride {
    period: 1.2,
    states: WALKING_STATES,
    knee: (6.0, &[bump(22.0, 0.12, 0.06), bump(62.0, 0.72, 0.09)]),
    ankle: (6.0, &[bump(12.0, 0.45, 0.12), bump(-16.0, 0.63, 0.045)]),
    knee_current: &[bump(5200.0, 0.15, 0.06), bump(-2400.0, 0.88, 0.04)],
    ankle_current: &[bump(-5000.0, 0.55, 0.06), bump(900.0, 0.78, 0.06)],
    stance: 0.6,
    load: &[bump(0.9, 0.13, 0.07), bump(0.7, 0.30, 0.10), bump(1.15, 0.48, 0.06)],
};

const DOWNSLOPE_WALKING: Stride = Stride {
    period: 1.15,
    states: WALKING_STATES,
    knee: (5.0, &[bump(25.0, 0.18, 0.07), bump(60.0, 0.72, 0.085)]),
    ankle: (-4.0, &[bump(-10.0, 0.05, 0.03), bump(8.0, 0.42, 0.10), bump(-12.0, 0.62, 0.045)]),
    knee_current: &[bump(-3800.0, 0.20, 0.07), bump(-2600.0, 0.88, 0.04)],
    ankle_current: &[bump(1800.0, 0.08, 0.04), bump(-3000.0, 0.55, 0.06)],
    stance: 0.6,
    load: &[bump(1.15, 0.12, 0.06), bump(0.75, 0.30, 0.10), bump(0.95, 0.46, 0.06)],
};

fn standing(phase: f64, gain: f64) -> Pose {
    let angle = TAU * phase;

    Pose {
        state: if phase < 0.6 { 1.0 } else { 2.0 },
        knee: 4.0 + gain * 2.5 * angle.sin(),
        ankle: 1.0 + gain * 2.0 * (angle + 1.2).sin(),
        knee_current: 900.0 + gain * 500.0 * (angle + 0.4).sin(),
        ankle_current: -500.0 + gain * 350.0 * (angle + 1.6).sin(),
        load: 0.5 + gain * 0.08 * (angle + 0.8).sin(),
        center_of_pressure: 0.45 + gain * 0.08 * (angle + 2.0).sin(),
    }
}

fn sitting(phase: f64, gain: f64) -> Pose {
    let angle = TAU * phase;

    Pose {
        state: if phase < 0.7 { 1.0 } else { 2.0 },
        knee: 88.0 + gain * 3.0 * angle.sin(),
        ankle: 4.0 + gain * 2.0 * (angle + 0.5).sin(),
        knee_current: gain * 150.0 * angle.cos(),
        ankle_current: gain * 80.0 * (angle + 0.5).cos(),
        load: 0.06 + gain * 0.02 * angle.sin(),
        center_of_pressure: 0.4,
    }
}

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
        load: 0.1
            + 0.4 * standing
            + gain * (bump(0.6, 0.40, 0.05).at_phase(phase) + bump(0.3, 0.90, 0.04).at_phase(phase)),
        center_of_pressure: 0.4 + gain * bump(0.25, 0.38, 0.06).at_phase(phase),
    }
}

fn smoothstep(from: f64, to: f64, x: f64) -> f64 {
    let x = ((x - from) / (to - from)).clamp(0.0, 1.0);
    x * x * (3.0 - 2.0 * x)
}

struct Rng(u64);

impl Rng {
    fn new() -> Self {
        Self(uuid::Uuid::new_v4().as_u128() as u64 | 1)
    }

    fn unit(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }

    fn noise(&mut self, amplitude: f64) -> f64 {
        (self.unit() + self.unit() - 1.0) * amplitude
    }
}

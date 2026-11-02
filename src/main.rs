use std::io;
use std::net::UdpSocket;
use std::sync::Arc;
use std::thread;

use crate::mqtt::MqttPublisher;
use crate::prosthesis::{MockProsthesis, Prosthesis, RealProsthesis};

mod config;
mod mqtt;
mod models;
mod prosthesis;

const UDP_LOGS_PORT: u16 = 25000;

fn get_local_ip() -> String {
    match UdpSocket::bind("0.0.0.0:0") {
        Ok(socket) => {
            if socket.connect("8.8.8.8:80").is_ok() {
                socket.local_addr()
                    .map(|addr| addr.ip().to_string())
                    .unwrap_or_else(|_| "127.0.0.1".to_string())
            } else {
                "127.0.0.1".to_string()
            }
        }
        Err(_) => "127.0.0.1".to_string(),
    }
}

const MAX_DATAGRAM_SIZE: usize = 65_535;

fn listen_for_udp_logs(publisher: Option<Arc<MqttPublisher>>) -> io::Result<()> {
    let socket = UdpSocket::bind(("0.0.0.0", UDP_LOGS_PORT))?;
    let local_ip = get_local_ip();
    println!("Listening for UDP logs on {}:{}", local_ip, UDP_LOGS_PORT);

    let mut buffer = vec![0u8; MAX_DATAGRAM_SIZE];
    loop {
        match socket.recv_from(&mut buffer) {
            Ok((n, _addr)) => {
                let log = String::from_utf8_lossy(&buffer[..n]);
                if let Some(ref pub_) = publisher {
                    pub_.publish(log.trim());
                }
            }
            Err(e) => eprintln!("Failed to read from socket: {e}"),
        }
    }
}

struct Options {
    mock: bool,
    startup_check: bool,
}

fn parse_options() -> Options {
    let mut options = Options {
        mock: false,
        startup_check: true,
    };

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--mock" => options.mock = true,
            "--no-startup-check" => options.startup_check = false,
            other => eprintln!(
                "Ignoring unknown argument {other:?} (the options are --mock and --no-startup-check)"
            ),
        }
    }

    options
}

fn run() -> io::Result<i32> {
    let options = parse_options();

    let prosthesis: Arc<dyn Prosthesis> = if options.mock {
        eprintln!(
            "[MOCK] Mock mode: the prosthesis is simulated. Commands always succeed without starting \
             RoboticLegControl, and made-up sensor readings are sent to UDP port {}.",
            UDP_LOGS_PORT
        );
        MockProsthesis::spawn(UDP_LOGS_PORT)
    } else {
        if !options.startup_check {
            eprintln!(
                "[CMD] Startup check disabled (--no-startup-check): a start succeeds as soon as \
                 RoboticLegControl is launched, without watching its buffer."
            );
        }

        Arc::new(RealProsthesis {
            startup_check: options.startup_check,
        })
    };

    let publisher = match MqttPublisher::from_env(prosthesis) {
        Ok(publisher) => Some(Arc::new(publisher)),
        Err(err) => {
            eprintln!("MQTT init error: {err}");
            None
        }
    };

    let udp_logs_thread = thread::spawn(move || listen_for_udp_logs(publisher));

    let _logs_result = udp_logs_thread
        .join()
        .map_err(|_| io::Error::other("UDP logs thread panicked"))?;

    Ok(0)
}

fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    match run() {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("prosthesis-communication-bridge error: {err}");
            std::process::exit(1);
        }
    }
}

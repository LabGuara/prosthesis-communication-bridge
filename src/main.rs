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

fn mock_requested() -> bool {
    let mut mock = false;

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--mock" => mock = true,
            other => eprintln!("Ignoring unknown argument {other:?} (the only option is --mock)"),
        }
    }

    mock
}

fn run() -> io::Result<i32> {
    let prosthesis: Arc<dyn Prosthesis> = if mock_requested() {
        eprintln!(
            "[MOCK] Mock mode: the prosthesis is simulated. Commands always succeed without starting \
             RoboticLegControl, and made-up sensor readings are sent to UDP port {}.",
            UDP_LOGS_PORT
        );
        MockProsthesis::spawn(UDP_LOGS_PORT)
    } else {
        Arc::new(RealProsthesis)
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
            eprintln!("prosthesis-logger error: {err}");
            std::process::exit(1);
        }
    }
}

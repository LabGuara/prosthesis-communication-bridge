use std::io::{self, Read};
use std::net::{TcpListener, UdpSocket};
use std::sync::Arc;
use std::thread;

use crate::mqtt::MqttPublisher;

mod config;
mod mqtt;
mod models;

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

fn listen_for_tcp_commands(publisher: Option<Arc<MqttPublisher>>) -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:25001")?;
    let local_ip = get_local_ip();
    println!("Listening for TCP commands on {}:25001", local_ip);

    for stream in listener.incoming() {
        match stream {
            Ok(mut socket) => {
                let mut buffer = [0; 1024];
                while let Ok(n) = socket.read(&mut buffer) {
                    if n == 0 { break; } // Connection closed by client
                    let command = String::from_utf8_lossy(&buffer[..n]);
                    println!("Received command: {}", command.trim());
                    if let Some(ref pub_) = publisher {
                        pub_.publish(command.trim());
                    }
                }
            }
            Err(e) => eprintln!("Connection failed: {e}"),
        }
    }

    Ok(())
}

fn listen_for_tcp_logs(publisher: Option<Arc<MqttPublisher>>) -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:25002")?;
    let local_ip = get_local_ip();
    println!("Listening for TCP logs on {}:25002", local_ip);

    for stream in listener.incoming() {
        match stream {
            Ok(mut socket) => {
                let mut buffer = [0; 1024];
                match socket.read(&mut buffer) {
                    Ok(n) => {
                        let log = String::from_utf8_lossy(&buffer[..n]);
                        println!("{}", log.trim());
                        if let Some(ref pub_) = publisher {
                            pub_.publish(log.trim());
                        }
                    }
                    Err(e) => eprintln!("Failed to read from socket: {e}"),
                }
            }
            Err(e) => eprintln!("Connection failed: {e}"),
        }
    }

    Ok(())
}

fn run() -> io::Result<i32> {
    let publisher = match MqttPublisher::from_env() {
        Ok(publisher) => Some(Arc::new(publisher)),
        Err(err) => {
            eprintln!("MQTT init error: {err}");
            None
        }
    };

    let publisher_commands = publisher.clone();
    let publisher_logs = publisher.clone();

    let tcp_commands_thread = thread::spawn(move || listen_for_tcp_commands(publisher_commands));
    let tcp_logs_thread = thread::spawn(move || listen_for_tcp_logs(publisher_logs));

    let _commands_result = tcp_commands_thread
        .join()
        .map_err(|_| io::Error::other("TCP commands thread panicked"))?;

    let _logs_result = tcp_logs_thread
        .join()
        .map_err(|_| io::Error::other("TCP logs thread panicked"))?;

    Ok(0)
}

fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    match run() {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("prothesis-logger error: {err}");
            std::process::exit(1);
        }
    }
}

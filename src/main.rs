use std::env;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;

use crate::mqtt::MqttPublisher;

mod config;
mod mqtt;
mod models;

fn read_and_forward_stream<R: Read>(
    mut reader: R,
    mut destination: impl Write,
    publisher: Option<Arc<MqttPublisher>>,
) -> io::Result<()> {
    let buffered_reader = BufReader::new(&mut reader);

    for line in buffered_reader.lines() {
        let line = line?;

        if let Some(publisher) = &publisher {
            publisher.publish(&line);
        }

        writeln!(destination, "{line}")?;
        destination.flush()?;
    }

    Ok(())
}

fn forward_commands_to_child_stdin(
    receiver: mpsc::Receiver<String>,
    mut child_stdin: impl Write,
) -> io::Result<()> {
    for message in receiver {
        writeln!(child_stdin, "{message}")?;
        child_stdin.flush()?;
    }

    Ok(())
}

fn run() -> io::Result<i32> {
    let (command_sender, command_receiver) = mpsc::channel::<String>();

    let publisher = match MqttPublisher::from_env(command_sender) {
        Ok(publisher) => Some(Arc::new(publisher)),
        Err(err) => {
            eprintln!("MQTT init error: {err}");
            None
        }
    };

    let mut arguments = env::args().skip(1);
    
    let Some(executable) = arguments.next() else {
        eprintln!("Usage: prothesis-logger <executable> [args...]");
        return Ok(2);
    };

    let child_args: Vec<String> = arguments.collect();

    let mut child = Command::new(&executable)
        .args(&child_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("failed to capture child stdin"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("failed to capture child stdout"))?;

    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("failed to capture child stderr"))?;

    let stdout_publisher = publisher.clone();
    let stdout_thread = thread::spawn(move || {
        let destination = io::stdout();
        let destination_lock = destination.lock();
        read_and_forward_stream(stdout, destination_lock, stdout_publisher)
    });

    let stderr_publisher = publisher.clone();
    let stderr_thread = thread::spawn(move || {
        let destination = io::stderr();
        let destination_lock = destination.lock();
        read_and_forward_stream(stderr, destination_lock, stderr_publisher)
    });

    let stdin_thread = thread::spawn(move || forward_commands_to_child_stdin(command_receiver, child_stdin));

    let status = child.wait()?;

    let stdout_result = stdout_thread
        .join()
        .map_err(|_| io::Error::other("stdout thread panicked"))?;

    let stderr_result = stderr_thread
        .join()
        .map_err(|_| io::Error::other("stderr thread panicked "))?;

    drop(publisher);

    let stdin_result = stdin_thread
        .join()
        .map_err(|_| io::Error::other("stdin thread panicked"))?;

    stdout_result?;
    stderr_result?;
    stdin_result?;

    Ok(status.code().unwrap_or(1))
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

use std::env;
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::thread;

fn read_and_forward_stream<R: Read>(
    mut reader: R,
    mut destination: impl Write
) -> io::Result<()> {
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        destination.write_all(&buffer[..bytes_read])?;
        destination.flush()?;
    }

    Ok(())
}

fn run() -> io::Result<i32> {
    let mut arguments = env::args().skip(1);
    
    let Some(executable) = arguments.next() else {
        eprintln!("Usage: prothesis-logger <executable> [args...]");
        return Ok(2);
    };

    let child_args: Vec<String> = arguments.collect();

    let mut child = Command::new(&executable)
        .args(&child_args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("failed to capture child stdout"))?;

    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("failed to capture child stderr"))?;

    let stdout_thread = thread::spawn(move || {
        let destination = io::stdout();
        let destination_lock = destination.lock();
        read_and_forward_stream(stdout, destination_lock)
    });

    let stderr_thread = thread::spawn(move || {
        let destination = io::stderr();
        let destination_lock = destination.lock();
        read_and_forward_stream(stderr, destination_lock)
    });

    let status = child.wait()?;

    let stdout_result = stdout_thread
        .join()
        .map_err(|_| io::Error::other("stdout thread panicked"))?;

    let stderr_result = stderr_thread
        .join()
        .map_err(|_| io::Error::other("stderr thread panicked"))?;

    stdout_result?;
    stderr_result?;

    Ok(status.code().unwrap_or(1))
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("prothesis-logger error: {err}");
            std::process::exit(1);
        }
    }
}

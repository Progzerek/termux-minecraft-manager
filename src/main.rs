use std::{
    io::{BufRead, BufReader, Error, ErrorKind, Stdin, Stdout, Write},
    os::unix::io,
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

use crate::State::Offline;

enum State {
    Offline,
    Starting,
    Online,
    Stopping,
    Crashed,
}

struct ServerManager {
    instance: Option<Child>,
    stdin: Option<ChildStdin>,
    status: State,
}

fn run_sh(path: &str) -> std::io::Result<Child> {
    let result: Result<Child, std::io::Error> = Command::new("bash")
        .arg("run.sh")
        .arg("nogui")
        .current_dir(path)
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    result
}

fn dump_logs(stdout: ChildStdout) {
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);

        for line in reader.lines() {
            let line = line.expect("failed to read stdout");
            println!("[out] {line}");
        }
    });
}

fn send_command(command: &str, server: &mut ServerManager) -> std::io::Result<()> {
    let Some(stdin) = server.stdin.as_mut() else {
        // no server / no stdin
        return Err(Error::new(
            ErrorKind::NotFound,
            "Stdin not found! Check if the server is running",
        ));
    };
    let line = format!("{command}\n");
    stdin.write_all(line.as_bytes())?;
    stdin.flush()?;
    Ok(())
}

fn main() {
    let path = "/data/data/com.termux/files/home/servers/anvil";
    println!("Hello, world!");
    match run_sh(&path) {
        Ok(mut child) => {
            print!("Starting...");
            let stdout = child.stdout.take().unwrap();
            let stdin = child.stdin.take().unwrap();
            let stderr = child.stderr.take();
            let mut server = ServerManager {
                instance: Some(child),
                status: State::Starting,
                stdin: Some(stdin),
            };
            dump_logs(stdout);
            print!("starting loop")
            for line in std::io::stdin().lines() {
                let command = line.unwrap();
                send_command(command.trim(), &mut server).unwrap();
            }
            print!("ending loop")
        }
        Err(error) => {
            print!("starting failed {error}")
        }
    }
    print!("main exited")
}

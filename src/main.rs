use std::{
    io::{BufRead, BufReader, Stdout},
    path::Path,
    process::{Child, ChildStdout, Command, Stdio},
};

enum State {
    Offline,
    Starting,
    Online,
    Stopping,
    Crashed,
}

struct ServerManager {
    instance: Option<Child>,
    status: State,
}

fn run_sh(path: &str) -> std::io::Result<Child> {
    let result: Result<Child, std::io::Error> = Command::new("bash")
        .arg("run.sh")
        .arg("nogui")
        .current_dir(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    result
}

fn dumpLogs(stdout: ChildStdout) {
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);

        for line in reader.lines() {
            let line = line.expect("failed to read stdout");
            println!("[out] {line}");
        }
    });
}

fn main() {
    let path = "/data/data/com.termux/files/home/servers/anvil";
    println!("Hello, world!");
    match run_sh(&path) {
        Ok(mut child) => {
            print!("Starting...");
            let stdout = child.stdout.take().unwrap();

            //let stderr = child.stderr.take();
            dumpLogs(stdout)
        }
        Err(error) => {
            print!("starting failed {error}")
        }
    }
}

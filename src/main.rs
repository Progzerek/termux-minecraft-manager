use std::{
    io::{BufRead, BufReader, Error, ErrorKind, Write},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio},
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
    stdin: Option<ChildStdin>,
    status: State,
}

impl ServerManager {
    fn new() -> Self {
        Self {
            instance: None,
            stdin: None,
            status: State::Offline,
        }
    }

    fn start(&mut self, server_dir: &str) -> std::io::Result<()> {
        match self.status {
            State::Offline | State::Crashed => {
                match run_sh(&server_dir) {
                    Ok(child) => {
                        let mut child = child;
                        self.status = State::Starting;
                        let stdout = child
                            .stdout
                            .take()
                            .expect("Failed to pipe Server Output (stdout)");
                        let stdin = child
                            .stdin
                            .take()
                            .expect("Failed to pipe Server Input (stdin)");
                        let stderr = child
                            .stderr
                            .take()
                            .expect("Failed to pipe Server Error (stderr)");
                        self.instance = Some(child);
                        self.stdin = Some(stdin);
                        dump_logs(stdout);
                        dump_errors(stderr);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
                Ok(())
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "server is already starting, running, or stopping",
                ));
            }
        }
    }

    //fn send_command(&mut self, command: &str) -> std::io::Result<()> {
    // writes to self.stdin
    //}
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

fn dump_errors(stdout: ChildStderr) {
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);

        for line in reader.lines() {
            let line = line.expect("failed to read stderr");
            println!("[Error] {line}");
        }
    });
}

fn send_command(command: &str, server: &mut ServerManager) -> std::io::Result<()> {
    let Some(stdin) = server.stdin.as_mut() else {
        // no server / no stdin
        return Err(Error::new(
            ErrorKind::NotFound,
            "Server Input not found! Check if the server is running",
        ));
    };
    let line = format!("{command}\n");
    stdin.write_all(line.as_bytes())?;
    stdin.flush()?;
    Ok(())
}

fn old_main() {
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
            print!("starting loop");
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

fn main() {
    let mut server = ServerManager::new();
    let path = "/data/data/com.termux/files/home/servers/anvil";
    match server.start(path) {
        Ok(_) => print!("success"),
        Err(e) => print!("{e}"),
    };
}

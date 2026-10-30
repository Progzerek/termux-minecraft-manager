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
                        println!("Start success");
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
                        println!("Dumping logs");
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

    fn send_command(&mut self, command: &str) -> std::io::Result<()> {
        let Some(stdin) = self.stdin.as_mut() else {
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
}

fn run_sh(path: &str) -> std::io::Result<Child> {
    println!("Running start command");
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

fn dump_errors(stderr: ChildStderr) {
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);

        for line in reader.lines() {
            let line = line.expect("failed to read stderr");
            println!("[Error] {line}");
        }
    });
}

fn main() {
    let mut server = ServerManager::new();
    let path = "/data/data/com.termux/files/home/servers/anvil";
    match server.start(path) {
        Ok(_) => println!("success"),
        Err(e) => println!("{e}"),
    };
    for line in std::io::stdin().lines() {
        let command = line.unwrap();
        server.send_command(command.trim()).unwrap();
    }
}

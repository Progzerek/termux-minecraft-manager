use std::{
    env,
    io::{BufRead, BufReader, Error, ErrorKind, Write},
};
use tokio::process::{Child, ChildStdin, ChildStdout, ChildStderr, Command};
use tokio::io::{AsyncWriteExt, BufReader};
#[derive(PartialEq)]
enum State {
    Offline,
    Running,
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

    pub async fn start(&mut self, server_dir: &str) -> std::io::Result<()> {
        match self.status {
            State::Offline => {
                match run_sh(&server_dir) {
                    Ok(child) => {
                        println!("Start success");
                        let mut child = child;
                        self.status = State::Running;
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
                        handle_logs(stdout);
                        handle_err(stderr);
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

    pub async fn send_command(&mut self, command: &str) -> std::io::Result<()> {
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

    pub async fn stop(&mut self) -> std::io::Result<()> {
        if self.status != State::Running {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Server is not Online",
            ));
        };
        self.send_command("stop").await;
        let Some(child) = self.instance.as_mut() else {
            return Err(Error::new(ErrorKind::NotFound, "No server to stop!"));
        };
        child.wait()?;
        self.status = State::Offline;
        self.instance = None;
        println!("offline");
        Ok(())
    }

    pub async fn log_loop(&mut self, path: &str) -> std::io::Result<()> {
        for line in std::io::stdin().lines() {
            let command = line?;
            match command.trim() {
                "stopCMD" => {
                    if let Err(e) = self.stop() {
                        eprintln!("{e}");
                    }
                }
                "startCMD" => {
                    if let Err(e) = self.start(path) {
                        eprintln!("{e}");
                    }
                }
                "exit" => break,
                _ => {
                    if let Err(e) = self.send_command(command.trim()) {
                        eprintln!("{e}");
                    }
                }
            }
        }
        Ok(())
    }
    pub fn exit(&mut self) -> Result<(), Error> {
        if self.status == State::Running {
            if let Err(e) = self.stop() {
                eprintln!("{e}");
                return Err(e);
            }
        }
        println!("Exiting program");
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

fn handle_logs(stdout: ChildStdout) {
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);

        for line in reader.lines() {
            let line = line.expect("failed to read stdout");

            println!("[out] {line}");
        }
    });
}

fn handle_err(stderr: ChildStderr) {
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);

        for line in reader.lines() {
            let line = line.expect("failed to read stderr");
            println!("[Error] {line}");
        }
    });
}

pub fn init() -> std::io::Result<()> {
    let mut server = ServerManager::new();
    let path = env::var("RUN_SH_PATH").expect("Expected path in env");
    match server.start(path.trim()) {
        Ok(_) => println!("success"),
        Err(e) => println!("{e}"),
    };
    server.log_loop(&path);
    Ok(())
}

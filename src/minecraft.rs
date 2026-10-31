use std::{env, process::Stdio};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin},
    process::{Child, ChildStdin, Command},
};

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
        if self.status != State::Offline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "server is already starting, running, or stopping",
            ));
        }

        let mut child = run_sh(server_dir).await?;

        let stdout = child
            .stdout
            .take()
            .expect("Failed to pipe Server Output (stdout)");

        let stderr = child
            .stderr
            .take()
            .expect("Failed to pipe Server Error (stderr)");

        let server_stdin = child
            .stdin
            .take()
            .expect("Failed to pipe Server Input (stdin)");

        self.instance = Some(child);
        self.stdin = Some(server_stdin);
        self.status = State::Running;

        println!("Start success");
        println!("Dumping logs");

        tokio::spawn(handle_logs(stdout));
        tokio::spawn(handle_err(stderr));

        Ok(())
    }

    pub async fn send_command(&mut self, command: &str) -> std::io::Result<()> {
        let Some(stdin) = self.stdin.as_mut() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Server Input not found! Check if the server is running",
            ));
        };

        let line = format!("{command}\n");
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await?;

        Ok(())
    }

    pub async fn stop(&mut self) -> std::io::Result<()> {
        if self.status != State::Running {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Server is not Online",
            ));
        }

        self.send_command("stop").await?;

        let Some(child) = self.instance.as_mut() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No server to stop!",
            ));
        };

        child.wait().await?;

        self.status = State::Offline;
        self.instance = None;
        self.stdin = None;

        println!("offline");

        Ok(())
    }

    pub async fn log_loop(&mut self, path: &str) -> std::io::Result<()> {
        let mut input = BufReader::new(stdin()).lines();

        while let Some(line) = input.next_line().await? {
            let command = line.trim();

            match command {
                "stop" => {
                    if let Err(e) = self.stop().await {
                        eprintln!("{e}");
                    }
                }
                "startCMD" => {
                    if let Err(e) = self.start(path).await {
                        eprintln!("{e}");
                    }
                }
                "exit" => break,
                _ => {
                    if let Err(e) = self.send_command(command).await {
                        eprintln!("{e}");
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn exit(&mut self) -> Result<(), std::io::Error> {
        if self.status == State::Running {
            self.stop().await?;
        }

        println!("Exiting program");

        Ok(())
    }
}

async fn run_sh(path: &str) -> std::io::Result<Child> {
    println!("Running start command");

    Command::new("bash")
        .arg("run.sh")
        .arg("nogui")
        .current_dir(path)
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
}

async fn handle_logs(stdout: tokio::process::ChildStdout) {
    let mut lines = BufReader::new(stdout).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("[out] {line}");
    }
}

async fn handle_err(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("[Error] {line}");
    }
}

pub async fn init() -> std::io::Result<()> {
    let mut server = ServerManager::new();
    let path = env::var("RUN_SH_PATH").expect("Expected path in env");
    let path = path.trim().to_string();

    if let Err(e) = server.start(&path).await {
        println!("{e}");
    } else {
        println!("success");
    }

    server.log_loop(&path).await?;

    server.exit().await?;

    Ok(())
}

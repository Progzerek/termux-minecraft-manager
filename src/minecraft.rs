use std::collections::VecDeque;
use std::fmt;
use std::{env, process::Stdio, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin},
    process::{Child, ChildStdin, Command},
    sync::Mutex,
};

#[derive(PartialEq)]
enum State {
    Offline,
    Starting,
    Online,
    Stopping,
}

#[derive(Debug)]
pub enum ServerError {
    NotOnline,
    Busy,
    NoProcess,
    MissingPipe { pipe: String },
    FailedPipe { pipe: String },
    Io(std::io::Error),
    CommandFailed { command: String, reason: String },
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerError::Busy => write!(f, "Server is bussy Starting/Stopping"),
            ServerError::NotOnline => write!(f, "Server is not online"),
            ServerError::NoProcess => write!(f, "No server process found"),
            ServerError::MissingPipe { pipe } => write!(f, "Server pipe {pipe} is missing"),
            ServerError::Io(error) => write!(f, "I/O error: {error}"),
            ServerError::CommandFailed { command, reason } => {
                write!(f, "Command {command} failed: {reason}")
            }
            ServerError::FailedPipe { pipe } => write!(f, "Pipe {pipe} failed"),
        }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        ServerError::Io(error)
    }
}

const MAX_LOG_LINES: usize = 100;

pub struct ServerManager {
    instance: Option<Child>,
    stdin: Option<ChildStdin>,
    status: State,
    logs: VecDeque<String>,
}

impl ServerManager {
    pub fn new() -> Self {
        Self {
            instance: None,
            stdin: None,
            status: State::Offline,
            logs: VecDeque::with_capacity(100),
        }
    }

    pub fn status_text(&self) -> &'static str {
        match self.status {
            State::Online => "Online",
            State::Offline => "Offline",
            State::Starting => "Starting",
            State::Stopping => "Stopping",
        }
    }

    pub fn recent_logs(&self, count: usize) -> String {
        self.logs
            .iter()
            .rev()
            .take(count)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn push_log(&mut self, line: String) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
        }

        self.logs.push_back(line);
    }

    pub async fn start(
        &mut self,
        server_dir: &str,
        server_arc: Arc<Mutex<ServerManager>>,
    ) -> Result<(), ServerError> {
        if self.status != State::Offline {
            return Err(ServerError::Busy);
        }

        let result = run_wake_lock_command(true).await;
        println!("Failed to run wake_lock: {:?}", result);

        let mut child = run_sh(server_dir).await?;

        let stdout = child.stdout.take().ok_or(ServerError::FailedPipe {
            pipe: format!("StdOut"),
        })?;

        let stderr = child.stderr.take().ok_or(ServerError::FailedPipe {
            pipe: format!("StdErr"),
        })?;

        let server_stdin = child.stdin.take().ok_or(ServerError::FailedPipe {
            pipe: format!("StdIn"),
        })?;

        self.instance = Some(child);
        self.stdin = Some(server_stdin);
        self.status = State::Starting;

        println!("Start success");
        println!("Dumping logs");

        tokio::spawn(handle_logs(stdout, server_arc));
        tokio::spawn(handle_err(stderr));

        Ok(())
    }

    pub async fn send_command(&mut self, command: &str) -> Result<(), ServerError> {
        if self.status != State::Online {
            return Err(ServerError::NotOnline);
        }
        let Some(stdin) = self.stdin.as_mut() else {
            return Err(ServerError::MissingPipe {
                pipe: format!("Stdin"),
            });
        };

        let line = format!("{command}\n");
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await?;

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), ServerError> {
        self.send_command("stop").await?;
        self.status = State::Stopping;

        let Some(child) = self.instance.as_mut() else {
            return Err(ServerError::NoProcess);
        };

        child.wait().await?;

        self.status = State::Offline;
        self.instance = None;
        self.stdin = None;

        run_wake_lock_command(false).await?;

        println!("offline");

        Ok(())
    }

    pub async fn exit(&mut self) -> Result<(), ServerError> {
        if self.status == State::Online {
            self.stop().await?;
        }
        println!("Exiting program");

        Ok(())
    }
}

async fn run_sh(path: &str) -> Result<tokio::process::Child, std::io::Error> {
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

async fn run_wake_lock_command(lock: bool) -> Result<(), ServerError> {
    let command = if lock {
        "termux-wake-lock"
    } else {
        "termux-wake-unlock"
    };
    let status = Command::new(command).status().await?;
    println!("{command} sent");

    if !status.success() {
        return Err(ServerError::CommandFailed {
            command: format!("{command}"),
            reason: format!("Failed"),
        });
    }

    Ok(())
}

pub async fn command_loop(
    server_arc: Arc<Mutex<ServerManager>>,
    path: &str,
) -> Result<(), ServerError> {
    let mut input = BufReader::new(stdin()).lines();

    while let Some(line) = input.next_line().await? {
        let command = line.trim();

        match command {
            "stop" => {
                let mut server = server_arc.lock().await;
                if let Err(e) = server.stop().await {
                    eprintln!("{e}");
                }
            }
            "start" => {
                let mut server = server_arc.lock().await;
                if let Err(e) = server.start(path, Arc::clone(&server_arc)).await {
                    eprintln!("{e}");
                }
            }
            "exit" => break,
            _ => {
                let mut server = server_arc.lock().await;
                if let Err(e) = server.send_command(command).await {
                    eprintln!("{e}");
                }
            }
        }
    }

    Ok(())
}

async fn handle_logs(stdout: tokio::process::ChildStdout, server_arc: Arc<Mutex<ServerManager>>) {
    let mut lines = BufReader::new(stdout).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("[out] {line}");
        {
            let mut server = server_arc.lock().await;
            server.push_log(line.clone());
        }

        if line.contains("Done") && line.contains("For help, type") {
            let mut server = server_arc.lock().await;
            if server.status == State::Starting {
                server.status = State::Online;
                println!("Online");
            }
        }
    }
}

async fn handle_err(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("[Error] {line}");
    }
}

pub async fn start_loop(server_arc: Arc<Mutex<ServerManager>>) -> Result<(), ServerError> {
    let path = env::var("RUN_SH_PATH").expect("Expected path in env");
    let path = path.trim().to_string();
    {
        let mut server = server_arc.lock().await;

        if let Err(e) = server.start(&path, Arc::clone(&server_arc)).await {
            println!("{e}");
        } else {
            println!("success");
        }
    }
    command_loop(Arc::clone(&server_arc), &path).await?;
    let mut server = server_arc.lock().await;
    server.exit().await?;
    Ok(())
}

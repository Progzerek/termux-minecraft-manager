use std::{collections::VecDeque, env, fmt, process::Stdio, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin},
    process::{Child, ChildStdin, Command},
    sync::Mutex,
    time::sleep,
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    Offline,
    Starting,
    Online,
    Stopping,
    Crashed,
}

impl ServerState {
    pub fn as_text(self) -> &'static str {
        match self {
            ServerState::Offline => "Offline",
            ServerState::Starting => "Starting",
            ServerState::Online => "Online",
            ServerState::Stopping => "Stopping",
            ServerState::Crashed => "Exited unexpectedly",
        }
    }
}

#[derive(Debug)]
pub enum ServerError {
    NotOnline,
    NotOffline,
    NoProcess,
    MissingPipe { pipe: String },
    FailedPipe { pipe: String },
    Io(std::io::Error),
    CommandFailed { command: String, reason: String },
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerError::NotOffline => write!(f, "Server is not offline"),
            ServerError::NotOnline => write!(f, "Server is not online"),
            ServerError::NoProcess => write!(f, "No server process found"),
            ServerError::MissingPipe { pipe } => {
                write!(f, "Server pipe {pipe} is missing")
            }
            ServerError::FailedPipe { pipe } => {
                write!(f, "Server pipe {pipe} failed")
            }
            ServerError::Io(error) => write!(f, "I/O error: {error}"),
            ServerError::CommandFailed { command, reason } => {
                write!(f, "Command {command} failed: {reason}")
            }
        }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        ServerError::Io(error)
    }
}

const MAX_LOG_LINES: usize = 100;

// ---------------------------------------------------------------------------
// Server manager
// ---------------------------------------------------------------------------

pub struct ServerManager {
    process: Option<Child>,
    stdin: Option<ChildStdin>,
    state: ServerState,
    logs: VecDeque<String>,
}

impl ServerManager {
    pub fn new() -> Self {
        Self {
            process: None,
            stdin: None,
            state: ServerState::Offline,
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
        }
    }

    pub fn state(&self) -> ServerState {
        self.state
    }

    pub fn state_text(&self) -> &'static str {
        self.state.as_text()
    }

    pub fn recent_logs(&self, count: usize) -> String {
        let start = self.logs.len().saturating_sub(count);

        self.logs
            .iter()
            .skip(start)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn push_log(&mut self, line: String) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
        }

        self.logs.push_back(line);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub async fn start(
        &mut self,
        server_dir: &str,
        server: Arc<Mutex<ServerManager>>,
    ) -> Result<(), ServerError> {
        if self.state != ServerState::Offline {
            return Err(ServerError::NotOffline);
        }

        run_wake_lock(true).await?;

        let mut child = start_server_process(server_dir).await?;

        let stdout = child.stdout.take().ok_or_else(|| ServerError::FailedPipe {
            pipe: "stdout".to_string(),
        })?;

        let stderr = child.stderr.take().ok_or_else(|| ServerError::FailedPipe {
            pipe: "stderr".to_string(),
        })?;

        let stdin = child.stdin.take().ok_or_else(|| ServerError::FailedPipe {
            pipe: "stdin".to_string(),
        })?;

        self.process = Some(child);
        self.stdin = Some(stdin);
        self.state = ServerState::Starting;

        println!("Server process started");

        tokio::spawn(read_server_stdout(stdout, Arc::clone(&server)));
        tokio::spawn(read_server_stderr(stderr));
        tokio::spawn(supervise_server(Arc::clone(&server)));

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), ServerError> {
        if self.state != ServerState::Online {
            return Err(ServerError::NotOnline);
        }

        self.send_console_command("stop").await?;
        self.state = ServerState::Stopping;

        let Some(process) = self.process.as_mut() else {
            return Err(ServerError::NoProcess);
        };

        process.wait().await?;
        self.cleanup().await;

        println!("Server stopped");

        Ok(())
    }

    pub async fn exit(&mut self) -> Result<(), ServerError> {
        if self.state == ServerState::Online {
            self.stop().await?;
        }

        println!("Exiting program");

        Ok(())
    }

    async fn cleanup(&mut self) {
        self.state = ServerState::Offline;
        self.process = None;
        self.stdin = None;

        if let Err(error) = run_wake_lock(false).await {
            eprintln!("{error}");
        }

        println!("Server state cleaned up");
    }

    // -----------------------------------------------------------------------
    // Console commands
    // -----------------------------------------------------------------------

    pub async fn send_console_command(&mut self, command: &str) -> Result<(), ServerError> {
        if self.state != ServerState::Online {
            return Err(ServerError::NotOnline);
        }

        let Some(stdin) = self.stdin.as_mut() else {
            return Err(ServerError::MissingPipe {
                pipe: "stdin".to_string(),
            });
        };

        let line = format!("{command}\n");

        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await?;

        Ok(())
    }

    pub async fn get_players(&mut self) -> Result<Vec<String>, ServerError> {
        self.send_console_command("list").await?;

        // Give the server a moment to write its response to stdout.
        sleep(Duration::from_millis(100)).await;

        for line in self.logs.iter().rev() {
            if line.contains("players online:") {
                return Ok(parse_player_names(line));
            }
        }

        Ok(Vec::new())
    }

    // -----------------------------------------------------------------------
    // Process supervision
    // -----------------------------------------------------------------------

    async fn check_process(&mut self) -> Result<ServerState, std::io::Error> {
        let Some(process) = self.process.as_mut() else {
            self.cleanup().await;
            return Ok(ServerState::Offline);
        };

        match process.try_wait()? {
            Some(_) => {
                self.state = ServerState::Crashed;
                self.cleanup().await;

                Ok(ServerState::Offline)
            }
            None => Ok(self.state),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_player_names(line: &str) -> Vec<String> {
    let Some((_, player_names)) = line.split_once("players online:") else {
        return Vec::new();
    };

    let player_names = player_names.trim();

    if player_names.is_empty() {
        return Vec::new();
    }

    player_names
        .split(',')
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

async fn start_server_process(server_dir: &str) -> Result<Child, std::io::Error> {
    println!("Starting server process");

    Command::new("bash")
        .arg("run.sh")
        .arg("nogui")
        .current_dir(server_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
}

async fn run_wake_lock(lock: bool) -> Result<(), ServerError> {
    let command = if lock {
        "termux-wake-lock"
    } else {
        "termux-wake-unlock"
    };

    let status = Command::new(command).status().await?;

    if !status.success() {
        return Err(ServerError::CommandFailed {
            command: command.to_string(),
            reason: "command returned a non-zero exit code".to_string(),
        });
    }

    println!("{command} executed");

    Ok(())
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

async fn supervise_server(server: Arc<Mutex<ServerManager>>) {
    loop {
        sleep(Duration::from_secs(1)).await;

        let mut server = server.lock().await;

        if server.process.is_none() {
            break;
        }

        match server.check_process().await {
            Ok(ServerState::Offline) => break,
            Ok(_) => {}
            Err(error) => {
                eprintln!("Server supervision failed: {error}");
                break;
            }
        }
    }
}

async fn read_server_stdout(
    stdout: tokio::process::ChildStdout,
    server: Arc<Mutex<ServerManager>>,
) {
    let mut lines = BufReader::new(stdout).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("[stdout] {line}");

        {
            let mut server = server.lock().await;
            server.push_log(line.clone());
        }

        if line.contains("Done") && line.contains("For help, type") {
            let mut server = server.lock().await;

            if server.state == ServerState::Starting {
                server.state = ServerState::Online;
                println!("Server is online");
            }
        }
    }
}

async fn read_server_stderr(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("[stderr] {line}");
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn start_loop(server_arc: Arc<Mutex<ServerManager>>) -> Result<(), ServerError> {
    let server_dir = env::var("RUN_SH_PATH")
        .expect("RUN_SH_PATH environment variable is required")
        .trim()
        .to_string();

    {
        let mut server = server_arc.lock().await;

        if let Err(error) = server.start(&server_dir, Arc::clone(&server_arc)).await {
            eprintln!("{error}");
        } else {
            println!("Server startup requested");
        }
    }

    command_loop(Arc::clone(&server_arc), &server_dir).await?;

    let mut server = server_arc.lock().await;
    server.exit().await?;

    Ok(())
}

async fn command_loop(
    server: Arc<Mutex<ServerManager>>,
    server_dir: &str,
) -> Result<(), ServerError> {
    let mut input = BufReader::new(stdin()).lines();

    while let Some(line) = input.next_line().await? {
        let command = line.trim();

        match command {
            "start" => {
                let mut server_l = server.lock().await;

                if let Err(error) = server_l.start(server_dir, Arc::clone(&server)).await {
                    eprintln!("{error}");
                }
            }
            "stop" => {
                let mut server = server.lock().await;

                if let Err(error) = server.stop().await {
                    eprintln!("{error}");
                }
            }
            "exit" => break,
            command => {
                let mut server = server.lock().await;

                if let Err(error) = server.send_console_command(command).await {
                    eprintln!("{error}");
                }
            }
        }
    }

    Ok(())
}

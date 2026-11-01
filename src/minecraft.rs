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

pub struct ServerManager {
    instance: Option<Child>,
    stdin: Option<ChildStdin>,
    status: State,
}

impl ServerManager {
    pub fn new() -> Self {
        Self {
            instance: None,
            stdin: None,
            status: State::Offline,
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

    pub async fn start(
        &mut self,
        server_dir: &str,
        server_arc: Arc<Mutex<ServerManager>>,
    ) -> std::io::Result<()> {
        if self.status != State::Offline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "server is already starting, running, or stopping",
            ));
        }

        let mut child = run_sh(server_dir).await?;

        let stdout = child.stdout.take().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to pipe Server Output (stdout)",
            )
        })?;

        let stderr = child.stderr.take().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to pipe Server Error (stderr)",
            )
        })?;

        let server_stdin = child.stdin.take().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to pipe Server Input (stdin)",
            )
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

    pub async fn send_command(&mut self, command: &str) -> std::io::Result<()> {
        if self.status != State::Online {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Server is not Online",
            ));
        }
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
        self.send_command("stop").await?;
        self.status = State::Stopping;

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

    pub async fn exit(&mut self) -> Result<(), std::io::Error> {
        if self.status == State::Online {
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

pub async fn command_loop(
    server_arc: Arc<Mutex<ServerManager>>,
    path: &str,
) -> std::io::Result<()> {
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
        if line.contains("Done") && line.contains("For help, type:") {
            let mut server = server_arc.lock().await;
            server.status = State::Online;
        }
        println!("[out] {line}");
    }
}

async fn handle_err(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("[Error] {line}");
    }
}

pub async fn start_loop(server_arc: Arc<Mutex<ServerManager>>) -> std::io::Result<()> {
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

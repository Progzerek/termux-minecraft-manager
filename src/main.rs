use std::{
    path::Path,
    process::{Child, Command},
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

fn run_sh() -> Child {
    let path = "/data/data/com.termux/files/home/servers/anvil";

    let child = Command::new("bash")
        .arg("run.sh")
        .current_dir(&path)
        .spawn()
        .expect("Failed to execute command");

    child
}

fn main() {
    println!("Hello, world!");
    let child = run_sh();
}

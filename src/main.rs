use std::process::Child;

enum State{
    Offline,
    Starting,
    Online,
    Stopping,
    Crashed
}

struct ServerManager{
    instance: Option<Child>,
    status: State
}

fn main() {
    println!("Hello, world!");
}

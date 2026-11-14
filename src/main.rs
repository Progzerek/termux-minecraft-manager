use std::sync::Arc;
use tokio::sync::Mutex;

mod ai;
mod discord_bot;
mod minecraft;

#[tokio::main]
async fn main() -> Result<(), minecraft::ServerError> {
    dotenvy::dotenv().expect("Failed to load .env");

    let server = Arc::new(Mutex::new(minecraft::ServerManager::new()));
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

    server.lock().await.set_event_sender(event_tx);

    let discord = tokio::spawn(discord_bot::start_loop(Arc::clone(&server), event_rx));

    let result = minecraft::start_loop(server).await;

    discord.abort();
    let _ = discord.await;

    result
}

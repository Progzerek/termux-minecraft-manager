use std::sync::Arc;
use tokio::sync::Mutex;

mod discord_bot;
mod minecraft;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().expect("Failed to load .env");

    let server = Arc::new(Mutex::new(minecraft::ServerManager::new()));

    let discord = tokio::spawn(discord_bot::start_loop(Arc::clone(&server)));

    let result = minecraft::start_loop(server).await;

    discord.abort();
    let _ = discord.await;

    result
}

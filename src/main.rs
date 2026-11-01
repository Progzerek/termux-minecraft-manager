use std::sync::Arc;
use tokio::sync::Mutex;

mod discord_bot;
mod minecraft;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().expect("Expected .env file");

    let discord = tokio::spawn(discord_bot::start_loop());

    let server = Arc::new(Mutex::new(minecraft::ServerManager::new()));

    minecraft::start_loop(server).await?;

    discord.await?;
    Ok(())
}

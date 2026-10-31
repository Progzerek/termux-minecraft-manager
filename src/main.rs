mod discord_bot;
mod minecraft;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().expect("Expected .env file");

    let discord = tokio::spawn(discord_bot::start_loop());

    minecraft::start_loop().await?;

    discord.await?;
    Ok(())
}

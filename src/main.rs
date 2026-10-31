mod minecraft;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().expect("Expected .env file");
    minecraft::init().await
}

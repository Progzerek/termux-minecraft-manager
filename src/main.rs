mod minecraft;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    minecraft::init().await
}

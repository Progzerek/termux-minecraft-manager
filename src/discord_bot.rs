use std::env;
use std::sync::Arc;

use serenity::async_trait;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::prelude::*;

use crate::minecraft::ServerManager;

struct Handler {
    server: Arc<Mutex<ServerManager>>,
}

#[async_trait]
impl EventHandler for Handler {
    // Set a handler for the `message` event. This is called whenever a new message is received.
    //
    // Event handlers are dispatched through a threadpool, and so multiple events can be
    // dispatched simultaneously.
    async fn message(&self, ctx: Context, msg: Message) {
        if msg.content == "!ping" {
            // Sending a message can fail, due to a network error, an authentication error, or lack
            // of permissions to post in the channel, so log to stdout when some error happens,
            // with a description of it.
            if let Err(why) = msg.channel_id.say(&ctx.http, "Pong!").await {
                println!("Error sending message: {why:?}");
            }
        }
        match msg.content.trim() {
            "!ping" => handle_ping(msg, ctx).await,
            "!status" => handle_status(msg, ctx, &self).await,
            _ => {}
        }
    }

    // Set a handler to be called on the `ready` event. This is called when a shard is booted, and
    // a READY payload is sent by Discord. This payload contains data like the current user's guild
    // Ids, current user data, private channels, and more.
    //
    // In this case, just print what the current user's username is.
    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }
}

async fn handle_ping(msg: Message, ctx: Context) {
    if let Err(why) = msg.channel_id.say(&ctx.http, "Pong!").await {
        println!("Error sending message: {why:?}");
    }
}
async fn handle_status(msg: Message, ctx: Context, handler: &Handler) {
    let server = handler.server.lock().await;
    let status = server.status_text();
    let message = format!("Server status: {}", status);
    if let Err(why) = msg.channel_id.say(&ctx.http, message).await {
        println!("Error sending message: {why:?}");
    }
}

pub async fn start_loop(server_arc: Arc<Mutex<ServerManager>>) -> Result<(), SerenityError> {
    // Configure the client with your Discord bot token in the environment.
    dotenvy::dotenv().expect("Failed to get .env");
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    // Set gateway intents, which decides what events the bot will be notified about
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;

    // Create a new instance of the Client, logging in as a bot. This will automatically prepend
    // your bot token with "Bot ", which is a requirement by Discord for bot users.
    let handler = Handler {
        server: Arc::clone(&server_arc),
    };
    let mut client = Client::builder(&token, intents)
        .event_handler(handler)
        .await
        .expect("Err creating client");

    // Finally, start a single shard, and start listening to events.
    //
    // Shards will automatically attempt to reconnect, and will perform exponential backoff until
    // it reconnects.
    if let Err(why) = client.start().await {
        println!("Client error: {why:?}");
        return Err(why);
    } else {
        Ok(())
    }
}

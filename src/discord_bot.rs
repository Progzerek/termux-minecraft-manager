use std::env;
use std::sync::Arc;
use std::time::Duration;

use serenity::all::EditMessage;
use serenity::async_trait;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::prelude::*;

use crate::ai;
use crate::minecraft::ServerManager;

struct Handler {
    server: Arc<Mutex<ServerManager>>,
    run_path: String,
    discord_id: u64,
}

const AI_SYSTEM_PROMT: &str = "You are a savage, witty Discord bot that roasts users using dark humor. You must ALWAYS respond in exactly ONE single, short, punchy sentence. Do not include any preambles or greetings. Be brutal, deeply sarcastic, and unhinged, but keep it funny. If someone is getting insulted in the message, roast the one in the message, connecting to the previus insult. No one is taking it seriusly so let it all out. Reply in hungarian, or english, depending what the user is using.";

#[async_trait]
impl EventHandler for Handler {
    // Set a handler for the `message` event. This is called whenever a new message is received.
    //
    // Event handlers are dispatched through a threadpool, and so multiple events can be
    // dispatched simultaneously.

    async fn message(&self, ctx: Context, msg: Message) {
        match msg.content.trim() {
            "!ping" => handle_ping(msg, ctx).await,
            "!status" => handle_status(msg, ctx, &self).await,
            "!bence buzi?" => handle_buzi(msg, ctx).await,
            "!stop" => handle_stop(false, msg, ctx, &self).await,
            "!stop force" => handle_stop(true, msg, ctx, &self).await,
            "!start" => handle_start(msg, ctx, &self).await,
            "!list" => handle_list(msg, ctx, &self).await,
            "!help" => handle_help(msg, ctx).await,
            "!tps" => send_command("neoforge tps", msg, ctx, &self).await,
            "!logs" => handle_logs(msg, ctx, &self, 10, true).await,
            command if command.starts_with("!console ") => {
                handle_console(msg, ctx, &self).await;
            }
            message if message.starts_with("!grok ") => {
                handle_ai(msg, ctx).await;
            }
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

async fn handle_help(msg: Message, ctx: Context) {
    let help = r#"## Minecraft Server Bot Commands
### Server Management
    *   `!start` – **Starts** the Minecraft server.
    *   `!stop` – **Stops** the Minecraft server.
    *   `!status` – Displays the **current status** of the server.
### Info & Server Commands
    *   `!ping` – Network test. The bot replies with **Pong!**.
    *   `!list` – Shows the list of **online players**.
    *   `!tps` – Checks the server **performance / TPS** (`neoforge tps`).
    *   `!logs` – Fetches the latest server **log files**. (OP)
    *   `!console <command>` – Sends a direct **console command** to the server. (OP)
### Artificial Intelligence (AI)
    *   `!grok <prompt>` – **Grok AI chat.**
### Other
    *   `!help` – Displays this **help menu**.
    *   `!bence buzi?` – Answers one of life's greatest (not) mysteries.
"#;

    if let Err(why) = msg.channel_id.say(&ctx.http, help).await {
        println!("Error sending message: {why:?}");
    }
}

async fn handle_ai(msg: Message, ctx: Context) {
    let mut reply = match msg.channel_id.say(&ctx.http, "*Thinking...*").await {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };
    let Some(promt) = msg.content.strip_prefix("!grok ") else {
        if let Err(why) = reply
            .edit(&ctx.http, EditMessage::new().content("No promt to send"))
            .await
        {
            println!("Error sending message: {why:?}");
        }
        return;
    };
    let answer = ai::chat(promt, AI_SYSTEM_PROMT, 0.5).await;
    match answer {
        Ok(promt) => {
            if let Err(why) = reply
                .edit(&ctx.http, EditMessage::new().content(format!("{promt}")))
                .await
            {
                println!("Error sending message: {why:?}");
            }
        }
        Err(e) => {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!("Failed to send promt: {e}")),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
        }
    }
}

async fn handle_list(msg: Message, ctx: Context, handler: &Handler) {
    let mut reply = match msg
        .channel_id
        .say(&ctx.http, format!("**Sending list command**"))
        .await
    {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };
    let result = {
        let mut server = handler.server.lock().await;
        server.get_players().await
    };
    match result {
        Ok(players) => {
            if !players.is_empty() {
                if let Err(why) = reply
                    .edit(
                        &ctx.http,
                        EditMessage::new()
                            .content(format!("**Players online:** {}", players.join(", "))),
                    )
                    .await
                {
                    println!("Error sending message: {why:?}");
                }
            } else {
                if let Err(why) = reply
                    .edit(
                        &ctx.http,
                        EditMessage::new().content(format!("**No players** online")),
                    )
                    .await
                {
                    println!("Error sending message: {why:?}");
                }
            }
        }
        Err(e) => {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!("An error occured: \n```{e}```")),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
            return;
        }
    }
}

async fn send_command(command: &'static str, msg: Message, ctx: Context, handler: &Handler) {
    let mut reply = match msg
        .channel_id
        .say(&ctx.http, format!("**Sending {command} command**"))
        .await
    {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };
    let result = {
        let mut server = handler.server.lock().await;
        server.send_command(command).await
    };
    match result {
        Ok(()) => {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!("Command {command} sent")),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
            handle_logs(msg, ctx, handler, 1, false).await
        }
        Err(e) => {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!("An error occured: \n```{e}```")),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
            return;
        }
    }
}

async fn handle_ping(msg: Message, ctx: Context) {
    if let Err(why) = msg.channel_id.say(&ctx.http, "Pong!").await {
        println!("Error sending message: {why:?}");
    }
}
async fn handle_buzi(msg: Message, ctx: Context) {
    if let Err(why) = msg.channel_id.say(&ctx.http, "Igen bence buzi").await {
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

async fn handle_stop(force: bool, msg: Message, ctx: Context, handler: &Handler) {
    let initial = if force {
        "**Force stopping server...**"
    } else {
        "**Checking if stop is possible...**"
    };
    let mut reply = match msg.channel_id.say(&ctx.http, format!("{initial}")).await {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };
    if force {
        if msg.author.id != handler.discord_id {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!("You are not OP")),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
            return;
        }
    };
    if !force {
        let result = {
            let mut server = handler.server.lock().await;
            server.get_players().await
        };

        let players = match result {
            Ok(players) => players,
            Err(e) => {
                if let Err(why) = reply
                    .edit(
                        &ctx.http,
                        EditMessage::new().content(format!("An error occured: \n```{e}```")),
                    )
                    .await
                {
                    println!("Error sending message: {why:?}");
                }
                return;
            }
        };

        if !players.is_empty() {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!(
                        "**Can't stop, players are still online: {}**",
                        players.join(", ")
                    )),
                )
                .await
            {
                println!("Error sending message: {why:?}");
                return;
            }
            return;
        };
    }
    if let Err(why) = reply
        .edit(
            &ctx.http,
            EditMessage::new().content("**Stopping server...**"),
        )
        .await
    {
        println!("Error sending message: {why:?}");
    }
    {
        let result = {
            let mut server = handler.server.lock().await;
            server.stop().await
        };
        match result {
            Ok(_) => {
                if let Err(why) = reply
                    .edit(&ctx.http, EditMessage::new().content("**Server stopped.**"))
                    .await
                {
                    println!("Error sending message: {why:?}");
                }
            }
            Err(e) => {
                if let Err(why) = reply
                    .edit(
                        &ctx.http,
                        EditMessage::new().content(format!("An error occured: \n```{e}```")),
                    )
                    .await
                {
                    println!("Error sending message: {why:?}");
                }
            }
        }
    }
}

async fn handle_start(msg: Message, ctx: Context, handler: &Handler) {
    let mut reply = match msg.channel_id.say(&ctx.http, "**Initializing...**").await {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };
    let path = &handler.run_path;
    let result = {
        let mut server = handler.server.lock().await;
        server.start(&path, Arc::clone(&handler.server)).await
    };
    match result {
        Ok(_) => {}
        Err(e) => {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!("An error occured: \n```{e}```")),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
            return;
        }
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut seconds: u64 = 0;

    loop {
        interval.tick().await;
        seconds += 1;

        if let Err(why) = reply
            .edit(
                &ctx.http,
                EditMessage::new().content(format!("**Starting server...(ET: {seconds}s)**")),
            )
            .await
        {
            println!("Error sending message: {why:?}");
        }

        let status = {
            let server = handler.server.lock().await;
            server.status_text()
        };

        match status {
            "Online" => {
                if let Err(why) = reply
                    .edit(
                        &ctx.http,
                        EditMessage::new().content("**Server is online**"),
                    )
                    .await
                {
                    println!("Error sending message: {why:?}");
                }
                break;
            }
            "Starting" => {}
            _ => {
                if let Err(why) = reply
                    .edit(
                        &ctx.http,
                        EditMessage::new().content("Incorrect state detected!"),
                    )
                    .await
                {
                    println!("Error sending message: {why:?}");
                }
                break;
            }
        }
    }
}

async fn handle_logs(
    msg: Message,
    ctx: Context,
    handler: &Handler,
    line_count: usize,
    need_op: bool,
) {
    if msg.author.id != handler.discord_id && need_op == true {
        if let Err(why) = msg.channel_id.say(&ctx.http, "You are not OP").await {
            println!("Error sending message: {why:?}");
        }
        return;
    };
    let mut reply = match msg.channel_id.say(&ctx.http, "*Sending logs...*").await {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };
    let logs = {
        let server = handler.server.lock().await;
        server.recent_logs(line_count)
    };
    let message;
    if logs.is_empty() {
        message = format!("No logs available");
    } else {
        message = format!("Last {line_count} lines:\n```{}```", logs);
    }

    if let Err(why) = reply
        .edit(&ctx.http, EditMessage::new().content(message))
        .await
    {
        println!("Error sending message: {why:?}");
    }
}

async fn handle_console(msg: Message, ctx: Context, handler: &Handler) {
    if msg.author.id != handler.discord_id {
        if let Err(why) = msg.channel_id.say(&ctx.http, "You are not OP").await {
            println!("Error sending message: {why:?}");
        }
        return;
    };
    let mut reply = match msg.channel_id.say(&ctx.http, "*Sending command..*").await {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };

    let Some(minecraft_command) = msg.content.strip_prefix("!console ") else {
        if let Err(why) = reply
            .edit(&ctx.http, EditMessage::new().content("No command to send"))
            .await
        {
            println!("Error sending message: {why:?}");
        }
        return;
    };
    let result = {
        let mut server = handler.server.lock().await;
        server.send_command(minecraft_command).await
    };
    match result {
        Ok(()) => {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!("{minecraft_command} command sent!")),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            handle_logs(msg, ctx, handler, 1, false).await;
            return;
        }
        Err(e) => {
            if let Err(why) = reply
                .edit(
                    &ctx.http,
                    EditMessage::new().content(format!(
                        "An error occured while sending command: \n```{e}```"
                    )),
                )
                .await
            {
                println!("Error sending message: {why:?}");
            }
            return;
        }
    }
}

pub async fn start_loop(server_arc: Arc<Mutex<ServerManager>>) -> Result<(), SerenityError> {
    // Configure the client with your Discord bot token in the environment.
    dotenvy::dotenv().expect("Failed to get .env");
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    let owner_id = env::var("OWNER_ID")
        .expect("Expected OWNER_ID")
        .parse::<u64>()
        .expect("OWNER_ID must be a valid Discord user ID");
    let path = env::var("RUN_SH_PATH").expect("Expected RUN_SH_PATH");
    // Set gateway intents, which decides what events the bot will be notified about
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;

    // Create a new instance of the Client, logging in as a bot. This will automatically prepend
    // your bot token with "Bot ", which is a requirement by Discord for bot users.
    let handler = Handler {
        server: Arc::clone(&server_arc),
        run_path: path,
        discord_id: owner_id,
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

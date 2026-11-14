use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serenity::all::{
    ChannelId, CreateAllowedMentions, CreateMessage, EditMessage, MessageFlags, UserId,
};
use serenity::async_trait;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::prelude::*;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::ai;
use crate::backup;
use crate::minecraft::{ServerEvent, ServerManager};

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
            "!backup" => handle_backup(msg, ctx, &self).await,
            "!say" => {
                if let Err(why) = msg.channel_id.say(&ctx.http, "No message to send").await {
                    println!("Error sending message: {why:?}");
                }
            }
            command if command.starts_with("!say ") => {
                handle_say(msg, ctx, &self).await;
            }
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
    *   `!backup` – Creates a **world backup**; refuses if players are online. (OP)
### Info & Server Commands
    *   `!ping` – Network test. The bot replies with **Pong!**.
    *   `!list` – Shows the list of **online players**.
    *   `!tps` – Checks the server **performance / TPS** (`neoforge tps`).
    *   `!logs` – Fetches the latest server **log files**. (OP)
    *   `!say <message>` – Sends a chat **message** to the Minecraft server. (OP)
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
        server.send_console_command(command).await
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
    let status = server.state_text();
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
            server.state_text()
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
        server.send_console_command(minecraft_command).await
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

fn backup_notification(content: impl Into<String>) -> CreateMessage {
    CreateMessage::new()
        .content(content)
        .flags(MessageFlags::SUPPRESS_NOTIFICATIONS)
        .allowed_mentions(CreateAllowedMentions::new())
}

async fn handle_backup(msg: Message, ctx: Context, handler: &Handler) {
    if msg.author.id != handler.discord_id {
        if let Err(why) = msg.channel_id.say(&ctx.http, "You are not OP").await {
            println!("Error sending message: {why:?}");
        }
        return;
    }

    let config = match backup::load_config(Path::new(&handler.run_path)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            if let Err(why) = msg.channel_id.say(&ctx.http, "BACKUP_DIR is not set").await {
                println!("Error sending message: {why:?}");
            }
            return;
        }
        Err(error) => {
            if let Err(why) = msg
                .channel_id
                .say(&ctx.http, format!("Backup configuration error: {error}"))
                .await
            {
                println!("Error sending message: {why:?}");
            }
            return;
        }
    };

    let mut reply = match msg.channel_id.say(&ctx.http, "*Starting backup...*").await {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };

    let (progress_tx, mut progress_rx) =
        tokio::sync::mpsc::unbounded_channel::<backup::BackupProgress>();
    let http = ctx.http.clone();
    let mut progress_message = reply.clone();

    let editor = tokio::spawn(async move {
        while let Some(progress) = progress_rx.recv().await {
            if let Err(why) = progress_message
                .edit(&http, EditMessage::new().content(progress.message()))
                .await
            {
                println!("Error editing backup progress: {why:?}");
            }
        }
    });

    let progress_sink: backup::ProgressSink = Arc::new(move |progress| {
        let _ = progress_tx.send(progress);
    });

    let result = backup::perform_backup(&handler.server, &config, true, Some(progress_sink)).await;

    let _ = editor.await;

    let content = match result {
        Ok(outcome) => {
            let size = outcome.bytes as f64 / 1024.0 / 1024.0;
            let restart = if outcome.restarted {
                "server restarted"
            } else {
                "server left offline"
            };

            format!(
                "**Backup completed:** `{}` ({size:.1} MB), {restart}",
                outcome.archive.display()
            )
        }
        Err(backup::BackupError::PlayersOnline(players)) => format!(
            "**Backup not started, players are online:** {}",
            players.join(", ")
        ),
        Err(error) => format!("Backup failed: \n```{error}```"),
    };

    if let Err(why) = reply
        .edit(&ctx.http, EditMessage::new().content(content))
        .await
    {
        println!("Error sending message: {why:?}");
    }
}

async fn handle_say(msg: Message, ctx: Context, handler: &Handler) {
    if msg.author.id != handler.discord_id {
        if let Err(why) = msg.channel_id.say(&ctx.http, "You are not OP").await {
            println!("Error sending message: {why:?}");
        }
        return;
    }

    let Some(text) = msg.content.strip_prefix("!say ") else {
        return;
    };
    let text = text.trim().replace(['\n', '\r'], " ");

    if text.is_empty() {
        if let Err(why) = msg.channel_id.say(&ctx.http, "No message to send").await {
            println!("Error sending message: {why:?}");
        }
        return;
    }

    let mut reply = match msg.channel_id.say(&ctx.http, "*Sending message...*").await {
        Ok(message) => message,
        Err(why) => {
            println!("Error sending message: {why:?}");
            return;
        }
    };

    let command = format!("say {text}");
    let result = {
        let mut server = handler.server.lock().await;
        server.send_console_command(&command).await
    };

    let content = match result {
        Ok(()) => format!("**Message sent:** {text}"),
        Err(e) => format!("An error occured while sending message: \n```{e}```"),
    };

    if let Err(why) = reply
        .edit(&ctx.http, EditMessage::new().content(content))
        .await
    {
        println!("Error sending message: {why:?}");
    }
}

pub async fn start_loop(
    server_arc: Arc<Mutex<ServerManager>>,
    mut event_rx: UnboundedReceiver<ServerEvent>,
) -> Result<(), SerenityError> {
    // Configure the client with your Discord bot token in the environment.
    dotenvy::dotenv().expect("Failed to get .env");
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    let owner_id = env::var("OWNER_ID")
        .expect("Expected OWNER_ID")
        .parse::<u64>()
        .expect("OWNER_ID must be a valid Discord user ID");
    let path = env::var("RUN_SH_PATH").expect("Expected RUN_SH_PATH");
    let notify_channel = env::var("NOTIFY_CHANNEL_ID")
        .ok()
        .and_then(|id| id.trim().parse::<u64>().ok());
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

    if notify_channel.is_none() {
        println!("NOTIFY_CHANNEL_ID not set; crash notifications disabled");
    }

    let http = client.http.clone();

    tokio::spawn(async move {
        let owner = UserId::new(owner_id);
        let mut dm_channel = None;
        let mut backup_message: Option<Message> = None;

        while let Some(event) = event_rx.recv().await {
            match event {
                ServerEvent::Crashed(status) => {
                    let Some(channel_id) = notify_channel else {
                        continue;
                    };

                    if let Err(why) = ChannelId::new(channel_id)
                        .say(&http, format!("Server exited unexpectedly ({status})"))
                        .await
                    {
                        println!("Error sending crash notification: {why:?}");
                    }
                }
                ServerEvent::Chat { player, message } => {
                    if dm_channel.is_none() {
                        match owner.create_dm_channel(&http).await {
                            Ok(channel) => dm_channel = Some(channel),
                            Err(why) => {
                                println!("Error creating direct message channel: {why:?}");
                                continue;
                            }
                        }
                    }

                    let Some(channel) = dm_channel.as_ref() else {
                        continue;
                    };

                    if let Err(why) = channel
                        .say(&http, format!("**<{player}>** {message}"))
                        .await
                    {
                        println!("Error sending chat notification: {why:?}");
                        dm_channel = None;
                    }
                }
                ServerEvent::BackupProgress { message } => {
                    let Some(channel_id) = notify_channel else {
                        continue;
                    };

                    if let Some(progress_message) = backup_message.as_mut() {
                        if let Err(why) = progress_message
                            .edit(&http, EditMessage::new().content(&message))
                            .await
                        {
                            println!("Error editing backup progress: {why:?}");
                            backup_message = None;
                        }
                    } else {
                        match ChannelId::new(channel_id)
                            .send_message(&http, backup_notification(&message))
                            .await
                        {
                            Ok(sent) => backup_message = Some(sent),
                            Err(why) => println!("Error sending backup progress: {why:?}"),
                        }
                    }
                }
                ServerEvent::BackupFinished { message } => {
                    let Some(channel_id) = notify_channel else {
                        println!("{message}");
                        continue;
                    };

                    let channel = ChannelId::new(channel_id);

                    match backup_message.take() {
                        Some(mut progress_message) => {
                            if let Err(why) = progress_message
                                .edit(&http, EditMessage::new().content(&message))
                                .await
                            {
                                println!("Error editing backup notification: {why:?}");
                                let _ = channel
                                    .send_message(&http, backup_notification(message))
                                    .await;
                            }
                        }
                        None => {
                            if let Err(why) = channel
                                .send_message(&http, backup_notification(message))
                                .await
                            {
                                println!("Error sending backup notification: {why:?}");
                            }
                        }
                    }
                }
            }
        }
    });

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

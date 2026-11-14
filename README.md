# Termux Minecraft Manager

This is a neoforge minecraft manager specificaly made for termux with discord integration.

## Server features
- Start the neoforge server in a child process
- Safely stop the server
- See the logs
- Write  commands to the server console
- Automatically back up the world every day and keep the last 14 archives
## Discord features
- Send commands to the bot with the `!` prefix
- Minecraft chat messages are forwarded to `OWNER_ID` via direct message
- Optionally notify a channel when the server exits unexpectedly (`NOTIFY_CHANNEL_ID`)
### Server Management
  `!start` – **Starts** the Minecraft server.
  
  `!stop` – **Stops** the Minecraft server.
  
  `!status` – Displays the **current status** of the server.
  
  `!backup` – Creates a **world backup**. (OP)
### Info & Server Commands
  `!ping` – Network test. The bot replies with **Pong!**.
  
  `!list` – Shows the list of **online players**.
  
  `!tps` – Checks the server **performance / TPS** (`neoforge tps`).
  
  `!logs` – Fetches the latest server **log files**. (OP)
  
  `!say <message>` – Sends a chat **message** to the Minecraft server. (OP)
  
  `!console <command>` – Sends a direct **console command** to the server. (OP)
### AI
`!grok <prompt>` – **Grok AI chat.**
### Other
`!help` – Displays this **help menu**.

`!bence buzi?` – Answers one of life's greatest (not) mysteries.

## AI
- Send a promt to ai, witch will respond according to the system promt
- You have to provide the API, that supports the OpenAi format

## Backups
- Set `BACKUP_DIR` to the folder where daily world archives should be stored.
- Optionally set `BACKUP_TIME` in `HH:MM` format (default `05:00`).
- The server warns players 5 minutes and 1 minute before the backup, stops, zips the `world` folder, restarts if it was running, and keeps the last 14 archives.
- Backups are disabled when `BACKUP_DIR` is empty.

# Architecture
`minecraft.rs` - handles the server

`discord.rs` - handles the discord bot

`backup.rs` - handles daily world backups and retention

`ai.rs` - handles the ai api calls

`main.rs` - creates the ServerManager struct, starts the minecraft server, backup scheduler and discord bot

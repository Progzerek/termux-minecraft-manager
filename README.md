# Termux Minecraft Manager

This is a neoforge minecraft manager specificaly made for termux with discord integration.

# Main features
## Base features
- Start the neoforge server in a child process
- Safely stop the server
- See the logs
- Write  commands to the server console
## Discord features
- Send commands to the bot with the `!` prefix
### Server Management
  `!start` – **Starts** the Minecraft server.
  
  `!stop` – **Stops** the Minecraft server.
  
  `!status` – Displays the **current status** of the server.
### Info & Server Commands
  `!ping` – Network test. The bot replies with **Pong!**.
  
  `!list` – Shows the list of **online players**.
  
  `!tps` – Checks the server **performance / TPS** (`neoforge tps`).
  
  `!logs` – Fetches the latest server **log files**. (OP)
  
  `!console <command>` – Sends a direct **console command** to the server. (OP)
### AI
`!grok <prompt>` – **Grok AI chat.**
### Other
`!help` – Displays this **help menu**.

`!bence buzi?` – Answers one of life's greatest (not) mysteries.

## AI
- Send a promt to ai, witch will respond according to the system promt
- You have to provide the API, that supports the OpenAi format

# Architecture
`minecraft.rs` - handles the server

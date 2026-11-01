use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;

const MODEL: &str = "mistralai/mistral-large-4-1t-a52b";
const API_URL: &str = "https://integrate.api.nvidia.com/v1/chat/completions";

#[derive(Serialize)]
struct Message {
    role: &'static str,
    content: String,
}

#[derive(Serialize)]
struct ChatRequest {
    model: &'static str,
    messages: Vec<Message>,
    max_tokens: u32,
    stream: bool,
    temperature: f32,
    top_p: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
    reasoning_effort: &'static str,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: ResponseMessage,
}

#[derive(Deserialize)]
struct ResponseMessage {
    content: String,
}

/// Call NVIDIA NIM chat completion API.
///
/// # Arguments
/// * `message` - The user message
/// * `system_prompt` - System prompt to use
/// * `temperature` - Sampling temperature (e.g., 0.5)
///
/// Returns the full assistant response as a String.
pub async fn chat(
    message: &str,
    system_prompt: &str,
    temperature: f32,
) -> Result<String, Box<dyn std::error::Error>> {
    // Load .env file if present
    dotenvy::dotenv().ok();

    let api_key =
        env::var("NVIDIA_API_KEY").map_err(|_| "NVIDIA_API_KEY not set in .env or environment")?;

    let client = Client::new();

    let payload = ChatRequest {
        model: MODEL,
        messages: vec![
            Message {
                role: "system",
                content: system_prompt.to_string(),
            },
            Message {
                role: "user",
                content: message.to_string(),
            },
        ],
        max_tokens: 4096,
        stream: false, // false so we can return the full string easily
        temperature,
        top_p: 1.0,
        frequency_penalty: 0.0,
        presence_penalty: 0.0,
        reasoning_effort: "high",
    };

    let response = client
        .post(API_URL)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await?;
        return Err(format!("API error {}: {}", status, body).into());
    }

    let parsed: ChatResponse = response.json().await?;

    parsed
        .choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| "Empty response from API".into())
}

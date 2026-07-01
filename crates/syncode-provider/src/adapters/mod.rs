//! Per-provider adapter implementations

pub mod anthropic;
pub mod claude;
pub mod codex;
pub mod cursor;
pub mod gemini;
pub mod grok;
pub mod kilo;
pub mod openai;
pub mod opencode;
pub mod pi;

// Re-exports
pub use anthropic::AnthropicAdapter;
pub use anthropic::AnthropicConfig;
pub use claude::ClaudeAdapter;
pub use claude::ClaudeConfig;
pub use codex::CodexAdapter;
pub use codex::CodexConfig;
// cursor & grok are ACP-backed: exposed as spec/create helpers (not bespoke
// adapter structs). See `adapters::cursor` / `adapters::grok`.
pub use cursor::{create as create_cursor, spec as cursor_spec};
pub use gemini::GeminiAdapter;
pub use gemini::GeminiConfig;
pub use grok::{create as create_grok, spec as grok_spec};
pub use kilo::KiloAdapter;
pub use kilo::KiloConfig;
pub use openai::OpenAIAdapter;
pub use openai::OpenAIConfig;
pub use opencode::OpenCodeAdapter;
pub use opencode::OpenCodeConfig;
pub use pi::PiAdapter;
pub use pi::PiConfig;

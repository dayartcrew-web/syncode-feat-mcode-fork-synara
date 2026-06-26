//! Per-provider adapter implementations

pub mod anthropic;
pub mod claude;
pub mod codex;
pub mod cursor;
pub mod gemini;
pub mod grok;
pub mod kilo;
pub mod opencode;
pub mod openai;
pub mod pi;

// Re-exports
pub use anthropic::AnthropicAdapter;
pub use anthropic::AnthropicConfig;
pub use claude::ClaudeAdapter;
pub use claude::ClaudeConfig;
pub use codex::CodexAdapter;
pub use codex::CodexConfig;
pub use cursor::CursorAdapter;
pub use cursor::CursorConfig;
pub use gemini::GeminiAdapter;
pub use gemini::GeminiConfig;
pub use grok::GrokAdapter;
pub use grok::GrokConfig;
pub use kilo::KiloAdapter;
pub use kilo::KiloConfig;
pub use opencode::OpenCodeAdapter;
pub use opencode::OpenCodeConfig;
pub use openai::OpenAIAdapter;
pub use openai::OpenAIConfig;
pub use pi::PiAdapter;
pub use pi::PiConfig;

//! Syncode Provider — Multi-Provider AI Agent Abstraction
//!
//! Provider adapter trait, registry, and per-provider implementations.
//! Supports 10 providers: Codex, Claude, Cursor, Gemini, Grok, Kilo, OpenCode,
//! Pi, Anthropic (HTTP), and OpenAI (HTTP).
//!
//! Architecture:
//! - `trait_def` — `ProviderAdapter` trait, JSON-RPC types, session context
//! - `session` — `SessionState` tracking per-turn lifecycle
//! - `registry` — Provider discovery, configuration, status aggregation
//! - `adapters` — Per-provider implementations (Codex is the reference)

pub mod adapters;
pub mod registry;
pub mod session;
pub mod trait_def;

// Re-exports for convenience
pub use trait_def::{
    ProviderAdapter, ProviderAdapterError, ProviderCapability, ProviderConfig,
    ProviderError, ProviderEvent, ProviderId, ProviderRequest, ProviderResponse,
    ProviderStatus, ProviderStream, SessionContext, UsageInfo,
    PROVIDER_ANTHROPIC, PROVIDER_CLAUDE, PROVIDER_CODEX, PROVIDER_CURSOR, PROVIDER_GEMINI,
    PROVIDER_GROK, PROVIDER_KILO, PROVIDER_OPENAI, PROVIDER_OPENCODE, PROVIDER_PI, ALL_PROVIDERS,
};
pub use session::{SessionManager, SessionState, SessionStateStatus, SessionTransitionError};

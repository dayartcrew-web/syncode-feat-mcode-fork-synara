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
//! - `subprocess` — NDJSON JSON-RPC subprocess transport (foundation for ACP providers)
//! - `acp` — ACP (Agent Client Protocol) client over the subprocess transport
//! - `acp_provider` — `ProviderAdapter` impl wrapping `AcpClient` (cursor/grok/gemini)
//! - `adapters` — Per-provider implementations (Codex is the reference)

pub mod acp;
pub mod acp_provider;
pub mod adapters;
pub mod codex_app_server;
pub mod opencode_server;
pub mod pi_rpc;
pub mod registry;
pub mod session;
pub mod subprocess;
pub mod trait_def;

// Re-exports for convenience
pub use acp::{AcpClient, PROTOCOL_VERSION as ACP_PROTOCOL_VERSION, PromptResult};
pub use acp_provider::{AcpProvider, AcpProviderConfig};
pub use codex_app_server::{
    CodexAppServerClient, TurnResult as CodexTurnResult, TurnStatus as CodexTurnStatus,
};
pub use opencode_server::{
    KILO_CLI_SPEC, ModelRef as OpenCodeModelRef, OPENCODE_CLI_SPEC, OpenCodeAuth,
    OpenCodeCompatibleCliSpec, OpenCodeServerClient, TurnOutcome as OpenCodeTurnOutcome,
    TurnStatus as OpenCodeTurnStatus,
};
pub use registry::{ProviderOptionInfo, all_provider_option_infos};
pub use session::{
    DEFAULT_IDLE_TTL_SECS, ENV_IDLE_TTL_SECS, FileResumeCursorStore, IDLE_STOP_SWEEP_INTERVAL_SECS,
    InMemoryResumeCursorStore, PersistedSessionCursor, RehydratedSession, RehydrationOutcome,
    ResumeCursorStore, ResumeCursorStoreError, SessionIdentity, SessionManager, SessionState,
    SessionStateStatus, SessionTransitionError, configured_idle_ttl_secs,
};
pub use trait_def::{
    ALL_PROVIDERS, ExternalThreadMessage, PROVIDER_ANTHROPIC, PROVIDER_CLAUDE, PROVIDER_CODEX,
    PROVIDER_CURSOR, PROVIDER_GEMINI, PROVIDER_GROK, PROVIDER_KILO, PROVIDER_OPENAI,
    PROVIDER_OPENCODE, PROVIDER_PI, ProviderAdapter, ProviderAdapterError, ProviderCapability,
    ProviderConfig, ProviderError, ProviderEvent, ProviderId, ProviderRequest, ProviderResponse,
    ProviderStatus, ProviderStream, SessionContext, UsageInfo,
};

//! Gemini provider — ACP subprocess configuration for the Google Gemini CLI.
//!
//! Gemini speaks the Agent Client Protocol over stdio. This module owns Gemini's
//! ACP spawn configuration; the protocol itself is implemented by the shared
//! [`AcpProvider`], so Gemini is *just* spec configuration — no separate trait
//! implementation.
//!
//! Spawn form (matches the mcode ACP integration):
//! ```text
//! gemini --acp
//! ```
//!
//! # Note on wire quirks
//!
//! mcode drives Gemini with a bespoke adapter (manual `child_process` + manual
//! JSON-RPC parse) rather than its shared ACP session runtime, which hints that
//! Gemini's ACP surface may have quirks (e.g. non-standard `initialize` params
//! or `session/*` behavior). syncode routes Gemini through the standard
//! [`AcpClient`]; real-binary interop is exercised by the gated E2E test at
//! `tests/gemini_e2e.rs` (run with `SYNICODE_ACP_E2E=1`), which surfaces any
//! provider-specific handling by driving a full real turn.

use crate::acp_provider::{AcpProvider, AcpProviderConfig};
use crate::subprocess::SubprocessSpec;
use crate::trait_def::{PROVIDER_GEMINI, ProviderCapability};

/// The fixed `gemini` argument list (`--acp` selects the ACP stdio transport).
pub fn build_args() -> Vec<String> {
    vec!["--acp".to_string()]
}

/// Build the ACP spawn config + identity for the Gemini provider.
pub fn spec() -> AcpProviderConfig {
    AcpProviderConfig {
        provider_id: PROVIDER_GEMINI.to_string(),
        spec: SubprocessSpec::new("gemini").args(build_args()),
        capabilities: vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::Vision,
            ProviderCapability::FileSystem,
            ProviderCapability::SystemPrompt,
        ],
        available_models: vec!["gemini-2.5-pro".to_string(), "gemini-2.5-flash".to_string()],
        client_name: "syncode".to_string(),
    }
}

/// Construct a fresh (un-spawned) Gemini [`AcpProvider`].
pub fn create() -> AcpProvider {
    AcpProvider::new(spec())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trait_def::ProviderAdapter;

    #[test]
    fn build_args_is_acp_flag() {
        assert_eq!(build_args(), vec!["--acp".to_string()]);
    }

    #[test]
    fn spec_identity_and_command() {
        let config = spec();
        assert_eq!(config.provider_id, PROVIDER_GEMINI);
        assert_eq!(config.spec.command, "gemini");
        assert_eq!(config.spec.args, vec!["--acp".to_string()]);
        assert!(config.capabilities.contains(&ProviderCapability::Streaming));
        assert!(config.capabilities.contains(&ProviderCapability::ToolUse));
        assert!(config.capabilities.contains(&ProviderCapability::Vision));
        assert!(
            config
                .capabilities
                .contains(&ProviderCapability::FileSystem)
        );
        assert_eq!(
            config.available_models,
            vec!["gemini-2.5-pro".to_string(), "gemini-2.5-flash".to_string(),]
        );
        assert_eq!(config.client_name, "syncode");
    }

    #[test]
    fn create_builds_acp_provider_with_gemini_identity() {
        let provider = create();
        assert_eq!(provider.provider_id(), PROVIDER_GEMINI);
        assert!(!provider.capabilities().is_empty());
    }
}

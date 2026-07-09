//! Cursor provider — ACP subprocess configuration for the Cursor CLI.
//!
//! Cursor speaks the Agent Client Protocol over stdio. This module owns Cursor's
//! ACP spawn configuration; the protocol itself is implemented by the shared
//! [`AcpProvider`], so Cursor is *just* spec configuration — no separate trait
//! implementation.
//!
//! Spawn form (matches the mcode ACP integration):
//! ```text
//! cursor-agent [-e <apiEndpoint>] acp
//! ```
//! The optional API endpoint is layered in from the `SYNICODE_CURSOR_ENDPOINT`
//! environment variable when present.

use crate::acp_provider::{AcpProvider, AcpProviderConfig};
use crate::subprocess::SubprocessSpec;
use crate::trait_def::{PROVIDER_CURSOR, ProviderCapability};

/// Environment variable overriding the Cursor API endpoint.
pub const ENV_ENDPOINT: &str = "SYNICODE_CURSOR_ENDPOINT";

/// Build the `cursor-agent` argument list given an optional API endpoint.
///
/// The endpoint, when provided, is passed as `-e <endpoint>` *before* `acp`,
/// matching the mcode spawn form. This is a pure helper so the flag ordering is
/// unit-testable without touching the process environment.
pub fn build_args(endpoint: Option<&str>) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(ep) = endpoint {
        let trimmed = ep.trim();
        if !trimmed.is_empty() {
            args.push("-e".to_string());
            args.push(trimmed.to_string());
        }
    }
    args.push("acp".to_string());
    args
}

/// Read the optional Cursor API endpoint from the environment, if set.
fn env_endpoint() -> Option<String> {
    std::env::var(ENV_ENDPOINT)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Build the ACP spawn config + identity for the Cursor provider, honoring an
/// optional endpoint from the environment.
pub fn spec() -> AcpProviderConfig {
    spec_with(env_endpoint().as_deref())
}

/// Build the config from an explicit endpoint value (env-free; used by tests).
pub fn spec_with(endpoint: Option<&str>) -> AcpProviderConfig {
    AcpProviderConfig {
        provider_id: PROVIDER_CURSOR.to_string(),
        spec: SubprocessSpec::new("cursor-agent").args(build_args(endpoint)),
        capabilities: vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::FileSystem,
            ProviderCapability::SystemPrompt,
        ],
        available_models: vec!["cursor-default".to_string()],
        client_name: "syncode".to_string(),
        // Cursor-agent gates sessions behind the locally-cached Cursor
        // account via the `cursor_login` auth method. The ACP `authenticate`
        // step is mandatory for Cursor before `session/new` (matches mcode's
        // `CursorAcpSupport` authMethodId).
        auth_method_id: Some("cursor_login".to_string()),
        auth_meta: None,
    }
}

/// Construct a fresh (un-spawned) Cursor [`AcpProvider`] from the env-configured spec.
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
    fn build_args_baseline_just_acp() {
        assert_eq!(build_args(None), vec!["acp".to_string()]);
    }

    #[test]
    fn build_args_with_endpoint_inserts_dash_e_before_acp() {
        assert_eq!(
            build_args(Some("https://my.cursor.sh")),
            vec![
                "-e".to_string(),
                "https://my.cursor.sh".to_string(),
                "acp".to_string()
            ]
        );
    }

    #[test]
    fn build_args_ignores_blank_endpoint() {
        assert_eq!(build_args(Some("   ")), vec!["acp".to_string()]);
    }

    #[test]
    fn build_args_tail_is_invariant() {
        // `-e <ep>` may lead, but `acp` is always the trailing command.
        for endpoint in [None, Some("https://a.b"), Some("  ")] {
            let args = build_args(endpoint);
            assert_eq!(args.last(), Some(&"acp".to_string()), "{args:?}");
        }
    }

    #[test]
    fn spec_with_identity_and_command() {
        let config = spec_with(None);
        assert_eq!(config.provider_id, PROVIDER_CURSOR);
        assert_eq!(config.spec.command, "cursor-agent");
        assert_eq!(config.spec.args, vec!["acp".to_string()]);
        assert!(config.capabilities.contains(&ProviderCapability::Streaming));
        assert!(config.capabilities.contains(&ProviderCapability::ToolUse));
        assert!(
            config
                .capabilities
                .contains(&ProviderCapability::FileSystem)
        );
        assert_eq!(config.available_models, vec!["cursor-default".to_string()]);
        assert_eq!(config.client_name, "syncode");
    }

    #[test]
    fn spec_with_endpoint_layers_dash_e() {
        let config = spec_with(Some("https://ep.example"));
        assert_eq!(
            config.spec.args,
            vec![
                "-e".to_string(),
                "https://ep.example".to_string(),
                "acp".to_string()
            ]
        );
    }

    #[test]
    fn spec_acp_command_is_invariant_under_env() {
        // The trailing `acp` command holds whether or not an env endpoint is set;
        // the env-reading wrapper is a thin pass-through to `spec_with`.
        let config = spec();
        assert_eq!(config.spec.command, "cursor-agent");
        assert_eq!(config.spec.args.last(), Some(&"acp".to_string()));
        assert_eq!(config.provider_id, PROVIDER_CURSOR);
    }

    #[test]
    fn create_builds_acp_provider_with_cursor_identity() {
        let provider = create();
        assert_eq!(provider.provider_id(), PROVIDER_CURSOR);
        assert!(!provider.capabilities().is_empty());
    }
}

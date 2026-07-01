//! Grok provider — ACP subprocess configuration for the xAI Grok CLI.
//!
//! Grok speaks the Agent Client Protocol over stdio. This module owns Grok's ACP
//! spawn configuration; the protocol itself is implemented by the shared
//! [`AcpProvider`], so Grok is *just* spec configuration — no separate trait
//! implementation.
//!
//! Spawn form (matches the mcode ACP integration):
//! ```text
//! grok agent [--always-approve] [-m <model>] [--reasoning-effort <effort>] --no-leader stdio
//! ```
//! The optional flags are layered in from `SYNICODE_*` environment variables when
//! present. The positional shape (`agent … --no-leader stdio`) is invariant.

use crate::acp_provider::{AcpProvider, AcpProviderConfig};
use crate::subprocess::SubprocessSpec;
use crate::trait_def::{PROVIDER_GROK, ProviderCapability};

/// Enable `--always-approve` when set to `1`/`true`/`yes`.
pub const ENV_ALWAYS_APPROVE: &str = "SYNICODE_GROK_ALWAYS_APPROVE";
/// Override the model passed via `-m <model>`.
pub const ENV_MODEL: &str = "SYNICODE_GROK_MODEL";
/// Override the reasoning effort passed via `--reasoning-effort <effort>`.
pub const ENV_REASONING_EFFORT: &str = "SYNICODE_GROK_REASONING_EFFORT";

/// Optional Grok spawn flags resolved from the environment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrokFlags {
    pub always_approve: bool,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
}

impl GrokFlags {
    /// Resolve the optional flags from `SYNICODE_*` environment variables.
    ///
    /// Only the `SYNICODE_` namespace is consulted so the baseline spawn form
    /// stays deterministic in environments that may set generic `GROK_*` vars.
    pub fn from_env() -> Self {
        let always_approve = std::env::var(ENV_ALWAYS_APPROVE)
            .ok()
            .map(|v| is_truthy(&v))
            .unwrap_or(false);
        let model = env_value(ENV_MODEL);
        let reasoning_effort = env_value(ENV_REASONING_EFFORT);
        Self {
            always_approve,
            model,
            reasoning_effort,
        }
    }
}

/// Interpret a `--always-approve` env value: `1`/`true`/`yes` (case-insensitive
/// on the alpha forms) are truthy; everything else is falsy.
fn is_truthy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes"
    )
}

fn env_value(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Build the `grok` argument list for the given flags.
///
/// Order: `agent`, optionally `--always-approve`, optionally `-m <model>`,
/// optionally `--reasoning-effort <effort>`, then the invariant tail
/// `--no-leader stdio`. This is a pure helper so flag ordering is unit-testable
/// without touching the process environment.
pub fn build_args(flags: &GrokFlags) -> Vec<String> {
    let mut args = vec!["agent".to_string()];
    if flags.always_approve {
        args.push("--always-approve".to_string());
    }
    if let Some(model) = &flags.model {
        args.push("-m".to_string());
        args.push(model.clone());
    }
    if let Some(effort) = &flags.reasoning_effort {
        args.push("--reasoning-effort".to_string());
        args.push(effort.clone());
    }
    args.push("--no-leader".to_string());
    args.push("stdio".to_string());
    args
}

/// Build the ACP spawn config + identity for the Grok provider, layering in
/// optional flags from the environment.
pub fn spec() -> AcpProviderConfig {
    spec_with(&GrokFlags::from_env())
}

/// Build the config from explicit flags (env-free; used by tests).
pub fn spec_with(flags: &GrokFlags) -> AcpProviderConfig {
    AcpProviderConfig {
        provider_id: PROVIDER_GROK.to_string(),
        spec: SubprocessSpec::new("grok").args(build_args(flags)),
        capabilities: vec![
            ProviderCapability::Streaming,
            ProviderCapability::ToolUse,
            ProviderCapability::SystemPrompt,
        ],
        available_models: vec!["grok-default".to_string()],
        client_name: "syncode".to_string(),
    }
}

/// Construct a fresh (un-spawned) Grok [`AcpProvider`] from the env-configured spec.
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
    fn build_args_baseline() {
        assert_eq!(
            build_args(&GrokFlags::default()),
            vec!["agent", "--no-leader", "stdio"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn build_args_all_flags_in_order() {
        let flags = GrokFlags {
            always_approve: true,
            model: Some("grok-3".to_string()),
            reasoning_effort: Some("high".to_string()),
        };
        assert_eq!(
            build_args(&flags),
            vec![
                "agent",
                "--always-approve",
                "-m",
                "grok-3",
                "--reasoning-effort",
                "high",
                "--no-leader",
                "stdio"
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn build_args_only_model() {
        let flags = GrokFlags {
            model: Some("grok-2".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_args(&flags),
            vec!["agent", "-m", "grok-2", "--no-leader", "stdio"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn build_args_tail_is_invariant() {
        // Regardless of flags, the form is always `agent … --no-leader stdio`.
        let cases = [
            GrokFlags::default(),
            GrokFlags {
                always_approve: true,
                ..Default::default()
            },
            GrokFlags {
                model: Some("m".to_string()),
                reasoning_effort: Some("low".to_string()),
                ..Default::default()
            },
        ];
        for flags in cases {
            let args = build_args(&flags);
            assert_eq!(args.first(), Some(&"agent".to_string()), "{args:?}");
            assert_eq!(args.last(), Some(&"stdio".to_string()), "{args:?}");
            assert!(args.contains(&"--no-leader".to_string()), "{args:?}");
        }
    }

    #[test]
    fn is_truthy_recognizes_enabled_forms() {
        for v in ["1", "true", "TRUE", " yes ", "Yes"] {
            assert!(is_truthy(v), "{v:?} should be truthy");
        }
        for v in ["0", "false", "", "no", "y", "anything"] {
            assert!(!is_truthy(v), "{v:?} should be falsy");
        }
    }

    #[test]
    fn spec_with_identity_and_baseline_command() {
        let config = spec_with(&GrokFlags::default());
        assert_eq!(config.provider_id, PROVIDER_GROK);
        assert_eq!(config.spec.command, "grok");
        assert_eq!(
            config.spec.args,
            vec!["agent", "--no-leader", "stdio"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
        assert!(config.capabilities.contains(&ProviderCapability::Streaming));
        assert!(config.capabilities.contains(&ProviderCapability::ToolUse));
        assert_eq!(config.available_models, vec!["grok-default".to_string()]);
        assert_eq!(config.client_name, "syncode");
    }

    #[test]
    fn spec_positional_form_is_invariant_under_env() {
        // The `agent … --no-leader stdio` form holds whether or not env vars are
        // set; `from_env` is a thin pass-through to `spec_with`.
        let config = spec();
        assert_eq!(config.spec.command, "grok");
        assert_eq!(config.spec.args.first(), Some(&"agent".to_string()));
        assert_eq!(config.spec.args.last(), Some(&"stdio".to_string()));
        assert!(config.spec.args.contains(&"--no-leader".to_string()));
        assert_eq!(config.provider_id, PROVIDER_GROK);
    }

    #[test]
    fn create_builds_acp_provider_with_grok_identity() {
        let provider = create();
        assert_eq!(provider.provider_id(), PROVIDER_GROK);
        assert!(!provider.capabilities().is_empty());
    }
}

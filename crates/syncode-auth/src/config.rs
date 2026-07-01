//! Auth mode & transport-level auth configuration
//!
//! [`AuthMode`] mirrors MCode's `ServerAuthPolicy` literals: it expresses
//! *where* the server is reachable from and therefore *whether* authentication
//! is required. The WS layer consults [`AuthMode::requires_authentication`]
//! to decide whether to gate connections.
//!
//! [`WsAuthConfig`] bundles everything the WS transport needs to authenticate
//! connections and authorize dispatch: the mode, the authenticator (if any),
//! and the shared session registry.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::authenticator::Authenticator;
use crate::session::SessionRegistry;

/// Where the server is reachable from, and therefore whether auth is required.
///
/// Mirrors MCode's `ServerAuthPolicy` literals:
/// - `unsafe-no-auth` — no auth (dev only)
/// - `desktop-managed-local` — single-user desktop, auth handled by the OS user
/// - `loopback-browser` — reachable only from localhost, browser-driven
/// - `remote-reachable` — exposed to the network, auth **required**
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    /// No authentication. Dev/test only — every connection is trusted.
    #[default]
    UnsafeNoAuth,
    /// Single-user desktop; the OS user boundary is the trust boundary.
    /// Auth is NOT enforced on the WS layer (the desktop owns access).
    DesktopManagedLocal,
    /// Loopback-only (127.0.0.1). Auth is NOT enforced — the network boundary
    /// is the trust boundary.
    LoopbackBrowser,
    /// Exposed to the network. Authentication is **required** for every
    /// connection and command dispatch is authorized per-principal.
    RemoteReachable,
}

impl AuthMode {
    /// Whether connections MUST authenticate before dispatching commands.
    ///
    /// Only `RemoteReachable` requires it; the local/loopback/desktop modes
    /// treat the network or OS-user boundary as sufficient.
    pub fn requires_authentication(self) -> bool {
        matches!(self, AuthMode::RemoteReachable)
    }
}

/// Transport-level auth configuration consumed by the WS layer.
///
/// `authenticator` and `sessions` are `Option`/shared because local modes
/// don't need them — when [`AuthMode::requires_authentication`] is false,
/// the WS layer skips auth entirely.
#[derive(Clone)]
pub struct WsAuthConfig {
    pub mode: AuthMode,
    /// Used only when `mode.requires_authentication()`. `None` in local modes.
    pub authenticator: Option<Arc<dyn Authenticator>>,
    /// Shared session registry. In requiring modes this is the live session
    /// store the authenticator mints into; in local modes it may still be
    /// present (cheap) but is unused.
    pub sessions: Arc<SessionRegistry>,
}

impl WsAuthConfig {
    /// The local/no-auth configuration. Used by the default `WsState` builders.
    pub fn no_auth() -> Self {
        Self {
            mode: AuthMode::UnsafeNoAuth,
            authenticator: None,
            sessions: Arc::new(SessionRegistry::new()),
        }
    }

    /// A remote-reachable configuration requiring authentication.
    pub fn remote(authenticator: Arc<dyn Authenticator>) -> Self {
        Self {
            mode: AuthMode::RemoteReachable,
            authenticator: Some(authenticator),
            sessions: Arc::new(SessionRegistry::new()),
        }
    }

    /// Convenience: is auth enforced on this config?
    pub fn requires_authentication(&self) -> bool {
        self.mode.requires_authentication()
    }
}

impl std::fmt::Debug for WsAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsAuthConfig")
            .field("mode", &self.mode)
            .field("has_authenticator", &self.authenticator.is_some())
            .field("sessions", &self.sessions.len())
            .finish()
    }
}

impl Default for WsAuthConfig {
    fn default() -> Self {
        Self::no_auth()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_remote_requires_authentication() {
        assert!(!AuthMode::UnsafeNoAuth.requires_authentication());
        assert!(!AuthMode::DesktopManagedLocal.requires_authentication());
        assert!(!AuthMode::LoopbackBrowser.requires_authentication());
        assert!(AuthMode::RemoteReachable.requires_authentication());
    }

    #[test]
    fn default_mode_is_unsafe_no_auth() {
        assert_eq!(AuthMode::default(), AuthMode::UnsafeNoAuth);
    }

    #[test]
    fn no_auth_config_does_not_require_auth() {
        let cfg = WsAuthConfig::no_auth();
        assert!(!cfg.requires_authentication());
        assert!(cfg.authenticator.is_none());
    }

    #[test]
    fn auth_mode_serializes_kebab_case() {
        assert_eq!(
            serde_json::to_string(&AuthMode::RemoteReachable).unwrap(),
            "\"remote-reachable\""
        );
        let back: AuthMode = serde_json::from_str("\"unsafe-no-auth\"").unwrap();
        assert_eq!(back, AuthMode::UnsafeNoAuth);
    }
}

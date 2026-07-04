//! Authorization for RPC dispatch — method → permission mapping + per-connection
//! principal resolution.
//!
//! The WS layer authenticates a connection once (via the `auth/bootstrap`
//! method, which exchanges a credential for a session token) and then, on
//! every subsequent request, resolves the connection's bound principal and
//! checks it against the permission the requested method requires.
//!
//! Methods are partitioned into three tiers:
//! - **Public** — `ping`, `rpc/listMethods`, `auth/*`. Always allowed, even
//!   pre-authentication (you need them to bootstrap).
//! - **Read** — `*/list`, `*/get`, `push/*`. Requires `Permission::Read`.
//! - **Write** — `*/create`, `*/start`, `*/complete`, `pause`, `resume`,
//!   `cancel`. Requires `Permission::Write`.

use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use syncode_auth::WsAuthConfig;
use syncode_auth::policy::{Permission, PolicyDecision};
use syncode_auth::principal::Principal;
use syncode_auth::session::SessionToken;

use crate::{ConnectionId, JsonRpcResponse};

/// The permission a method requires, or `None` if the method is public
/// (callable pre-authentication — bootstrap/system methods).
pub fn required_permission(method: &str) -> Option<Permission> {
    match method {
        // ─── Public: bootstrap & system (always allowed) ───────────────
        "ping" | "rpc/listMethods" | "auth/bootstrap" | "auth/status" | "auth/logout" => None,

        // ─── Push subscription: treated as Read (observability) ────────
        "push/subscribe" | "push/unsubscribe" => Some(Permission::Read),

        // ─── Read methods ───────────────────────────────────────────────
        "project/list" | "project/get" => Some(Permission::Read),
        "thread/list" | "thread/get" => Some(Permission::Read),
        "turn/list" | "turn/get" => Some(Permission::Read),
        // Server config/settings/lifecycle read RPCs (T6c-4). These surface
        // server-side configuration + diagnostics — observability-tier, so
        // Read. The slash keys are the canonical dispatch targets (the dot
        // forms route through authz under whatever string the client sends,
        // but `required_permission` is keyed on the slash form the dispatcher
        // invokes after `authorize`; the authz gate runs on the raw method
        // string before dispatch — we cover both forms defensively).
        "server/getConfig"
        | "server/getSettings"
        | "server/getEnvironment"
        | "server/getDiagnostics"
        | "server/welcome"
        | "server.getConfig"
        | "server.getSettings"
        | "server.getEnvironment"
        | "server.getDiagnostics"
        | "server.welcome" => Some(Permission::Read),
        // Server push subscriptions (T6c-4 stubs). Treated as Read like
        // `push/subscribe`.
        "server/subscribeConfig"
        | "server/subscribeSettings"
        | "server/subscribeProviderStatuses"
        | "server/subscribeLifecycle"
        | "server.subscribeConfig"
        | "server.subscribeSettings"
        | "server.subscribeProviderStatuses"
        | "server.subscribeLifecycle" => Some(Permission::Read),

        // ─── Write methods (mutate domain state) ────────────────────────
        "project/create" => Some(Permission::Write),
        "thread/create" | "thread/pause" | "thread/resume" | "thread/cancel" => {
            Some(Permission::Write)
        }
        "turn/start" | "turn/complete" => Some(Permission::Write),

        // ─── Pairing-link management (AUTH-1) ────────────────────────────
        //
        // Creating / revoking / listing pairing credentials is privileged: a
        // pairing credential grants a NEW principal the configured role on
        // bootstrap, so only an authenticated principal with Write (i.e. an
        // Owner; Clients default to Read-only) may mint or revoke them. Even
        // `list` requires Write — the credential strings are sensitive and
        // must not surface to read-only consumers.
        //
        // Both the MCode dot-name AND the slash form resolve here so the
        // authz gate behaves identically regardless of which form the client
        // sends (the dispatcher accepts both; see `dispatch_method`).
        "auth.createPairingCredential"
        | "auth/create-pairing-credential"
        | "auth/createPairingCredential" => Some(Permission::Write),
        "auth.revokePairingLink"
        | "auth/revoke-pairing-link"
        | "auth/revokePairingLink" => Some(Permission::Write),
        "auth.listPairingLinks"
        | "auth/list-pairing-links"
        | "auth/listPairingLinks" => Some(Permission::Write),

        // ─── Client-session management (AUTH-2) ───────────────────────────
        //
        // Four privileged RPCs that surface + manage the *live* WS sessions
        // (authenticated connections). Unlike pairing links (bootstrap
        // credentials), these operate on currently-connected principals:
        //   - `listClientSessions`  → enumerate active authenticated connections
        //   - `revokeClientSession` → invalidate one connection's auth (force
        //     re-auth); the next protected call returns UNAUTHORIZED.
        //   - `getWebSocketToken`   → issue a fresh bearer token bound to the
        //     calling principal (e.g. for a reconnecting client or a sub-flow
        //     that needs a discrete token).
        //   - `getSessionState`     → return the calling session's auth state
        //     (role, principal info, expiry).
        //
        // All four are gated by Write: enumerating/revoking sessions is
        // privileged (an attacker could otherwise discover active owners or
        // kick them off), and minting tokens is obviously privileged. Even
        // `getSessionState` is Write-gated for symmetry — it surfaces the
        // effective principal + policy, which a read-only Client shouldn't be
        // able to probe about other sessions' reachability. The MCode UI only
        // invokes these from the owner-scoped Settings/Security panel.
        //
        // Both dot-name AND slash form resolve here (matches AUTH-1 + the
        // git.*/server.* convention) so the authz gate behaves identically
        // regardless of which form the client sends.
        "auth.listClientSessions"
        | "auth/list-client-sessions"
        | "auth/listClientSessions" => Some(Permission::Write),
        "auth.revokeClientSession"
        | "auth/revoke-client-session"
        | "auth/revokeClientSession" => Some(Permission::Write),
        "auth.getWebSocketToken"
        | "auth/get-web-socket-token"
        | "auth/getWebSocketToken" => Some(Permission::Write),
        "auth.getSessionState"
        | "auth/get-session-state"
        | "auth/getSessionState" => Some(Permission::Write),

        // Unknown method → no permission gate here; the dispatcher will
        // return METHOD_NOT_FOUND downstream. We don't pre-reject so the
        // error path stays uniform regardless of auth state.
        _ => None,
    }
}

/// Per-connection authenticated session state.
///
/// A connection is "authenticated" if it has a bound principal here. In
/// `AuthMode::UnsafeNoAuth` (and other non-requiring modes) this map is
/// ignored — every connection is trusted.
#[derive(Debug, Default)]
pub struct ConnectionAuth {
    /// Maps a connection to its authenticated principal (if any).
    principals: HashMap<ConnectionId, Principal>,
}

impl ConnectionAuth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a principal to a connection (post-authentication).
    pub fn set(&mut self, conn_id: ConnectionId, principal: Principal) {
        self.principals.insert(conn_id, principal);
    }

    /// The principal bound to a connection, if any.
    pub fn get(&self, conn_id: ConnectionId) -> Option<&Principal> {
        self.principals.get(&conn_id)
    }

    /// Clear a connection's principal (logout / disconnect).
    /// Returns whether one was present.
    pub fn clear(&mut self, conn_id: ConnectionId) -> bool {
        self.principals.remove(&conn_id).is_some()
    }

    /// Enumerate every authenticated (connection, principal) pair. AUTH-2's
    /// `auth.listClientSessions` consults this to surface the live session
    /// roster. Returns a cloned snapshot (callers iterate without holding the
    /// lock). No filtering is applied here — expiry (if any) is enforced
    /// upstream by the caller via `Principal::is_expired`.
    pub fn list(&self) -> Vec<(ConnectionId, Principal)> {
        self.principals
            .iter()
            .map(|(id, p)| (*id, p.clone()))
            .collect()
    }
}

/// The outcome of an authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzOutcome {
    /// The call may proceed.
    Allow,
    /// Auth is required but the connection has not authenticated.
    Unauthorized,
    /// The connection authenticated but lacks the required permission.
    Forbidden {
        required: Permission,
        decision_reason: String,
    },
}

/// Decide whether `conn_id` may dispatch `method` under `config`.
///
/// - Non-requiring auth mode → always `Allow`.
/// - Requiring mode, public method → `Allow` (bootstrap methods bypass).
/// - Requiring mode, protected method, no principal → `Unauthorized`.
/// - Requiring mode, protected method, principal present → check policy.
pub async fn authorize(
    config: &WsAuthConfig,
    conn_auth: &RwLock<ConnectionAuth>,
    conn_id: ConnectionId,
    method: &str,
) -> AuthzOutcome {
    // Non-requiring mode: open. Every method is allowed.
    if !config.requires_authentication() {
        return AuthzOutcome::Allow;
    }

    // Public method: allow even pre-auth (so the client can bootstrap).
    let Some(required) = required_permission(method) else {
        return AuthzOutcome::Allow;
    };

    // Protected method: must have an authenticated principal.
    let principal = match conn_auth.read().await.get(conn_id).cloned() {
        Some(p) => p,
        None => return AuthzOutcome::Unauthorized,
    };

    match principal.can(&required) {
        PolicyDecision::Allow => AuthzOutcome::Allow,
        PolicyDecision::Deny { reason } => AuthzOutcome::Forbidden {
            required,
            decision_reason: reason,
        },
    }
}

/// The shared, connection-scoped auth state held by `WsState`.
#[derive(Clone, Default)]
pub struct SharedConnectionAuth {
    inner: Arc<RwLock<ConnectionAuth>>,
}

impl SharedConnectionAuth {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set(&self, conn_id: ConnectionId, principal: Principal) {
        self.inner.write().await.set(conn_id, principal);
    }

    pub async fn get(&self, conn_id: ConnectionId) -> Option<Principal> {
        self.inner.read().await.get(conn_id).cloned()
    }

    pub async fn clear(&self, conn_id: ConnectionId) -> bool {
        self.inner.write().await.clear(conn_id)
    }

    /// Snapshot of every authenticated (connection, principal) pair. AUTH-2's
    /// `auth.listClientSessions` reaches for this; the result is a cloned vec
    /// so callers don't hold the lock while serializing.
    pub async fn list_sessions(&self) -> Vec<(ConnectionId, Principal)> {
        self.inner.read().await.list()
    }

    /// Authorize a call. Thin wrapper over [`authorize`] using this store.
    pub async fn authorize(
        &self,
        config: &WsAuthConfig,
        conn_id: ConnectionId,
        method: &str,
    ) -> AuthzOutcome {
        authorize(config, &self.inner, conn_id, method).await
    }
}

impl std::fmt::Debug for SharedConnectionAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedConnectionAuth")
            .finish_non_exhaustive()
    }
}

/// JSON-RPC error codes for auth failures. Negative, in the JSON-RPC
/// "reserved implementation-defined server-errors" band (-32099..-32000).
pub mod auth_error_codes {
    /// Connection must authenticate before dispatching protected methods.
    pub const UNAUTHORIZED: i32 = -32001;
    /// Authenticated, but the principal lacks the required permission.
    pub const FORBIDDEN: i32 = -32003;
}

/// Map an [`AuthzOutcome`] to a JSON-RPC error response.
pub fn authz_error_response(id: serde_json::Value, outcome: &AuthzOutcome) -> JsonRpcResponse {
    match outcome {
        AuthzOutcome::Unauthorized => JsonRpcResponse::error(
            Some(id),
            auth_error_codes::UNAUTHORIZED,
            "Authentication required: call auth/bootstrap with a credential first",
        ),
        AuthzOutcome::Forbidden {
            required,
            decision_reason,
        } => JsonRpcResponse::error(
            Some(id),
            auth_error_codes::FORBIDDEN,
            format!(
                "Forbidden: requires {:?} permission ({})",
                required, decision_reason
            ),
        ),
        AuthzOutcome::Allow => {
            // Defensive: should never be called with Allow.
            JsonRpcResponse::error(
                Some(id),
                crate::error_codes::INTERNAL_ERROR,
                "authz_error_response called with Allow outcome",
            )
        }
    }
}

/// Result of a bootstrap attempt — the session token to return to the client.
#[derive(Debug, Clone)]
pub struct BootstrapResult {
    pub token: SessionToken,
    pub principal: Principal,
}

/// Attempt to authenticate `credential` via the config's authenticator and
/// bind the resulting principal to `conn_id`. Returns the token + principal
/// on success, or an error code/message pair on failure.
pub async fn bootstrap(
    config: &WsAuthConfig,
    conn_auth: &SharedConnectionAuth,
    conn_id: ConnectionId,
    credential: &str,
) -> Result<BootstrapResult, (i32, String)> {
    let authenticator = config.authenticator.as_ref().ok_or((
        auth_error_codes::UNAUTHORIZED,
        "Authentication not configured for this server".to_string(),
    ))?;

    let now = Utc::now();
    let session = authenticator
        .authenticate(credential, now)
        .await
        .map_err(|e| match e {
            syncode_auth::AuthError::InvalidCredential => (
                auth_error_codes::UNAUTHORIZED,
                "Invalid credential".to_string(),
            ),
            syncode_auth::AuthError::Expired => (
                auth_error_codes::UNAUTHORIZED,
                "Session expired".to_string(),
            ),
            syncode_auth::AuthError::Internal(msg) => (
                crate::error_codes::INTERNAL_ERROR,
                format!("Authentication failure: {msg}"),
            ),
        })?;

    conn_auth.set(conn_id, session.principal.clone()).await;
    Ok(BootstrapResult {
        token: session.token,
        principal: session.principal,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_methods_have_no_permission() {
        for m in [
            "ping",
            "rpc/listMethods",
            "auth/bootstrap",
            "auth/status",
            "auth/logout",
        ] {
            assert_eq!(required_permission(m), None, "{} should be public", m);
        }
    }

    #[test]
    fn read_methods_require_read() {
        for m in [
            "project/list",
            "project/get",
            "thread/list",
            "thread/get",
            "turn/list",
            "turn/get",
            "push/subscribe",
            "push/unsubscribe",
        ] {
            assert_eq!(required_permission(m), Some(Permission::Read), "{}", m);
        }
    }

    #[test]
    fn write_methods_require_write() {
        for m in [
            "project/create",
            "thread/create",
            "thread/pause",
            "thread/resume",
            "thread/cancel",
            "turn/start",
            "turn/complete",
            // AUTH-1 pairing-link management — all three gated by Write
            // (minting/revoking credentials is privileged; listing surfaces
            // sensitive credential strings).
            "auth.createPairingCredential",
            "auth/create-pairing-credential",
            "auth.revokePairingLink",
            "auth/revoke-pairing-link",
            "auth.listPairingLinks",
            "auth/list-pairing-links",
            // AUTH-2 client-session management — all four gated by Write
            // (enumerating/revoking sessions + minting tokens is privileged).
            "auth.listClientSessions",
            "auth/list-client-sessions",
            "auth.revokeClientSession",
            "auth/revoke-client-session",
            "auth.getWebSocketToken",
            "auth/get-web-socket-token",
            "auth.getSessionState",
            "auth/get-session-state",
        ] {
            assert_eq!(required_permission(m), Some(Permission::Write), "{}", m);
        }
    }

    #[test]
    fn unknown_method_is_treated_as_public_for_authz() {
        // Unknown methods aren't gated by authz — they fall through to the
        // dispatcher which returns METHOD_NOT_FOUND. (We don't pre-reject so
        // the error is identical whether or not you're authenticated.)
        assert_eq!(required_permission("does/not/exist"), None);
    }

    #[tokio::test]
    async fn non_requiring_mode_allows_everything() {
        let config = WsAuthConfig::no_auth();
        let conn_auth = SharedConnectionAuth::new();

        for (conn, method) in [(1, "project/create"), (2, "turn/start"), (3, "ping")] {
            assert_eq!(
                conn_auth.authorize(&config, conn, method).await,
                AuthzOutcome::Allow,
                "{}/{}",
                conn,
                method
            );
        }
    }

    #[tokio::test]
    async fn requiring_mode_unauth_rejects_write() {
        let config = WsAuthConfig::no_auth();
        let remote = make_remote_config();
        let conn_auth = SharedConnectionAuth::new();
        let _ = config;

        // No principal bound → write is Unauthorized.
        assert_eq!(
            conn_auth.authorize(&remote, 1, "project/create").await,
            AuthzOutcome::Unauthorized
        );
    }

    #[tokio::test]
    async fn requiring_mode_public_method_allowed_pre_auth() {
        let remote = make_remote_config();
        let conn_auth = SharedConnectionAuth::new();

        // Even without a principal, bootstrap/system methods are callable.
        for method in ["ping", "auth/bootstrap", "auth/status"] {
            assert_eq!(
                conn_auth.authorize(&remote, 1, method).await,
                AuthzOutcome::Allow,
                "{}",
                method
            );
        }
    }

    #[tokio::test]
    async fn client_role_read_only_boundary() {
        let remote = make_remote_config();
        let conn_auth = SharedConnectionAuth::new();
        conn_auth
            .set(
                1,
                Principal::new_never_expiring("bob", syncode_auth::principal::Role::Client),
            )
            .await;

        // Read allowed, write forbidden.
        assert_eq!(
            conn_auth.authorize(&remote, 1, "project/list").await,
            AuthzOutcome::Allow
        );
        let outcome = conn_auth.authorize(&remote, 1, "project/create").await;
        assert!(matches!(outcome, AuthzOutcome::Forbidden { .. }));
    }

    #[tokio::test]
    async fn owner_can_write() {
        let remote = make_remote_config();
        let conn_auth = SharedConnectionAuth::new();
        conn_auth
            .set(
                1,
                Principal::new_never_expiring("alice", syncode_auth::principal::Role::Owner),
            )
            .await;
        assert_eq!(
            conn_auth.authorize(&remote, 1, "project/create").await,
            AuthzOutcome::Allow
        );
    }

    /// Build a remote-reachable config with a shared-secret authenticator
    /// for tests.
    fn make_remote_config() -> WsAuthConfig {
        use std::sync::Arc;
        use std::sync::Mutex;
        use syncode_auth::OWNER_TOKEN_KEY;
        use syncode_auth::authenticator::SharedSecretAuthenticator;
        use syncode_auth::secret_store::{InMemorySecretStore, SecretStore};

        let mut store = InMemorySecretStore::new();
        store.store(OWNER_TOKEN_KEY, "sk-test");
        let store: Arc<Mutex<dyn SecretStore>> = Arc::new(Mutex::new(store));
        let sessions = Arc::new(syncode_auth::session::SessionRegistry::new());
        let auth = SharedSecretAuthenticator::new(store, sessions);
        WsAuthConfig::remote(Arc::new(auth))
    }
}

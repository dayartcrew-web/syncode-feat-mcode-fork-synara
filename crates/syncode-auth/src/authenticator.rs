//! Authenticators — validate a presented credential and mint a [`Principal`]
//!
//! The [`Authenticator`] trait is the seam between an incoming credential
//! (bearer token, pairing code, password) and an authenticated identity.
//! Implementations decide *how* a credential maps to a principal; the
//! WebSocket transport only cares that one is returned on success.
//!
//! [`SharedSecretAuthenticator`] is the reference implementation: it accepts
//! a single shared secret (the `owner_token` stored in a [`SecretStore`])
//! and mints an [`Owner`](crate::principal::Role::Owner) principal + session
//! on a match. This is the local-first `bearer-session-token` bootstrap
//! path — appropriate for the desktop/loopback deployments MCode labels
//! `desktop-managed-local` and `loopback-browser`.

use chrono::Duration;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use thiserror::Error;

use crate::principal::{Principal, Role};
use crate::secret_store::SecretStore;
use crate::session::{SessionRegistry, SessionToken};

/// Errors raised during authentication.
#[derive(Debug, Error)]
pub enum AuthError {
    /// The presented credential did not match any known principal.
    #[error("invalid credential")]
    InvalidCredential,
    /// The credential resolved to a principal whose session has expired.
    #[error("session expired")]
    Expired,
    /// An internal failure (store I/O, misconfiguration) prevented auth.
    #[error("internal auth error: {0}")]
    Internal(String),
}

/// The result of a successful authentication: a bound session token plus the
/// principal it authenticates. The token is what the client presents on
/// subsequent requests; the principal is what the authz gate evaluates.
#[derive(Debug, Clone)]
pub struct AuthenticatedSession {
    pub token: SessionToken,
    pub principal: Principal,
}

/// Authenticate a presented credential into a live session.
///
/// Implementations are expected to be cheap and side-effect-free beyond
/// registry mutation (issuing a session). Network calls, if any, should be
/// wrapped internally.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync {
    /// Validate `credential`, mint a session, and return it.
    async fn authenticate(
        &self,
        credential: &str,
        now: DateTime<Utc>,
    ) -> Result<AuthenticatedSession, AuthError>;

    /// Resolve an existing session token to its principal, iff still valid.
    /// Default implementation consults the shared [`SessionRegistry`].
    async fn validate_session(
        &self,
        token: &SessionToken,
        now: DateTime<Utc>,
    ) -> Result<Principal, AuthError>;
}

/// The shared-secret bootstrap authenticator.
///
/// Holds an [`Arc`] over a [`SecretStore`] (so it can be shared with other
/// components) and a [`SessionRegistry`] where it records issued sessions.
/// The credential is compared against the secret stored under
/// [`SharedSecretAuthenticator::TOKEN_KEY`]; on match it mints an `Owner`
/// principal valid for `ttl`.
///
/// `ttl` defaults to 24 hours. Callers building a long-lived server may
/// override it via [`SharedSecretAuthenticator::with_ttl`].
pub struct SharedSecretAuthenticator {
    secret_store: Arc<std::sync::Mutex<dyn SecretStore>>,
    sessions: Arc<SessionRegistry>,
    ttl: Duration,
}

/// Key under which the owner bootstrap token is stored in the [`SecretStore`].
pub const OWNER_TOKEN_KEY: &str = "owner_token";

impl SharedSecretAuthenticator {
    /// The secret-store key for the owner bootstrap token.
    pub const TOKEN_KEY: &'static str = OWNER_TOKEN_KEY;

    /// Build with an existing session registry (so the WS layer can share it).
    pub fn new(
        secret_store: Arc<std::sync::Mutex<dyn SecretStore>>,
        sessions: Arc<SessionRegistry>,
    ) -> Self {
        Self {
            secret_store,
            sessions,
            ttl: Duration::hours(24),
        }
    }

    /// Override the session TTL (how long a minted principal stays valid).
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }
}

#[async_trait::async_trait]
impl Authenticator for SharedSecretAuthenticator {
    async fn authenticate(
        &self,
        credential: &str,
        now: DateTime<Utc>,
    ) -> Result<AuthenticatedSession, AuthError> {
        let expected = self
            .secret_store
            .lock()
            .unwrap()
            .retrieve(OWNER_TOKEN_KEY)
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        // Constant-time-ish comparison is overkill for a local-first shared
        // secret, but we avoid short-circuiting on a length mismatch to keep
        // timing leakage minimal.
        let matches = constant_time_eq(credential.as_bytes(), expected.as_bytes());
        if !matches {
            return Err(AuthError::InvalidCredential);
        }

        let principal = Principal::new("owner", Role::Owner, self.ttl);
        // `now` is accepted so callers can drive deterministic tests; we use
        // it only to short-circuit a principal whose TTL is already in the
        // past (a misconfigured clock).
        if principal.is_expired(now) {
            return Err(AuthError::Expired);
        }

        let token = self.sessions.issue(principal.clone());
        Ok(AuthenticatedSession { token, principal })
    }

    async fn validate_session(
        &self,
        token: &SessionToken,
        now: DateTime<Utc>,
    ) -> Result<Principal, AuthError> {
        self.sessions.validate(token, now).ok_or(AuthError::Expired)
    }
}

/// Byte-wise equality that does not short-circuit on the first mismatch.
///
/// Runs in time proportional to the shorter slice and folds a length mismatch
/// into the accumulator, so the result is timing-invariant for equal-length
/// inputs of the same length and reveals no positional mismatch information.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // Fold a length mismatch into `diff` (non-zero if lengths differ).
    let mut diff = (a.len() as u32) ^ (b.len() as u32);
    let min = a.len().min(b.len());
    for i in 0..min {
        diff |= (a[i] ^ b[i]) as u32;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret_store::InMemorySecretStore;

    fn build() -> (SharedSecretAuthenticator, Arc<SessionRegistry>) {
        let mut store = InMemorySecretStore::new();
        store.store(OWNER_TOKEN_KEY, "sk-owner-secret");
        let store: Arc<std::sync::Mutex<dyn SecretStore>> = Arc::new(std::sync::Mutex::new(store));
        let sessions = Arc::new(SessionRegistry::new());
        let auth = SharedSecretAuthenticator::new(store, Arc::clone(&sessions));
        (auth, sessions)
    }

    #[tokio::test]
    async fn valid_credential_mints_owner_session() {
        let (auth, sessions) = build();
        let result = auth.authenticate("sk-owner-secret", Utc::now()).await;
        assert!(result.is_ok(), "{:?}", result.err());
        let s = result.unwrap();
        assert_eq!(s.principal.role, Role::Owner);
        assert!(sessions.validate(&s.token, Utc::now()).is_some());
    }

    #[tokio::test]
    async fn wrong_credential_is_rejected() {
        let (auth, _) = build();
        let err = auth.authenticate("wrong", Utc::now()).await.unwrap_err();
        assert!(matches!(err, AuthError::InvalidCredential));
    }

    #[tokio::test]
    async fn validate_session_returns_principal_for_live_token() {
        let (auth, _) = build();
        let session = auth
            .authenticate("sk-owner-secret", Utc::now())
            .await
            .unwrap();
        let principal = auth.validate_session(&session.token, Utc::now()).await;
        assert!(principal.is_ok());
        assert_eq!(principal.unwrap().role, Role::Owner);
    }

    #[tokio::test]
    async fn validate_session_rejects_unknown_token() {
        let (auth, _) = build();
        let bogus = SessionToken("nope".into());
        let err = auth.validate_session(&bogus, Utc::now()).await.unwrap_err();
        assert!(matches!(err, AuthError::Expired));
    }

    #[tokio::test]
    async fn missing_owner_token_is_internal_error() {
        let store: Arc<std::sync::Mutex<dyn SecretStore>> =
            Arc::new(std::sync::Mutex::new(InMemorySecretStore::new()));
        let sessions = Arc::new(SessionRegistry::new());
        let auth = SharedSecretAuthenticator::new(store, sessions);

        let err = auth.authenticate("anything", Utc::now()).await.unwrap_err();
        assert!(matches!(err, AuthError::Internal(_)), "{:?}", err);
    }

    #[test]
    fn constant_time_eq_correctness() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }
}

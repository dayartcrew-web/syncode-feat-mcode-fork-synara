//! Session registry — maps opaque session tokens to live [`Principal`]s
//!
//! Sessions are the runtime coupling point between authentication (a one-time
//! credential check) and authorization (per-request permission checks). A
//! connection authenticates once and receives a token; subsequent requests
//! present the token and the registry resolves it to the [`Principal`] that
//! the authz gate evaluates.
//!
//! The registry is in-memory and `Send + Sync` (wrapped in `RwLock`). Tokens
//! are opaque, random, non-guessable strings — they are NOT derived from the
//! principal's identity, so leaking a token never reveals the subject.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::RwLock;
use syncode_core::EntityId;

use crate::principal::Principal;

/// An opaque session token. Serialized transparently as a string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionToken(pub String);

impl SessionToken {
    /// Generate a fresh, random token (UUID v4 — 122 bits of entropy).
    pub fn generate() -> Self {
        Self(EntityId::new().as_str())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// In-memory registry of live sessions keyed by token.
///
/// `validate` honors expiry: an expired session is treated as absent and
/// returns `None` (callers should then require re-authentication). Expired
/// entries are not eagerly evicted — they are lazily filtered on read and
/// may be purged via [`SessionRegistry::purge_expired`].
#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: RwLock<HashMap<String, Principal>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored sessions (including possibly-expired ones not yet
    /// purged). Intended for diagnostics/tests.
    pub fn len(&self) -> usize {
        self.sessions.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.read().unwrap().is_empty()
    }

    /// Register a principal and return a fresh token bound to it.
    pub fn issue(&self, principal: Principal) -> SessionToken {
        let token = SessionToken::generate();
        self.sessions
            .write()
            .unwrap()
            .insert(token.0.clone(), principal);
        token
    }

    /// Resolve a token to its principal, iff present AND not expired (relative
    /// to `now`). Returns `None` for unknown tokens and for expired sessions.
    pub fn validate(&self, token: &SessionToken, now: DateTime<Utc>) -> Option<Principal> {
        let sessions = self.sessions.read().unwrap();
        let principal = sessions.get(&token.0)?;
        if principal.is_expired(now) {
            return None;
        }
        Some(principal.clone())
    }

    /// Revoke a single session by token. Returns whether a session was removed.
    pub fn revoke(&self, token: &SessionToken) -> bool {
        self.sessions.write().unwrap().remove(&token.0).is_some()
    }

    /// Revoke every session whose principal subject matches `subject`.
    /// Returns the count removed.
    pub fn revoke_all_for_subject(&self, subject: &str) -> usize {
        let mut sessions = self.sessions.write().unwrap();
        let to_remove: Vec<String> = sessions
            .iter()
            .filter(|(_, p)| p.subject == subject)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &to_remove {
            sessions.remove(k);
        }
        to_remove.len()
    }

    /// Remove every session whose principal has expired (relative to `now`).
    /// Returns the count purged.
    pub fn purge_expired(&self, now: DateTime<Utc>) -> usize {
        let mut sessions = self.sessions.write().unwrap();
        let to_remove: Vec<String> = sessions
            .iter()
            .filter(|(_, p)| p.is_expired(now))
            .map(|(k, _)| k.clone())
            .collect();
        for k in &to_remove {
            sessions.remove(k);
        }
        to_remove.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::Role;
    use chrono::Duration;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn issue_and_validate_roundtrip() {
        let reg = SessionRegistry::new();
        let principal = Principal::new_never_expiring("alice", Role::Owner);
        let token = reg.issue(principal);

        assert!(reg.validate(&token, now()).is_some());
    }

    #[test]
    fn validate_unknown_token_returns_none() {
        let reg = SessionRegistry::new();
        let bogus = SessionToken("does-not-exist".into());
        assert!(reg.validate(&bogus, now()).is_none());
    }

    #[test]
    fn expired_session_validates_as_none() {
        let reg = SessionRegistry::new();
        // 1-second TTL — expired once we advance the clock.
        let principal = Principal::new("alice", Role::Owner, Duration::seconds(1));
        let token = reg.issue(principal);

        assert!(reg.validate(&token, now()).is_some()); // still alive
        assert!(reg.validate(&token, now() + Duration::seconds(2)).is_none()); // expired
    }

    #[test]
    fn revoke_removes_session() {
        let reg = SessionRegistry::new();
        let token = reg.issue(Principal::new_never_expiring("alice", Role::Owner));

        assert!(reg.revoke(&token));
        assert!(reg.validate(&token, now()).is_none());
        // Second revoke is a no-op.
        assert!(!reg.revoke(&token));
    }

    #[test]
    fn revoke_all_for_subject_removes_matching_sessions() {
        let reg = SessionRegistry::new();
        let t1 = reg.issue(Principal::new_never_expiring("alice", Role::Owner));
        let t2 = reg.issue(Principal::new_never_expiring("alice", Role::Client));
        let t3 = reg.issue(Principal::new_never_expiring("bob", Role::Client));

        let removed = reg.revoke_all_for_subject("alice");
        assert_eq!(removed, 2);
        assert!(reg.validate(&t1, now()).is_none());
        assert!(reg.validate(&t2, now()).is_none());
        assert!(reg.validate(&t3, now()).is_some()); // bob untouched
    }

    #[test]
    fn purge_expired_clears_only_stale_sessions() {
        let reg = SessionRegistry::new();
        let _alive = reg.issue(Principal::new_never_expiring("alice", Role::Owner));
        let _stale = reg.issue(Principal::new("bob", Role::Client, Duration::seconds(1)));

        // Advance past the stale session's TTL.
        let future = now() + Duration::seconds(5);
        let purged = reg.purge_expired(future);
        assert_eq!(purged, 1);
        assert_eq!(reg.len(), 1); // only alice remains
    }

    #[test]
    fn token_is_non_empty_and_unique() {
        let t1 = SessionToken::generate();
        let t2 = SessionToken::generate();
        assert!(!t1.as_str().is_empty());
        assert_ne!(t1, t2, "generated tokens should not collide");
    }
}

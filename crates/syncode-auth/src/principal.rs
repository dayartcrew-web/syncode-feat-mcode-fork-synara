//! Authenticated principals & roles
//!
//! A [`Principal`] is the authenticated identity behind a WebSocket connection.
//! It carries a [`Role`] (the coarse-grained capability tier, mirroring MCode's
//! `owner`/`client` roles) plus the permission set that role grants. The
//! permission set is expressed as an [`AuthPolicy`], so the existing
//! `AuthPolicy::evaluate` machinery (deny wins over allow) decides
//! authorization uniformly.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use syncode_core::Timestamp;

use crate::policy::{AuthPolicy, Permission, PolicyDecision};

/// Coarse-grained role tiers. Mirrors MCode's `AuthSessionRole` literals
/// (`owner` | `client`), expressed here in `snake_case` over the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Full control — every [`Permission`] is granted by default.
    Owner,
    /// Read-only consumer — only [`Permission::Read`] by default.
    Client,
}

impl Role {
    /// The default permission policy for a role.
    ///
    /// - `Owner` → allows Read, Write, Admin, ManageProviders
    /// - `Client` → allows Read only
    ///
    /// The returned policy has an empty deny-list; callers may layer explicit
    /// `deny(...)` overrides on top (deny always wins).
    pub fn default_permissions(self) -> AuthPolicy {
        match self {
            Role::Owner => AuthPolicy::new()
                .allow(Permission::Read)
                .allow(Permission::Write)
                .allow(Permission::Admin)
                .allow(Permission::ManageProviders),
            Role::Client => AuthPolicy::new().allow(Permission::Read),
        }
    }

    /// All permissions a role grants by default. Useful for tests and for
    /// materializing the policy without going through [`AuthPolicy`].
    pub fn default_permission_set(self) -> HashSet<Permission> {
        let policy = self.default_permissions();
        [
            Permission::Read,
            Permission::Write,
            Permission::Admin,
            Permission::ManageProviders,
        ]
        .into_iter()
        .filter(|p| policy.evaluate(p) == PolicyDecision::Allow)
        .collect()
    }
}

/// An authenticated principal — the identity behind a connection.
///
/// `permissions` is the effective [`AuthPolicy`] for this principal (role
/// defaults plus any per-principal overrides). `expires_at` bounds the
/// session's lifetime; once it elapses the principal is no longer valid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    /// Stable identifier for the principal (e.g. the subject string).
    pub id: String,
    /// Human-readable subject (e.g. username, device label).
    pub subject: String,
    pub role: Role,
    /// Effective allow/deny policy (role defaults + per-principal overrides).
    pub permissions: AuthPolicySerializable,
    pub issued_at: Timestamp,
    pub expires_at: Timestamp,
}

/// A serializable view of [`AuthPolicy`] (which isn't `Serialize` itself).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthPolicySerializable {
    pub allowed: HashSet<Permission>,
    pub denied: HashSet<Permission>,
}

impl AuthPolicySerializable {
    /// Build from a role's default permissions (no overrides).
    pub fn from_role(role: Role) -> Self {
        let policy = role.default_permissions();
        // Reconstruct the allow set by probing each known permission.
        let allowed = role.default_permission_set();
        // default_permissions never denies anything, so denied is empty here.
        let _ = &policy; // (kept for parity with AuthPolicy semantics)
        Self {
            allowed,
            denied: HashSet::new(),
        }
    }

    /// Convert back into an [`AuthPolicy`] for evaluation.
    pub fn to_policy(&self) -> AuthPolicy {
        let mut policy = AuthPolicy::new();
        for p in &self.allowed {
            policy = policy.allow(*p);
        }
        for p in &self.denied {
            policy = policy.deny(*p);
        }
        policy
    }
}

impl Principal {
    /// Mint a new principal for a subject + role, valid for `ttl` from now.
    pub fn new(subject: impl Into<String>, role: Role, ttl: Duration) -> Self {
        let now = Utc::now();
        let subject = subject.into();
        Self {
            id: subject.clone(),
            subject,
            role,
            permissions: AuthPolicySerializable::from_role(role),
            issued_at: Timestamp::from_datetime(now),
            expires_at: Timestamp::from_datetime(now + ttl),
        }
    }

    /// A principal with no expiry — intended for tests/dev only.
    pub fn new_never_expiring(subject: impl Into<String>, role: Role) -> Self {
        let now = Utc::now();
        let far_future = now + Duration::days(365 * 100);
        let subject = subject.into();
        Self {
            id: subject.clone(),
            subject,
            role,
            permissions: AuthPolicySerializable::from_role(role),
            issued_at: Timestamp::from_datetime(now),
            expires_at: Timestamp::from_datetime(far_future),
        }
    }

    /// Whether this principal's session has expired (relative to `now`).
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.as_datetime() <= &now
    }

    /// Evaluate a permission against the effective policy. Does NOT consult
    /// expiry — the caller should reject an expired principal before authz.
    pub fn can(&self, permission: &Permission) -> PolicyDecision {
        self.permissions.to_policy().evaluate(permission)
    }
}

impl From<AuthPolicySerializable> for AuthPolicy {
    fn from(s: AuthPolicySerializable) -> Self {
        s.to_policy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_default_policy_allows_all() {
        let policy = Role::Owner.default_permissions();
        assert_eq!(policy.evaluate(&Permission::Read), PolicyDecision::Allow);
        assert_eq!(policy.evaluate(&Permission::Write), PolicyDecision::Allow);
        assert_eq!(policy.evaluate(&Permission::Admin), PolicyDecision::Allow);
        assert_eq!(
            policy.evaluate(&Permission::ManageProviders),
            PolicyDecision::Allow
        );
    }

    #[test]
    fn client_default_policy_is_read_only() {
        let policy = Role::Client.default_permissions();
        assert_eq!(policy.evaluate(&Permission::Read), PolicyDecision::Allow);
        assert!(matches!(
            policy.evaluate(&Permission::Write),
            PolicyDecision::Deny { .. }
        ));
        assert!(matches!(
            policy.evaluate(&Permission::Admin),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn principal_can_uses_effective_policy() {
        let principal = Principal::new_never_expiring("alice", Role::Owner);
        assert_eq!(principal.can(&Permission::Write), PolicyDecision::Allow);

        let client = Principal::new_never_expiring("bob", Role::Client);
        assert_eq!(client.can(&Permission::Read), PolicyDecision::Allow);
        assert!(matches!(
            client.can(&Permission::Write),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn principal_is_expired_respects_expiry() {
        let now = Utc::now();
        let p = Principal::new("alice", Role::Owner, Duration::hours(1));
        assert!(!p.is_expired(now));
        assert!(p.is_expired(now + Duration::hours(2)));
    }

    #[test]
    fn policy_serializable_roundtrip_preserves_decisions() {
        let original = Role::Owner.default_permissions();
        let s = AuthPolicySerializable::from_role(Role::Owner);
        let rebuilt: AuthPolicy = s.into();
        for perm in [
            Permission::Read,
            Permission::Write,
            Permission::Admin,
            Permission::ManageProviders,
        ] {
            assert_eq!(
                original.evaluate(&perm),
                rebuilt.evaluate(&perm),
                "decision mismatch for {:?}",
                perm
            );
        }
    }

    #[test]
    fn role_serializes_snake_case() {
        assert_eq!(serde_json::to_string(&Role::Owner).unwrap(), "\"owner\"");
        assert_eq!(serde_json::to_string(&Role::Client).unwrap(), "\"client\"");
    }
}

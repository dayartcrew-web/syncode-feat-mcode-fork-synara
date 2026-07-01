//! Auth policies
//!
//! A simple allow/deny policy over a [`Permission`] set. This is the scaffold
//! for a future RBAC/ABAC engine; today it answers "is this permission allowed?"
//! given an explicit allow-list and an overriding deny-list.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Coarse-grained capabilities a principal may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Read,
    Write,
    Admin,
    ManageProviders,
}

/// The outcome of evaluating a permission against a policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    Deny { reason: String },
}

/// An explicit allow-list / deny-list policy. Deny always wins over allow.
#[derive(Debug, Clone, Default)]
pub struct AuthPolicy {
    allowed: HashSet<Permission>,
    denied: HashSet<Permission>,
}

impl AuthPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allow(mut self, p: Permission) -> Self {
        self.allowed.insert(p);
        self
    }
    pub fn deny(mut self, p: Permission) -> Self {
        self.denied.insert(p);
        self
    }

    /// Evaluate: Deny if explicitly denied or not explicitly allowed.
    pub fn evaluate(&self, p: &Permission) -> PolicyDecision {
        if self.denied.contains(p) {
            return PolicyDecision::Deny {
                reason: format!("{:?} explicitly denied", p),
            };
        }
        if self.allowed.contains(p) {
            return PolicyDecision::Allow;
        }
        PolicyDecision::Deny {
            reason: format!("{:?} not in allow-list", p),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_explicit() {
        let policy = AuthPolicy::new().allow(Permission::Read);
        assert_eq!(policy.evaluate(&Permission::Read), PolicyDecision::Allow);
    }

    #[test]
    fn deny_unlisted() {
        let policy = AuthPolicy::new().allow(Permission::Read);
        assert!(matches!(
            policy.evaluate(&Permission::Write),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn deny_overrides_allow() {
        let policy = AuthPolicy::new()
            .allow(Permission::Admin)
            .deny(Permission::Admin);
        assert!(matches!(
            policy.evaluate(&Permission::Admin),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn empty_policy_denies_all() {
        let policy = AuthPolicy::new();
        assert!(matches!(
            policy.evaluate(&Permission::Read),
            PolicyDecision::Deny { .. }
        ));
    }
}

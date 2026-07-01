//! Credential management
//!
//! Credentials associate a secret value (API key, OAuth token, password) with a
//! provider. Values are stored opaquely; [`Credential::redact`] produces a
//! display-safe mask for logs and UIs.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use syncode_core::{EntityId, Timestamp};

/// The kind of secret a credential holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    ApiKey,
    OAuthToken,
    BasicPassword,
}

/// A provider credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credential {
    pub id: EntityId,
    /// Provider this credential authenticates against (e.g. "anthropic").
    pub provider_id: String,
    pub kind: CredentialKind,
    /// The secret value. Never expose directly — use [`Credential::redact`].
    pub value: String,
    pub created_at: Timestamp,
}

impl Credential {
    /// Create a new credential, capturing the creation timestamp.
    pub fn new(
        provider_id: impl Into<String>,
        kind: CredentialKind,
        value: impl Into<String>,
    ) -> Self {
        Self {
            id: EntityId::new(),
            provider_id: provider_id.into(),
            kind,
            value: value.into(),
            created_at: Timestamp::now(),
        }
    }

    /// A display-safe mask of the secret (e.g. `••••••••3XYZ`).
    ///
    /// Shows the last 4 characters; shorter secrets are fully masked.
    pub fn redact(&self) -> String {
        let len = self.value.chars().count();
        if len <= 4 {
            return "•".repeat(len.max(1));
        }
        let tail: String = self
            .value
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("{}{}", "•".repeat(len - 4), tail)
    }
}

/// In-memory credential registry keyed by credential ID.
#[derive(Debug, Default)]
pub struct CredentialStore {
    inner: HashMap<String, Credential>,
}

impl CredentialStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a credential, returning its ID.
    pub fn add(&mut self, credential: Credential) -> EntityId {
        let id = credential.id;
        self.inner.insert(id.to_string(), credential);
        id
    }

    pub fn get(&self, id: &EntityId) -> Option<&Credential> {
        self.inner.get(&id.to_string())
    }

    /// All credentials for a given provider.
    pub fn list_for_provider(&self, provider_id: &str) -> Vec<&Credential> {
        self.inner
            .values()
            .filter(|c| c.provider_id == provider_id)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_masks_all_but_last_four() {
        let cred = Credential::new("anthropic", CredentialKind::ApiKey, "sk-ant-abc123XYZ");
        let m = cred.redact();
        assert!(m.ends_with("3XYZ") && m.chars().count() == 16, "{}", m);
    }

    #[test]
    fn redact_short_secret_fully_masked() {
        let cred = Credential::new("openai", CredentialKind::ApiKey, "abc");
        let m = cred.redact();
        assert_eq!(m.chars().count(), 3);
        assert!(m.chars().all(|c| c == '\u{2022}'));
    }

    #[test]
    fn store_add_get_and_list() {
        let mut store = CredentialStore::new();
        let c1 = Credential::new("anthropic", CredentialKind::ApiKey, "sk-ant-11112222");
        let c2 = Credential::new("openai", CredentialKind::ApiKey, "sk-9999aaaa");
        let id1 = store.add(c1);
        let _id2 = store.add(c2);

        assert_eq!(store.len(), 2);
        assert!(store.get(&id1).is_some());
        assert_eq!(store.list_for_provider("anthropic").len(), 1);
        assert_eq!(store.list_for_provider("grok").len(), 0);
    }

    #[test]
    fn credential_kind_serializes_snake_case() {
        let json = serde_json::to_string(&CredentialKind::OAuthToken).unwrap();
        let back: CredentialKind = serde_json::from_str(&json).unwrap();
        assert_eq!(back, CredentialKind::OAuthToken);
    }
}

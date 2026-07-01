//! Secret storage
//!
//! Abstracts where raw secret bytes live (env vars, keyring, file, in-memory).
//! The trait is synchronous by design — a real keyring/vault backend can wrap
//! its own async I/O internally. [`InMemorySecretStore`] is the default and the
//! reference for tests.

use std::collections::HashMap;
use thiserror::Error;

/// Errors from secret-store operations.
#[derive(Debug, Error)]
pub enum SecretStoreError {
    #[error("secret not found for key: {0}")]
    NotFound(String),
}

/// A backend that stores opaque secret strings keyed by name.
pub trait SecretStore: Send + Sync {
    /// Store (or overwrite) a secret under `key`.
    fn store(&mut self, key: &str, value: &str);
    /// Retrieve a secret. Errors if the key is absent.
    fn retrieve(&self, key: &str) -> Result<String, SecretStoreError>;
    /// Delete a secret; returns true if one was present.
    fn delete(&mut self, key: &str) -> bool;
    /// Whether a secret exists for `key`.
    fn exists(&self, key: &str) -> bool;
}

/// Plain in-memory secret store.
#[derive(Debug, Default)]
pub struct InMemorySecretStore {
    inner: HashMap<String, String>,
}

impl InMemorySecretStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl SecretStore for InMemorySecretStore {
    fn store(&mut self, key: &str, value: &str) {
        self.inner.insert(key.to_string(), value.to_string());
    }

    fn retrieve(&self, key: &str) -> Result<String, SecretStoreError> {
        self.inner
            .get(key)
            .cloned()
            .ok_or_else(|| SecretStoreError::NotFound(key.to_string()))
    }

    fn delete(&mut self, key: &str) -> bool {
        self.inner.remove(key).is_some()
    }

    fn exists(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_retrieve_roundtrip() {
        let mut store = InMemorySecretStore::new();
        store.store("anthropic_key", "sk-ant-xxx");
        assert_eq!(store.retrieve("anthropic_key").unwrap(), "sk-ant-xxx");
        assert!(store.exists("anthropic_key"));
    }

    #[test]
    fn retrieve_missing_returns_not_found() {
        let store = InMemorySecretStore::new();
        let err = store.retrieve("nope").unwrap_err();
        assert!(matches!(err, SecretStoreError::NotFound(_)));
    }

    #[test]
    fn delete_returns_presence_and_clears() {
        let mut store = InMemorySecretStore::new();
        store.store("k", "v");
        assert!(store.delete("k"));
        assert!(!store.exists("k"));
        assert!(!store.delete("k"));
    }

    #[test]
    fn store_overwrites_existing() {
        let mut store = InMemorySecretStore::new();
        store.store("k", "v1");
        store.store("k", "v2");
        assert_eq!(store.retrieve("k").unwrap(), "v2");
        assert_eq!(store.len(), 1);
    }
}

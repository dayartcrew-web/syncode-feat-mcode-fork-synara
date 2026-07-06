//! Server settings/config on-disk persistence (SRV-1).
//!
//! Stores the two top-level JSON documents managed by
//! `syncode_ws::settings::ServerSettingsState` — the MCode `ServerConfig`
//! (`server_config` table) and `ServerSettings` (`server_settings` table) —
//! in the same SQLite database as the event store. Each table is a
//! single-row key/value store keyed by `SINGLETON_KEY`; an upsert replaces
//! the whole document on every write-through, mirroring the in-memory
//! `setConfig` (replace) and `updateSettings`/`patchSettings` (merge-then-
//! persist) semantics.
//!
//! # Tables
//!
//! Both tables are created additively by [`crate::init_database`] (`CREATE
//! TABLE IF NOT EXISTS`), so this composes with an existing database and is
//! forward-compatible with the SRV-2 migrations crate.
//!
//! # Backward compatibility
//!
//! [`load_config`]/[`load_settings`] return `Ok(None)` on a fresh or
//! pre-SRV-1 database (no row yet), so callers fall back to defaults —
//! identical to the pre-SRV-1 in-memory behavior.

use crate::SqlitePool;
use serde_json::Value;
use thiserror::Error;

/// Fixed primary-key for the single-row config/settings documents. The MCode
/// UI models the server config/settings as a single document each, so a single
/// upsertable row per table is sufficient.
pub const SINGLETON_KEY: &str = "singleton";

/// Errors that can occur during settings persistence operations.
#[derive(Debug, Error)]
pub enum SettingsStoreError {
    /// SQLite query/bind failure.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    /// JSON serialization/deserialization failure (corrupted row or a
    /// non-object stored value).
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Load the persisted `ServerConfig` document.
///
/// Returns `Ok(None)` when no row exists (fresh database, pre-SRV-1 schema,
/// or the document was never written) — callers fall back to defaults.
pub async fn load_config(pool: &SqlitePool) -> Result<Option<Value>, SettingsStoreError> {
    load_document(pool, "server_config").await
}

/// Load the persisted `ServerSettings` document.
///
/// Returns `Ok(None)` when no row exists — callers fall back to defaults.
pub async fn load_settings(pool: &SqlitePool) -> Result<Option<Value>, SettingsStoreError> {
    load_document(pool, "server_settings").await
}

/// Persist (upsert) the `ServerConfig` document. Replaces any prior row.
pub async fn save_config(pool: &SqlitePool, config: &Value) -> Result<(), SettingsStoreError> {
    save_document(pool, "server_config", config).await
}

/// Persist (upsert) the `ServerSettings` document. Replaces any prior row.
pub async fn save_settings(pool: &SqlitePool, settings: &Value) -> Result<(), SettingsStoreError> {
    save_document(pool, "server_settings", settings).await
}

/// Generic single-row load. Returns the deserialized JSON value or `None` when
/// the row is absent. Shared by both config and settings tables — only the
/// table name differs.
async fn load_document(
    pool: &SqlitePool,
    table: &'static str,
) -> Result<Option<Value>, SettingsStoreError> {
    // Table name is a compile-time constant (not user input), so inline
    // formatting is safe from SQL injection.
    let query = format!("SELECT value FROM {table} WHERE key = ?");
    let row: Option<(String,)> = sqlx::query_as(&query)
        .bind(SINGLETON_KEY)
        .fetch_optional(pool)
        .await?;
    match row {
        Some((raw,)) => {
            let value = serde_json::from_str::<Value>(&raw)
                .map_err(|e| SettingsStoreError::Serialization(e.to_string()))?;
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

/// Generic single-row upsert. Serializes the value to JSON and replaces any
/// prior row with `SINGLETON_KEY`. Shared by both tables.
async fn save_document(
    pool: &SqlitePool,
    table: &'static str,
    value: &Value,
) -> Result<(), SettingsStoreError> {
    let data = serde_json::to_string(value)
        .map_err(|e| SettingsStoreError::Serialization(e.to_string()))?;
    let query = format!(
        "INSERT INTO {table} (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value"
    );
    sqlx::query(&query)
        .bind(SINGLETON_KEY)
        .bind(&data)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an in-memory pool with the SRV-1 schema initialized.
    async fn setup() -> SqlitePool {
        crate::init_database(std::path::Path::new(""))
            .await
            .expect("init_database should succeed")
    }

    #[tokio::test]
    async fn load_returns_none_on_fresh_db() {
        let pool = setup().await;
        // No prior write — both loads should return None (defaults).
        assert!(load_config(&pool).await.unwrap().is_none());
        assert!(load_settings(&pool).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_then_load_roundtrip() {
        let pool = setup().await;
        let cfg = serde_json::json!({ "cwd": "/x", "authMode": "unsafe-no-auth" });
        let settings = serde_json::json!({ "defaultThreadEnvMode": "local" });

        save_config(&pool, &cfg).await.unwrap();
        save_settings(&pool, &settings).await.unwrap();

        let loaded_cfg = load_config(&pool).await.unwrap().unwrap();
        let loaded_settings = load_settings(&pool).await.unwrap().unwrap();

        assert_eq!(loaded_cfg, cfg);
        assert_eq!(loaded_settings, settings);
    }

    #[tokio::test]
    async fn save_upserts_replacing_prior_row() {
        let pool = setup().await;
        let first = serde_json::json!({ "v": 1 });
        let second = serde_json::json!({ "v": 2 });

        save_config(&pool, &first).await.unwrap();
        save_config(&pool, &second).await.unwrap();

        let loaded = load_config(&pool).await.unwrap().unwrap();
        assert_eq!(loaded, second);
        // Confirm only one row remains (no duplicate insert).
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM server_config WHERE key = 'singleton'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1);
    }
}

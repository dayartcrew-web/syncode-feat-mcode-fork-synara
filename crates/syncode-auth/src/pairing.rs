//! Pairing links — short-TTL credentials a new client presents to bootstrap
//!
//! A [`PairingLink`] is a one-shot bootstrap credential an already-authenticated
//! Owner mints (e.g. via `auth.createPairingCredential`) so a second device can
//! join without possessing the long-lived owner token. Each link carries:
//!
//! - `id` — stable handle the owner uses to revoke / list.
//! - `credential` — the opaque secret the joining client presents to
//!   `auth/bootstrap`. Random, non-guessable (UUID v4, 122 bits of entropy) —
//!   same generation strategy as [`crate::session::SessionToken`].
//! - `expires_at` — short TTL (default 15 min). An expired link is treated as
//!   absent on read and is excluded from `list`.
//! - `role` — the [`Role`] the joining client will be granted on bootstrap.
//!
//! The [`PairingLinkStore`] trait abstracts persistence so the bearer-session
//! path stays backend-agnostic. Two implementations ship:
//!
//! - [`InMemoryPairingLinkStore`] — HashMap-backed; the default in
//!   `AuthMode::UnsafeNoAuth` / tests / `WsState::new_in_memory`.
//! - [`SqlitePairingLinkStore`] — SQLite-backed via `sqlx`; persists across
//!   restarts. The `pairing_links(id, credential, expires_at, role)` table is
//!   created idempotently on construction (mirrors syncode-persistence's
//!   `init_database` DDL pattern).
//!
//! # Security notes
//!
//! Pairing links are short-lived *and* revocable by design. Credential strings
//! are returned to the caller exactly once on `create`; they are stored so the
//! authenticator can later match a presented credential during bootstrap.
//! `list` returns handles + metadata only — never log the `credential` field.

use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::RwLock;
use syncode_core::Timestamp;
use thiserror::Error;

use crate::principal::Role;

/// Default time-to-live for a freshly minted pairing link (15 minutes).
///
/// Pairing flows are interactive ("approve the new device now"); a short TTL
/// bounds the blast radius of a leaked link. Callers may override via
/// [`PairingLinkStore::create_with_ttl`].
pub const DEFAULT_PAIRING_TTL: Duration = Duration::minutes(15);

/// A short-TTL bootstrap credential a new client presents to authenticate.
///
/// See the [module docs](self) for the field semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairingLink {
    /// Stable identifier for the link (UUID v4 string). Used as the revocation
    /// / list handle. NOT the secret presented during bootstrap.
    pub id: String,
    /// The opaque secret a joining client presents to `auth/bootstrap`. Random,
    /// non-guessable (UUID v4). Returned to the minter once on `create`.
    pub credential: String,
    /// When the link stops being honored. Short by design
    /// ([`DEFAULT_PAIRING_TTL`]).
    pub expires_at: Timestamp,
    /// The [`Role`] a successful bootstrap with this credential grants.
    pub role: Role,
}

impl PairingLink {
    /// Whether this link has expired relative to `now`.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.as_datetime() <= &now
    }
}

/// Errors from pairing-link store operations.
#[derive(Debug, Error)]
pub enum PairingStoreError {
    /// A SQL / I/O failure prevented the operation.
    #[error("pairing store error: {0}")]
    Database(#[from] sqlx::Error),
    /// A migration / DDL failure prevented store initialization.
    #[error("pairing store migration error: {0}")]
    Migration(String),
}

/// Backend-agnostic pairing-link persistence.
///
/// Implementations are expected to be cheap and side-effect-free beyond row
/// inserts/deletes. `list` MUST exclude expired links (lazily filtered against
/// `now`), so callers never observe stale entries.
#[async_trait::async_trait]
pub trait PairingLinkStore: Send + Sync {
    /// Mint a new pairing link with the default TTL ([`DEFAULT_PAIRING_TTL`]).
    async fn create(&self, role: Role) -> Result<PairingLink, PairingStoreError> {
        self.create_with_ttl(role, DEFAULT_PAIRING_TTL).await
    }

    /// Mint a new pairing link with an explicit TTL. Returns the link (the
    /// `credential` is the secret the joining client must present).
    async fn create_with_ttl(
        &self,
        role: Role,
        ttl: Duration,
    ) -> Result<PairingLink, PairingStoreError>;

    /// Revoke a link by id. Returns whether a (live or expired) row was
    /// removed. Idempotent — a second call returns `false`.
    async fn revoke(&self, id: &str) -> Result<bool, PairingStoreError>;

    /// List all non-expired links (relative to `now`). Excludes the stale ones
    /// so callers never see dead handles.
    async fn list(&self, now: DateTime<Utc>) -> Result<Vec<PairingLink>, PairingStoreError>;

    /// Resolve a presented credential to its link, iff present AND not expired.
    /// The authenticator consults this during bootstrap.
    async fn get_by_credential(
        &self,
        credential: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<PairingLink>, PairingStoreError>;
}

/// Generate a fresh opaque credential string (UUID v4 — 122 bits of entropy).
///
/// Mirrors [`crate::session::SessionToken::generate`]'s entropy source so the
/// two token families are uniformly unguessable.
fn generate_credential() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Generate a fresh stable id for a pairing link.
fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// In-memory implementation
// ---------------------------------------------------------------------------

/// In-memory `PairingLinkStore` — HashMap behind a `RwLock`.
///
/// The default for `AuthMode::UnsafeNoAuth` and the `WsState::new_in_memory`
/// test path: pairing links survive only for the process lifetime, which is
/// fine when the trust boundary is the OS user / loopback.
#[derive(Debug, Default)]
pub struct InMemoryPairingLinkStore {
    links: RwLock<HashMap<String, PairingLink>>,
}

impl InMemoryPairingLinkStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored links (including possibly-expired ones not yet
    /// filtered). Intended for diagnostics / tests.
    pub fn len(&self) -> usize {
        self.links.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.links.read().unwrap().is_empty()
    }
}

#[async_trait::async_trait]
impl PairingLinkStore for InMemoryPairingLinkStore {
    async fn create_with_ttl(
        &self,
        role: Role,
        ttl: Duration,
    ) -> Result<PairingLink, PairingStoreError> {
        let link = PairingLink {
            id: generate_id(),
            credential: generate_credential(),
            expires_at: Timestamp::from_datetime(Utc::now() + ttl),
            role,
        };
        self.links
            .write()
            .unwrap()
            .insert(link.id.clone(), link.clone());
        Ok(link)
    }

    async fn revoke(&self, id: &str) -> Result<bool, PairingStoreError> {
        Ok(self.links.write().unwrap().remove(id).is_some())
    }

    async fn list(&self, now: DateTime<Utc>) -> Result<Vec<PairingLink>, PairingStoreError> {
        let links = self.links.read().unwrap();
        Ok(links
            .values()
            .filter(|l| !l.is_expired(now))
            .cloned()
            .collect())
    }

    async fn get_by_credential(
        &self,
        credential: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<PairingLink>, PairingStoreError> {
        let links = self.links.read().unwrap();
        Ok(links
            .values()
            .find(|l| l.credential == credential && !l.is_expired(now))
            .cloned())
    }
}

// ---------------------------------------------------------------------------
// SQLite implementation
// ---------------------------------------------------------------------------

/// SQLite-backed `PairingLinkStore`.
///
/// Persists the `pairing_links(id, credential, expires_at, role)` table across
/// restarts. The schema is created idempotently on [`SqlitePairingLinkStore::new`]
/// (mirrors syncode-persistence's `init_database` DDL pattern). `expires_at` is
/// stored as an ISO-8601 string so it round-trips through chrono losslessly.
///
/// Share the same `SqlitePool` the rest of the server uses — the pairing table
/// is independent and adds no foreign keys, so it can live in the same DB file
/// without coordinating migrations with the event store.
pub struct SqlitePairingLinkStore {
    pool: sqlx::SqlitePool,
}

impl SqlitePairingLinkStore {
    /// Construct against an existing pool, creating the `pairing_links` table
    /// if absent. Idempotent — safe to call on every server start.
    pub async fn new(pool: sqlx::SqlitePool) -> Result<Self, PairingStoreError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pairing_links (
                id          TEXT    PRIMARY KEY,
                credential  TEXT    NOT NULL,
                expires_at  TEXT    NOT NULL,
                role        TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pairing_credential ON pairing_links(credential);
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| PairingStoreError::Migration(e.to_string()))?;
        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl PairingLinkStore for SqlitePairingLinkStore {
    async fn create_with_ttl(
        &self,
        role: Role,
        ttl: Duration,
    ) -> Result<PairingLink, PairingStoreError> {
        let link = PairingLink {
            id: generate_id(),
            credential: generate_credential(),
            expires_at: Timestamp::from_datetime(Utc::now() + ttl),
            role,
        };
        sqlx::query(
            "INSERT INTO pairing_links (id, credential, expires_at, role) VALUES (?, ?, ?, ?)",
        )
        .bind(&link.id)
        .bind(&link.credential)
        .bind(link.expires_at.as_datetime().to_rfc3339())
        .bind(role_to_str(link.role))
        .execute(&self.pool)
        .await?;
        Ok(link)
    }

    async fn revoke(&self, id: &str) -> Result<bool, PairingStoreError> {
        let res = sqlx::query("DELETE FROM pairing_links WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn list(&self, now: DateTime<Utc>) -> Result<Vec<PairingLink>, PairingStoreError> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT id, credential, expires_at, role FROM pairing_links ORDER BY expires_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        let now_str = now.to_rfc3339();
        Ok(rows
            .into_iter()
            .map(row_to_link)
            .filter(|l| l.expires_at.as_datetime().to_rfc3339() > now_str)
            .collect())
    }

    async fn get_by_credential(
        &self,
        credential: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<PairingLink>, PairingStoreError> {
        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT id, credential, expires_at, role FROM pairing_links WHERE credential = ?",
        )
        .bind(credential)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else { return Ok(None) };
        let link = row_to_link(row);
        if link.is_expired(now) {
            return Ok(None);
        }
        Ok(Some(link))
    }
}

/// Serialize a [`Role`] for the `role` column (snake_case to match the wire
/// format — `owner` | `client`).
fn role_to_str(role: Role) -> &'static str {
    match role {
        Role::Owner => "owner",
        Role::Client => "client",
    }
}

/// Parse a `role` column value back into a [`Role`]. Unknown values default to
/// `Client` (least-privilege) — defensive against a future Role variant having
/// been written by a newer server then read back here.
fn str_to_role(s: &str) -> Role {
    match s {
        "owner" => Role::Owner,
        _ => Role::Client,
    }
}

/// Map a decoded row into a [`PairingLink`].
fn row_to_link(
    (id, credential, expires_at_str, role_str): (String, String, String, String),
) -> PairingLink {
    let expires_at = DateTime::parse_from_rfc3339(&expires_at_str)
        .map(|dt| Timestamp::from_datetime(dt.with_timezone(&Utc)))
        .unwrap_or_else(|_| Timestamp::from_datetime(Utc::now()));
    PairingLink {
        id,
        credential,
        expires_at,
        role: str_to_role(&role_str),
    }
}

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    // ── In-memory: create / list / revoke ────────────────────────────────

    #[tokio::test]
    async fn inmemory_create_returns_non_empty_credential_and_lists() {
        let store = InMemoryPairingLinkStore::new();
        let link = store.create(Role::Owner).await.unwrap();

        assert!(!link.id.is_empty());
        assert!(!link.credential.is_empty());
        assert_ne!(link.id, link.credential, "id and credential must differ");
        assert_eq!(link.role, Role::Owner);

        let listed = store.list(now()).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, link.id);
        // list MUST redact nothing here but must surface the same handle.
        assert_eq!(listed[0].credential, link.credential);
    }

    #[tokio::test]
    async fn inmemory_revoke_removes_link_and_is_idempotent() {
        let store = InMemoryPairingLinkStore::new();
        let link = store.create(Role::Client).await.unwrap();

        assert!(
            store.revoke(&link.id).await.unwrap(),
            "first revoke removes"
        );
        assert!(
            !store.revoke(&link.id).await.unwrap(),
            "second revoke is a no-op"
        );
        assert!(store.list(now()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn inmemory_list_excludes_expired_links() {
        let store = InMemoryPairingLinkStore::new();
        let _alive = store.create(Role::Owner).await.unwrap();
        let short = store
            .create_with_ttl(Role::Client, Duration::seconds(1))
            .await
            .unwrap();

        // Still present immediately.
        assert_eq!(store.list(now()).await.unwrap().len(), 2);

        // Advance past the short link's TTL.
        let future = now() + Duration::seconds(3);
        let listed = store.list(future).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_ne!(listed[0].id, short.id, "expired link excluded");
    }

    #[tokio::test]
    async fn inmemory_get_by_credential_returns_only_live_links() {
        let store = InMemoryPairingLinkStore::new();
        let link = store
            .create_with_ttl(Role::Owner, Duration::seconds(10))
            .await
            .unwrap();

        // Live credential resolves.
        let resolved = store
            .get_by_credential(&link.credential, now())
            .await
            .unwrap();
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().id, link.id);

        // Bogus credential does not.
        assert!(
            store
                .get_by_credential("nope", now())
                .await
                .unwrap()
                .is_none()
        );

        // After expiry, the same credential no longer resolves.
        let expired = store
            .get_by_credential(&link.credential, now() + Duration::seconds(20))
            .await
            .unwrap();
        assert!(expired.is_none());
    }

    // ── SQLite: persistence + create / list / revoke ─────────────────────

    /// Spin up an isolated on-disk SQLite DB per test (tempfile). `:memory:`
    /// with a multi-connection pool is awkward (each connection owns its own
    /// DB), and `cache=shared` collides across parallel tests, so a fresh
    /// tempfile is the cleanest isolation strategy. The tempfile lives for the
    /// test's duration; the OS cleans it up.
    async fn sqlite_store() -> SqlitePairingLinkStore {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("pairing_test.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        // Hold the tempdir for the test's lifetime by leaking it — tests are
        // short-lived processes and the OS reclaims the space on exit. (This
        // avoids needing to thread a lifetime through the pool.)
        std::mem::forget(dir);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        SqlitePairingLinkStore::new(pool).await.unwrap()
    }

    #[tokio::test]
    async fn sqlite_create_then_list_roundtrip() {
        let store = sqlite_store().await;
        let link = store.create(Role::Owner).await.unwrap();

        assert!(!link.credential.is_empty());
        let listed = store.list(now()).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, link.id);
        assert_eq!(listed[0].role, Role::Owner);
    }

    #[tokio::test]
    async fn sqlite_revoke_removes_row() {
        let store = sqlite_store().await;
        let link = store.create(Role::Client).await.unwrap();

        assert!(store.revoke(&link.id).await.unwrap());
        assert!(!store.revoke(&link.id).await.unwrap());
        assert!(store.list(now()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sqlite_list_excludes_expired_and_get_by_credential_works() {
        let store = sqlite_store().await;
        let _alive = store.create(Role::Owner).await.unwrap();
        let short = store
            .create_with_ttl(Role::Client, Duration::seconds(1))
            .await
            .unwrap();

        // Both present now.
        assert_eq!(store.list(now()).await.unwrap().len(), 2);
        assert!(
            store
                .get_by_credential(&short.credential, now())
                .await
                .unwrap()
                .is_some()
        );

        // Advance clock: short link drops out of list + credential lookup.
        let future = now() + Duration::seconds(3);
        assert_eq!(store.list(future).await.unwrap().len(), 1);
        assert!(
            store
                .get_by_credential(&short.credential, future)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn sqlite_table_creation_is_idempotent() {
        // Calling `new` twice on the same pool must not error (CREATE TABLE
        // IF NOT EXISTS). Verifies the migration is safe to re-run on startup.
        let store = sqlite_store().await;
        let again = SqlitePairingLinkStore::new(store.pool.clone()).await;
        assert!(again.is_ok(), "second construction should succeed");
    }

    #[test]
    fn role_str_roundtrip() {
        assert_eq!(role_to_str(Role::Owner), "owner");
        assert_eq!(role_to_str(Role::Client), "client");
        assert_eq!(str_to_role("owner"), Role::Owner);
        assert_eq!(str_to_role("client"), Role::Client);
        // Unknown → least-privilege.
        assert_eq!(str_to_role("future-role"), Role::Client);
    }
}

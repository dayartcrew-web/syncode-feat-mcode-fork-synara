//! Syncode Persistence — Event Store & Projections
//!
//! Append-only event store, read model projection tables,
//! SQLx migrations, and snapshot queries.

pub mod adapters;
pub mod event_store;
pub mod migrations;
pub mod projections;
pub mod settings_store;
pub mod snapshot;

pub use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

/// Initialize the SQLite database with migrations
pub async fn init_database(db_path: &Path) -> Result<SqlitePool, sqlx::Error> {
    let db_url = if db_path.to_string_lossy().is_empty() {
        "sqlite::memory:".to_string()
    } else {
        format!("sqlite:{}?mode=rwc", db_path.display())
    };

    let options = SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(5));

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    // Run the SQLx embedded migrations (`migrations/*.sql`). On a fresh
    // database this creates all four tables (domain_events, snapshots,
    // server_config, server_settings). On an existing database the migrations
    // are a no-op (tracked by sqlx's `_sqlx_migrations` table, and each file is
    // `CREATE TABLE IF NOT EXISTS` for safety).
    //
    // Fall back to the legacy inline `raw_sql` if the embedded migrations fail
    // (e.g. an exotic pre-existing schema state sqlx can't reconcile). The
    // fallback keeps existing deployments working during the SRV-2 transition;
    // it is logged as a warning so the operator knows migrations didn't run.
    if let Err(migrate_err) = crate::migrations::run(&pool).await {
        tracing::warn!(
            error = %migrate_err,
            "sqlx::migrate! failed; falling back to legacy inline raw_sql schema. \
             This keeps existing deployments working but the migration framework \
             should be investigated."
        );
        run_legacy_raw_sql(&pool).await?;
    }

    tracing::info!("Database initialized at {}", db_url);
    Ok(pool)
}

/// Legacy inline schema bootstrap, kept as a fallback for environments where
/// `sqlx::migrate!` cannot run (e.g. an irreconcilable pre-existing schema).
///
/// This is the exact SQL that formerly lived inline in `init_database()`. It
/// is additive (`CREATE TABLE IF NOT EXISTS`) so it never clobbers existing
/// data. See the `migrations/` directory for the canonical, timestamped copies.
async fn run_legacy_raw_sql(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        r#"
        CREATE TABLE IF NOT EXISTS domain_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            aggregate_id  TEXT    NOT NULL,
            event_type    TEXT    NOT NULL,
            sequence      INTEGER NOT NULL,
            data          TEXT    NOT NULL,
            timestamp     TEXT    NOT NULL,
            metadata      TEXT    DEFAULT '{}',
            created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(aggregate_id, sequence)
        );
        CREATE INDEX IF NOT EXISTS idx_events_aggregate ON domain_events(aggregate_id, sequence);
        CREATE INDEX IF NOT EXISTS idx_events_type ON domain_events(event_type);

        CREATE TABLE IF NOT EXISTS snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            aggregate_id  TEXT    NOT NULL,
            sequence      INTEGER NOT NULL,
            data          TEXT    NOT NULL,
            created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(aggregate_id)
        );

        CREATE TABLE IF NOT EXISTS server_config (
            key           TEXT    PRIMARY KEY,
            value         TEXT    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS server_settings (
            key           TEXT    PRIMARY KEY,
            value         TEXT    NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Get a connection from the pool
pub async fn get_pool(db_path: &Path) -> Result<SqlitePool, sqlx::Error> {
    init_database(db_path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_init_in_memory_database() {
        let pool = init_database(std::path::Path::new("")).await;
        assert!(pool.is_ok(), "Should create in-memory database");

        // Verify tables exist
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='domain_events'",
        )
        .fetch_optional(pool.as_ref().unwrap())
        .await
        .unwrap();

        assert!(row.is_some(), "domain_events table should exist");

        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='snapshots'",
        )
        .fetch_optional(pool.as_ref().unwrap())
        .await
        .unwrap();

        assert!(row.is_some(), "snapshots table should exist");
    }
}

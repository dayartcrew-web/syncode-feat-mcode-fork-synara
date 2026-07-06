//! SQLx embedded migrations.
//!
//! The `migrations/` directory (relative to this crate's `src/` parent, i.e.
//! `crates/syncode-persistence/migrations/`) holds the canonical, timestamped
//! SQLx migration files:
//!
//! | File | Table(s) |
//! |------|----------|
//! | `20240101000001_domain_events.sql` | `domain_events` (+ indexes) |
//! | `20240101000002_snapshots.sql`     | `snapshots` |
//! | `20240101000003_server_config.sql` | `server_config` (SRV-1) |
//! | `20240101000004_server_settings.sql` | `server_settings` (SRV-1) |
//!
//! [`run`] applies all pending migrations to a connection. It is the primary
//! schema-bootstrap path; [`crate::init_database`] calls it and falls back to
//! the legacy inline `raw_sql` only if the embedded migrations fail (logged as
//! a warning), so existing deployments keep working during the transition.

use sqlx::SqlitePool;

/// Compile-time-checked SQLx migrator for this crate's `migrations/` directory.
///
/// The `sqlx::migrate!` macro embeds the migration files at build time and
/// validates them against the `sqlx` `migrate` feature. Resolves to a
/// [`sqlx::migrate::Migrator`].
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run all pending migrations against the given pool.
///
/// On a fresh database all four migrations apply (creating `domain_events`,
/// `snapshots`, `server_config`, `server_settings`); on an already-migrated
/// database this is a no-op (tracked by sqlx's `_sqlx_migrations` table).
pub async fn run(pool: &SqlitePool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrate_runs_clean_on_empty_db() {
        // A fresh in-memory database should accept all migrations without error
        // and create the expected tables.
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        run(&pool).await.expect("migrations should run clean");

        for table in [
            "domain_events",
            "snapshots",
            "server_config",
            "server_settings",
        ] {
            let row: Option<(i64,)> =
                sqlx::query_as("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?")
                    .bind(table)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            assert!(row.is_some(), "{table} row should exist");
            assert_eq!(row.unwrap().0, 1, "{table} table should be created");
        }

        // Re-running should be a no-op (idempotent) — sqlx tracks applied
        // migrations in `_sqlx_migrations`, so the second run does nothing.
        run(&pool).await.expect("re-run should be clean");
    }

    #[tokio::test]
    async fn migrate_is_idempotent_with_legacy_raw_sql_schema() {
        // Simulate a legacy database that already has the tables created via the
        // former inline `raw_sql` path. The migrations use `CREATE TABLE IF NOT
        // EXISTS`, so applying them on top of the legacy schema must not error.
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            r#"
            CREATE TABLE domain_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL, sequence INTEGER NOT NULL, data TEXT NOT NULL,
                timestamp TEXT NOT NULL, metadata TEXT DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(aggregate_id, sequence)
            );
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT, aggregate_id TEXT NOT NULL,
                sequence INTEGER NOT NULL, data TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE(aggregate_id)
            );
            CREATE TABLE server_config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE server_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // The migration runner tracks applied state via its own `_sqlx_migrations`
        // table; on a legacy DB that table is absent, so sqlx will attempt to run
        // the migrations. Because they are all `CREATE TABLE IF NOT EXISTS`, the
        // pre-existing tables are left untouched and no error is raised.
        let result = run(&pool).await;
        assert!(result.is_ok(), "migrations must compose with legacy schema");
    }
}

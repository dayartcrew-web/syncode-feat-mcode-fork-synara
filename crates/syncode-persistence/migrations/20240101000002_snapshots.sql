-- Aggregate snapshots — periodic serialized state to avoid full event replay.
--
-- Extracted from the former inline `raw_sql` in `init_database()` (SRV-2).
-- Idempotent (`CREATE TABLE IF NOT EXISTS`) so it composes with an existing
-- legacy schema and replays cleanly on a fresh DB.

CREATE TABLE IF NOT EXISTS snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_id  TEXT    NOT NULL,
    sequence      INTEGER NOT NULL,
    data          TEXT    NOT NULL,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(aggregate_id)
);

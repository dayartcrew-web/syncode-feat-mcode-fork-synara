-- Domain event store — append-only event log backing the CQRS/ES pipeline.
--
-- Extracted from the former inline `raw_sql` in `init_database()` (SRV-2).
-- `CREATE TABLE IF NOT EXISTS` keeps the migration idempotent: it replays
-- cleanly on a fresh DB and is a no-op on an existing DB that already created
-- the table via the legacy `raw_sql` path.

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

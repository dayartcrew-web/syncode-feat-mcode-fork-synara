-- Syncode Persistence: Initial Schema
-- Event Store, Read Model Projections, Snapshots

CREATE TABLE IF NOT EXISTS domain_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_id  TEXT    NOT NULL,
    event_type    TEXT    NOT NULL,
    sequence      INTEGER NOT NULL,
    data          TEXT    NOT NULL,          -- JSON serialized event
    timestamp     TEXT    NOT NULL,          -- ISO 8601
    metadata      TEXT    DEFAULT '{}',      -- JSON metadata
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(aggregate_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate ON domain_events(aggregate_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_type ON domain_events(event_type);

-- Snapshot table for aggregate state
CREATE TABLE IF NOT EXISTS snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_id  TEXT    NOT NULL,
    sequence      INTEGER NOT NULL,          -- sequence at snapshot time
    data          TEXT    NOT NULL,          -- JSON serialized state
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),

    UNIQUE(aggregate_id)
);

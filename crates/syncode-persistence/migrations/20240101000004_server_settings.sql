-- Server settings persistence (SRV-1, carried into migrations by SRV-2).
--
-- Single-row key/value table holding the serialized `ServerSettings` JSON
-- document (see crates/syncode-ws/src/settings.rs). A fixed `singleton` key
-- holds the document — the MCode UI models the server settings as a single
-- document, so a single upsertable row is sufficient.
--
-- Idempotent (`CREATE TABLE IF NOT EXISTS`) so it composes with an existing
-- legacy schema and replays cleanly on a fresh DB.

CREATE TABLE IF NOT EXISTS server_settings (
    key           TEXT    PRIMARY KEY,
    value         TEXT    NOT NULL
);

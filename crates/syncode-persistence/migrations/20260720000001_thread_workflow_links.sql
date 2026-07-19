-- Sidecar table mapping chat thread_id -> active workflow_id.
-- Threads are event-sourced aggregates; this table avoids touching the
-- aggregate while still allowing O(1) lookup of "which workflow is
-- active for this thread?".
CREATE TABLE IF NOT EXISTS thread_workflow_links (
    thread_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    linked_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_thread_workflow_links_workflow
    ON thread_workflow_links(workflow_id);

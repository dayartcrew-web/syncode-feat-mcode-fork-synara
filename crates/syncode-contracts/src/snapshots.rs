//! Snapshot DTOs for snapshot-then-stream WebSocket subscriptions.
//!
//! When a client subscribes to a push channel (or reconnects and re-subscribes),
//! the server emits an initial **snapshot** of the current read-model state for
//! that channel's scope, then continues with live deltas. These types are the
//! wire shapes for those snapshots, shared between Rust and the TypeScript
//! frontend via ts-rs.
//!
//! Field types intentionally mirror the orchestration read-model views
//! (`syncode-orchestration::read_model`) — plain `String` ids/timestamps — so
//! the WS layer can map views → DTOs with trivial field copies.
//!
//! On the wire, a snapshot rides as a push notification:
//! ```text
//! { "jsonrpc": "2.0", "method": "push/<channel>",
//!   "params": { "event_type": "snapshot", "aggregate_id": null,
//!               "data": <one of these DTOs as JSON> } }
//! ```

use serde::{Deserialize, Serialize};
use ts_rs::TS;

// ─── Summary types (slim views for snapshot payloads) ───────────────

/// A project's key fields, as carried in a snapshot. Faithful to
/// `syncode_orchestration::ProjectView` (slimmed to the fields a client needs
/// for shell/list views).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    pub root_path: String,
    pub provider_id: Option<String>,
    pub default_model: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub thread_count: u32,
}

/// A thread's key fields, as carried in a snapshot. Faithful to
/// `syncode_orchestration::ThreadView`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ThreadSummary {
    pub id: String,
    pub project_id: String,
    pub provider_id: String,
    pub model: String,
    pub status: String,
    pub title: Option<String>,
    pub git_checkpoint: Option<String>,
    pub runtime_mode: String,
    pub interaction_mode: String,
    pub turn_count: u32,
    pub created_at: String,
    pub updated_at: String,
}

/// A turn's key fields, as carried in a thread-detail snapshot. Faithful to
/// `syncode_orchestration::TurnView`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TurnSummary {
    pub id: String,
    pub thread_id: String,
    pub sequence: u32,
    pub user_input: String,
    pub assistant_output: Option<String>,
    pub status: String,
    pub git_checkpoint: Option<String>,
    pub files_modified: Vec<String>,
    pub duration_ms: Option<u64>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

/// A message in a thread-detail snapshot. Faithful to
/// `syncode_orchestration::MessageView` (NOT the contracts `MessageView`, which
/// is a different, session-oriented type).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MessageSummary {
    pub id: String,
    pub turn_id: String,
    pub role: String,
    pub content: String,
    pub content_type: String,
    pub token_count: Option<u32>,
    pub tool_name: Option<String>,
    pub tool_call_id: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub is_streaming: bool,
}

/// An activity log entry in a full snapshot. Faithful to
/// `syncode_orchestration::ActivityView`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ActivitySummary {
    pub id: String,
    pub activity_type: String,
    pub description: String,
    pub project_id: Option<String>,
    pub thread_id: Option<String>,
    #[ts(type = "Record<string, unknown>")]
    pub metadata: serde_json::Value,
    pub created_at: String,
}

// ─── Snapshot envelopes (one per scope) ──────────────────────────────

/// The scope a snapshot covers. Carried in every snapshot envelope so the
/// client knows which of the snapshot DTOs to decode `data` as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum SnapshotScope {
    /// Shell view: all projects + all threads (the sidebar/navigation state).
    Shell,
    /// One thread's detail: the thread + its turns + messages.
    Thread,
    /// Entire read model (every collection). Used by the `*` wildcard channel.
    Full,
}

/// Shell-channel snapshot — projects + threads. Sent on `orchestration`
/// channel subscribe (when no `threadId` is given).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ShellSnapshot {
    pub scope: SnapshotScope,
    pub projects: Vec<ProjectSummary>,
    pub threads: Vec<ThreadSummary>,
    /// ISO-8601 timestamp at which the snapshot was read.
    pub snapshot_at: String,
}

/// Thread-detail snapshot — one thread + its turns + messages. Sent on
/// `orchestration` channel subscribe when a `threadId` is given.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ThreadDetailSnapshot {
    pub scope: SnapshotScope,
    pub thread: ThreadSummary,
    pub turns: Vec<TurnSummary>,
    pub messages: Vec<MessageSummary>,
    pub snapshot_at: String,
}

/// Full-store snapshot — every collection. Sent on `*` wildcard subscribe.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FullSnapshot {
    pub scope: SnapshotScope,
    pub projects: Vec<ProjectSummary>,
    pub threads: Vec<ThreadSummary>,
    pub turns: Vec<TurnSummary>,
    pub messages: Vec<MessageSummary>,
    pub activities: Vec<ActivitySummary>,
    pub snapshot_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_snapshot_roundtrip() {
        let snap = ShellSnapshot {
            scope: SnapshotScope::Shell,
            projects: vec![ProjectSummary {
                id: "p1".into(),
                name: "Demo".into(),
                root_path: "/tmp/demo".into(),
                provider_id: None,
                default_model: None,
                created_at: "2026-01-01T00:00:00Z".into(),
                updated_at: "2026-01-01T00:00:00Z".into(),
                thread_count: 1,
            }],
            threads: vec![],
            snapshot_at: "2026-01-01T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: ShellSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.scope, SnapshotScope::Shell);
        assert_eq!(back.projects.len(), 1);
        assert_eq!(back.projects[0].name, "Demo");
    }

    #[test]
    fn thread_detail_snapshot_roundtrip() {
        let snap = ThreadDetailSnapshot {
            scope: SnapshotScope::Thread,
            thread: thread_summary("t1"),
            turns: vec![turn_summary("turn-1")],
            messages: vec![],
            snapshot_at: "2026-01-01T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: ThreadDetailSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.thread.id, "t1");
        assert_eq!(back.turns.len(), 1);
    }

    #[test]
    fn full_snapshot_roundtrip() {
        let snap = FullSnapshot {
            scope: SnapshotScope::Full,
            projects: vec![],
            threads: vec![thread_summary("t1")],
            turns: vec![turn_summary("turn-1")],
            messages: vec![message_summary("m1")],
            activities: vec![],
            snapshot_at: "2026-01-01T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: FullSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.scope, SnapshotScope::Full);
        assert_eq!(back.threads.len(), 1);
        assert_eq!(back.messages.len(), 1);
    }

    #[test]
    fn snapshot_scope_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&SnapshotScope::Thread).unwrap(),
            "\"thread\""
        );
        let back: SnapshotScope = serde_json::from_str("\"full\"").unwrap();
        assert_eq!(back, SnapshotScope::Full);
    }

    fn thread_summary(id: &str) -> ThreadSummary {
        ThreadSummary {
            id: id.into(),
            project_id: "p1".into(),
            provider_id: "anthropic".into(),
            model: "claude".into(),
            status: "active".into(),
            title: None,
            git_checkpoint: None,
            runtime_mode: "full-access".into(),
            interaction_mode: "default".into(),
            turn_count: 0,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    fn turn_summary(id: &str) -> TurnSummary {
        TurnSummary {
            id: id.into(),
            thread_id: "t1".into(),
            sequence: 1,
            user_input: "hi".into(),
            assistant_output: None,
            status: "running".into(),
            git_checkpoint: None,
            files_modified: vec![],
            duration_ms: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            completed_at: None,
        }
    }

    fn message_summary(id: &str) -> MessageSummary {
        MessageSummary {
            id: id.into(),
            turn_id: "turn-1".into(),
            role: "user".into(),
            content: "hi".into(),
            content_type: "text".into(),
            token_count: None,
            tool_name: None,
            tool_call_id: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            is_streaming: false,
        }
    }
}

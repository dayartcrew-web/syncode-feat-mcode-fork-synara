//! Domain events — immutable facts about things that happened
//!
//! These are the core events in the Syncode system, persisted to the event store
//! and used to reconstruct state via replay.

use serde::{Deserialize, Serialize};
use crate::domain::primitives::{EntityId, Timestamp};

/// All domain event types in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum DomainEvent {
    // ─── Project Events ────────────────────────────────────────────────
    ProjectCreated {
        id: EntityId,
        name: String,
        root_path: String,
        created_at: Timestamp,
    },
    ProjectUpdated {
        id: EntityId,
        provider_id: Option<String>,
        default_model: Option<String>,
        updated_at: Timestamp,
    },

    // ─── Thread Events ──────────────────────────────────────────────────
    ThreadCreated {
        id: EntityId,
        project_id: EntityId,
        provider_id: String,
        model: String,
        created_at: Timestamp,
    },
    ThreadStatusChanged {
        id: EntityId,
        old_status: String,
        new_status: String,
        updated_at: Timestamp,
    },
    ThreadTitleSet {
        id: EntityId,
        title: String,
    },
    ThreadCheckpointSet {
        id: EntityId,
        git_ref: String,
    },

    // ─── Turn Events ────────────────────────────────────────────────────
    TurnStarted {
        id: EntityId,
        thread_id: EntityId,
        sequence: u32,
        user_input: String,
        created_at: Timestamp,
    },
    TurnCompleted {
        id: EntityId,
        assistant_output: String,
        duration_ms: u64,
        completed_at: Timestamp,
    },
    TurnFailed {
        id: EntityId,
        error: String,
        completed_at: Timestamp,
    },
    TurnCancelled {
        id: EntityId,
        completed_at: Timestamp,
    },
    TurnFilesModified {
        id: EntityId,
        files: Vec<String>,
    },
    TurnCheckpointSet {
        id: EntityId,
        git_ref: String,
    },

    // ─── Message Events ───────────────────────────────────────────────
    MessageAdded {
        id: EntityId,
        turn_id: EntityId,
        role: String,
        content: String,
        created_at: Timestamp,
    },

    // ─── Activity Events ────────────────────────────────────────────────
    ActivityLogged {
        id: EntityId,
        activity_type: String,
        description: String,
        created_at: Timestamp,
    },
}

impl DomainEvent {
    /// Get the aggregate ID this event belongs to
    pub fn aggregate_id(&self) -> EntityId {
        match self {
            Self::ProjectCreated { id, .. }
            | Self::ProjectUpdated { id, .. }
            | Self::ThreadCreated { id, .. }
            | Self::ThreadStatusChanged { id, .. }
            | Self::ThreadTitleSet { id, .. }
            | Self::ThreadCheckpointSet { id, .. }
            | Self::TurnStarted { id, .. }
            | Self::TurnCompleted { id, .. }
            | Self::TurnFailed { id, .. }
            | Self::TurnCancelled { id, .. }
            | Self::TurnFilesModified { id, .. }
            | Self::TurnCheckpointSet { id, .. }
            | Self::MessageAdded { id, .. }
            | Self::ActivityLogged { id, .. } => *id,
        }
    }

    /// Get the event type name as a string
    pub fn event_type_name(&self) -> &'static str {
        match self {
            Self::ProjectCreated { .. } => "ProjectCreated",
            Self::ProjectUpdated { .. } => "ProjectUpdated",
            Self::ThreadCreated { .. } => "ThreadCreated",
            Self::ThreadStatusChanged { .. } => "ThreadStatusChanged",
            Self::ThreadTitleSet { .. } => "ThreadTitleSet",
            Self::ThreadCheckpointSet { .. } => "ThreadCheckpointSet",
            Self::TurnStarted { .. } => "TurnStarted",
            Self::TurnCompleted { .. } => "TurnCompleted",
            Self::TurnFailed { .. } => "TurnFailed",
            Self::TurnCancelled { .. } => "TurnCancelled",
            Self::TurnFilesModified { .. } => "TurnFilesModified",
            Self::TurnCheckpointSet { .. } => "TurnCheckpointSet",
            Self::MessageAdded { .. } => "MessageAdded",
            Self::ActivityLogged { .. } => "ActivityLogged",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_aggregate_id_returns_entity_id() {
        let id = EntityId::new();
        let ev = DomainEvent::ProjectCreated {
            id,
            name: "test".to_string(),
            root_path: "/test".to_string(),
            created_at: Timestamp::now(),
        };
        assert_eq!(ev.aggregate_id(), id);
    }

    #[test]
    fn event_type_name_matches_variant() {
        let id = EntityId::new();
        let events = vec![
            (DomainEvent::ProjectCreated { id, name: "p".into(), root_path: "/p".into(), created_at: Timestamp::now() }, "ProjectCreated"),
            (DomainEvent::ThreadCreated { id, project_id: id, provider_id: "p".into(), model: "m".into(), created_at: Timestamp::now() }, "ThreadCreated"),
            (DomainEvent::TurnStarted { id, thread_id: id, sequence: 1, user_input: "hi".into(), created_at: Timestamp::now() }, "TurnStarted"),
            (DomainEvent::MessageAdded { id, turn_id: id, role: "user".into(), content: "msg".into(), created_at: Timestamp::now() }, "MessageAdded"),
            (DomainEvent::ActivityLogged { id, activity_type: "session_started".into(), description: "d".into(), created_at: Timestamp::now() }, "ActivityLogged"),
        ];
        for (ev, expected_name) in events {
            assert_eq!(ev.event_type_name(), expected_name);
        }
    }

    #[test]
    fn event_serialization_roundtrip() {
        let id = EntityId::new();
        let ev = DomainEvent::ProjectCreated {
            id,
            name: "serde-project".to_string(),
            root_path: "/tmp/serde".to_string(),
            created_at: Timestamp::now(),
        };
        let json = serde_json::to_string(&ev).expect("serialize");
        let back: DomainEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.event_type_name(), "ProjectCreated");
        assert_eq!(back.aggregate_id(), id);
    }

    #[test]
    fn event_tagged_serialization_includes_event_type() {
        let id = EntityId::new();
        let ev = DomainEvent::ThreadStatusChanged {
            id,
            old_status: "active".into(),
            new_status: "paused".into(),
            updated_at: Timestamp::now(),
        };
        let json = serde_json::to_value(&ev).expect("to_value");
        assert_eq!(json["event_type"], "ThreadStatusChanged");
        assert!(json.get("data").is_some());
    }
}

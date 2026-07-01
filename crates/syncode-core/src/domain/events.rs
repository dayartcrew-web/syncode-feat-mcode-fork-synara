//! Domain events — immutable facts about things that happened
//!
//! These are the core events in the Syncode system, persisted to the event store
//! and used to reconstruct state via replay.
//!
//! `DomainEvent` is the pure payload — the "what happened" data.
//! `Envelope` wraps a `DomainEvent` with stream-level metadata (sequence, timestamp)
//! and is what gets persisted to the event store.

use crate::domain::primitives::{DomainEvent as DomainEventTrait, EntityId, Timestamp};
use serde::{Deserialize, Serialize};

/// All domain event types in the system (the payload).
///
/// Each variant carries only the data relevant to that event type.
/// Stream-level metadata (sequence, timestamp) lives on [`Envelope`].
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
    /// A project was deleted (tombstone). Faithful to mcode `project.deleted`
    /// payload `{ projectId, deletedAt }` — hard, event-sourced delete.
    ProjectDeleted {
        id: EntityId,
        deleted_at: Timestamp,
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
    /// A thread was rolled back to a previously-captured git checkpoint.
    ThreadReverted {
        id: EntityId,
        git_ref: String,
        reverted_at: Timestamp,
    },
    /// A thread was archived. Faithful to mcode `thread.archived` {threadId, archivedAt}.
    ThreadArchived {
        id: EntityId,
        archived_at: Timestamp,
    },
    /// A thread was unarchived (restored to active). Faithful to mcode
    /// `thread.unarchived`.
    ThreadUnarchived {
        id: EntityId,
        unarchived_at: Timestamp,
    },
    /// A thread was deleted (tombstone). Faithful to mcode `thread.deleted`
    /// {threadId, deletedAt} — hard, event-sourced delete.
    ThreadDeleted {
        id: EntityId,
        deleted_at: Timestamp,
    },
    /// Messages were imported into a thread from a source thread (handoff/fork).
    /// Faithful to mcode's internal `thread.messages.import`: records the new
    /// thread, its source, and the number of imported messages. The message
    /// bodies live in the command; this event is the durable record of the
    /// import (read-model materialization of imported bodies is deferred).
    ThreadMessagesImported {
        thread_id: EntityId,
        source_thread_id: EntityId,
        count: u32,
        imported_at: Timestamp,
    },
    /// A request to stop the active provider session for a thread. Faithful to
    /// mcode `thread.session.stop` → ThreadSessionStopRequestedPayload
    /// {threadId, createdAt}. The "Requested" naming mirrors mcode: the actual
    /// session stop is an async side effect handled by the command reactor.
    ThreadSessionStopRequested {
        id: EntityId,
        requested_at: Timestamp,
    },
    /// A thread's runtime mode was set. Faithful to mcode
    /// `thread.runtime-mode-set` {threadId, runtimeMode, updatedAt}.
    ThreadRuntimeModeSet {
        id: EntityId,
        runtime_mode: String,
        updated_at: Timestamp,
    },
    /// A thread's provider interaction mode was set. Faithful to mcode
    /// `thread.interaction-mode-set` {threadId, interactionMode, updatedAt}.
    ThreadInteractionModeSet {
        id: EntityId,
        interaction_mode: String,
        updated_at: Timestamp,
    },
    /// A client responded to a pending provider approval request for a thread.
    /// Faithful to mcode `thread.approval.respond` — mcode dispatches the
    /// response to the provider with no dedicated orchestration payload; this
    /// event is the durable record of the response (consistent with syncode's
    /// `ThreadSessionStopRequested` Requested-style pattern for async provider ops).
    ThreadApprovalResponded {
        id: EntityId,
        request_id: String,
        decision: String,
        responded_at: Timestamp,
    },
    /// A client responded to a pending provider user-input request for a thread.
    /// Faithful to mcode `thread.user-input.respond`.
    ThreadUserInputResponded {
        id: EntityId,
        request_id: String,
        answers: String,
        responded_at: Timestamp,
    },
    /// A thread message was edited and a new provider turn triggered from it.
    /// Faithful to mcode `thread.message.edit-and-resend` (no dedicated payload;
    /// the resend surfaces via provider ingestion).
    ThreadMessageEditedAndResent {
        id: EntityId,
        message_id: EntityId,
        text: String,
        edited_at: Timestamp,
    },
    /// A thread's provider session state was set. Faithful to mcode
    /// `thread.session-set` {threadId, session: OrchestrationSession}. mcode nests
    /// the session under a `session` object; syncode flattens its fields onto the
    /// event (the established convention here — events carry typed primitives).
    /// The session models provider-session lifecycle: status, the active turn, the
    /// last error, and the mode it is running under.
    ThreadSessionSet {
        id: EntityId,
        status: String,
        provider_name: Option<String>,
        runtime_mode: String,
        active_turn_id: Option<EntityId>,
        last_error: Option<String>,
        updated_at: Timestamp,
    },
    /// A request to dispatch a queued turn to the provider for a thread. Faithful
    /// to mcode `thread.turn.dispatch-queued` → `thread.turn-start-requested`. The
    /// "Requested" naming mirrors mcode: the actual turn dispatch is an async side
    /// effect handled by the command reactor.
    TurnDispatchRequested {
        id: EntityId,
        message_id: EntityId,
        runtime_mode: String,
        interaction_mode: String,
        dispatch_mode: String,
        requested_at: Timestamp,
    },

    // ─── Pinned Message Events (thread sub-aggregate) ───────────────────
    /// A message was pinned to a thread. Faithful to mcode
    /// `thread.pinned-message-added` {threadId, pin:{messageId,label,done,pinnedAt}, updatedAt}.
    PinnedMessageAdded {
        thread_id: EntityId,
        message_id: EntityId,
        label: Option<String>,
        done: bool,
        pinned_at: Timestamp,
        updated_at: Timestamp,
    },
    /// A pinned message was removed from a thread. Faithful to mcode
    /// `thread.pinned-message-removed` {threadId, messageId, updatedAt}.
    PinnedMessageRemoved {
        thread_id: EntityId,
        message_id: EntityId,
        updated_at: Timestamp,
    },
    /// A pinned message's done flag was set. Faithful to mcode
    /// `thread.pinned-message-done-set` {threadId, messageId, done, updatedAt}.
    PinnedMessageDoneSet {
        thread_id: EntityId,
        message_id: EntityId,
        done: bool,
        updated_at: Timestamp,
    },
    /// A pinned message's label was set. Faithful to mcode
    /// `thread.pinned-message-label-set` {threadId, messageId, label, updatedAt}.
    PinnedMessageLabelSet {
        thread_id: EntityId,
        message_id: EntityId,
        label: Option<String>,
        updated_at: Timestamp,
    },

    // ─── Marker Events (thread sub-aggregate) ──────────────────────
    /// A marker was added to a thread message. Faithful to mcode
    /// `thread.marker-added` {threadId, marker:{id,messageId,startOffset,endOffset,selectedText,style,color,label,done}, updatedAt}.
    MarkerAdded {
        thread_id: EntityId,
        marker_id: EntityId,
        message_id: EntityId,
        start_offset: u64,
        end_offset: u64,
        selected_text: String,
        style: String,
        color: String,
        label: Option<String>,
        done: bool,
        created_at: Timestamp,
        updated_at: Timestamp,
    },
    /// A marker was removed from a thread. Faithful to mcode
    /// `thread.marker-removed` {threadId, markerId, updatedAt}.
    MarkerRemoved {
        thread_id: EntityId,
        marker_id: EntityId,
        updated_at: Timestamp,
    },
    /// A marker's done flag was set. Faithful to mcode
    /// `thread.marker-done-set` {threadId, markerId, done, updatedAt}.
    MarkerDoneSet {
        thread_id: EntityId,
        marker_id: EntityId,
        done: bool,
        updated_at: Timestamp,
    },
    /// A marker's label was set. Faithful to mcode
    /// `thread.marker-label-set` {threadId, markerId, label, updatedAt}.
    MarkerLabelSet {
        thread_id: EntityId,
        marker_id: EntityId,
        label: Option<String>,
        updated_at: Timestamp,
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
    /// An in-progress turn was interrupted (e.g. user pressed stop) while still running.
    TurnInterrupted {
        id: EntityId,
        interrupted_at: Timestamp,
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
    /// `thread.message.assistant.delta` → append a chunk to a streamed assistant
    /// message (creating it on the first delta). Syncode-native modeling: mcode
    /// reuses `thread.message-sent` with a streaming flag; syncode uses a
    /// dedicated append event so the create-vs-append decision lives in the
    /// projector (mirroring pinned-message / marker materialization). The message
    /// id is caller-supplied (the active turn's assistant message id), unlike
    /// [`DomainEvent::MessageAdded`] which self-generates one.
    MessageDeltaAppended {
        id: EntityId,
        turn_id: EntityId,
        delta: String,
        created_at: Timestamp,
    },
    /// `thread.message.assistant.complete` → finalize a streamed assistant message.
    MessageStreamingFinalized {
        id: EntityId,
        finalized_at: Timestamp,
    },

    // ─── Proposed Plan & Checkpoint Events (thread sub-aggregates) ──────
    /// `thread.proposed-plan.upsert` → upsert a proposed plan on a thread.
    /// Faithful to mcode (orchestration.ts:1274-1280, decider.ts:1521-1540):
    /// guards thread existence and echoes the plan. The projector dedups by
    /// `thread_id:plan_id` (upsert semantics). mcode enforces no count cap here.
    ProposedPlanUpserted {
        thread_id: EntityId,
        plan_id: String,
        turn_id: Option<EntityId>,
        plan_markdown: String,
        implemented_at: Option<String>,
        implementation_thread_id: Option<EntityId>,
        created_at: Timestamp,
        updated_at: Timestamp,
    },
    /// `thread.turn.diff.complete` → record a turn's diff checkpoint summary.
    /// Faithful to mcode (orchestration.ts:1282-1294, decider.ts:1542-1570).
    /// The projector dedups by `thread_id:turn_id` (one checkpoint per turn).
    TurnDiffCompleted {
        thread_id: EntityId,
        turn_id: EntityId,
        checkpoint_turn_count: u32,
        checkpoint_ref: String,
        status: String,
        files: Vec<CheckpointFile>,
        assistant_message_id: Option<EntityId>,
        completed_at: Timestamp,
    },

    // ─── Revert / Rollback Events (read-model truncation) ─────────────
    /// `thread.revert.complete` → truncate a thread to `turn_count` turns.
    /// Distinct from the git-checkpoint [`DomainEvent::ThreadReverted`]: this is
    /// a turn-sequence truncation (mcode `thread.reverted` {turnCount}). The
    /// projector removes turns (and their messages/checkpoints/plans) with
    /// sequence > turn_count; the event store is untouched (ES invariant — the
    /// read model is a projection, replayed from the full event log).
    ThreadRevertCompleted {
        thread_id: EntityId,
        turn_count: u32,
        reverted_at: Timestamp,
    },
    /// `thread.conversation.rollback` → request rolling a conversation back to
    /// a message (mcode `thread.conversation-rollback-requested`).
    /// Requested-style async; the actual removed turns are carried by
    /// [`DomainEvent::ConversationRolledBack`].
    ConversationRollbackRequested {
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
        requested_at: Timestamp,
    },
    /// `thread.conversation.rollback.complete` → a conversation rollback was
    /// applied (mcode `thread.conversation-rolled-back`). The projector removes
    /// the turns in `removed_turn_ids` (and their messages/plans/checkpoints);
    /// the event store is untouched.
    ConversationRolledBack {
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
        removed_turn_ids: Vec<EntityId>,
        rolled_back_at: Timestamp,
    },

    // ─── Activity Events ────────────────────────────────────────────────
    ActivityLogged {
        id: EntityId,
        activity_type: String,
        description: String,
        /// Thread this activity belongs to. `#[serde(default)]` keeps old persisted
        /// events (which predate the field) deserializable as `None`.
        #[serde(default)]
        thread_id: Option<EntityId>,
        created_at: Timestamp,
    },
}

/// A file in a turn-diff checkpoint summary (mcode OrchestrationCheckpointFile).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointFile {
    pub path: String,
    pub kind: String,
    pub additions: u32,
    pub deletions: u32,
}

impl DomainEvent {
    /// Get the aggregate ID this event belongs to
    pub fn aggregate_id(&self) -> EntityId {
        match self {
            Self::ProjectCreated { id, .. }
            | Self::ProjectUpdated { id, .. }
            | Self::ProjectDeleted { id, .. }
            | Self::ThreadCreated { id, .. }
            | Self::ThreadStatusChanged { id, .. }
            | Self::ThreadTitleSet { id, .. }
            | Self::ThreadCheckpointSet { id, .. }
            | Self::ThreadReverted { id, .. }
            | Self::ThreadArchived { id, .. }
            | Self::ThreadUnarchived { id, .. }
            | Self::ThreadDeleted { id, .. }
            | Self::ThreadSessionStopRequested { id, .. }
            | Self::ThreadRuntimeModeSet { id, .. }
            | Self::ThreadInteractionModeSet { id, .. }
            | Self::ThreadApprovalResponded { id, .. }
            | Self::ThreadUserInputResponded { id, .. }
            | Self::ThreadMessageEditedAndResent { id, .. }
            | Self::ThreadSessionSet { id, .. }
            | Self::TurnDispatchRequested { id, .. }
            | Self::TurnStarted { id, .. }
            | Self::TurnCompleted { id, .. }
            | Self::TurnFailed { id, .. }
            | Self::TurnCancelled { id, .. }
            | Self::TurnInterrupted { id, .. }
            | Self::TurnFilesModified { id, .. }
            | Self::TurnCheckpointSet { id, .. }
            | Self::MessageAdded { id, .. }
            | Self::MessageDeltaAppended { id, .. }
            | Self::MessageStreamingFinalized { id, .. }
            | Self::ActivityLogged { id, .. } => *id,

            // Events keyed by a differently-named aggregate field (thread sub-aggregates).
            Self::ThreadMessagesImported { thread_id, .. }
            | Self::PinnedMessageAdded { thread_id, .. }
            | Self::PinnedMessageRemoved { thread_id, .. }
            | Self::PinnedMessageDoneSet { thread_id, .. }
            | Self::PinnedMessageLabelSet { thread_id, .. }
            | Self::MarkerAdded { thread_id, .. }
            | Self::MarkerRemoved { thread_id, .. }
            | Self::MarkerDoneSet { thread_id, .. }
            | Self::MarkerLabelSet { thread_id, .. }
            | Self::ProposedPlanUpserted { thread_id, .. }
            | Self::TurnDiffCompleted { thread_id, .. }
            | Self::ThreadRevertCompleted { thread_id, .. }
            | Self::ConversationRollbackRequested { thread_id, .. }
            | Self::ConversationRolledBack { thread_id, .. } => *thread_id,
        }
    }

    /// Get the event type name as a string
    pub fn event_type_name(&self) -> &'static str {
        match self {
            Self::ProjectCreated { .. } => "ProjectCreated",
            Self::ProjectUpdated { .. } => "ProjectUpdated",
            Self::ProjectDeleted { .. } => "ProjectDeleted",
            Self::ThreadCreated { .. } => "ThreadCreated",
            Self::ThreadStatusChanged { .. } => "ThreadStatusChanged",
            Self::ThreadTitleSet { .. } => "ThreadTitleSet",
            Self::ThreadCheckpointSet { .. } => "ThreadCheckpointSet",
            Self::ThreadReverted { .. } => "ThreadReverted",
            Self::ThreadArchived { .. } => "ThreadArchived",
            Self::ThreadUnarchived { .. } => "ThreadUnarchived",
            Self::ThreadDeleted { .. } => "ThreadDeleted",
            Self::ThreadMessagesImported { .. } => "ThreadMessagesImported",
            Self::ThreadSessionStopRequested { .. } => "ThreadSessionStopRequested",
            Self::ThreadRuntimeModeSet { .. } => "ThreadRuntimeModeSet",
            Self::ThreadInteractionModeSet { .. } => "ThreadInteractionModeSet",
            Self::ThreadApprovalResponded { .. } => "ThreadApprovalResponded",
            Self::ThreadUserInputResponded { .. } => "ThreadUserInputResponded",
            Self::ThreadMessageEditedAndResent { .. } => "ThreadMessageEditedAndResent",
            Self::ThreadSessionSet { .. } => "ThreadSessionSet",
            Self::TurnDispatchRequested { .. } => "TurnDispatchRequested",
            Self::PinnedMessageAdded { .. } => "PinnedMessageAdded",
            Self::PinnedMessageRemoved { .. } => "PinnedMessageRemoved",
            Self::PinnedMessageDoneSet { .. } => "PinnedMessageDoneSet",
            Self::PinnedMessageLabelSet { .. } => "PinnedMessageLabelSet",
            Self::MarkerAdded { .. } => "MarkerAdded",
            Self::MarkerRemoved { .. } => "MarkerRemoved",
            Self::MarkerDoneSet { .. } => "MarkerDoneSet",
            Self::MarkerLabelSet { .. } => "MarkerLabelSet",
            Self::TurnStarted { .. } => "TurnStarted",
            Self::TurnCompleted { .. } => "TurnCompleted",
            Self::TurnFailed { .. } => "TurnFailed",
            Self::TurnCancelled { .. } => "TurnCancelled",
            Self::TurnInterrupted { .. } => "TurnInterrupted",
            Self::TurnFilesModified { .. } => "TurnFilesModified",
            Self::TurnCheckpointSet { .. } => "TurnCheckpointSet",
            Self::MessageAdded { .. } => "MessageAdded",
            Self::MessageDeltaAppended { .. } => "MessageDeltaAppended",
            Self::MessageStreamingFinalized { .. } => "MessageStreamingFinalized",
            Self::ProposedPlanUpserted { .. } => "ProposedPlanUpserted",
            Self::TurnDiffCompleted { .. } => "TurnDiffCompleted",
            Self::ThreadRevertCompleted { .. } => "ThreadRevertCompleted",
            Self::ConversationRollbackRequested { .. } => "ConversationRollbackRequested",
            Self::ConversationRolledBack { .. } => "ConversationRolledBack",
            Self::ActivityLogged { .. } => "ActivityLogged",
        }
    }
}

/// Envelope wrapping a domain event with stream-level metadata.
///
/// This is what the event store persists. The `sequence` is the position
/// within the aggregate's event stream (for optimistic concurrency).
/// The `timestamp` is when the event was created/written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// The domain event payload
    pub event: DomainEvent,
    /// Monotonically increasing sequence within the aggregate stream
    pub sequence: u64,
    /// Timestamp when this event was created
    pub timestamp: Timestamp,
}

impl Envelope {
    /// Wrap a domain event with stream metadata
    pub fn new(event: DomainEvent, sequence: u64) -> Self {
        Self {
            event,
            sequence,
            timestamp: Timestamp::now(),
        }
    }

    /// Wrap with an explicit timestamp (e.g. when replaying from store)
    pub fn with_timestamp(event: DomainEvent, sequence: u64, timestamp: Timestamp) -> Self {
        Self {
            event,
            sequence,
            timestamp,
        }
    }
}

impl DomainEventTrait for Envelope {
    fn event_type(&self) -> &str {
        self.event.event_type_name()
    }

    fn aggregate_id(&self) -> EntityId {
        self.event.aggregate_id()
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
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
            (
                DomainEvent::ProjectCreated {
                    id,
                    name: "p".into(),
                    root_path: "/p".into(),
                    created_at: Timestamp::now(),
                },
                "ProjectCreated",
            ),
            (
                DomainEvent::ThreadCreated {
                    id,
                    project_id: id,
                    provider_id: "p".into(),
                    model: "m".into(),
                    created_at: Timestamp::now(),
                },
                "ThreadCreated",
            ),
            (
                DomainEvent::TurnStarted {
                    id,
                    thread_id: id,
                    sequence: 1,
                    user_input: "hi".into(),
                    created_at: Timestamp::now(),
                },
                "TurnStarted",
            ),
            (
                DomainEvent::MessageAdded {
                    id,
                    turn_id: id,
                    role: "user".into(),
                    content: "msg".into(),
                    created_at: Timestamp::now(),
                },
                "MessageAdded",
            ),
            (
                DomainEvent::ActivityLogged {
                    id,
                    activity_type: "session_started".into(),
                    description: "d".into(),
                    thread_id: None,
                    created_at: Timestamp::now(),
                },
                "ActivityLogged",
            ),
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

    #[test]
    fn envelope_implements_domain_event_trait() {
        let id = EntityId::new();
        let event = DomainEvent::ProjectCreated {
            id,
            name: "test".into(),
            root_path: "/test".into(),
            created_at: Timestamp::now(),
        };
        let envelope = Envelope::new(event, 1);

        assert_eq!(envelope.event_type(), "ProjectCreated");
        assert_eq!(envelope.aggregate_id(), id);
        assert_eq!(envelope.sequence(), 1);
        // timestamp should be recent
        assert!(envelope.timestamp().to_millis() > 0);
    }

    #[test]
    fn envelope_serialization_roundtrip() {
        let id = EntityId::new();
        let event = DomainEvent::ThreadCreated {
            id,
            project_id: EntityId::new(),
            provider_id: "anthropic".into(),
            model: "claude-3".into(),
            created_at: Timestamp::now(),
        };
        let envelope = Envelope::new(event, 42);
        let json = serde_json::to_string(&envelope).expect("serialize");
        let back: Envelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.sequence(), 42);
        assert_eq!(back.event.event_type_name(), "ThreadCreated");
        assert_eq!(back.aggregate_id(), id);
    }

    #[test]
    fn envelope_with_timestamp() {
        let id = EntityId::new();
        let ts = Timestamp::now();
        let event = DomainEvent::TurnCompleted {
            id,
            assistant_output: "done".into(),
            duration_ms: 500,
            completed_at: ts,
        };
        let envelope = Envelope::with_timestamp(event, 10, ts);
        assert_eq!(envelope.timestamp(), ts);
        assert_eq!(envelope.sequence(), 10);
    }
}

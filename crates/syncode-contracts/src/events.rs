//! Domain-event DTO mirror — **Tier 2** of the contracts bridge.
//!
//! Mirrors Syncode's 44 `syncode_core::domain::events::DomainEvent` variants
//! as a **tagged TypeScript discriminated union** so push payloads on the
//! `push/orchestration` channel become typed instead of `Record<string, unknown>`
//! (see `CONTRACTS-BRIDGE-DESIGN.md` §4 / §6.3).
//!
//! ## Tag strategy
//!
//! `#[serde(tag = "eventType", content = "data", rename_all = "camelCase",
//! rename_all_fields = "camelCase")]` emits
//! `{"eventType":"projectCreated","data":{...}}`. The outer `rename_all`
//! camelCases the variant-name tag; `rename_all_fields` (serde ≥ 1.0.157)
//! camelCases the fields inside each struct variant (the outer `rename_all`
//! alone only renames variant names, not their fields on a tagged enum).
//! ts-rs honors the same `rename_all = "camelCase"` on `#[ts(...)]` and
//! generates a TS discriminated union keyed on `eventType` (the 44 tag
//! strings are camelCase variant names: `projectCreated`,
//! `threadStatusChanged`, …).
//!
//! ## Wire-parity caveat
//!
//! The model here is **camelCase** (matching MCode's frontend expectations,
//! design §3.3). Syncode's WS push envelope (`syncode-ws/src/push.rs`) currently
//! emits **snake_case** keys (`event_type`, `aggregate_id`); the server wire
//! update is the T5 transport task. This module models the TYPE surface only.
//!
//! ## Conventions
//!
//! - **camelCase on both serde + ts** (matches T1 conventions; see `rpc.rs`).
//! - **bigint-safe:** any `u64`/`usize`/`i64` field carries
//!   `#[ts(type = "number")]` (or `number | null` where optional) because
//!   `JSON.parse` yields `number` but ts-rs would otherwise emit `bigint`.
//! - **DTO mirror pattern:** the contracts crate owns these TS-facing enums;
//!   domain crates stay free of serialization-for-TS concerns. The domain
//!   `DomainEvent` is projected into `DomainEventDto` via the `From` impl
//!   below (core's `EntityId(Uuid)` → contracts' `EntityId(String)` and
//!   core's `Timestamp(DateTime<Utc>)` → contracts' `Timestamp(String)`).
//!
//! Source enum: `crates/syncode-core/src/domain/events.rs` (44 variants).

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{EntityId, Timestamp};

// ─── Helper: project core → DTO primitives ─────────────────────────────
//
// Core's `EntityId`/`Timestamp` are typed wrappers (Uuid / DateTime); the
// contracts DTOs are string wrappers (ts-rs exports them as `string`). The
// projection lives inline in the `From` impl below via these two fns.

// Helpers named `to_id`/`to_ts` (not `id`/`ts`) to avoid shadowing by the
// local pattern-bound fields of the same name in the `From` impl below.
fn to_id(core_id: syncode_core::domain::primitives::EntityId) -> EntityId {
    EntityId(core_id.as_str())
}

fn to_ts(core_ts: syncode_core::domain::primitives::Timestamp) -> Timestamp {
    // Core's Timestamp wraps a `DateTime<Utc>`; serialize it as an RFC 3339
    // string (matching the contracts Timestamp wire format).
    Timestamp(core_ts.as_datetime().to_rfc3339())
}

/// A file in a turn-diff checkpoint summary (mcode `OrchestrationCheckpointFile`).
/// Mirrors `syncode_core::domain::events::CheckpointFile` for the
/// `TurnDiffCompleted` payload.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct CheckpointFileDto {
    pub path: String,
    pub kind: String,
    pub additions: u32,
    pub deletions: u32,
}

/// Tagged union mirror of Syncode's 44 `DomainEvent` variants.
///
/// Discriminator key is `eventType` (camelCase variant names); payload under
/// `data`. ts-rs generates this as a TS discriminated union — see module docs.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(
    tag = "eventType",
    content = "data",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[ts(export, rename_all = "camelCase")]
pub enum DomainEventDto {
    // ─── Project Events (3) ─────────────────────────────────────────────
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
    ProjectDeleted {
        id: EntityId,
        deleted_at: Timestamp,
    },

    // ─── Thread Events (18) ─────────────────────────────────────────────
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
    ThreadReverted {
        id: EntityId,
        git_ref: String,
        reverted_at: Timestamp,
    },
    ThreadArchived {
        id: EntityId,
        archived_at: Timestamp,
    },
    ThreadUnarchived {
        id: EntityId,
        unarchived_at: Timestamp,
    },
    ThreadDeleted {
        id: EntityId,
        deleted_at: Timestamp,
    },
    ThreadMessagesImported {
        thread_id: EntityId,
        source_thread_id: EntityId,
        count: u32,
        imported_at: Timestamp,
    },
    ThreadSessionStopRequested {
        id: EntityId,
        requested_at: Timestamp,
    },
    ThreadRuntimeModeSet {
        id: EntityId,
        runtime_mode: String,
        updated_at: Timestamp,
    },
    ThreadInteractionModeSet {
        id: EntityId,
        interaction_mode: String,
        updated_at: Timestamp,
    },
    ThreadApprovalResponded {
        id: EntityId,
        request_id: String,
        decision: String,
        responded_at: Timestamp,
    },
    ThreadUserInputResponded {
        id: EntityId,
        request_id: String,
        answers: String,
        responded_at: Timestamp,
    },
    ThreadMessageEditedAndResent {
        id: EntityId,
        message_id: EntityId,
        text: String,
        edited_at: Timestamp,
    },
    ThreadSessionSet {
        id: EntityId,
        status: String,
        provider_name: Option<String>,
        runtime_mode: String,
        active_turn_id: Option<EntityId>,
        last_error: Option<String>,
        updated_at: Timestamp,
    },
    TurnDispatchRequested {
        id: EntityId,
        message_id: EntityId,
        runtime_mode: String,
        interaction_mode: String,
        dispatch_mode: String,
        requested_at: Timestamp,
    },

    // ─── Pinned Message Events (4, thread sub-aggregate) ────────────────
    PinnedMessageAdded {
        thread_id: EntityId,
        message_id: EntityId,
        label: Option<String>,
        done: bool,
        pinned_at: Timestamp,
        updated_at: Timestamp,
    },
    PinnedMessageRemoved {
        thread_id: EntityId,
        message_id: EntityId,
        updated_at: Timestamp,
    },
    PinnedMessageDoneSet {
        thread_id: EntityId,
        message_id: EntityId,
        done: bool,
        updated_at: Timestamp,
    },
    PinnedMessageLabelSet {
        thread_id: EntityId,
        message_id: EntityId,
        label: Option<String>,
        updated_at: Timestamp,
    },

    // ─── Marker Events (4, thread sub-aggregate) ────────────────────────
    MarkerAdded {
        thread_id: EntityId,
        marker_id: EntityId,
        message_id: EntityId,
        #[ts(type = "number")]
        start_offset: u64,
        #[ts(type = "number")]
        end_offset: u64,
        selected_text: String,
        style: String,
        color: String,
        label: Option<String>,
        done: bool,
        created_at: Timestamp,
        updated_at: Timestamp,
    },
    MarkerRemoved {
        thread_id: EntityId,
        marker_id: EntityId,
        updated_at: Timestamp,
    },
    MarkerDoneSet {
        thread_id: EntityId,
        marker_id: EntityId,
        done: bool,
        updated_at: Timestamp,
    },
    MarkerLabelSet {
        thread_id: EntityId,
        marker_id: EntityId,
        label: Option<String>,
        updated_at: Timestamp,
    },

    // ─── Turn Events (7) ────────────────────────────────────────────────
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
        #[ts(type = "number")]
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

    // ─── Message Events (3) ─────────────────────────────────────────────
    MessageAdded {
        id: EntityId,
        turn_id: EntityId,
        role: String,
        content: String,
        created_at: Timestamp,
    },
    MessageDeltaAppended {
        id: EntityId,
        turn_id: EntityId,
        delta: String,
        created_at: Timestamp,
    },
    MessageStreamingFinalized {
        id: EntityId,
        finalized_at: Timestamp,
    },

    // ─── Proposed Plan & Checkpoint Events (2, thread sub-aggregates) ───
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
    TurnDiffCompleted {
        thread_id: EntityId,
        turn_id: EntityId,
        checkpoint_turn_count: u32,
        checkpoint_ref: String,
        status: String,
        files: Vec<CheckpointFileDto>,
        assistant_message_id: Option<EntityId>,
        completed_at: Timestamp,
    },

    // ─── Revert / Rollback Events (3, read-model truncation) ────────────
    ThreadRevertCompleted {
        thread_id: EntityId,
        turn_count: u32,
        reverted_at: Timestamp,
    },
    ConversationRollbackRequested {
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
        requested_at: Timestamp,
    },
    ConversationRolledBack {
        thread_id: EntityId,
        message_id: EntityId,
        num_turns: u32,
        removed_turn_ids: Vec<EntityId>,
        rolled_back_at: Timestamp,
    },

    // ─── Activity Events (1) ────────────────────────────────────────────
    ActivityLogged {
        id: EntityId,
        activity_type: String,
        description: String,
        #[serde(default)]
        thread_id: Option<EntityId>,
        created_at: Timestamp,
    },
}

impl From<&syncode_core::DomainEvent> for DomainEventDto {
    /// Project a domain `DomainEvent` into the TS-facing DTO mirror.
    ///
    /// Field-by-field: primitive types map directly; core's typed `EntityId`/
    /// `Timestamp` primitives convert to the contracts string wrappers via the
    /// `id`/`ts` helpers above; `CheckpointFile` (core) → `CheckpointFileDto`.
    fn from(ev: &syncode_core::DomainEvent) -> Self {
        use syncode_core::DomainEvent as E;

        match ev {
            // ─── Project ────────────────────────────────────────────────
            E::ProjectCreated {
                id,
                name,
                root_path,
                created_at,
            } => Self::ProjectCreated {
                id: to_id(*id),
                name: name.clone(),
                root_path: root_path.clone(),
                created_at: to_ts(*created_at),
            },
            E::ProjectUpdated {
                id,
                provider_id,
                default_model,
                updated_at,
            } => Self::ProjectUpdated {
                id: to_id(*id),
                provider_id: provider_id.clone(),
                default_model: default_model.clone(),
                updated_at: to_ts(*updated_at),
            },
            E::ProjectDeleted { id, deleted_at } => Self::ProjectDeleted {
                id: to_id(*id),
                deleted_at: to_ts(*deleted_at),
            },

            // ─── Thread ─────────────────────────────────────────────────
            E::ThreadCreated {
                id,
                project_id,
                provider_id,
                model,
                created_at,
            } => Self::ThreadCreated {
                id: to_id(*id),
                project_id: to_id(*project_id),
                provider_id: provider_id.clone(),
                model: model.clone(),
                created_at: to_ts(*created_at),
            },
            E::ThreadStatusChanged {
                id,
                old_status,
                new_status,
                updated_at,
            } => Self::ThreadStatusChanged {
                id: to_id(*id),
                old_status: old_status.clone(),
                new_status: new_status.clone(),
                updated_at: to_ts(*updated_at),
            },
            E::ThreadTitleSet { id, title } => Self::ThreadTitleSet {
                id: to_id(*id),
                title: title.clone(),
            },
            E::ThreadCheckpointSet { id, git_ref } => Self::ThreadCheckpointSet {
                id: to_id(*id),
                git_ref: git_ref.clone(),
            },
            E::ThreadReverted {
                id,
                git_ref,
                reverted_at,
            } => Self::ThreadReverted {
                id: to_id(*id),
                git_ref: git_ref.clone(),
                reverted_at: to_ts(*reverted_at),
            },
            E::ThreadArchived { id, archived_at } => Self::ThreadArchived {
                id: to_id(*id),
                archived_at: to_ts(*archived_at),
            },
            E::ThreadUnarchived { id, unarchived_at } => Self::ThreadUnarchived {
                id: to_id(*id),
                unarchived_at: to_ts(*unarchived_at),
            },
            E::ThreadDeleted { id, deleted_at } => Self::ThreadDeleted {
                id: to_id(*id),
                deleted_at: to_ts(*deleted_at),
            },
            E::ThreadMessagesImported {
                thread_id,
                source_thread_id,
                count,
                imported_at,
            } => Self::ThreadMessagesImported {
                thread_id: to_id(*thread_id),
                source_thread_id: to_id(*source_thread_id),
                count: *count,
                imported_at: to_ts(*imported_at),
            },
            E::ThreadSessionStopRequested { id, requested_at } => {
                Self::ThreadSessionStopRequested {
                    id: to_id(*id),
                    requested_at: to_ts(*requested_at),
                }
            }
            E::ThreadRuntimeModeSet {
                id,
                runtime_mode,
                updated_at,
            } => Self::ThreadRuntimeModeSet {
                id: to_id(*id),
                runtime_mode: runtime_mode.clone(),
                updated_at: to_ts(*updated_at),
            },
            E::ThreadInteractionModeSet {
                id,
                interaction_mode,
                updated_at,
            } => Self::ThreadInteractionModeSet {
                id: to_id(*id),
                interaction_mode: interaction_mode.clone(),
                updated_at: to_ts(*updated_at),
            },
            E::ThreadApprovalResponded {
                id,
                request_id,
                decision,
                responded_at,
            } => Self::ThreadApprovalResponded {
                id: to_id(*id),
                request_id: request_id.clone(),
                decision: decision.clone(),
                responded_at: to_ts(*responded_at),
            },
            E::ThreadUserInputResponded {
                id,
                request_id,
                answers,
                responded_at,
            } => Self::ThreadUserInputResponded {
                id: to_id(*id),
                request_id: request_id.clone(),
                answers: answers.clone(),
                responded_at: to_ts(*responded_at),
            },
            E::ThreadMessageEditedAndResent {
                id,
                message_id,
                text,
                edited_at,
            } => Self::ThreadMessageEditedAndResent {
                id: to_id(*id),
                message_id: to_id(*message_id),
                text: text.clone(),
                edited_at: to_ts(*edited_at),
            },
            E::ThreadSessionSet {
                id,
                status,
                provider_name,
                runtime_mode,
                active_turn_id,
                last_error,
                updated_at,
            } => Self::ThreadSessionSet {
                id: to_id(*id),
                status: status.clone(),
                provider_name: provider_name.clone(),
                runtime_mode: runtime_mode.clone(),
                active_turn_id: active_turn_id.map(to_id),
                last_error: last_error.clone(),
                updated_at: to_ts(*updated_at),
            },
            E::TurnDispatchRequested {
                id,
                message_id,
                runtime_mode,
                interaction_mode,
                dispatch_mode,
                requested_at,
            } => Self::TurnDispatchRequested {
                id: to_id(*id),
                message_id: to_id(*message_id),
                runtime_mode: runtime_mode.clone(),
                interaction_mode: interaction_mode.clone(),
                dispatch_mode: dispatch_mode.clone(),
                requested_at: to_ts(*requested_at),
            },

            // ─── Pinned Message ─────────────────────────────────────────
            E::PinnedMessageAdded {
                thread_id,
                message_id,
                label,
                done,
                pinned_at,
                updated_at,
            } => Self::PinnedMessageAdded {
                thread_id: to_id(*thread_id),
                message_id: to_id(*message_id),
                label: label.clone(),
                done: *done,
                pinned_at: to_ts(*pinned_at),
                updated_at: to_ts(*updated_at),
            },
            E::PinnedMessageRemoved {
                thread_id,
                message_id,
                updated_at,
            } => Self::PinnedMessageRemoved {
                thread_id: to_id(*thread_id),
                message_id: to_id(*message_id),
                updated_at: to_ts(*updated_at),
            },
            E::PinnedMessageDoneSet {
                thread_id,
                message_id,
                done,
                updated_at,
            } => Self::PinnedMessageDoneSet {
                thread_id: to_id(*thread_id),
                message_id: to_id(*message_id),
                done: *done,
                updated_at: to_ts(*updated_at),
            },
            E::PinnedMessageLabelSet {
                thread_id,
                message_id,
                label,
                updated_at,
            } => Self::PinnedMessageLabelSet {
                thread_id: to_id(*thread_id),
                message_id: to_id(*message_id),
                label: label.clone(),
                updated_at: to_ts(*updated_at),
            },

            // ─── Marker ─────────────────────────────────────────────────
            E::MarkerAdded {
                thread_id,
                marker_id,
                message_id,
                start_offset,
                end_offset,
                selected_text,
                style,
                color,
                label,
                done,
                created_at,
                updated_at,
            } => Self::MarkerAdded {
                thread_id: to_id(*thread_id),
                marker_id: to_id(*marker_id),
                message_id: to_id(*message_id),
                start_offset: *start_offset,
                end_offset: *end_offset,
                selected_text: selected_text.clone(),
                style: style.clone(),
                color: color.clone(),
                label: label.clone(),
                done: *done,
                created_at: to_ts(*created_at),
                updated_at: to_ts(*updated_at),
            },
            E::MarkerRemoved {
                thread_id,
                marker_id,
                updated_at,
            } => Self::MarkerRemoved {
                thread_id: to_id(*thread_id),
                marker_id: to_id(*marker_id),
                updated_at: to_ts(*updated_at),
            },
            E::MarkerDoneSet {
                thread_id,
                marker_id,
                done,
                updated_at,
            } => Self::MarkerDoneSet {
                thread_id: to_id(*thread_id),
                marker_id: to_id(*marker_id),
                done: *done,
                updated_at: to_ts(*updated_at),
            },
            E::MarkerLabelSet {
                thread_id,
                marker_id,
                label,
                updated_at,
            } => Self::MarkerLabelSet {
                thread_id: to_id(*thread_id),
                marker_id: to_id(*marker_id),
                label: label.clone(),
                updated_at: to_ts(*updated_at),
            },

            // ─── Turn ───────────────────────────────────────────────────
            E::TurnStarted {
                id,
                thread_id,
                sequence,
                user_input,
                created_at,
            } => Self::TurnStarted {
                id: to_id(*id),
                thread_id: to_id(*thread_id),
                sequence: *sequence,
                user_input: user_input.clone(),
                created_at: to_ts(*created_at),
            },
            E::TurnCompleted {
                id,
                assistant_output,
                duration_ms,
                completed_at,
            } => Self::TurnCompleted {
                id: to_id(*id),
                assistant_output: assistant_output.clone(),
                duration_ms: *duration_ms,
                completed_at: to_ts(*completed_at),
            },
            E::TurnFailed {
                id,
                error,
                completed_at,
            } => Self::TurnFailed {
                id: to_id(*id),
                error: error.clone(),
                completed_at: to_ts(*completed_at),
            },
            E::TurnCancelled { id, completed_at } => Self::TurnCancelled {
                id: to_id(*id),
                completed_at: to_ts(*completed_at),
            },
            E::TurnInterrupted { id, interrupted_at } => Self::TurnInterrupted {
                id: to_id(*id),
                interrupted_at: to_ts(*interrupted_at),
            },
            E::TurnFilesModified { id, files } => Self::TurnFilesModified {
                id: to_id(*id),
                files: files.clone(),
            },
            E::TurnCheckpointSet { id, git_ref } => Self::TurnCheckpointSet {
                id: to_id(*id),
                git_ref: git_ref.clone(),
            },

            // ─── Message ────────────────────────────────────────────────
            E::MessageAdded {
                id,
                turn_id,
                role,
                content,
                created_at,
            } => Self::MessageAdded {
                id: to_id(*id),
                turn_id: to_id(*turn_id),
                role: role.clone(),
                content: content.clone(),
                created_at: to_ts(*created_at),
            },
            E::MessageDeltaAppended {
                id,
                turn_id,
                delta,
                created_at,
            } => Self::MessageDeltaAppended {
                id: to_id(*id),
                turn_id: to_id(*turn_id),
                delta: delta.clone(),
                created_at: to_ts(*created_at),
            },
            E::MessageStreamingFinalized { id, finalized_at } => Self::MessageStreamingFinalized {
                id: to_id(*id),
                finalized_at: to_ts(*finalized_at),
            },

            // ─── Proposed Plan & Checkpoint ─────────────────────────────
            E::ProposedPlanUpserted {
                thread_id,
                plan_id,
                turn_id,
                plan_markdown,
                implemented_at,
                implementation_thread_id,
                created_at,
                updated_at,
            } => Self::ProposedPlanUpserted {
                thread_id: to_id(*thread_id),
                plan_id: plan_id.clone(),
                turn_id: turn_id.map(to_id),
                plan_markdown: plan_markdown.clone(),
                implemented_at: implemented_at.clone(),
                implementation_thread_id: implementation_thread_id.map(to_id),
                created_at: to_ts(*created_at),
                updated_at: to_ts(*updated_at),
            },
            E::TurnDiffCompleted {
                thread_id,
                turn_id,
                checkpoint_turn_count,
                checkpoint_ref,
                status,
                files,
                assistant_message_id,
                completed_at,
            } => Self::TurnDiffCompleted {
                thread_id: to_id(*thread_id),
                turn_id: to_id(*turn_id),
                checkpoint_turn_count: *checkpoint_turn_count,
                checkpoint_ref: checkpoint_ref.clone(),
                status: status.clone(),
                files: files
                    .iter()
                    .map(|f| CheckpointFileDto {
                        path: f.path.clone(),
                        kind: f.kind.clone(),
                        additions: f.additions,
                        deletions: f.deletions,
                    })
                    .collect(),
                assistant_message_id: assistant_message_id.map(to_id),
                completed_at: to_ts(*completed_at),
            },

            // ─── Revert / Rollback ──────────────────────────────────────
            E::ThreadRevertCompleted {
                thread_id,
                turn_count,
                reverted_at,
            } => Self::ThreadRevertCompleted {
                thread_id: to_id(*thread_id),
                turn_count: *turn_count,
                reverted_at: to_ts(*reverted_at),
            },
            E::ConversationRollbackRequested {
                thread_id,
                message_id,
                num_turns,
                requested_at,
            } => Self::ConversationRollbackRequested {
                thread_id: to_id(*thread_id),
                message_id: to_id(*message_id),
                num_turns: *num_turns,
                requested_at: to_ts(*requested_at),
            },
            E::ConversationRolledBack {
                thread_id,
                message_id,
                num_turns,
                removed_turn_ids,
                rolled_back_at,
            } => Self::ConversationRolledBack {
                thread_id: to_id(*thread_id),
                message_id: to_id(*message_id),
                num_turns: *num_turns,
                removed_turn_ids: removed_turn_ids.iter().map(|i| to_id(*i)).collect(),
                rolled_back_at: to_ts(*rolled_back_at),
            },

            // ─── Activity ───────────────────────────────────────────────
            E::ActivityLogged {
                id,
                activity_type,
                description,
                thread_id,
                created_at,
            } => Self::ActivityLogged {
                id: to_id(*id),
                activity_type: activity_type.clone(),
                description: description.clone(),
                thread_id: thread_id.map(to_id),
                created_at: to_ts(*created_at),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_core::domain::events::{CheckpointFile, DomainEvent};
    use syncode_core::domain::primitives::{EntityId as CoreId, Timestamp as CoreTs};

    /// Build a deterministic core id from a stable UUID string (test helper).
    fn cid() -> CoreId {
        CoreId::parse("00000000-0000-0000-0000-000000000001").unwrap()
    }

    #[test]
    fn project_created_roundtrip_preserves_event_type_tag() {
        let dto = DomainEventDto::ProjectCreated {
            id: EntityId("p1".into()),
            name: "demo".into(),
            root_path: "/tmp/x".into(),
            created_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        // Tag is the camelCase variant name, payload under `data`.
        assert!(
            json.contains("\"eventType\":\"projectCreated\""),
            "tag camelCase: {json}"
        );
        assert!(json.contains("\"data\""), "data wrapper: {json}");
        assert!(
            json.contains("\"rootPath\"") && json.contains("\"createdAt\""),
            "camelCase fields: {json}"
        );
        assert!(!json.contains("root_path"), "snake leaked: {json}");

        // Round-trips back to the same variant.
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::ProjectCreated { name, .. } => assert_eq!(name, "demo"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn thread_status_changed_roundtrip() {
        let dto = DomainEventDto::ThreadStatusChanged {
            id: EntityId("t1".into()),
            old_status: "active".into(),
            new_status: "paused".into(),
            updated_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(
            json.contains("\"eventType\":\"threadStatusChanged\""),
            "{json}"
        );
        assert!(json.contains("\"oldStatus\""), "{json}");
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::ThreadStatusChanged { new_status, .. } => {
                assert_eq!(new_status, "paused");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn turn_completed_roundtrip_bigint_safe() {
        let dto = DomainEventDto::TurnCompleted {
            id: EntityId("turn1".into()),
            assistant_output: "done".into(),
            duration_ms: 123_456,
            completed_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains("\"eventType\":\"turnCompleted\""), "{json}");
        assert!(
            json.contains("\"durationMs\":123456"),
            "duration emitted: {json}"
        );
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::TurnCompleted { duration_ms, .. } => {
                assert_eq!(duration_ms, 123_456);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn message_added_roundtrip() {
        let dto = DomainEventDto::MessageAdded {
            id: EntityId("m1".into()),
            turn_id: EntityId("turn1".into()),
            role: "user".into(),
            content: "hello".into(),
            created_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains("\"eventType\":\"messageAdded\""), "{json}");
        assert!(json.contains("\"turnId\""), "{json}");
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::MessageAdded { content, .. } => assert_eq!(content, "hello"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pinned_message_added_roundtrip() {
        let dto = DomainEventDto::PinnedMessageAdded {
            thread_id: EntityId("t1".into()),
            message_id: EntityId("m1".into()),
            label: Some("note".into()),
            done: false,
            pinned_at: Timestamp("2026-07-02T00:00:00Z".into()),
            updated_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(
            json.contains("\"eventType\":\"pinnedMessageAdded\""),
            "{json}"
        );
        assert!(json.contains("\"threadId\""), "{json}");
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::PinnedMessageAdded { label, .. } => {
                assert_eq!(label.as_deref(), Some("note"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn marker_added_roundtrip_bigint_offsets() {
        let dto = DomainEventDto::MarkerAdded {
            thread_id: EntityId("t1".into()),
            marker_id: EntityId("mk1".into()),
            message_id: EntityId("m1".into()),
            start_offset: 10,
            end_offset: 20,
            selected_text: "sel".into(),
            style: "highlight".into(),
            color: "#fff".into(),
            label: None,
            done: false,
            created_at: Timestamp("2026-07-02T00:00:00Z".into()),
            updated_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains("\"eventType\":\"markerAdded\""), "{json}");
        assert!(json.contains("\"startOffset\":10"), "{json}");
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::MarkerAdded { end_offset, .. } => assert_eq!(end_offset, 20),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn proposed_plan_upserted_roundtrip() {
        let dto = DomainEventDto::ProposedPlanUpserted {
            thread_id: EntityId("t1".into()),
            plan_id: "plan-1".into(),
            turn_id: None,
            plan_markdown: "# plan".into(),
            implemented_at: None,
            implementation_thread_id: None,
            created_at: Timestamp("2026-07-02T00:00:00Z".into()),
            updated_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(
            json.contains("\"eventType\":\"proposedPlanUpserted\""),
            "{json}"
        );
        assert!(json.contains("\"planMarkdown\""), "{json}");
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::ProposedPlanUpserted { plan_id, .. } => {
                assert_eq!(plan_id, "plan-1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn turn_diff_completed_roundtrip_with_files() {
        let dto = DomainEventDto::TurnDiffCompleted {
            thread_id: EntityId("t1".into()),
            turn_id: EntityId("turn1".into()),
            checkpoint_turn_count: 3,
            checkpoint_ref: "ref-abc".into(),
            status: "clean".into(),
            files: vec![CheckpointFileDto {
                path: "src/a.ts".into(),
                kind: "modified".into(),
                additions: 5,
                deletions: 1,
            }],
            assistant_message_id: None,
            completed_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(
            json.contains("\"eventType\":\"turnDiffCompleted\""),
            "{json}"
        );
        assert!(json.contains("\"checkpointTurnCount\""), "{json}");
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::TurnDiffCompleted { files, .. } => {
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].path, "src/a.ts");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn thread_revert_completed_roundtrip() {
        let dto = DomainEventDto::ThreadRevertCompleted {
            thread_id: EntityId("t1".into()),
            turn_count: 5,
            reverted_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(
            json.contains("\"eventType\":\"threadRevertCompleted\""),
            "{json}"
        );
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::ThreadRevertCompleted { turn_count, .. } => {
                assert_eq!(turn_count, 5);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn conversation_rolled_back_roundtrip() {
        let dto = DomainEventDto::ConversationRolledBack {
            thread_id: EntityId("t1".into()),
            message_id: EntityId("m1".into()),
            num_turns: 2,
            removed_turn_ids: vec![EntityId("turn1".into()), EntityId("turn2".into())],
            rolled_back_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(
            json.contains("\"eventType\":\"conversationRolledBack\""),
            "{json}"
        );
        assert!(json.contains("\"removedTurnIds\""), "{json}");
        let back: DomainEventDto = serde_json::from_str(&json).unwrap();
        match back {
            DomainEventDto::ConversationRolledBack {
                removed_turn_ids, ..
            } => {
                assert_eq!(removed_turn_ids.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn activity_logged_roundtrip_with_optional_thread() {
        let dto = DomainEventDto::ActivityLogged {
            id: EntityId("a1".into()),
            activity_type: "session_started".into(),
            description: "started".into(),
            thread_id: Some(EntityId("t1".into())),
            created_at: Timestamp("2026-07-02T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains("\"eventType\":\"activityLogged\""), "{json}");
        assert!(json.contains("\"threadId\":\"t1\""), "{json}");
        // `#[serde(default)]` lets missing `threadId` deserialize as None.
        let json_no_thread = r#"{"eventType":"activityLogged","data":{"id":"a2","activityType":"x","description":"d","createdAt":"t"}}"#;
        let back: DomainEventDto = serde_json::from_str(json_no_thread).unwrap();
        match back {
            DomainEventDto::ActivityLogged {
                thread_id: None, ..
            } => {}
            _ => panic!("wrong variant or thread_id not None"),
        }
    }

    #[test]
    fn from_core_domain_event_projects_all_aggregate_kinds() {
        // One representative per aggregate sub-kind, covering the id() and ts()
        // projections and the per-variant field mapping.
        let i = cid();
        let core_now = CoreTs::now();

        let cases: Vec<(syncode_core::DomainEvent, &str)> = vec![
            (
                DomainEvent::ProjectCreated {
                    id: i,
                    name: "p".into(),
                    root_path: "/p".into(),
                    created_at: core_now,
                },
                "projectCreated",
            ),
            (
                DomainEvent::ThreadCreated {
                    id: i,
                    project_id: i,
                    provider_id: "prov".into(),
                    model: "m".into(),
                    created_at: core_now,
                },
                "threadCreated",
            ),
            (
                DomainEvent::TurnStarted {
                    id: i,
                    thread_id: i,
                    sequence: 1,
                    user_input: "hi".into(),
                    created_at: core_now,
                },
                "turnStarted",
            ),
            (
                DomainEvent::MessageAdded {
                    id: i,
                    turn_id: i,
                    role: "user".into(),
                    content: "c".into(),
                    created_at: core_now,
                },
                "messageAdded",
            ),
            (
                DomainEvent::PinnedMessageAdded {
                    thread_id: i,
                    message_id: i,
                    label: None,
                    done: false,
                    pinned_at: core_now,
                    updated_at: core_now,
                },
                "pinnedMessageAdded",
            ),
            (
                DomainEvent::MarkerAdded {
                    thread_id: i,
                    marker_id: i,
                    message_id: i,
                    start_offset: 0,
                    end_offset: 1,
                    selected_text: "".into(),
                    style: "s".into(),
                    color: "c".into(),
                    label: None,
                    done: false,
                    created_at: core_now,
                    updated_at: core_now,
                },
                "markerAdded",
            ),
            (
                DomainEvent::ProposedPlanUpserted {
                    thread_id: i,
                    plan_id: "p1".into(),
                    turn_id: None,
                    plan_markdown: "".into(),
                    implemented_at: None,
                    implementation_thread_id: None,
                    created_at: core_now,
                    updated_at: core_now,
                },
                "proposedPlanUpserted",
            ),
            (
                DomainEvent::TurnDiffCompleted {
                    thread_id: i,
                    turn_id: i,
                    checkpoint_turn_count: 0,
                    checkpoint_ref: "r".into(),
                    status: "ok".into(),
                    files: vec![CheckpointFile {
                        path: "f".into(),
                        kind: "k".into(),
                        additions: 0,
                        deletions: 0,
                    }],
                    assistant_message_id: None,
                    completed_at: core_now,
                },
                "turnDiffCompleted",
            ),
            (
                DomainEvent::ActivityLogged {
                    id: i,
                    activity_type: "x".into(),
                    description: "d".into(),
                    thread_id: Some(i),
                    created_at: core_now,
                },
                "activityLogged",
            ),
        ];

        for (core_ev, expected_tag) in cases {
            let dto: DomainEventDto = (&core_ev).into();
            let json = serde_json::to_string(&dto).unwrap();
            assert!(
                json.contains(&format!("\"eventType\":\"{expected_tag}\"")),
                "From<DomainEvent> tag mismatch for {expected_tag}: {json}"
            );
            // The projected id must round-trip as a hyphenated UUID string.
            assert!(
                json.contains("00000000-0000-0000-0000-000000000001"),
                "id projection lost for {expected_tag}: {json}"
            );
        }
    }

    #[test]
    fn from_core_domain_event_project_deleted_and_thread_sub_aggregate() {
        // Cover ProjectDeleted (project tombstone), a thread sub-aggregate
        // keyed by `thread_id` (ThreadRevertCompleted), and
        // ConversationRollbackRequested — none of which appear above.
        let i = cid();
        let now = CoreTs::now();
        let cases: Vec<(syncode_core::DomainEvent, &str)> = vec![
            (
                DomainEvent::ProjectDeleted {
                    id: i,
                    deleted_at: now,
                },
                "projectDeleted",
            ),
            (
                DomainEvent::ThreadRevertCompleted {
                    thread_id: i,
                    turn_count: 2,
                    reverted_at: now,
                },
                "threadRevertCompleted",
            ),
            (
                DomainEvent::ConversationRollbackRequested {
                    thread_id: i,
                    message_id: i,
                    num_turns: 1,
                    requested_at: now,
                },
                "conversationRollbackRequested",
            ),
        ];
        for (core_ev, expected_tag) in cases {
            let dto: DomainEventDto = (&core_ev).into();
            let json = serde_json::to_string(&dto).unwrap();
            assert!(
                json.contains(&format!("\"eventType\":\"{expected_tag}\"")),
                "{expected_tag}: {json}"
            );
        }
    }
}

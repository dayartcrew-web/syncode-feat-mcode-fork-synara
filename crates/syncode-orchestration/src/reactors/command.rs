//! Provider Command Reactor
//!
//! Translates domain-level intents (from the Decider's Commands) into
//! actual provider adapter calls.
//!
//! This is the "write side" of the provider bridge:
//! - Command::StartTurn → adapter.start_session() + adapter.send_request()
//! - Command::FailTurn → adapter.interrupt()
//! - Command::CancelTurn → adapter.stop_session()
//! - Command::PauseThread → adapter.interrupt() all sessions
//! - Command::CancelThread → adapter.stop_session() all sessions

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use syncode_core::EntityId;
use syncode_memory::{MemoryProvider, NO_PRIOR_CONTEXT};
use syncode_provider::{
    ProviderCapability, ProviderEvent, ProviderRequest, SessionContext, SessionIdentity,
    SessionManager, SessionStateStatus,
};

use crate::decider::Command;

/// Result of executing a command on a provider
#[derive(Debug, Clone)]
pub struct CommandReaction {
    /// Whether the command was handled
    pub handled: bool,
    /// Session ID if a session was started (for StartTurn)
    pub session_id: Option<String>,
    /// Provider events collected during execution (stub for now)
    pub events: Vec<ProviderEvent>,
}

// ---------------------------------------------------------------------------
// P0-7: Queued turn pipeline
// ---------------------------------------------------------------------------

/// A turn waiting for the prior in-flight turn on its thread to complete
/// before it can be dispatched to the provider.
///
/// The queued-turn pipeline (P0-7) guarantees no two turns for the same
/// thread run simultaneously: when [`Command::DispatchQueuedTurn`] arrives
/// while the thread already has an active `Processing` session, the turn is
/// parked here instead of dispatched, and [`crate::reactors::ingestion`]
/// drains the next entry when the in-flight turn's `TurnCompleted` event
/// flows through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedTurn {
    /// The thread this turn belongs to (the queue key).
    pub thread_id: EntityId,
    /// The message the queued turn dispatches (faithful to mcode's
    /// `thread.turn.dispatch-queued {messageId}`).
    pub message_id: EntityId,
    /// Runtime mode stamped on the dispatch.
    pub runtime_mode: String,
    /// Interaction mode stamped on the dispatch.
    pub interaction_mode: String,
    /// Dispatch mode (e.g. `"queue"`).
    pub dispatch_mode: String,
}

/// Per-thread FIFO queue of [`QueuedTurn`]s waiting for the active turn on
/// their thread to complete.
///
/// Lives on [`ProviderCommandReactor`] so the queue survives across commands
/// and is observable to the ingestion reactor's completion drain. The queue
/// preserves insertion order within a thread (FIFO) so turns run in the order
/// the client submitted them.
#[derive(Debug, Default)]
pub struct TurnQueue {
    queues: tokio::sync::RwLock<HashMap<String, VecDeque<QueuedTurn>>>,
}

impl TurnQueue {
    /// Create an empty turn queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push `turn` onto the back of its thread's queue.
    pub async fn enqueue(&self, turn: QueuedTurn) {
        let mut queues = self.queues.write().await;
        queues
            .entry(turn.thread_id.as_str())
            .or_default()
            .push_back(turn);
    }

    /// Pop the next queued turn for `thread_id` (FIFO), or `None` if the
    /// thread has no queued turns.
    pub async fn dequeue(&self, thread_id: &str) -> Option<QueuedTurn> {
        let mut queues = self.queues.write().await;
        let queue = queues.get_mut(thread_id)?;
        let next = queue.pop_front();
        // Drop empty per-thread queues so `has_queued` / introspection stay
        // cheap and the map doesn't accumulate empty entries.
        if queue.is_empty() {
            queues.remove(thread_id);
        }
        next
    }

    /// Whether `thread_id` has at least one queued turn waiting.
    pub async fn has_queued(&self, thread_id: &str) -> bool {
        self.queues
            .read()
            .await
            .get(thread_id)
            .is_some_and(|q| !q.is_empty())
    }

    /// Total queued turns across all threads (observability / tests).
    pub async fn len(&self) -> usize {
        self.queues
            .read()
            .await
            .values()
            .map(|q| q.len())
            .sum()
    }

    /// Whether every thread's queue is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

/// Default working directory stamped on a provider session when the
/// `StartTurn` command (which carries no working-dir field) flows through the
/// reactor. Centralized here so [`ProviderCommandReactor::ensure_session_for_thread`]
/// builds a stable [`SessionIdentity`] for the production path.
const DEFAULT_WORKING_DIR: &str = "/tmp/syncode";

/// Outcome of [`ProviderCommandReactor::ensure_session_for_thread`].
///
/// Mirrors mcode's `ensureSessionForThread` decision tree:
/// - no active session for the thread → [`EnsureOutcome::Fresh`]
/// - active session whose recorded identity matches the request →
///   [`EnsureOutcome::Reused`]
/// - active session whose identity differs (model/provider/working-dir
///   changed) → [`EnsureOutcome::Restarted`]: the old session is stopped and a
///   new one started, carrying the old session's resume cursor.
#[derive(Debug, Clone)]
pub enum EnsureOutcome {
    /// No prior active session for the thread — a fresh one was started.
    Fresh {
        /// The newly created provider session id.
        session_id: String,
    },
    /// An active session with a matching identity was reused (no restart).
    Reused {
        /// The reused session id.
        session_id: String,
    },
    /// The prior session's identity differed — it was stopped and a new one
    /// started, carrying the old session's resume cursor.
    Restarted {
        /// The stopped session id.
        old_session_id: String,
        /// The newly created session id.
        new_session_id: String,
        /// The resume cursor carried over from the old session, if any.
        resume_cursor: Option<String>,
    },
}

impl EnsureOutcome {
    /// The session id the caller should target for the current turn, whatever
    /// the outcome (freshly started, reused, or restarted).
    pub fn session_id(&self) -> &str {
        match self {
            EnsureOutcome::Fresh { session_id }
            | EnsureOutcome::Reused { session_id }
            | EnsureOutcome::Restarted {
                new_session_id: session_id,
                ..
            } => session_id,
        }
    }

    /// Whether this outcome started a new provider session (Fresh or
    /// Restarted). `Reused` returns `false`.
    pub fn started_new_session(&self) -> bool {
        matches!(
            self,
            EnsureOutcome::Fresh { .. } | EnsureOutcome::Restarted { .. }
        )
    }
}

/// The command reactor bridges domain commands to provider adapter calls.
///
/// It holds a reference to a `SessionManager` for session lifecycle and a
/// [`TurnQueue`] for the queued-turn pipeline (P0-7): turns that arrive while
/// their thread already has an in-flight `Processing` session are parked
/// rather than dispatched, and drained by the ingestion reactor when the
/// in-flight turn completes.
///
/// When a [`MemoryProvider`] is attached via [`Self::with_memory`] (P3-4),
/// every freshly started provider session has its `system_prompt` augmented
/// with the memory context retrieved for the turn's thread — grounding the
/// provider in prior interactions without the caller having to assemble it.
pub struct ProviderCommandReactor {
    session_manager: SessionManager,
    turn_queue: TurnQueue,
    /// Optional persistent-memory backing. When `Some`, retrieved context is
    /// injected into the system prompt of every freshly started session (the
    /// Fresh and Restarted paths of `ensure_session_for_thread`). Reused
    /// sessions are left untouched — their prompt was already grounded when
    /// they were started.
    memory: Option<Arc<dyn MemoryProvider>>,
}

impl ProviderCommandReactor {
    /// Create a new command reactor
    pub fn new(session_manager: SessionManager) -> Self {
        Self {
            session_manager,
            turn_queue: TurnQueue::new(),
            memory: None,
        }
    }

    /// Attach a persistent-memory backing so freshly started provider sessions
    /// are grounded in retrieved context (P3-4).
    ///
    /// On the Fresh and Restarted paths of `ensure_session_for_thread`, the
    /// reactor queries `memory.retrieve_context(thread_id, user_input)` and
    /// appends the result to the session's `system_prompt` (or seeds one when
    /// the caller supplied `None`). The Reused path skips injection — that
    /// session was already grounded when it was started, and re-querying
    /// memory on every turn would needlessly bloat an in-flight conversation.
    ///
    /// Consumes `self` and returns the configured reactor (builder style).
    /// `new()` remains the zero-arg default for callers that don't yet wire
    /// memory, so existing construction sites compile unchanged.
    #[must_use]
    pub fn with_memory(mut self, memory: Arc<dyn MemoryProvider>) -> Self {
        self.memory = Some(memory);
        self
    }

    /// The attached memory provider, if any (P3-4). Exposed for inspection by
    /// tests and integration callers; the reactor itself routes through this
    /// internally during session start.
    pub fn memory(&self) -> Option<&Arc<dyn MemoryProvider>> {
        self.memory.as_ref()
    }

    /// Get a reference to the session manager
    pub fn session_manager(&self) -> &SessionManager {
        &self.session_manager
    }

    /// Get a reference to the per-thread turn queue (P0-7).
    pub fn turn_queue(&self) -> &TurnQueue {
        &self.turn_queue
    }

    /// React to a domain command by invoking the provider adapter.
    ///
    /// Returns a `CommandReaction` indicating what happened.
    /// This does NOT produce domain events — the ingestion reactor handles
    /// the reverse direction (provider events → domain events).
    pub async fn react(
        &self,
        command: &Command,
        adapter: &syncode_provider::registry::SharedAdapter,
        turn_id_hint: Option<EntityId>,
    ) -> Result<CommandReaction, CommandReactorError> {
        match command {
            Command::StartTurn {
                thread_id,
                sequence,
                user_input,
            } => {
                self.handle_start_turn(turn_id_hint, *thread_id, *sequence, user_input, adapter)
                    .await
            }

            Command::FailTurn { id, error: _ } => self.handle_fail_turn(*id, adapter).await,

            Command::CancelTurn { id } => self.handle_cancel_turn(*id, adapter).await,

            Command::InterruptTurn { id } => {
                // Interrupt an in-progress turn: interrupt the provider session (if any).
                self.handle_interrupt_turn(*id, adapter).await
            }

            Command::PauseThread { id: _ } => {
                // Pause thread: interrupt all active sessions
                let results = self.session_manager.interrupt_all(adapter).await;
                Ok(CommandReaction {
                    handled: !results.is_empty(),
                    session_id: None,
                    events: vec![],
                })
            }

            Command::CancelThread { id: _ } => {
                // Cancel thread: stop all active sessions
                let active = self.session_manager.list_active_sessions().await;
                for sid in active {
                    let _ = self.session_manager.stop_session(adapter, &sid).await;
                }
                Ok(CommandReaction {
                    handled: true,
                    session_id: None,
                    events: vec![],
                })
            }

            Command::StopThreadSession { id } => {
                // Stop the active provider sessions backing THIS thread (not every
                // thread's sessions). Uses the SessionManager's thread→session index;
                // among a thread's sessions we stop the ones still active.
                let mut handled = false;
                let sessions = self
                    .session_manager
                    .get_sessions_by_thread(&id.as_str())
                    .await;
                for session in &sessions {
                    if session.is_active() {
                        let _ = self
                            .session_manager
                            .stop_session(adapter, &session.id)
                            .await;
                        handled = true;
                    }
                }
                Ok(CommandReaction {
                    handled,
                    session_id: None,
                    events: vec![],
                })
            }

            // Provider-dispatch commands (T6 turn interactions). The Decider records
            // the client's response/edits via Requested-style events; these arms
            // dispatch the actual response to the provider session currently
            // Processing the thread. Faithful to mcode's approval/user-input
            // response-requested dispatch. If no session is Processing the thread
            // (nothing awaiting input), there is nothing to dispatch (handled = false).
            Command::RespondThreadApproval {
                id,
                request_id,
                decision,
            } => {
                let payload = serde_json::json!({
                    "request_id": request_id,
                    "decision": decision,
                });
                let session_id = self
                    .dispatch_to_thread_session(*id, "approval/respond", payload, adapter)
                    .await?;
                Ok(CommandReaction {
                    handled: session_id.is_some(),
                    session_id,
                    events: vec![],
                })
            }
            Command::RespondThreadUserInput {
                id,
                request_id,
                answers,
            } => {
                let payload = serde_json::json!({
                    "request_id": request_id,
                    "answers": answers,
                });
                let session_id = self
                    .dispatch_to_thread_session(*id, "user-input/respond", payload, adapter)
                    .await?;
                Ok(CommandReaction {
                    handled: session_id.is_some(),
                    session_id,
                    events: vec![],
                })
            }
            Command::EditAndResendThreadMessage {
                id,
                message_id,
                text,
            } => {
                let payload = serde_json::json!({
                    "message_id": message_id.as_str(),
                    "text": text,
                });
                let session_id = self
                    .dispatch_to_thread_session(*id, "message/edit-and-resend", payload, adapter)
                    .await?;
                Ok(CommandReaction {
                    handled: session_id.is_some(),
                    session_id,
                    events: vec![],
                })
            }
            Command::DispatchQueuedTurn {
                id,
                message_id,
                runtime_mode,
                interaction_mode,
                dispatch_mode,
            } => {
                // P0-7: queued-turn pipeline. When a session is actively
                // `Processing` the thread, dispatching a second turn
                // immediately would collide (two turns for the same thread
                // running simultaneously). The collision-avoidance policy:
                //
                //  - **Steering-capable provider + active session** → redirect
                //    the in-flight generation via `steer_turn` (P0-3). Steering
                //    IS collision-avoidance: it redirects the same turn rather
                //    than starting a parallel one, so the turn is handled
                //    without queueing.
                //  - **Non-steering provider + active session** → park the turn
                //    in the per-thread [`TurnQueue`]. The ingestion reactor
                //    drains it when the in-flight turn completes
                //    (`TurnCompleted`), guaranteeing no two turns for the same
                //    thread run at once.
                //  - **No active session** → dispatch immediately (nothing to
                //    collide with).
                let payload = serde_json::json!({
                    "message_id": message_id.as_str(),
                    "runtime_mode": runtime_mode,
                    "interaction_mode": interaction_mode,
                    "dispatch_mode": dispatch_mode,
                });

                let active_session_id = self.active_session_id_for_thread(*id).await;
                let supports_steering = match &active_session_id {
                    Some(_) => self.provider_supports_steering(adapter).await,
                    None => false,
                };

                if let Some(session_id) = &active_session_id
                    && supports_steering
                {
                    // Steering fast-path: redirect the in-flight generation.
                    let session_id = session_id.clone();
                    let mut steer_params = match payload {
                        serde_json::Value::Object(map) => map,
                        _ => serde_json::Map::new(),
                    };
                    steer_params.insert(
                        "session_id".to_string(),
                        serde_json::Value::String(session_id.clone()),
                    );
                    steer_params.insert(
                        "method".to_string(),
                        serde_json::Value::String("turn/dispatch-queued".to_string()),
                    );
                    let guard = adapter.read().await;
                    guard
                        .steer_turn(&session_id, serde_json::Value::Object(steer_params))
                        .await
                        .map_err(|e| CommandReactorError::ProviderError(e.to_string()))?;
                    return Ok(CommandReaction {
                        handled: true,
                        session_id: Some(session_id),
                        events: vec![],
                    });
                }

                if active_session_id.is_some() {
                    // Active session but no steering → queue the turn to avoid a
                    // collision. The ingestion reactor drains it when the
                    // in-flight turn completes.
                    self.turn_queue
                        .enqueue(QueuedTurn {
                            thread_id: *id,
                            message_id: *message_id,
                            runtime_mode: runtime_mode.clone(),
                            interaction_mode: interaction_mode.clone(),
                            dispatch_mode: dispatch_mode.clone(),
                        })
                        .await;
                    let queued_depth = self.turn_queue.len().await;
                    crate::log::info(&format!(
                        "queued turn parked behind active session (thread_id = {}, queued_depth = {queued_depth})",
                        id.as_str()
                    ));
                    return Ok(CommandReaction {
                        handled: true,
                        session_id: None,
                        events: vec![],
                    });
                }

                // No active session → dispatch immediately (nothing to collide
                // with). This path starts a fresh dispatch via send_request.
                let session_id = self
                    .dispatch_to_thread_session(*id, "turn/dispatch-queued", payload, adapter)
                    .await?;
                Ok(CommandReaction {
                    handled: session_id.is_some(),
                    session_id,
                    events: vec![],
                })
            }

            // Commands that don't need provider interaction
            Command::CreateProject { .. }
            | Command::UpdateProjectConfig { .. }
            | Command::DeleteProject { .. }
            | Command::CreateThread { .. }
            | Command::ResumeThread { .. }
            | Command::CompleteThread { .. }
            | Command::SetThreadTitle { .. }
            | Command::ArchiveThread { .. }
            | Command::UnarchiveThread { .. }
            | Command::DeleteThread { .. }
            | Command::SetThreadRuntimeMode { .. }
            | Command::SetThreadInteractionMode { .. }
            | Command::SetThreadSession { .. }
            | Command::AppendThreadActivity { .. }
            | Command::AddPinnedMessage { .. }
            | Command::RemovePinnedMessage { .. }
            | Command::SetPinnedMessageDone { .. }
            | Command::SetPinnedMessageLabel { .. }
            | Command::AddMarker { .. }
            | Command::RemoveMarker { .. }
            | Command::SetMarkerDone { .. }
            | Command::SetMarkerLabel { .. }
            | Command::HandoffCreateThread { .. }
            | Command::ForkCreateThread { .. }
            | Command::RevertToCheckpoint { .. }
            | Command::CompleteTurn { .. }
            | Command::RecordTurnFiles { .. }
            | Command::SetTurnCheckpoint { .. }
            | Command::AddMessage { .. }
            | Command::AppendAssistantDelta { .. }
            | Command::FinalizeAssistantMessage { .. }
            | Command::UpsertProposedPlan { .. }
            | Command::CompleteTurnDiff { .. }
            | Command::CompleteRevert { .. }
            | Command::ConversationRollback { .. }
            | Command::ConversationRollbackComplete { .. }
            | Command::ImportMessages { .. } => Ok(CommandReaction {
                handled: false,
                session_id: None,
                events: vec![],
            }),
        }
    }

    /// Find the id of the session currently Processing a thread, if any.
    ///
    /// Routes provider-dispatch commands (approval / user-input / edit-resend)
    /// to the one session actively Processing the thread. Uses the
    /// SessionManager's thread→session index; among a thread's sessions returns
    /// the most recent one in the Processing state (the session awaiting input).
    async fn active_session_id_for_thread(&self, thread_id: EntityId) -> Option<String> {
        self.session_manager
            .get_sessions_by_thread(&thread_id.as_str())
            .await
            .into_iter()
            .filter(|s| s.is_active() && s.status() == SessionStateStatus::Processing)
            .max_by_key(|s| s.created_at.timestamp_millis())
            .map(|s| s.id.clone())
    }

    /// Dispatch a JSON-RPC request to a thread's active Processing session.
    ///
    /// Returns the targeted session id on success, or `None` if no session is
    /// Processing the thread (nothing to dispatch to → `handled = false`). The
    /// `session_id` is injected into the request params so the provider adapter
    /// can correlate the response to its session (syncode's `send_request` is
    /// session-agnostic by design).
    async fn dispatch_to_thread_session(
        &self,
        thread_id: EntityId,
        method: &str,
        payload: serde_json::Value,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<Option<String>, CommandReactorError> {
        let Some(session_id) = self.active_session_id_for_thread(thread_id).await else {
            return Ok(None);
        };

        let mut params = match payload {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        params.insert(
            "session_id".to_string(),
            serde_json::Value::String(session_id.clone()),
        );

        let request = ProviderRequest::new(method, Some(serde_json::Value::Object(params)));
        let guard = adapter.read().await;
        guard
            .send_request(request)
            .await
            .map_err(|e| CommandReactorError::ProviderError(e.to_string()))?;

        Ok(Some(session_id))
    }

    /// Whether the (shared) adapter advertises [`ProviderCapability::Steering`].
    async fn provider_supports_steering(
        &self,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> bool {
        let guard = adapter.read().await;
        guard
            .capabilities()
            .contains(&ProviderCapability::Steering)
    }

    /// Ensure an active provider session exists for `thread_id`, restarting it
    /// lazily when the provider/model/working-dir has changed.
    ///
    /// This is syncode's counterpart to mcode's `ensureSessionForThread`. The
    /// decision tree (see PRD-REMAINING-GAPS.md §1):
    ///
    /// 1. **No active session** for the thread → start a fresh one, record its
    ///    identity, return [`EnsureOutcome::Fresh`].
    /// 2. **Active session whose recorded identity matches the request** →
    ///    reuse it (no stop/start), return [`EnsureOutcome::Reused`].
    /// 3. **Active session whose identity differs** (provider, model, or
    ///    working-dir changed) → stop the old session, capture its resume
    ///    cursor, start a new one, stamp the new identity + carry the cursor
    ///    over, return [`EnsureOutcome::Restarted`].
    ///
    /// The requested identity is built from the adapter's `provider_id`,
    /// `ctx.working_dir`, and the caller-supplied `requested_model`. Only the
    /// most recent active session for the thread is considered (older sessions
    /// are ignored — they are typically already stopped).
    ///
    /// `ctx.turn_id` is used to register the new session against the current
    /// turn in the `SessionManager`'s turn→session index.
    pub(crate) async fn ensure_session_for_thread(
        &self,
        mut ctx: SessionContext,
        requested_model: Option<String>,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<EnsureOutcome, CommandReactorError> {
        // Build the requested identity from the adapter's provider id + the
        // session context's working dir + the caller's model selection.
        let provider_id = {
            let guard = adapter.read().await;
            guard.provider_id().to_string()
        };
        let requested = SessionIdentity {
            provider_id,
            model: requested_model,
            working_dir: ctx.working_dir.clone(),
        };

        // Find the most recent active session for this thread (if any). Only
        // active sessions are candidates for reuse — completed/errored ones are
        // treated as "no session".
        let existing = self
            .session_manager
            .get_sessions_by_thread(&ctx.thread_id.as_str())
            .await
            .into_iter()
            .filter(|s| s.is_active())
            .max_by_key(|s| s.created_at.timestamp_millis());

        let Some(existing) = existing else {
            // (1) No active session → start fresh. When a memory provider is
            // attached, ground the new session's system prompt in the retrieved
            // context for this thread (P3-4). Retrieval happens only on the
            // start paths so a reused session is not re-queried.
            ctx = self.augment_ctx_with_memory(ctx).await;
            let session = self.start_and_stamp(ctx, requested, None, adapter).await?;
            return Ok(EnsureOutcome::Fresh {
                session_id: session.id.clone(),
            });
        };

        // (2) Active session with matching identity → reuse. No memory
        // augmentation: that session was already grounded when it started, and
        // re-querying memory on every turn would bloat an in-flight
        // conversation without changing the provider's already-seen context.
        if existing.identity().as_ref() == Some(&requested) {
            return Ok(EnsureOutcome::Reused {
                session_id: existing.id.clone(),
            });
        }

        // (3) Identity changed → stop the old session, carry its resume cursor,
        // and start a new one stamped with the requested identity. The new
        // session is grounded in freshly retrieved memory context (P3-4).
        let old_session_id = existing.id.clone();
        let resume_cursor = existing.resume_cursor();
        let _ = self
            .session_manager
            .stop_session(adapter, &old_session_id)
            .await;
        ctx = self.augment_ctx_with_memory(ctx).await;
        let session = self
            .start_and_stamp(ctx, requested, resume_cursor.clone(), adapter)
            .await?;
        Ok(EnsureOutcome::Restarted {
            old_session_id,
            new_session_id: session.id.clone(),
            resume_cursor,
        })
    }

    /// Retrieve memory context for the session's thread and append it to
    /// `ctx.system_prompt` (P3-4).
    ///
    /// Called on the Fresh and Restarted paths of
    /// [`Self::ensure_session_for_thread`] — i.e. only when a brand-new
    /// provider session is about to start. The context is scoped to the
    /// thread id (used as the memory `user_id` so each conversation has its
    /// own recalled history) and is appended to whatever system prompt the
    /// caller already assembled, separated by a blank line. When the caller
    /// supplied `None` for the system prompt, the retrieved context seeds
    /// one directly.
    ///
    /// The sentinel [`NO_PRIOR_CONTEXT`] is filtered out so a fresh thread
    /// (no history yet) doesn't get a literal "No prior context available."
    /// string injected into its prompt — in that case the prompt is left
    /// untouched. When no memory provider is attached, this is a no-op.
    async fn augment_ctx_with_memory(&self, mut ctx: SessionContext) -> SessionContext {
        let Some(memory) = self.memory.as_ref() else {
            return ctx;
        };
        // The thread id is the memory scope key: one conversation = one
        // recalled history. `user_input` is the retrieval query (reserved for
        // future semantic search; the current store returns the N most recent
        // interactions for the scope regardless of the query text).
        let retrieved = memory
            .retrieve_context(&ctx.thread_id.as_str(), &ctx.user_input)
            .await;
        // Skip the no-history sentinel so we don't pollute the prompt with a
        // literal "No prior context available." on a thread's very first turn.
        if retrieved.is_empty() || retrieved == NO_PRIOR_CONTEXT {
            return ctx;
        }
        let augmented = match ctx.system_prompt.take() {
            Some(existing) => format!("{existing}\n\n{retrieved}"),
            None => retrieved,
        };
        ctx.system_prompt = Some(augmented);
        ctx
    }

    /// Start a new session on the adapter, then stamp the requested identity
    /// and (optionally) carry over a resume cursor from a prior session.
    ///
    /// Factored out of [`Self::ensure_session_for_thread`] so the Fresh and
    /// Restarted paths share the same "start + record identity + record
    /// cursor" bookkeeping. The returned [`SessionState`] carries the stamped
    /// identity (so a subsequent `ensure_session_for_thread` can decide reuse
    /// vs. restart) and, when `resume_cursor` is `Some`, the carried cursor
    /// (so it survives rehydration per P0-4).
    async fn start_and_stamp(
        &self,
        ctx: SessionContext,
        identity: SessionIdentity,
        resume_cursor: Option<String>,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<std::sync::Arc<syncode_provider::SessionState>, CommandReactorError> {
        let session = self
            .session_manager
            .start_session_with_cursor(adapter, ctx, resume_cursor)
            .await?;
        // Stamp the identity so a subsequent ensure call can compare against it.
        session.set_identity(Some(identity));
        Ok(session)
    }

    /// Handle StartTurn: create a provider session and send the initial request
    async fn handle_start_turn(
        &self,
        turn_id: Option<EntityId>,
        thread_id: EntityId,
        sequence: u32,
        user_input: &str,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        let turn_id = turn_id.unwrap_or_default();

        // Check if a session already exists for this turn (idempotent retry).
        if let Some(existing) = self
            .session_manager
            .get_session_by_turn(&turn_id.as_str())
            .await
        {
            return Ok(CommandReaction {
                handled: true,
                session_id: Some(existing.id.clone()),
                events: vec![],
            });
        }

        // Build session context
        let ctx = SessionContext {
            thread_id,
            turn_id,
            working_dir: DEFAULT_WORKING_DIR.to_string(),
            system_prompt: Some("You are a helpful AI coding assistant.".to_string()),
            user_input: user_input.to_string(),
            context_files: vec![],
        };

        // P0-5: ensure a session exists for this thread, restarting it lazily
        // if the provider/model/working-dir changed since the last turn. The
        // production StartTurn command carries no model field, so the requested
        // model is `None` here — model-change restarts are exercised directly
        // via `ensure_session_for_thread` (and its tests).
        let outcome = self
            .ensure_session_for_thread(ctx, None, adapter)
            .await?;
        let session_id = outcome.session_id().to_string();

        // Send the initial request to the provider
        let request = ProviderRequest::new(
            "chat",
            Some(serde_json::json!({
                "input": user_input,
                "sequence": sequence,
            })),
        );

        let guard = adapter.read().await;
        let _resp = guard
            .send_request(request)
            .await
            .map_err(|e| CommandReactorError::ProviderError(e.to_string()))?;

        Ok(CommandReaction {
            handled: true,
            session_id: Some(session_id),
            events: vec![],
        })
    }

    /// Handle FailTurn: interrupt the session
    async fn handle_fail_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        self.interrupt_session_for_turn(turn_id, adapter).await
    }

    /// Handle InterruptTurn: interrupt the session (same lifecycle as FailTurn).
    async fn handle_interrupt_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        self.interrupt_session_for_turn(turn_id, adapter).await
    }

    /// Interrupt the provider session backing a turn, if one exists.
    ///
    /// Shared by `FailTurn` and `InterruptTurn` — both interrupt an in-flight
    /// provider session, differing only in the domain event the Decider emits.
    async fn interrupt_session_for_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        let session = self
            .session_manager
            .get_session_by_turn(&turn_id.as_str())
            .await;
        if let Some(session) = session {
            let _ = self
                .session_manager
                .interrupt_session(adapter, &session.id)
                .await;
            Ok(CommandReaction {
                handled: true,
                session_id: Some(session.id.clone()),
                events: vec![],
            })
        } else {
            Ok(CommandReaction {
                handled: false,
                session_id: None,
                events: vec![],
            })
        }
    }

    /// Handle CancelTurn: stop the session
    async fn handle_cancel_turn(
        &self,
        turn_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<CommandReaction, CommandReactorError> {
        let session = self
            .session_manager
            .get_session_by_turn(&turn_id.as_str())
            .await;
        if let Some(session) = session {
            let _ = self
                .session_manager
                .stop_session(adapter, &session.id)
                .await;
            Ok(CommandReaction {
                handled: true,
                session_id: Some(session.id.clone()),
                events: vec![],
            })
        } else {
            Ok(CommandReaction {
                handled: false,
                session_id: None,
                events: vec![],
            })
        }
    }

    /// Drain and dispatch the next queued turn for `thread_id`, if any.
    ///
    /// Called by the ingestion reactor (see
    /// [`crate::reactors::ingestion::dispatch_queued_turn_after_completion`])
    /// when a turn's `TurnCompleted` event flows through: the in-flight turn
    /// has finished, so the next parked turn (if one exists) is now free to
    /// dispatch without colliding. This is the drain half of the P0-7
    /// queued-turn pipeline — the enqueue half lives in the
    /// `DispatchQueuedTurn` command arm.
    ///
    /// Returns the dispatched session id when a queued turn was drained and
    /// dispatched, or `None` when the thread had no queued turn. Errors from
    /// the underlying dispatch propagate; a failed dispatch does NOT re-queue
    /// the turn (the caller may retry by re-issuing `DispatchQueuedTurn`).
    pub async fn dispatch_next_queued_turn(
        &self,
        thread_id: EntityId,
        adapter: &syncode_provider::registry::SharedAdapter,
    ) -> Result<Option<String>, CommandReactorError> {
        let Some(turn) = self.turn_queue.dequeue(&thread_id.as_str()).await else {
            return Ok(None);
        };
        let payload = serde_json::json!({
            "message_id": turn.message_id.as_str(),
            "runtime_mode": turn.runtime_mode,
            "interaction_mode": turn.interaction_mode,
            "dispatch_mode": turn.dispatch_mode,
        });
        let session_id = self
            .dispatch_to_thread_session(thread_id, "turn/dispatch-queued", payload, adapter)
            .await?;
        if session_id.is_none() {
            crate::log::warn(&format!(
                "drained queued turn had no active session to dispatch to; turn dropped (thread_id = {})",
                thread_id.as_str()
            ));
        }
        Ok(session_id)
    }
}

/// Errors during command reaction
#[derive(Debug, thiserror::Error)]
pub enum CommandReactorError {
    #[error("Provider error: {0}")]
    ProviderError(String),

    #[error("Session error: {0}")]
    SessionError(String),
}

impl From<syncode_provider::ProviderAdapterError> for CommandReactorError {
    fn from(e: syncode_provider::ProviderAdapterError) -> Self {
        CommandReactorError::ProviderError(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Arc;
    use syncode_provider::{ProviderAdapter, ProviderConfig, ProviderResponse, ProviderStatus};
    use tokio::sync::RwLock;

    /// Recorded (method, params) dispatch log shared between the mock and tests.
    type RecordedRequests = Arc<std::sync::Mutex<Vec<(String, Option<serde_json::Value>)>>>;

    /// Recorded `(session_id, payload)` entries for every `steer_turn` call.
    type RecordedSteers = Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>;

    /// Recorded system prompts captured from every `start_session` call.
    /// Used by the P3-4 context-injection tests to assert that retrieved
    /// memory context flowed into the provider session's system prompt.
    type RecordedSystemPrompts = Arc<std::sync::Mutex<Vec<Option<String>>>>;

    /// Mock adapter for command reactor tests
    struct CmdTestMock {
        started_sessions: std::sync::Mutex<Vec<String>>,
        interrupted: std::sync::Mutex<Vec<String>>,
        stopped: Arc<std::sync::Mutex<Vec<String>>>,
        /// (method, params) for every dispatched JSON-RPC request
        requests: RecordedRequests,
        /// (session_id, payload) for every `steer_turn` invocation.
        steers: RecordedSteers,
        /// When true the adapter advertises `ProviderCapability::Steering`.
        supports_steering: bool,
        /// System prompt captured from each `start_session` call (P3-4). One
        /// entry per started session, in start order. `None` means the caller
        /// supplied no system prompt; `Some(s)` means `s` was the (possibly
        /// memory-augmented) prompt the provider received.
        recorded_system_prompts: RecordedSystemPrompts,
    }

    impl CmdTestMock {
        fn new() -> Self {
            Self {
                started_sessions: std::sync::Mutex::new(Vec::new()),
                interrupted: std::sync::Mutex::new(Vec::new()),
                stopped: Arc::new(std::sync::Mutex::new(Vec::new())),
                requests: Arc::new(std::sync::Mutex::new(Vec::new())),
                steers: Arc::new(std::sync::Mutex::new(Vec::new())),
                supports_steering: false,
                recorded_system_prompts: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        /// Construct with shared recording handles the test can inspect directly
        /// (the adapter is read back as a `dyn ProviderAdapter`, so its fields
        /// are not reachable through the trait object).
        fn new_with_handles() -> (Self, Arc<std::sync::Mutex<Vec<String>>>, RecordedRequests) {
            let stopped = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let this = Self {
                started_sessions: std::sync::Mutex::new(Vec::new()),
                interrupted: std::sync::Mutex::new(Vec::new()),
                stopped: Arc::clone(&stopped),
                requests: Arc::clone(&requests),
                steers: Arc::new(std::sync::Mutex::new(Vec::new())),
                supports_steering: false,
                recorded_system_prompts: Arc::new(std::sync::Mutex::new(Vec::new())),
            };
            (this, stopped, requests)
        }

        /// Construct a steering-capable mock with shared recording handles for
        /// `send_request`, `steer_turn`, and `stop_session`. Used to exercise
        /// the `DispatchQueuedTurn` steer fast-path.
        fn new_steering_with_handles() -> (
            Self,
            Arc<std::sync::Mutex<Vec<String>>>,
            RecordedRequests,
            RecordedSteers,
        ) {
            let stopped = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let steers = Arc::new(std::sync::Mutex::new(Vec::new()));
            let this = Self {
                started_sessions: std::sync::Mutex::new(Vec::new()),
                interrupted: std::sync::Mutex::new(Vec::new()),
                stopped: Arc::clone(&stopped),
                requests: Arc::clone(&requests),
                steers: Arc::clone(&steers),
                supports_steering: true,
                recorded_system_prompts: Arc::new(std::sync::Mutex::new(Vec::new())),
            };
            (this, stopped, requests, steers)
        }

        /// Like `new_with_handles` but also returns the shared handle for the
        /// recorded system prompts (P3-4). Tests assert that memory context
        /// flowed into the prompt by reading this vector after a start.
        fn new_with_prompt_handles() -> (
            Self,
            Arc<std::sync::Mutex<Vec<String>>>,
            RecordedRequests,
            RecordedSystemPrompts,
        ) {
            let stopped = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let prompts = Arc::new(std::sync::Mutex::new(Vec::new()));
            let this = Self {
                started_sessions: std::sync::Mutex::new(Vec::new()),
                interrupted: std::sync::Mutex::new(Vec::new()),
                stopped: Arc::clone(&stopped),
                requests: Arc::clone(&requests),
                steers: Arc::new(std::sync::Mutex::new(Vec::new())),
                supports_steering: false,
                recorded_system_prompts: Arc::clone(&prompts),
            };
            (this, stopped, requests, prompts)
        }
    }

    #[async_trait::async_trait]
    impl ProviderAdapter for CmdTestMock {
        fn provider_id(&self) -> &str {
            "cmd-test-mock"
        }
        fn capabilities(&self) -> Vec<syncode_provider::ProviderCapability> {
            if self.supports_steering {
                vec![syncode_provider::ProviderCapability::Steering]
            } else {
                vec![]
            }
        }
        fn status(&self) -> ProviderStatus {
            ProviderStatus::Idle
        }
        fn available_models(&self) -> Vec<String> {
            vec!["mock".to_string()]
        }

        async fn spawn(
            &mut self,
            _config: ProviderConfig,
        ) -> Result<(), syncode_provider::ProviderAdapterError> {
            Ok(())
        }
        async fn shutdown(&mut self) -> Result<(), syncode_provider::ProviderAdapterError> {
            Ok(())
        }

        async fn interrupt(
            &self,
            session_id: &str,
        ) -> Result<(), syncode_provider::ProviderAdapterError> {
            self.interrupted
                .lock()
                .unwrap()
                .push(session_id.to_string());
            Ok(())
        }

        async fn start_session(
            &mut self,
            ctx: SessionContext,
        ) -> Result<String, syncode_provider::ProviderAdapterError> {
            let sid = format!("cmd-{}", uuid::Uuid::new_v4().hyphenated());
            self.started_sessions.lock().unwrap().push(sid.clone());
            // Capture the system prompt so P3-4 tests can assert that memory
            // context was injected. The vector mirrors `started_sessions` —
            // one entry per start, in start order.
            self.recorded_system_prompts
                .lock()
                .unwrap()
                .push(ctx.system_prompt.clone());
            Ok(sid)
        }

        async fn resume_session(
            &mut self,
            _session_id: &str,
        ) -> Result<(), syncode_provider::ProviderAdapterError> {
            Ok(())
        }

        async fn stop_session(
            &mut self,
            session_id: &str,
        ) -> Result<(), syncode_provider::ProviderAdapterError> {
            self.stopped.lock().unwrap().push(session_id.to_string());
            Ok(())
        }

        async fn send_request(
            &self,
            request: ProviderRequest,
        ) -> Result<ProviderResponse, syncode_provider::ProviderAdapterError> {
            self.requests
                .lock()
                .unwrap()
                .push((request.method.clone(), request.params.clone()));
            Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(1),
                result: Some(serde_json::json!({"ok": true})),
                error: None,
            })
        }

        async fn steer_turn(
            &self,
            session_id: &str,
            payload: serde_json::Value,
        ) -> Result<ProviderResponse, syncode_provider::ProviderAdapterError> {
            self.steers
                .lock()
                .unwrap()
                .push((session_id.to_string(), payload));
            Ok(ProviderResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(1),
                result: Some(serde_json::json!({"steered": true})),
                error: None,
            })
        }

        fn event_stream(
            &self,
            _session_id: &str,
        ) -> Result<syncode_provider::ProviderStream, syncode_provider::ProviderAdapterError>
        {
            Ok(Box::pin(tokio_stream::empty()))
        }

        async fn health_check(&self) -> Result<bool, syncode_provider::ProviderAdapterError> {
            Ok(true)
        }
    }

    pub(crate) fn make_shared_test_mock() -> syncode_provider::registry::SharedAdapter {
        Arc::new(RwLock::new(CmdTestMock::new()))
    }

    /// Like `make_shared_test_mock` but also returns shared handles for the
    /// recorded `stopped` session ids and dispatched `requests`.
    pub(crate) fn make_recorded_test_mock() -> (
        syncode_provider::registry::SharedAdapter,
        Arc<std::sync::Mutex<Vec<String>>>,
        RecordedRequests,
    ) {
        let (mock, stopped, requests) = CmdTestMock::new_with_handles();
        (Arc::new(RwLock::new(mock)), stopped, requests)
    }

    /// A steering-capable mock with recording handles for `send_request`,
    /// `steer_turn`, and `stop_session`. Advertises
    /// `ProviderCapability::Steering` so the reactor's steer fast-path engages.
    pub(crate) fn make_steering_test_mock() -> (
        syncode_provider::registry::SharedAdapter,
        Arc<std::sync::Mutex<Vec<String>>>,
        RecordedRequests,
        RecordedSteers,
    ) {
        let (mock, stopped, requests, steers) = CmdTestMock::new_steering_with_handles();
        (Arc::new(RwLock::new(mock)), stopped, requests, steers)
    }

    /// Like `make_recorded_test_mock` but also returns the shared handle for
    /// recorded system prompts (P3-4). Tests assert that memory context was
    /// injected into the provider session's system prompt by inspecting the
    /// prompt vector after `ensure_session_for_thread` starts a fresh session.
    pub(crate) fn make_prompt_recording_mock() -> (
        syncode_provider::registry::SharedAdapter,
        Arc<std::sync::Mutex<Vec<String>>>,
        RecordedRequests,
        RecordedSystemPrompts,
    ) {
        let (mock, stopped, requests, prompts) = CmdTestMock::new_with_prompt_handles();
        (Arc::new(RwLock::new(mock)), stopped, requests, prompts)
    }

    #[tokio::test]
    async fn start_turn_creates_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let turn_id = EntityId::new();
        let thread_id = EntityId::new();
        let command = Command::StartTurn {
            thread_id,
            sequence: 1,
            user_input: "Fix the bug".to_string(),
        };

        let result = reactor
            .react(&command, &adapter, Some(turn_id))
            .await
            .unwrap();
        assert!(result.handled);
        assert!(result.session_id.is_some());
    }

    #[tokio::test]
    async fn fail_turn_interrupts_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let turn_id = EntityId::new();
        let thread_id = EntityId::new();

        // Start a turn first
        reactor
            .react(
                &Command::StartTurn {
                    thread_id,
                    sequence: 1,
                    user_input: "test".to_string(),
                },
                &adapter,
                Some(turn_id),
            )
            .await
            .unwrap();

        // Now fail it
        let result = reactor
            .react(
                &Command::FailTurn {
                    id: turn_id,
                    error: "Something went wrong".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();
        assert!(result.handled);
    }

    #[tokio::test]
    async fn cancel_turn_stops_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let turn_id = EntityId::new();
        let thread_id = EntityId::new();

        reactor
            .react(
                &Command::StartTurn {
                    thread_id,
                    sequence: 1,
                    user_input: "test".to_string(),
                },
                &adapter,
                Some(turn_id),
            )
            .await
            .unwrap();

        let result = reactor
            .react(&Command::CancelTurn { id: turn_id }, &adapter, None)
            .await
            .unwrap();
        assert!(result.handled);
    }

    #[tokio::test]
    async fn non_provider_commands_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor
            .react(
                &Command::CreateProject {
                    name: "Test".to_string(),
                    root_path: "/tmp".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(!result.handled);
        assert!(result.session_id.is_none());
    }

    #[tokio::test]
    async fn fail_turn_no_session_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor
            .react(
                &Command::FailTurn {
                    id: EntityId::new(),
                    error: "error".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(!result.handled);
    }

    #[tokio::test]
    async fn cancel_turn_no_session_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor
            .react(
                &Command::CancelTurn {
                    id: EntityId::new(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(!result.handled);
    }

    #[tokio::test]
    async fn add_message_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let adapter = make_shared_test_mock();

        let result = reactor
            .react(
                &Command::AddMessage {
                    turn_id: EntityId::new(),
                    role: "user".to_string(),
                    content: "hello".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(!result.handled);
    }

    /// Helper: start a turn so a Processing session exists for the thread.
    async fn start_turn(
        reactor: &ProviderCommandReactor,
        adapter: &syncode_provider::registry::SharedAdapter,
        thread_id: EntityId,
    ) {
        reactor
            .react(
                &Command::StartTurn {
                    thread_id,
                    sequence: 1,
                    user_input: "hi".to_string(),
                },
                adapter,
                Some(EntityId::new()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn respond_approval_dispatches_to_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();
        start_turn(&reactor, &adapter, thread_id).await;

        let result = reactor
            .react(
                &Command::RespondThreadApproval {
                    id: thread_id,
                    request_id: "req-1".to_string(),
                    decision: "approved".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(result.handled);
        let session_id = result.session_id.clone().expect("session id");

        let reqs = requests.lock().unwrap().clone();
        // [0] = "chat" (StartTurn), [1] = approval/respond
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[1].0, "approval/respond");
        let params = reqs[1].1.as_ref().expect("params");
        assert_eq!(params["session_id"].as_str(), Some(session_id.as_str()));
        assert_eq!(params["request_id"].as_str(), Some("req-1"));
        assert_eq!(params["decision"].as_str(), Some("approved"));
    }

    #[tokio::test]
    async fn respond_user_input_dispatches_to_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();
        start_turn(&reactor, &adapter, thread_id).await;

        let result = reactor
            .react(
                &Command::RespondThreadUserInput {
                    id: thread_id,
                    request_id: "req-2".to_string(),
                    answers: "yes".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(result.handled);
        let reqs = requests.lock().unwrap().clone();
        assert_eq!(reqs.last().unwrap().0, "user-input/respond");
        let params = reqs.last().unwrap().1.as_ref().expect("params");
        assert_eq!(params["request_id"].as_str(), Some("req-2"));
        assert_eq!(params["answers"].as_str(), Some("yes"));
        assert!(params["session_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn edit_and_resend_dispatches_to_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();
        start_turn(&reactor, &adapter, thread_id).await;
        let message_id = EntityId::new();

        let result = reactor
            .react(
                &Command::EditAndResendThreadMessage {
                    id: thread_id,
                    message_id,
                    text: "edited".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(result.handled);
        let reqs = requests.lock().unwrap().clone();
        assert_eq!(reqs.last().unwrap().0, "message/edit-and-resend");
        let params = reqs.last().unwrap().1.as_ref().expect("params");
        assert_eq!(
            params["message_id"].as_str(),
            Some(message_id.as_str().as_str())
        );
        assert_eq!(params["text"].as_str(), Some("edited"));
    }

    #[tokio::test]
    async fn provider_dispatch_no_session_not_handled() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();

        // No session was started for this thread → nothing to dispatch to.
        let result = reactor
            .react(
                &Command::RespondThreadApproval {
                    id: EntityId::new(),
                    request_id: "req-x".to_string(),
                    decision: "denied".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();

        assert!(!result.handled);
        assert!(result.session_id.is_none());
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stop_thread_session_stops_only_its_thread() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, stopped, _requests) = make_recorded_test_mock();
        let thread_a = EntityId::new();
        let thread_b = EntityId::new();

        let a = start_turn_capture(&reactor, &adapter, thread_a).await;
        let b = start_turn_capture(&reactor, &adapter, thread_b).await;

        reactor
            .react(&Command::StopThreadSession { id: thread_a }, &adapter, None)
            .await
            .unwrap();

        let stopped = stopped.lock().unwrap().clone();
        assert!(stopped.contains(&a), "thread A's session should be stopped");
        assert!(
            !stopped.contains(&b),
            "thread B's session should be left running"
        );
    }

    /// Like start_turn but returns the created session id (for stop-scoping assertions).
    async fn start_turn_capture(
        reactor: &ProviderCommandReactor,
        adapter: &syncode_provider::registry::SharedAdapter,
        thread_id: EntityId,
    ) -> String {
        let r = reactor
            .react(
                &Command::StartTurn {
                    thread_id,
                    sequence: 1,
                    user_input: "hi".to_string(),
                },
                adapter,
                Some(EntityId::new()),
            )
            .await
            .unwrap();
        r.session_id.expect("session id")
    }

    // -----------------------------------------------------------------------
    // DispatchQueuedTurn → steerTurn tests (P0-3)
    // -----------------------------------------------------------------------

    /// Helper: dispatch a queued turn for a thread, returning the reaction.
    async fn dispatch_queued(
        reactor: &ProviderCommandReactor,
        adapter: &syncode_provider::registry::SharedAdapter,
        thread_id: EntityId,
    ) -> CommandReaction {
        reactor
            .react(
                &Command::DispatchQueuedTurn {
                    id: thread_id,
                    message_id: EntityId::new(),
                    runtime_mode: "standard".to_string(),
                    interaction_mode: "chat".to_string(),
                    dispatch_mode: "queue".to_string(),
                },
                adapter,
                None,
            )
            .await
            .unwrap()
    }

    /// When a steering-capable provider has an active (Processing) session,
    /// `DispatchQueuedTurn` must call `steer_turn` instead of `send_request`.
    #[tokio::test]
    async fn dispatch_queued_turn_steers_active_session_when_supported() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests, steers) = make_steering_test_mock();
        let thread_id = EntityId::new();

        // Start a turn → a Processing session exists for the thread.
        start_turn(&reactor, &adapter, thread_id).await;

        let result = dispatch_queued(&reactor, &adapter, thread_id).await;

        assert!(result.handled, "steer dispatch should be handled");
        let session_id = result.session_id.expect("session id");

        // The provider's steer_turn should record exactly one call targeting
        // the active session, carrying the queued-turn payload.
        let steers = steers.lock().unwrap().clone();
        assert_eq!(steers.len(), 1, "exactly one steer_turn call expected");
        assert_eq!(steers[0].0, session_id, "steer must target the active session");
        let payload = &steers[0].1;
        assert_eq!(
            payload["method"].as_str(),
            Some("turn/dispatch-queued"),
            "payload must carry the dispatch method for steer-aware providers"
        );
        assert_eq!(
            payload["dispatch_mode"].as_str(),
            Some("queue"),
            "payload must carry the queued-turn dispatch_mode"
        );

        // The steer fast-path must NOT also fire a send_request for the
        // dispatch (only the StartTurn "chat" request should be present).
        let reqs = requests.lock().unwrap().clone();
        assert_eq!(
            reqs.len(),
            1,
            "no extra send_request should fire when steering; got {reqs:?}"
        );
        assert_eq!(reqs[0].0, "chat", "only the initial StartTurn request expected");
    }

    /// When the provider does NOT support steering but a session is active,
    /// `DispatchQueuedTurn` parks the turn in the per-thread queue (P0-7)
    /// instead of dispatching immediately — this is the collision-avoidance
    /// contract: no two turns for the same thread run simultaneously. The
    /// turn is drained and dispatched by the ingestion reactor when the
    /// in-flight turn completes (see
    /// `dispatch_queued_turn_after_completion`).
    #[tokio::test]
    async fn dispatch_queued_turn_falls_back_to_send_request_without_capability() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        // Non-steering mock — capabilities() returns [].
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        start_turn(&reactor, &adapter, thread_id).await;

        let result = dispatch_queued(&reactor, &adapter, thread_id).await;

        // Handled (the turn was accepted) but NOT dispatched — it is queued.
        assert!(
            result.handled,
            "queued turn should be handled (accepted into the queue)"
        );
        assert!(
            result.session_id.is_none(),
            "queued turn must NOT dispatch immediately (collision avoidance)"
        );

        // No send_request fired for the dispatch — only the StartTurn "chat"
        // request is present. The turn waits in the queue.
        let reqs = requests.lock().unwrap().clone();
        assert_eq!(
            reqs.len(),
            1,
            "queued turn must not fire a dispatch request, got {reqs:?}"
        );
        assert_eq!(reqs[0].0, "chat", "only the initial StartTurn request");

        // The turn is parked in the thread's queue, waiting for completion.
        assert!(
            reactor.turn_queue().has_queued(&thread_id.as_str()).await,
            "queued turn must be visible in the per-thread queue"
        );
        assert_eq!(reactor.turn_queue().len().await, 1);
    }

    /// With no active Processing session, `DispatchQueuedTurn` is not handled
    /// regardless of steering capability (nothing to steer or dispatch to).
    #[tokio::test]
    async fn dispatch_queued_turn_no_active_session_not_handled_even_if_steering() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        // Steering-capable mock, but no turn started → no active session.
        let (adapter, _stopped, requests, steers) = make_steering_test_mock();

        let result = dispatch_queued(&reactor, &adapter, EntityId::new()).await;

        assert!(!result.handled, "nothing to dispatch to without an active session");
        assert!(result.session_id.is_none());

        // Neither path should have fired any provider call.
        assert!(
            requests.lock().unwrap().is_empty(),
            "no send_request should fire when no session is active"
        );
        assert!(
            steers.lock().unwrap().is_empty(),
            "no steer_turn should fire when no session is active"
        );
    }

    // -----------------------------------------------------------------------
    // P0-7: Queued-turn pipeline tests
    // -----------------------------------------------------------------------
    //
    // Collision-avoidance contract: when a `DispatchQueuedTurn` arrives while
    // the thread already has an active `Processing` session, the turn is parked
    // in the per-thread `TurnQueue` instead of dispatched. The ingestion
    // reactor drains the queue when the in-flight turn completes.

    /// With an active Processing session and a non-steering provider, two
    /// `DispatchQueuedTurn` commands on the SAME thread must NOT collide: both
    /// are parked in the thread's queue in FIFO order, and no dispatch fires.
    #[tokio::test]
    async fn p0_7_queued_turns_park_behind_active_session_fifo() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        // Non-steering mock with an active Processing session.
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();
        start_turn(&reactor, &adapter, thread_id).await;

        // Dispatch two queued turns back-to-back while the session is busy.
        let r1 = dispatch_queued(&reactor, &adapter, thread_id).await;
        let r2 = dispatch_queued(&reactor, &adapter, thread_id).await;

        // Both are accepted (handled) but neither dispatches — both are queued.
        assert!(r1.handled && r2.handled);
        assert!(r1.session_id.is_none() && r2.session_id.is_none());

        // The only request on the wire is the initial StartTurn "chat".
        assert_eq!(requests.lock().unwrap().len(), 1);

        // Both turns are queued for the thread, in FIFO order. Dequeue pops
        // the first-submitted turn first.
        assert_eq!(reactor.turn_queue().len().await, 2);
        let first = reactor.turn_queue().dequeue(&thread_id.as_str()).await.expect("first");
        let second = reactor.turn_queue().dequeue(&thread_id.as_str()).await.expect("second");
        assert_eq!(first.thread_id, thread_id);
        assert_eq!(second.thread_id, thread_id);
        assert_ne!(
            first.message_id, second.message_id,
            "the two queued turns are distinct"
        );
        // After draining both, the thread's queue is empty.
        assert!(!reactor.turn_queue().has_queued(&thread_id.as_str()).await);
        assert!(reactor.turn_queue().is_empty().await);
    }

    /// Turns queued for DIFFERENT threads don't collide — each thread has its
    /// own queue. A turn for thread B dispatches normally (no active session)
    /// while a turn for thread A is parked behind A's busy session.
    #[tokio::test]
    async fn p0_7_per_thread_isolation_no_cross_thread_collision() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_a = EntityId::new();
        let thread_b = EntityId::new();

        // Start a turn on thread A only — A has a busy session, B has none.
        start_turn(&reactor, &adapter, thread_a).await;

        // Queued turn for A → parked (A's session is busy).
        let ra = dispatch_queued(&reactor, &adapter, thread_a).await;
        assert!(ra.handled);
        assert!(ra.session_id.is_none(), "A's turn must queue");

        // Queued turn for B → no active session on B, so it has nothing to
        // dispatch to either (handled=false). The key assertion: B is NOT
        // blocked behind A's queue.
        let rb = reactor
            .react(
                &Command::DispatchQueuedTurn {
                    id: thread_b,
                    message_id: EntityId::new(),
                    runtime_mode: "standard".to_string(),
                    interaction_mode: "chat".to_string(),
                    dispatch_mode: "queue".to_string(),
                },
                &adapter,
                None,
            )
            .await
            .unwrap();
        assert!(!rb.handled, "B has no session → nothing to dispatch to");
        assert!(rb.session_id.is_none());

        // Only A has a queued turn; B's queue is untouched.
        assert!(reactor.turn_queue().has_queued(&thread_a.as_str()).await);
        assert!(!reactor.turn_queue().has_queued(&thread_b.as_str()).await);
        assert_eq!(reactor.turn_queue().len().await, 1);

        // No dispatch request fired for either queued turn.
        assert_eq!(requests.lock().unwrap().len(), 1, "only StartTurn's chat");
    }

    /// When the ingestion reactor observes a `TurnCompleted` for a thread with
    /// a parked turn, it drains the queue and dispatches the next turn —
    /// exercising `dispatch_queued_turn_after_completion` end-to-end.
    #[tokio::test]
    async fn p0_7_completion_drains_queued_turn_and_dispatches() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        // Start a turn (creates a busy Processing session) then park a queued
        // turn behind it.
        start_turn(&reactor, &adapter, thread_id).await;
        let queued = dispatch_queued(&reactor, &adapter, thread_id).await;
        assert!(queued.handled);
        assert!(reactor.turn_queue().has_queued(&thread_id.as_str()).await);

        // Simulate the ingestion reactor observing a TurnCompleted for this
        // thread: the in-flight turn is done, so the parked turn is drained
        // and dispatched.
        let completed_turn_id = EntityId::new();
        let dispatched = crate::reactors::ingestion::dispatch_queued_turn_after_completion(
            &reactor,
            thread_id,
            completed_turn_id,
            &adapter,
        )
        .await
        .expect("drain should succeed");

        // The drained turn dispatched to the active session.
        assert!(
            dispatched.is_some(),
            "drained turn must dispatch after completion"
        );

        // The dispatch fired turn/dispatch-queued via send_request.
        let reqs = requests.lock().unwrap().clone();
        // [0] = "chat" (StartTurn), [1] = "turn/dispatch-queued" (drained)
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[1].0, "turn/dispatch-queued");

        // The queue is now empty — the turn was drained.
        assert!(
            reactor.turn_queue().is_empty().await,
            "queue must be empty after drain"
        );

        // A second completion with no queued turn is a no-op (None).
        let again = crate::reactors::ingestion::dispatch_queued_turn_after_completion(
            &reactor,
            thread_id,
            EntityId::new(),
            &adapter,
        )
        .await
        .unwrap();
        assert!(again.is_none(), "no queued turn → no dispatch");
    }

    // -----------------------------------------------------------------------
    // P0-5: ensureSessionForThread tests
    // -----------------------------------------------------------------------
    //
    // The mcode `ensureSessionForThread` decision tree, exercised directly
    // against `ProviderCommandReactor::ensure_session_for_thread`:
    //   (1) no prior active session → Fresh
    //   (2) prior session with matching identity → Reused (no restart)
    //   (3) prior session with a changed model/provider/working-dir →
    //       Restarted (stop old, start new, carry resume cursor)

    /// Build a `SessionContext` for ensure tests, parameterized by thread id
    /// and working dir so tests can vary the identity.
    fn ensure_ctx(thread_id: EntityId, working_dir: &str) -> SessionContext {
        SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: working_dir.to_string(),
            system_prompt: None,
            user_input: "hi".to_string(),
            context_files: vec![],
        }
    }

    /// (1) When no active session exists for the thread, ensure starts a fresh
    /// session and stamps its identity. Exactly one session is tracked
    /// afterwards, and `stop_session` is never called.
    #[tokio::test]
    async fn ensure_session_fresh_when_no_prior_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, stopped, _requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        let outcome = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/proj"),
                Some("gpt-4".to_string()),
                &adapter,
            )
            .await
            .expect("ensure should succeed");

        assert!(
            matches!(outcome, EnsureOutcome::Fresh { .. }),
            "no prior session → Fresh, got {outcome:?}"
        );
        let session_id = outcome.session_id().to_string();
        assert!(!session_id.is_empty());

        // Fresh path → exactly one tracked session; no stops.
        assert_eq!(
            reactor.session_manager().session_count().await,
            1,
            "fresh path must track exactly one session"
        );
        assert!(
            stopped.lock().unwrap().is_empty(),
            "fresh path must NOT stop anything"
        );

        // The freshly started session records the requested identity, so a
        // follow-up call with the same identity reuses it.
        let outcome2 = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/proj"),
                Some("gpt-4".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome2, EnsureOutcome::Reused { .. }),
            "same identity → Reused, got {outcome2:?}"
        );
        assert_eq!(outcome2.session_id(), session_id, "reused session id must match");
    }

    /// (2) When an active session for the thread matches the requested
    /// identity, ensure reuses it without stopping or starting anything.
    #[tokio::test]
    async fn ensure_session_reused_when_identity_unchanged() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, stopped, _requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        // Seed: first ensure starts + stamps identity.
        let first = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/repo"),
                Some("claude-3.5".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        let seeded_id = first.session_id().to_string();
        assert_eq!(
            reactor.session_manager().session_count().await,
            1,
            "seed start tracks one session"
        );

        // Second call with the SAME identity → Reused, no new starts/stops.
        let second = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/repo"),
                Some("claude-3.5".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        assert!(
            matches!(second, EnsureOutcome::Reused { .. }),
            "matching identity → Reused, got {second:?}"
        );
        assert_eq!(second.session_id(), seeded_id);

        // Session count is unchanged — reuse must NOT start a new session.
        assert_eq!(
            reactor.session_manager().session_count().await,
            1,
            "reuse must NOT add a tracked session"
        );
        assert!(
            stopped.lock().unwrap().is_empty(),
            "reuse must NOT call stop_session"
        );
        assert!(
            !second.started_new_session(),
            "Reused outcome must report started_new_session == false"
        );
    }

    /// (3a) When the requested MODEL differs from the prior session's, ensure
    /// restarts: stops the old session and starts a new one.
    #[tokio::test]
    async fn ensure_session_restarts_on_model_change() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, stopped, _requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        // Start with model "v1".
        let first = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/p"),
                Some("model-v1".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        let old_id = first.session_id().to_string();

        // Request model "v2" → identity differs → restart.
        let second = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/p"),
                Some("model-v2".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        let restarted = match &second {
            EnsureOutcome::Restarted {
                old_session_id,
                new_session_id,
                ..
            } => {
                assert_eq!(
                    old_session_id, &old_id,
                    "old session id reported in Restarted must match the prior session"
                );
                assert_ne!(
                    new_session_id, &old_id,
                    "new session id must differ from the restarted (old) one"
                );
                second.clone()
            }
            other => panic!("model change → Restarted, got {other:?}"),
        };

        // The old session was stopped; the manager now tracks two sessions
        // (the stopped old one is still indexed, the new one added).
        let stopped_ids = stopped.lock().unwrap().clone();
        assert!(
            stopped_ids.contains(&old_id),
            "old session must be stopped on restart, got {stopped_ids:?}"
        );
        assert_eq!(
            reactor.session_manager().session_count().await,
            2,
            "restart adds a second tracked session (old kept + new)"
        );
        assert!(
            restarted.started_new_session(),
            "Restarted must report started_new_session == true"
        );
    }

    /// (3b) When the requested WORKING DIR differs from the prior session's,
    /// ensure restarts. Demonstrates provider/runtime-mode change detection
    /// (working dir is a proxy for the runtime context).
    #[tokio::test]
    async fn ensure_session_restarts_on_working_dir_change() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, stopped, _requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        let first = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/dir-a"),
                Some("same-model".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        let old_id = first.session_id().to_string();

        // Different working dir → identity differs → restart.
        let second = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/dir-b"),
                Some("same-model".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        assert!(
            matches!(second, EnsureOutcome::Restarted { .. }),
            "working-dir change → Restarted, got {second:?}"
        );
        assert!(stopped.lock().unwrap().contains(&old_id));
    }

    /// (3c) On restart, the old session's resume cursor is carried over to the
    /// new session. This is the P0-4 → P0-5 contract: a model/provider/working-
    /// dir change does not discard the provider-side conversation position.
    #[tokio::test]
    async fn ensure_session_restart_carries_resume_cursor() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, _stopped, _requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        // Start a session, then stamp a resume cursor on it (as a real adapter
        // would after the provider returns a thread id).
        let first = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/p"),
                Some("model-a".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        let old_id = first.session_id().to_string();
        {
            let session = reactor
                .session_manager()
                .get_session(&old_id)
                .await
                .expect("old session tracked");
            session.set_resume_cursor(Some("provider-thread-42".to_string()));
        }

        // Restart with a different model → cursor must carry to the new session.
        let second = reactor
            .ensure_session_for_thread(
                ensure_ctx(thread_id, "/tmp/p"),
                Some("model-b".to_string()),
                &adapter,
            )
            .await
            .unwrap();
        let restarted = match second {
            EnsureOutcome::Restarted {
                new_session_id,
                resume_cursor,
                ..
            } => {
                assert_eq!(
                    resume_cursor.as_deref(),
                    Some("provider-thread-42"),
                    "Restarted outcome must report the carried cursor"
                );
                new_session_id
            }
            other => panic!("expected Restarted, got {other:?}"),
        };

        // The new session in the manager carries the cursor — so it survives
        // rehydration (P0-4) and a future ensure call won't lose the position.
        let new_session = reactor
            .session_manager()
            .get_session(&restarted)
            .await
            .expect("new session tracked");
        assert_eq!(
            new_session.resume_cursor().as_deref(),
            Some("provider-thread-42"),
            "new session must carry the prior session's resume cursor"
        );
    }

    /// Integration: the production `StartTurn` command path flows through
    /// ensure_session_for_thread. Two StartTurn commands on the SAME thread
    /// (with the reactor's constant default identity) must reuse the session —
    /// proving ensure is wired into handle_start_turn and doesn't churn.
    #[tokio::test]
    async fn start_turn_routes_through_ensure_and_reuses_session() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        let (adapter, stopped, _requests) = make_recorded_test_mock();
        let thread_id = EntityId::new();

        // First StartTurn → starts a session.
        let first = reactor
            .react(
                &Command::StartTurn {
                    thread_id,
                    sequence: 1,
                    user_input: "hello".to_string(),
                },
                &adapter,
                Some(EntityId::new()),
            )
            .await
            .unwrap();
        let first_session = first.session_id.clone().expect("session id");
        assert_eq!(
            reactor.session_manager().session_count().await,
            1,
            "first StartTurn tracks one session"
        );

        // Second StartTurn on the same thread → ensure reuses (same default
        // working dir + provider_id + None model). No new start, no stop.
        let second = reactor
            .react(
                &Command::StartTurn {
                    thread_id,
                    sequence: 2,
                    user_input: "again".to_string(),
                },
                &adapter,
                Some(EntityId::new()),
            )
            .await
            .unwrap();
        assert_eq!(
            second.session_id.as_deref(),
            Some(first_session.as_str()),
            "second StartTurn on same thread must reuse the ensure-managed session"
        );
        assert_eq!(
            reactor.session_manager().session_count().await,
            1,
            "reuse path must not track a second session"
        );
        assert!(
            stopped.lock().unwrap().is_empty(),
            "reuse path must not stop the session"
        );
    }

    // -----------------------------------------------------------------------
    // P3-4: Memory context injection into provider sessions
    // -----------------------------------------------------------------------
    //
    // `ProviderCommandReactor::with_memory` wires a `MemoryProvider` so that
    // freshly started sessions (Fresh + Restarted paths of
    // `ensure_session_for_thread`) have their `system_prompt` augmented with
    // the retrieved context for the thread. The two tests below prove:
    //   (1) a thread with prior history → the new session's prompt contains
    //       the retrieved interaction text, appended to the base prompt.
    //   (2) a thread with NO history → the sentinel is filtered out and the
    //       base prompt is left untouched (no "No prior context available."
    //       leaks into the system prompt).

    /// A minimal in-memory `MemoryProvider` for context-injection tests.
    ///
    /// Returns a fixed context string so the test can assert on exact
    /// substring presence without coupling to SQLite formatting. Implements
    /// `persist_interaction` as a no-op (the injection path only calls
    /// `retrieve_context`).
    struct StubMemory {
        context: String,
    }

    #[async_trait::async_trait]
    impl MemoryProvider for StubMemory {
        async fn retrieve_context(&self, _user_id: &str, _query: &str) -> String {
            self.context.clone()
        }
        async fn persist_interaction(
            &self,
            _user_id: &str,
            _prompt: &str,
            _response: &str,
            _provider: &str,
            _tokens: u32,
        ) -> Result<(), syncode_memory::MemoryProviderError> {
            Ok(())
        }
    }

    /// (1) A freshly started session for a thread WITH prior memory history
    /// has its `system_prompt` augmented with the retrieved context. The base
    /// prompt is preserved and the memory block is appended after a blank
    /// line. This is the P3-4 happy path: retrieved context reaches the
    /// provider session's startup prompt.
    #[tokio::test]
    async fn memory_context_injected_into_fresh_session_prompt() {
        let memory: Arc<dyn MemoryProvider> = Arc::new(StubMemory {
            context: "## Prior Context\n### Interaction 1\nQ: prior question\nA: prior answer"
                .to_string(),
        });
        let reactor =
            ProviderCommandReactor::new(SessionManager::new()).with_memory(memory);
        let (adapter, _stopped, _requests, prompts) = make_prompt_recording_mock();
        let thread_id = EntityId::new();

        let ctx = SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: Some("You are a helpful AI coding assistant.".to_string()),
            user_input: "next turn".to_string(),
            context_files: vec![],
        };

        let outcome = reactor
            .ensure_session_for_thread(ctx, Some("claude".to_string()), &adapter)
            .await
            .expect("ensure should succeed");

        assert!(
            matches!(outcome, EnsureOutcome::Fresh { .. }),
            "no prior session → Fresh, got {outcome:?}"
        );

        // Exactly one start → exactly one recorded prompt.
        let recorded = prompts.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1, "expected one recorded system prompt");
        let prompt = recorded[0].as_ref().expect("prompt should be Some");

        // Base prompt preserved...
        assert!(
            prompt.contains("You are a helpful AI coding assistant."),
            "base system prompt must be preserved; got: {prompt}"
        );
        // ...and memory context appended.
        assert!(
            prompt.contains("## Prior Context"),
            "retrieved memory context must be injected; got: {prompt}"
        );
        assert!(
            prompt.contains("prior question"),
            "retrieved interaction text must be present; got: {prompt}"
        );
        // The memory block follows the base prompt (augmentation, not
        // replacement): the base prompt text appears before the context header.
        let base_pos = prompt.find("helpful AI").expect("base prompt present");
        let ctx_pos = prompt.find("## Prior Context").expect("context present");
        assert!(
            base_pos < ctx_pos,
            "base prompt must precede the injected context"
        );
    }

    /// (2) A freshly started session for a thread with NO prior history leaves
    /// the base prompt untouched — the `NO_PRIOR_CONTEXT` sentinel is filtered
    /// out so it never leaks into the provider's system prompt. Also covers
    /// the Restarted path: an identity change re-runs augmentation.
    #[tokio::test]
    async fn memory_sentinel_filtered_and_restarted_path_re_injects() {
        // Memory returns the no-history sentinel, simulating a fresh thread.
        let memory: Arc<dyn MemoryProvider> = Arc::new(StubMemory {
            context: NO_PRIOR_CONTEXT.to_string(),
        });
        let reactor =
            ProviderCommandReactor::new(SessionManager::new()).with_memory(memory);
        let (adapter, _stopped, _requests, prompts) = make_prompt_recording_mock();
        let thread_id = EntityId::new();

        // First ensure — Fresh path. Sentinel is filtered → base prompt intact.
        let ctx = SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: Some("base-instructions".to_string()),
            user_input: "first turn".to_string(),
            context_files: vec![],
        };
        let first = reactor
            .ensure_session_for_thread(ctx, Some("model-a".to_string()), &adapter)
            .await
            .expect("first ensure");
        assert!(matches!(first, EnsureOutcome::Fresh { .. }));

        let recorded = prompts.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        let prompt = recorded[0].as_ref().expect("prompt Some");
        assert_eq!(
            prompt, "base-instructions",
            "sentinel must be filtered — base prompt unchanged; got: {prompt}"
        );
        assert!(
            !prompt.contains(NO_PRIOR_CONTEXT),
            "sentinel string must NOT leak into the prompt; got: {prompt}"
        );

        // Now restart with a changed model — the Restarted path re-runs
        // augmentation. Replace the stub memory with one that returns real
        // context to prove augmentation fires on restart too.
        let reactor2 = ProviderCommandReactor::new(SessionManager::new())
            .with_memory(Arc::new(StubMemory {
                context: "## Prior Context\nQ: earlier\nA: earlier-answer".to_string(),
            })
                as Arc<dyn MemoryProvider>);
        let (adapter2, _stopped2, _requests2, prompts2) = make_prompt_recording_mock();

        // Seed a session first so the second call takes the Restarted path.
        let seed_ctx = SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: None,
            user_input: "seed".to_string(),
            context_files: vec![],
        };
        reactor2
            .ensure_session_for_thread(seed_ctx, Some("model-a".to_string()), &adapter2)
            .await
            .unwrap();

        // Changed model → Restarted. Memory augmentation should fire and seed
        // the prompt (which was `None`) directly from retrieved context.
        let restart_ctx = SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: None,
            user_input: "restart turn".to_string(),
            context_files: vec![],
        };
        let restarted = reactor2
            .ensure_session_for_thread(restart_ctx, Some("model-b".to_string()), &adapter2)
            .await
            .expect("restart ensure");
        assert!(
            matches!(restarted, EnsureOutcome::Restarted { .. }),
            "model change → Restarted, got {restarted:?}"
        );

        let recorded2 = prompts2.lock().unwrap().clone();
        // Two starts: seed (Fresh, no memory since prompt None + sentinel-less
        // stub returns real context here) + restart. The restart's prompt is
        // the last entry.
        assert!(
            recorded2.len() >= 2,
            "expected at least two recorded prompts (seed + restart)"
        );
        let restart_prompt = recorded2
            .last()
            .expect("restart prompt present")
            .as_ref()
            .expect("restart prompt Some");
        assert!(
            restart_prompt.contains("## Prior Context"),
            "Restarted path must re-inject memory context; got: {restart_prompt}"
        );
        assert!(
            restart_prompt.contains("earlier-answer"),
            "retrieved context must be present on restart; got: {restart_prompt}"
        );
    }

    /// (3) When no memory provider is attached, the reactor behaves exactly as
    /// before: the system prompt passes through to the adapter unchanged.
    /// Guards the backward-compatibility contract of `new()` (no memory) —
    /// existing construction sites must see no behavioural change.
    #[tokio::test]
    async fn no_memory_attached_leaves_prompt_untouched() {
        let reactor = ProviderCommandReactor::new(SessionManager::new());
        assert!(
            reactor.memory().is_none(),
            "new() must not attach a memory provider"
        );
        let (adapter, _stopped, _requests, prompts) = make_prompt_recording_mock();
        let thread_id = EntityId::new();

        let ctx = SessionContext {
            thread_id,
            turn_id: EntityId::new(),
            working_dir: "/tmp/proj".to_string(),
            system_prompt: Some("passthrough-prompt".to_string()),
            user_input: "turn".to_string(),
            context_files: vec![],
        };
        reactor
            .ensure_session_for_thread(ctx, None, &adapter)
            .await
            .unwrap();

        let recorded = prompts.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].as_deref(),
            Some("passthrough-prompt"),
            "no memory → prompt must pass through unchanged"
        );
    }
}

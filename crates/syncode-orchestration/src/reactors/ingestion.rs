//! Provider Runtime Ingestion Reactor
//!
//! Translates provider-level events (`ProviderEvent`) into domain events
//! (`DomainEvent`) that flow through the CQRS pipeline.
//!
//! This is the "read side" of the provider bridge:
//! - Provider emits Token → we produce a `MessageDeltaAppended` (streamed delta)
//! - Provider emits Completed → we produce TurnCompleted
//! - Provider emits ToolCall → we produce ActivityLogged
//! - Provider emits Error → we produce TurnFailed

use syncode_core::{EntityId, Timestamp};
use syncode_provider::ProviderEvent;

use crate::events::DomainEvent;
use crate::reactors::command::{CommandReactorError, ProviderCommandReactor};

/// Result of ingesting a provider event
#[derive(Debug, Clone)]
pub struct IngestionResult {
    /// Domain events produced from this provider event (may be empty)
    pub events: Vec<DomainEvent>,
    /// Whether the provider event was consumed
    pub consumed: bool,
}

/// Translates provider events into domain events.
///
/// Rules:
/// - `ProviderEvent::Started` → no domain event (session is internal)
/// - `ProviderEvent::Token` → `DomainEvent::MessageDeltaAppended` (a streamed
///   assistant-message delta). The turn id doubles as the streamed assistant
///   message id: `MessageDeltaAppended` is upsert-style on the message id, so
///   the first delta creates the message and subsequent deltas append to it.
///   The consumer in `pipeline` batches tokens (100ms window) before calling
///   this, so one event covers many tokens.
/// - `ProviderEvent::ToolCall` → `DomainEvent::ActivityLogged`
/// - `ProviderEvent::ToolResult` → `DomainEvent::ActivityLogged`
/// - `ProviderEvent::Completed` → `DomainEvent::TurnCompleted`
/// - `ProviderEvent::Error` → `DomainEvent::TurnFailed`
/// - `ProviderEvent::StatusChanged` → no domain event (infrastructure)
///
/// `thread_id` scopes any emitted `ActivityLogged` (ToolCall/ToolResult) to the
/// turn's owning thread; pass `None` when the thread can't be resolved.
///
/// `started_at` is the turn's wall-clock start (captured when its provider
/// stream begins). When `Some`, `TurnCompleted.duration_ms` is real elapsed
/// time; otherwise it falls back to the `total_tokens * 10` heuristic.
pub fn ingest_provider_event(
    event: ProviderEvent,
    turn_id: EntityId,
    thread_id: Option<EntityId>,
    started_at: Option<Timestamp>,
) -> IngestionResult {
    let now = Timestamp::now();

    match event {
        ProviderEvent::Started { .. } => IngestionResult {
            events: vec![],
            consumed: true,
        },

        // A token chunk becomes a streamed assistant-message delta. The turn id
        // is the message id (MessageDeltaAppended is id-upsertible), so the
        // first Token creates the message and later Tokens append to it. The
        // consumer batches many tokens into one delta to avoid WS flooding.
        ProviderEvent::Token { content, .. } => IngestionResult {
            events: vec![DomainEvent::MessageDeltaAppended {
                id: turn_id,
                turn_id,
                delta: content,
                created_at: now,
            }],
            consumed: true,
        },

        ProviderEvent::ToolCall {
            tool_name,
            tool_input,
            ..
        } => {
            let description = format!(
                "Provider tool call: {} {}",
                tool_name,
                truncate_json(&tool_input, 200)
            );
            IngestionResult {
                events: vec![DomainEvent::ActivityLogged {
                    id: EntityId::new(),
                    activity_type: "provider_tool_call".to_string(),
                    description,
                    // Scope to the turn's owning thread (resolved by the caller).
                    thread_id,
                    created_at: now,
                }],
                consumed: true,
            }
        }

        ProviderEvent::ToolResult {
            tool_name, result, ..
        } => {
            let description = format!(
                "Provider tool result: {} {}",
                tool_name,
                truncate_json(&result, 200)
            );
            IngestionResult {
                events: vec![DomainEvent::ActivityLogged {
                    id: EntityId::new(),
                    activity_type: "provider_tool_result".to_string(),
                    description,
                    thread_id,
                    created_at: now,
                }],
                consumed: true,
            }
        }

        ProviderEvent::Completed { output, usage, .. } => {
            // Prefer real wall-clock duration (stream start -> now) when a start
            // timestamp is available; fall back to the token-count heuristic for
            // the synchronous batch path (react() events carry no stream start).
            let duration_ms = match started_at {
                Some(start) => (now.to_millis() - start.to_millis()).max(0) as u64,
                None => usage
                    .as_ref()
                    .map(|u| (u.total_tokens as u64) * 10)
                    .unwrap_or(0),
            };
            IngestionResult {
                events: vec![DomainEvent::TurnCompleted {
                    id: turn_id,
                    assistant_output: output,
                    duration_ms,
                    completed_at: now,
                }],
                consumed: true,
            }
        }

        ProviderEvent::Error { message, code, .. } => IngestionResult {
            events: vec![DomainEvent::TurnFailed {
                id: turn_id,
                error: format!("[{}] {}", code.unwrap_or(0), message),
                completed_at: now,
            }],
            consumed: true,
        },

        ProviderEvent::StatusChanged { .. } => IngestionResult {
            events: vec![],
            consumed: true,
        },
    }
}

/// Truncate a JSON value for logging/metadata
fn truncate_json(value: &serde_json::Value, max_len: usize) -> String {
    let s = serde_json::to_string(value).unwrap_or_default();
    if s.len() <= max_len {
        s
    } else {
        format!("{}...", &s[..max_len])
    }
}

// ---------------------------------------------------------------------------
// P0-7: Queued-turn pipeline drain
// ---------------------------------------------------------------------------

/// Drain and dispatch the next queued turn for `thread_id` after a turn on
/// that thread completed.
///
/// This is the drain half of the queued-turn pipeline (P0-7). The enqueue
/// half lives in the `DispatchQueuedTurn` arm of
/// [`ProviderCommandReactor::react`]: when a turn arrives while the thread
/// already has an active `Processing` session (and the provider can't steer),
/// it is parked in the reactor's per-thread [`crate::reactors::command::TurnQueue`]
/// rather than dispatched. Once the in-flight turn completes, the pipeline
/// calls this to release the next parked turn — guaranteeing no two turns for
/// the same thread run simultaneously.
///
/// Call this from the ingestion path whenever a `TurnCompleted` (or
/// `TurnFailed`/`TurnCancelled`) event is observed for a thread. Returns:
/// - `Ok(Some(session_id))` when a queued turn was drained and dispatched,
/// - `Ok(None)` when the thread had no queued turn (the common case),
/// - `Err(_)` when a queued turn was dequeued but the dispatch failed. The
///   turn is NOT re-queued on failure (the caller may retry by re-issuing
///   `DispatchQueuedTurn`).
///
/// `thread_id` is the owning thread of the completed turn (the same value the
/// ingestion reactor resolves for activity-scoping). The completed `turn_id`
/// is accepted for logging correlation only — the queue is keyed by thread.
pub async fn dispatch_queued_turn_after_completion(
    reactor: &ProviderCommandReactor,
    thread_id: EntityId,
    completed_turn_id: EntityId,
    adapter: &syncode_provider::registry::SharedAdapter,
) -> Result<Option<String>, CommandReactorError> {
    // Fast path: nothing queued for this thread → no allocation, no dispatch.
    if !reactor.turn_queue().has_queued(&thread_id.as_str()).await {
        return Ok(None);
    }
    crate::log::info(&format!(
        "draining queued turn after turn completion (thread_id = {}, completed_turn_id = {})",
        thread_id.as_str(),
        completed_turn_id.as_str()
    ));
    reactor.dispatch_next_queued_turn(thread_id, adapter).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_provider::UsageInfo;

    fn make_turn_id() -> EntityId {
        EntityId::new()
    }

    #[test]
    fn ingest_started_produces_no_events() {
        let event = ProviderEvent::Started {
            session_id: "s1".to_string(),
        };
        let result = ingest_provider_event(event, make_turn_id(), None, None);
        assert!(result.events.is_empty());
        assert!(result.consumed);
    }

    #[test]
    fn ingest_token_produces_message_delta() {
        // Tokens are no longer silently consumed — each Token becomes a
        // MessageDeltaAppended carrying the token text, keyed by the turn id
        // (the streamed assistant message id). The consumer batches tokens
        // before calling this, so one event typically covers many tokens.
        let turn_id = make_turn_id();
        let event = ProviderEvent::Token {
            session_id: "s1".to_string(),
            content: "hello".to_string(),
        };
        let result = ingest_provider_event(event, turn_id, None, None);
        assert_eq!(result.events.len(), 1);
        match &result.events[0] {
            DomainEvent::MessageDeltaAppended {
                id,
                turn_id: tid,
                delta,
                ..
            } => {
                assert_eq!(*id, turn_id, "message id is the turn id");
                assert_eq!(*tid, turn_id);
                assert_eq!(delta, "hello");
            }
            other => panic!("expected MessageDeltaAppended, got {other:?}"),
        }
    }

    #[test]
    fn ingest_token_appends_to_same_message_id() {
        // Two consecutive Token events for the same turn reuse the turn id as
        // the streamed message id, so the projector appends rather than creating
        // two messages.
        let turn_id = make_turn_id();
        let first = ingest_provider_event(
            ProviderEvent::Token {
                session_id: "s1".into(),
                content: "foo ".into(),
            },
            turn_id,
            None,
            None,
        );
        let second = ingest_provider_event(
            ProviderEvent::Token {
                session_id: "s1".into(),
                content: "bar".into(),
            },
            turn_id,
            None,
            None,
        );
        let id_of = |ev: &DomainEvent| match ev {
            DomainEvent::MessageDeltaAppended { id, .. } => *id,
            _ => panic!("expected MessageDeltaAppended"),
        };
        assert_eq!(id_of(&first.events[0]), id_of(&second.events[0]));
    }

    #[test]
    fn ingest_tool_call_produces_activity() {
        let event = ProviderEvent::ToolCall {
            session_id: "s1".to_string(),
            tool_name: "read_file".to_string(),
            tool_input: serde_json::json!({"path": "/tmp/main.rs"}),
        };
        let result = ingest_provider_event(event, make_turn_id(), None, None);
        assert_eq!(result.events.len(), 1);
        match &result.events[0] {
            DomainEvent::ActivityLogged { activity_type, .. } => {
                assert_eq!(activity_type, "provider_tool_call");
            }
            _ => panic!("Expected ActivityLogged"),
        }
    }

    #[test]
    fn ingest_tool_result_produces_activity() {
        let event = ProviderEvent::ToolResult {
            session_id: "s1".to_string(),
            tool_name: "bash".to_string(),
            result: serde_json::json!({"exit_code": 0}),
        };
        let result = ingest_provider_event(event, make_turn_id(), None, None);
        assert_eq!(result.events.len(), 1);
        match &result.events[0] {
            DomainEvent::ActivityLogged { activity_type, .. } => {
                assert_eq!(activity_type, "provider_tool_result");
            }
            _ => panic!("Expected ActivityLogged"),
        }
    }

    #[test]
    fn ingest_completed_produces_turn_completed() {
        let turn_id = make_turn_id();
        let event = ProviderEvent::Completed {
            session_id: "s1".to_string(),
            output: "Here is the fix.".to_string(),
            usage: Some(UsageInfo {
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
            }),
        };
        let result = ingest_provider_event(event, turn_id, None, None);
        assert_eq!(result.events.len(), 1);
        match &result.events[0] {
            DomainEvent::TurnCompleted {
                id,
                assistant_output,
                duration_ms,
                ..
            } => {
                assert_eq!(*id, turn_id);
                assert_eq!(assistant_output, "Here is the fix.");
                assert_eq!(*duration_ms, 1500); // total_tokens * 10
            }
            _ => panic!("Expected TurnCompleted"),
        }
    }

    #[test]
    fn ingest_completed_without_usage() {
        let turn_id = make_turn_id();
        let event = ProviderEvent::Completed {
            session_id: "s1".to_string(),
            output: "response".to_string(),
            usage: None,
        };
        let result = ingest_provider_event(event, turn_id, None, None);
        assert_eq!(result.events.len(), 1);
        match &result.events[0] {
            DomainEvent::TurnCompleted { duration_ms, .. } => {
                assert_eq!(*duration_ms, 0);
            }
            _ => panic!("Expected TurnCompleted"),
        }
    }

    #[test]
    fn ingest_error_produces_turn_failed() {
        let turn_id = make_turn_id();
        let event = ProviderEvent::Error {
            session_id: "s1".to_string(),
            message: "Rate limit exceeded".to_string(),
            code: Some(429),
        };
        let result = ingest_provider_event(event, turn_id, None, None);
        assert_eq!(result.events.len(), 1);
        match &result.events[0] {
            DomainEvent::TurnFailed { id, error, .. } => {
                assert_eq!(*id, turn_id);
                assert!(error.contains("429"));
                assert!(error.contains("Rate limit"));
            }
            _ => panic!("Expected TurnFailed"),
        }
    }

    #[test]
    fn ingest_status_changed_produces_nothing() {
        let event = ProviderEvent::StatusChanged {
            status: syncode_provider::ProviderStatus::Busy,
        };
        let result = ingest_provider_event(event, make_turn_id(), None, None);
        assert!(result.events.is_empty());
    }

    #[test]
    fn truncate_json_short() {
        let val = serde_json::json!("short");
        let result = truncate_json(&val, 100);
        assert_eq!(result, "\"short\"");
    }

    #[test]
    fn truncate_json_long() {
        let val = serde_json::json!({"key": "x".repeat(300)});
        let result = truncate_json(&val, 50);
        assert!(result.ends_with("..."));
        assert!(result.len() <= 53);
    }

    #[test]
    fn ingest_completed_with_started_at_uses_wall_clock() {
        // A start timestamp ~2s ago yields a wall-clock duration (~2000ms),
        // NOT the total_tokens*10 heuristic (150 tokens -> 1500ms).
        let turn_id = make_turn_id();
        let started_at = Timestamp(chrono::Utc::now() - chrono::Duration::milliseconds(2000));
        let event = ProviderEvent::Completed {
            session_id: "s1".to_string(),
            output: "done".to_string(),
            usage: Some(UsageInfo {
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
            }),
        };
        let result = ingest_provider_event(event, turn_id, None, Some(started_at));
        match &result.events[0] {
            DomainEvent::TurnCompleted { duration_ms, .. } => {
                assert!(
                    (1500_u64..4000_u64).contains(duration_ms),
                    "expected wall-clock duration ~2000ms, got {duration_ms}"
                );
            }
            _ => panic!("Expected TurnCompleted"),
        }
    }

    #[test]
    fn ingest_completed_without_started_at_keeps_token_heuristic() {
        // No start timestamp -> the total_tokens*10 heuristic is preserved.
        let turn_id = make_turn_id();
        let event = ProviderEvent::Completed {
            session_id: "s1".to_string(),
            output: "done".to_string(),
            usage: Some(UsageInfo {
                input_tokens: 100,
                output_tokens: 50,
                total_tokens: 150,
            }),
        };
        let result = ingest_provider_event(event, turn_id, None, None);
        match &result.events[0] {
            DomainEvent::TurnCompleted { duration_ms, .. } => {
                assert_eq!(*duration_ms, 1500);
            }
            _ => panic!("Expected TurnCompleted"),
        }
    }
}

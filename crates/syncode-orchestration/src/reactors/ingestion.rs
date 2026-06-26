//! Provider Runtime Ingestion Reactor
//!
//! Translates provider-level events (`ProviderEvent`) into domain events
//! (`DomainEvent`) that flow through the CQRS pipeline.
//!
//! This is the "read side" of the provider bridge:
//! - Provider emits Token → we produce no domain event (internal)
//! - Provider emits Completed → we produce TurnCompleted
//! - Provider emits ToolCall → we produce ActivityLogged
//! - Provider emits Error → we produce TurnFailed

use syncode_core::{EntityId, Timestamp};
use syncode_provider::ProviderEvent;

use crate::events::DomainEvent;

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
/// - `ProviderEvent::Token` → no domain event (streaming, aggregated at completion)
/// - `ProviderEvent::ToolCall` → `DomainEvent::ActivityLogged`
/// - `ProviderEvent::ToolResult` → `DomainEvent::ActivityLogged`
/// - `ProviderEvent::Completed` → `DomainEvent::TurnCompleted`
/// - `ProviderEvent::Error` → `DomainEvent::TurnFailed`
/// - `ProviderEvent::StatusChanged` → no domain event (infrastructure)
pub fn ingest_provider_event(
    event: ProviderEvent,
    turn_id: EntityId,
) -> IngestionResult {
    let now = Timestamp::now();

    match event {
        ProviderEvent::Started { .. } => IngestionResult {
            events: vec![],
            consumed: true,
        },

        ProviderEvent::Token { .. } => IngestionResult {
            events: vec![],
            consumed: true,
        },

        ProviderEvent::ToolCall {
            tool_name,
            tool_input,
            ..
        } => {
            let description = format!("Provider tool call: {} {}", tool_name, truncate_json(&tool_input, 200));
            IngestionResult {
                events: vec![DomainEvent::ActivityLogged {
                    id: EntityId::new(),
                    activity_type: "provider_tool_call".to_string(),
                    description,
                    created_at: now,
                }],
                consumed: true,
            }
        }

        ProviderEvent::ToolResult {
            tool_name,
            result,
            ..
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
                    created_at: now,
                }],
                consumed: true,
            }
        }

        ProviderEvent::Completed { output, usage, .. } => {
            let duration_ms = usage.as_ref().map(|u| (u.total_tokens as u64) * 10).unwrap_or(0);
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
        let event = ProviderEvent::Started { session_id: "s1".to_string() };
        let result = ingest_provider_event(event, make_turn_id());
        assert!(result.events.is_empty());
        assert!(result.consumed);
    }

    #[test]
    fn ingest_token_produces_no_events() {
        let event = ProviderEvent::Token {
            session_id: "s1".to_string(),
            content: "hello".to_string(),
        };
        let result = ingest_provider_event(event, make_turn_id());
        assert!(result.events.is_empty());
    }

    #[test]
    fn ingest_tool_call_produces_activity() {
        let event = ProviderEvent::ToolCall {
            session_id: "s1".to_string(),
            tool_name: "read_file".to_string(),
            tool_input: serde_json::json!({"path": "/tmp/main.rs"}),
        };
        let result = ingest_provider_event(event, make_turn_id());
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
        let result = ingest_provider_event(event, make_turn_id());
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
        let result = ingest_provider_event(event, turn_id);
        assert_eq!(result.events.len(), 1);
        match &result.events[0] {
            DomainEvent::TurnCompleted { id, assistant_output, duration_ms, .. } => {
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
        let result = ingest_provider_event(event, turn_id);
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
        let result = ingest_provider_event(event, turn_id);
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
        let result = ingest_provider_event(event, make_turn_id());
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
}

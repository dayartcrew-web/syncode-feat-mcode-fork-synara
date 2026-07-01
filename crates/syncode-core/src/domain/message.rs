//! Message — individual messages within a conversation

use crate::domain::primitives::{EntityId, Timestamp};
use serde::{Deserialize, Serialize};

/// Message role
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    System,
    Tool,
}

/// Content type for message body
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ContentType {
    Text,
    Json,
    Code,
    ImageUrl,
}

/// A message is the atomic unit of conversation content.
/// Messages are immutable once created.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: EntityId,
    /// Parent turn ID
    pub turn_id: EntityId,
    pub role: MessageRole,
    pub content: String,
    pub content_type: ContentType,
    /// Token count (if available)
    pub token_count: Option<u32>,
    /// For tool messages: the tool name
    pub tool_name: Option<String>,
    /// For tool messages: the tool call ID
    pub tool_call_id: Option<String>,
    pub created_at: Timestamp,
}

impl Message {
    pub fn user_message(turn_id: EntityId, content: impl Into<String>) -> Self {
        Self {
            id: EntityId::new(),
            turn_id,
            role: MessageRole::User,
            content: content.into(),
            content_type: ContentType::Text,
            token_count: None,
            tool_name: None,
            tool_call_id: None,
            created_at: Timestamp::now(),
        }
    }

    pub fn assistant_message(turn_id: EntityId, content: impl Into<String>) -> Self {
        Self {
            id: EntityId::new(),
            turn_id,
            role: MessageRole::Assistant,
            content: content.into(),
            content_type: ContentType::Text,
            token_count: None,
            tool_name: None,
            tool_call_id: None,
            created_at: Timestamp::now(),
        }
    }

    pub fn system_message(turn_id: EntityId, content: impl Into<String>) -> Self {
        Self {
            id: EntityId::new(),
            turn_id,
            role: MessageRole::System,
            content: content.into(),
            content_type: ContentType::Text,
            token_count: None,
            tool_name: None,
            tool_call_id: None,
            created_at: Timestamp::now(),
        }
    }

    pub fn tool_message(
        turn_id: EntityId,
        content: impl Into<String>,
        tool_name: impl Into<String>,
        tool_call_id: impl Into<String>,
    ) -> Self {
        Self {
            id: EntityId::new(),
            turn_id,
            role: MessageRole::Tool,
            content: content.into(),
            content_type: ContentType::Json,
            token_count: None,
            tool_name: Some(tool_name.into()),
            tool_call_id: Some(tool_call_id.into()),
            created_at: Timestamp::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_message_defaults() {
        let tid = EntityId::new();
        let m = Message::user_message(tid, "Hello");
        assert_eq!(m.role, MessageRole::User);
        assert_eq!(m.content, "Hello");
        assert_eq!(m.content_type, ContentType::Text);
        assert!(m.tool_name.is_none());
    }

    #[test]
    fn assistant_message_defaults() {
        let tid = EntityId::new();
        let m = Message::assistant_message(tid, "Hi there!");
        assert_eq!(m.role, MessageRole::Assistant);
        assert_eq!(m.content, "Hi there!");
    }

    #[test]
    fn system_message_defaults() {
        let tid = EntityId::new();
        let m = Message::system_message(tid, "You are a helpful assistant.");
        assert_eq!(m.role, MessageRole::System);
    }

    #[test]
    fn tool_message_with_fields() {
        let tid = EntityId::new();
        let m = Message::tool_message(tid, r#"{"result": "ok"}"#, "bash", "call_123");
        assert_eq!(m.role, MessageRole::Tool);
        assert_eq!(m.content_type, ContentType::Json);
        assert_eq!(m.tool_name.as_deref(), Some("bash"));
        assert_eq!(m.tool_call_id.as_deref(), Some("call_123"));
    }

    #[test]
    fn message_serialization_roundtrip() {
        let tid = EntityId::new();
        let m = Message::user_message(tid, "Test content");
        let json = serde_json::to_string(&m).expect("serialize");
        let back: Message = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.role, m.role);
        assert_eq!(back.content, m.content);
        assert_eq!(back.turn_id, m.turn_id);
    }
}

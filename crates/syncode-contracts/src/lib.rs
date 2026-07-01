//! Syncode Contracts — Shared type definitions for Rust ↔ TypeScript bridge
//!
//! Types annotated with `#[derive(TS)]` generate `.d.ts` files when running:
//!   TS_RS_EXPORT_DIR=../../frontend/src/types cargo test -p syncode-contracts -- test_generate_ts_types

use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub mod snapshots;

// ─── Primitives ────────────────────────────────────────────────────────

/// Unique entity identifier (UUID string in JSON)
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(export, type = "string")]
pub struct EntityId(pub String);

impl EntityId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for EntityId {
    fn default() -> Self {
        Self::new()
    }
}

/// ISO 8601 timestamp string
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(export, type = "string")]
pub struct Timestamp(pub String);

impl Timestamp {
    pub fn now() -> Self {
        Self(chrono::Utc::now().to_rfc3339())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ─── Provider Types ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ProviderConfig {
    pub id: String,
    pub api_key: String,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct ProviderCapabilities {
    pub chat: bool,
    pub edit: bool,
    pub vision: bool,
    pub function_calling: bool,
    pub streaming: bool,
}

// ─── Session Types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct CreateSessionRequest {
    pub provider_id: String,
    pub model: String,
    pub working_directory: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct SessionView {
    pub id: EntityId,
    pub provider_id: String,
    pub model: String,
    pub working_directory: Option<String>,
    pub created_at: Timestamp,
    pub status: SessionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum SessionStatus {
    Idle,
    Running,
    Paused,
    Error,
}

// ─── Message Types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct MessageView {
    pub id: EntityId,
    pub role: MessageRole,
    pub content: String,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

// ─── Git Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct GitFileStatusView {
    pub path: String,
    pub index_status: FileStatusKind,
    pub working_tree_status: FileStatusKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum FileStatusKind {
    Unmodified,
    Modified,
    Added,
    Deleted,
    Renamed,
    Copied,
    Untracked,
    Ignored,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct GitStatusView {
    pub branch: Option<String>,
    pub head_detached: bool,
    pub files: Vec<GitFileStatusView>,
    pub ahead: u32,
    pub behind: u32,
}

// ─── JSON-RPC Types ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct JsonRpcRequestView {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub id: Option<String>,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "Record<string, unknown>")]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct JsonRpcResponseView {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "Record<string, unknown>")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub error: Option<JsonRpcErrorView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct JsonRpcErrorView {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "Record<string, unknown>")]
    pub data: Option<serde_json::Value>,
}

// ─── WebSocket Push Types ─────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, rename_all = "camelCase")]
pub struct PushEvent {
    pub channel: String,
    pub event_type: String,
    #[ts(type = "Record<string, unknown>")]
    pub data: serde_json::Value,
    pub timestamp: Timestamp,
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_id() {
        let id = EntityId::new();
        assert!(!id.as_str().is_empty());
        let json = serde_json::to_string(&id).unwrap();
        assert!(!json.starts_with('{'));
    }

    #[test]
    fn test_timestamp() {
        let ts = Timestamp::now();
        assert!(chrono::DateTime::parse_from_rfc3339(ts.as_str()).is_ok());
    }

    #[test]
    fn test_provider_config_roundtrip() {
        let config = ProviderConfig {
            id: "claude".into(),
            api_key: "sk-xxx".into(),
            base_url: Some("https://api.anthropic.com".into()),
            model: Some("claude-sonnet-4".into()),
            max_tokens: Some(8192),
            temperature: Some(0.7),
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: ProviderConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, "claude");
    }

    #[test]
    fn test_session_view_roundtrip() {
        let session = SessionView {
            id: EntityId::new(),
            provider_id: "claude".into(),
            model: "claude-sonnet-4".into(),
            working_directory: Some("/tmp/project".into()),
            created_at: Timestamp::now(),
            status: SessionStatus::Idle,
        };
        let json = serde_json::to_string(&session).unwrap();
        let decoded: SessionView = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.provider_id, "claude");
    }

    /// Wire-parity guard: the contracts surface must serialize **camelCase**
    /// field names so the JSON wire and the ts-rs-generated TS types agree
    /// (the latter is `#[ts(rename_all = "camelCase")]`). If this test fails,
    /// serde + ts-rs casing drifted apart on a core DTO.
    #[test]
    fn core_dtos_serialize_camel_case() {
        // SessionView: provider_id, working_directory, created_at
        let json = serde_json::to_string(&SessionView {
            id: EntityId::new(),
            provider_id: "claude".into(),
            model: "m".into(),
            working_directory: None,
            created_at: Timestamp("t".into()),
            status: SessionStatus::Idle,
        })
        .unwrap();
        assert!(
            json.contains("\"providerId\""),
            "SessionView camelCase: {json}"
        );
        assert!(json.contains("\"workingDirectory\""));
        assert!(json.contains("\"createdAt\""));
        assert!(!json.contains("\"provider_id\""), "snake leaked: {json}");

        // ProviderConfig: api_key, base_url, max_tokens
        let json = serde_json::to_string(&ProviderConfig {
            id: "claude".into(),
            api_key: "k".into(),
            base_url: None,
            model: None,
            max_tokens: None,
            temperature: None,
        })
        .unwrap();
        assert!(json.contains("\"apiKey\""), "ProviderConfig camelCase: {json}");
        assert!(json.contains("\"baseUrl\""));
        assert!(json.contains("\"maxTokens\""));

        // GitStatusView: head_detached
        let json = serde_json::to_string(&GitStatusView {
            branch: None,
            head_detached: false,
            files: vec![],
            ahead: 0,
            behind: 0,
        })
        .unwrap();
        assert!(json.contains("\"headDetached\""), "GitStatusView camelCase: {json}");

        // GitFileStatusView: index_status, working_tree_status
        let json = serde_json::to_string(&GitFileStatusView {
            path: "p".into(),
            index_status: FileStatusKind::Unmodified,
            working_tree_status: FileStatusKind::Unmodified,
        })
        .unwrap();
        assert!(json.contains("\"indexStatus\""), "GitFileStatusView camelCase: {json}");
        assert!(json.contains("\"workingTreeStatus\""));

        // CreateSessionRequest: provider_id, working_directory
        let json = serde_json::to_string(&CreateSessionRequest {
            provider_id: "p".into(),
            model: "m".into(),
            working_directory: None,
        })
        .unwrap();
        assert!(json.contains("\"providerId\""), "CreateSessionRequest camelCase: {json}");

        // PushEvent: event_type
        let json = serde_json::to_string(&PushEvent {
            channel: "c".into(),
            event_type: "t".into(),
            data: serde_json::Value::Null,
            timestamp: Timestamp("t".into()),
        })
        .unwrap();
        assert!(json.contains("\"eventType\""), "PushEvent camelCase: {json}");

        // JsonRpcRequestView: no snake fields, but check jsonrpc field stays
        // (it's a single word, no rename needed — sanity only).
        let json = serde_json::to_string(&JsonRpcRequestView {
            jsonrpc: "2.0".into(),
            id: None,
            method: "m".into(),
            params: None,
        })
        .unwrap();
        assert!(json.contains("\"jsonrpc\""), "JsonRpcRequestView: {json}");
    }

    /// Generate all TypeScript definitions to frontend/src/types/
    /// Run: TS_RS_EXPORT_DIR=../../frontend/src/types cargo test -p syncode-contracts -- test_generate_ts_types
    #[test]
    fn test_generate_ts_types() {
        EntityId::export().expect("export EntityId");
        Timestamp::export().expect("export Timestamp");
        ProviderConfig::export().expect("export ProviderConfig");
        ProviderCapabilities::export().expect("export ProviderCapabilities");
        CreateSessionRequest::export().expect("export CreateSessionRequest");
        SessionView::export().expect("export SessionView");
        SessionStatus::export().expect("export SessionStatus");
        MessageView::export().expect("export MessageView");
        MessageRole::export().expect("export MessageRole");
        GitFileStatusView::export().expect("export GitFileStatusView");
        FileStatusKind::export().expect("export FileStatusKind");
        GitStatusView::export().expect("export GitStatusView");
        JsonRpcRequestView::export().expect("export JsonRpcRequestView");
        JsonRpcResponseView::export().expect("export JsonRpcResponseView");
        JsonRpcErrorView::export().expect("export JsonRpcErrorView");
        PushEvent::export().expect("export PushEvent");

        // Snapshot DTOs (snapshot-then-stream subscriptions).
        snapshots::ProjectSummary::export().expect("export ProjectSummary");
        snapshots::ThreadSummary::export().expect("export ThreadSummary");
        snapshots::TurnSummary::export().expect("export TurnSummary");
        snapshots::MessageSummary::export().expect("export MessageSummary");
        snapshots::ActivitySummary::export().expect("export ActivitySummary");
        snapshots::SnapshotScope::export().expect("export SnapshotScope");
        snapshots::ShellSnapshot::export().expect("export ShellSnapshot");
        snapshots::ThreadDetailSnapshot::export().expect("export ThreadDetailSnapshot");
        snapshots::FullSnapshot::export().expect("export FullSnapshot");
    }
}

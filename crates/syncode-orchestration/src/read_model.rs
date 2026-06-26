//! Read models — denormalized query projections
//!
//! These are the materialized views built by the Projector from domain events.
//! They are optimized for read access patterns in the frontend.

use serde::{Deserialize, Serialize};

// ─── Project Read Model ──────────────────────────────────────────

/// Denormalized project view for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectView {
    pub id: String,
    pub name: String,
    pub root_path: String,
    pub provider_id: Option<String>,
    pub default_model: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// Number of threads in this project
    pub thread_count: u32,
}

// ─── Thread Read Model ──────────────────────────────────────────

/// Denormalized thread view for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadView {
    pub id: String,
    pub project_id: String,
    pub provider_id: String,
    pub model: String,
    pub status: String,
    pub title: Option<String>,
    pub git_checkpoint: Option<String>,
    pub turn_count: u32,
    pub created_at: String,
    pub updated_at: String,
}

// ─── Turn Read Model ─────────────────────────────────────────────

/// Denormalized turn view for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnView {
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

// ─── Message Read Model ──────────────────────────────────────────

/// Denormalized message view for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageView {
    pub id: String,
    pub turn_id: String,
    pub role: String,
    pub content: String,
    pub content_type: String,
    pub token_count: Option<u32>,
    pub tool_name: Option<String>,
    pub tool_call_id: Option<String>,
    pub created_at: String,
}

// ─── Activity Read Model ────────────────────────────────────────

/// Denormalized activity view for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityView {
    pub id: String,
    pub activity_type: String,
    pub description: String,
    pub project_id: Option<String>,
    pub thread_id: Option<String>,
    pub metadata: serde_json::Value,
    pub created_at: String,
}

//! Turn — a single user-assistant exchange within a thread

use crate::domain::primitives::{EntityId, Timestamp};
use serde::{Deserialize, Serialize};

/// Turn status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    /// User has sent input, waiting for provider response
    Pending,
    /// Provider is currently processing
    Running,
    /// Turn completed successfully
    Completed,
    /// Provider returned an error
    Error,
    /// Turn was cancelled/interrupted
    Cancelled,
}

/// A turn represents one round of interaction: user input → assistant response.
/// Each turn captures the full context needed for checkpointing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
    pub id: EntityId,
    /// Parent thread ID
    pub thread_id: EntityId,
    /// Sequence number within the thread (1-based)
    pub sequence: u32,
    /// The user's input message
    pub user_input: String,
    /// The provider's response (empty until completed)
    pub assistant_output: Option<String>,
    /// Current status
    pub status: TurnStatus,
    /// Git checkpoint ref after this turn completed
    pub git_checkpoint: Option<String>,
    /// Files modified during this turn
    pub files_modified: Vec<String>,
    /// Duration in milliseconds (None if still running)
    pub duration_ms: Option<u64>,
    pub created_at: Timestamp,
    pub completed_at: Option<Timestamp>,
}

impl Turn {
    pub fn new(thread_id: EntityId, sequence: u32, user_input: impl Into<String>) -> Self {
        Self {
            id: EntityId::new(),
            thread_id,
            sequence,
            user_input: user_input.into(),
            assistant_output: None,
            status: TurnStatus::Pending,
            git_checkpoint: None,
            files_modified: Vec::new(),
            duration_ms: None,
            created_at: Timestamp::now(),
            completed_at: None,
        }
    }

    pub fn start_running(&mut self) {
        self.status = TurnStatus::Running;
    }

    pub fn complete_with_response(&mut self, response: impl Into<String>) {
        self.assistant_output = Some(response.into());
        self.status = TurnStatus::Completed;
        self.completed_at = Some(Timestamp::now());
    }

    pub fn fail(&mut self, error: impl Into<String>) {
        self.assistant_output = Some(error.into());
        self.status = TurnStatus::Error;
        self.completed_at = Some(Timestamp::now());
    }

    pub fn cancel(&mut self) {
        self.status = TurnStatus::Cancelled;
        self.completed_at = Some(Timestamp::now());
    }

    pub fn set_git_checkpoint(&mut self, ref_name: impl Into<String>) {
        self.git_checkpoint = Some(ref_name.into());
    }

    pub fn add_modified_file(&mut self, path: impl Into<String>) {
        self.files_modified.push(path.into());
    }

    pub fn set_duration(&mut self, ms: u64) {
        self.duration_ms = Some(ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_new_starts_pending() {
        let tid = EntityId::new();
        let t = Turn::new(tid, 1, "Hello, world");
        assert_eq!(t.status, TurnStatus::Pending);
        assert!(t.assistant_output.is_none());
        assert!(t.duration_ms.is_none());
        assert!(t.completed_at.is_none());
        assert_eq!(t.user_input, "Hello, world");
    }

    #[test]
    fn turn_lifecycle_success() {
        let mut t = Turn::new(EntityId::new(), 1, "Fix the bug");
        t.start_running();
        assert_eq!(t.status, TurnStatus::Running);
        t.complete_with_response("Bug fixed!");
        assert_eq!(t.status, TurnStatus::Completed);
        assert_eq!(t.assistant_output.as_deref(), Some("Bug fixed!"));
        assert!(t.completed_at.is_some());
    }

    #[test]
    fn turn_lifecycle_failure() {
        let mut t = Turn::new(EntityId::new(), 1, "Do something");
        t.fail("API rate limit exceeded");
        assert_eq!(t.status, TurnStatus::Error);
        assert_eq!(
            t.assistant_output.as_deref(),
            Some("API rate limit exceeded")
        );
        assert!(t.completed_at.is_some());
    }

    #[test]
    fn turn_cancel() {
        let mut t = Turn::new(EntityId::new(), 1, "Long task");
        t.cancel();
        assert_eq!(t.status, TurnStatus::Cancelled);
        assert!(t.completed_at.is_some());
    }

    #[test]
    fn turn_git_checkpoint() {
        let mut t = Turn::new(EntityId::new(), 1, "Edit code");
        t.set_git_checkpoint("abc123");
        assert_eq!(t.git_checkpoint.as_deref(), Some("abc123"));
    }

    #[test]
    fn turn_modified_files() {
        let mut t = Turn::new(EntityId::new(), 1, "Refactor");
        t.add_modified_file("src/main.rs");
        t.add_modified_file("src/lib.rs");
        assert_eq!(t.files_modified.len(), 2);
    }

    #[test]
    fn turn_duration() {
        let mut t = Turn::new(EntityId::new(), 1, "Query");
        t.set_duration(1500);
        assert_eq!(t.duration_ms, Some(1500));
    }
}

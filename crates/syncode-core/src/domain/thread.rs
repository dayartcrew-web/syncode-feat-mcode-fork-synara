//! Thread aggregate — a conversation session within a project

use serde::{Deserialize, Serialize};
use crate::domain::primitives::{EntityId, Timestamp};

/// Thread status lifecycle
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadStatus {
    /// Thread is active and can receive messages
    Active,
    /// Thread is paused (user interrupted)
    Paused,
    /// Thread completed successfully
    Completed,
    /// Thread encountered an error
    Error,
    /// Thread was cancelled by user
    Cancelled,
}

/// A thread is a conversation session. It belongs to a project,
/// uses a specific provider/model, and contains a sequence of turns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    pub id: EntityId,
    /// Parent project ID
    pub project_id: EntityId,
    /// Provider used for this thread
    pub provider_id: String,
    /// Model used for this thread
    pub model: String,
    /// Current status
    pub status: ThreadStatus,
    /// Optional title (auto-generated or user-set)
    pub title: Option<String>,
    /// Git checkpoint ref at thread start
    pub git_checkpoint: Option<String>,
    /// Total turns in this thread
    pub turn_count: u32,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Thread {
    pub fn new(
        project_id: EntityId,
        provider_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        let now = Timestamp::now();
        Self {
            id: EntityId::new(),
            project_id,
            provider_id: provider_id.into(),
            model: model.into(),
            status: ThreadStatus::Active,
            title: None,
            git_checkpoint: None,
            turn_count: 0,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    pub fn pause(&mut self) {
        self.status = ThreadStatus::Paused;
        self.updated_at = Timestamp::now();
    }

    pub fn resume(&mut self) {
        self.status = ThreadStatus::Active;
        self.updated_at = Timestamp::now();
    }

    pub fn complete(&mut self) {
        self.status = ThreadStatus::Completed;
        self.updated_at = Timestamp::now();
    }

    pub fn error(&mut self) {
        self.status = ThreadStatus::Error;
        self.updated_at = Timestamp::now();
    }

    pub fn cancel(&mut self) {
        self.status = ThreadStatus::Cancelled;
        self.updated_at = Timestamp::now();
    }

    pub fn set_title(&mut self, title: impl Into<String>) {
        self.title = Some(title.into());
        self.updated_at = Timestamp::now();
    }

    pub fn set_git_checkpoint(&mut self, ref_name: impl Into<String>) {
        self.git_checkpoint = Some(ref_name.into());
    }

    pub fn increment_turn(&mut self) {
        self.turn_count += 1;
        self.updated_at = Timestamp::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_new_starts_active() {
        let pid = EntityId::new();
        let t = Thread::new(pid, "openai", "gpt-4");
        assert_eq!(t.status, ThreadStatus::Active);
        assert!(t.title.is_none());
        assert!(t.git_checkpoint.is_none());
        assert_eq!(t.turn_count, 0);
    }

    #[test]
    fn thread_lifecycle_pause_resume_complete() {
        let mut t = Thread::new(EntityId::new(), "anthropic", "claude-3");
        t.pause();
        assert_eq!(t.status, ThreadStatus::Paused);
        t.resume();
        assert_eq!(t.status, ThreadStatus::Active);
        t.complete();
        assert_eq!(t.status, ThreadStatus::Completed);
    }

    #[test]
    fn thread_lifecycle_error_cancel() {
        let mut t1 = Thread::new(EntityId::new(), "provider", "model");
        t1.error();
        assert_eq!(t1.status, ThreadStatus::Error);

        let mut t2 = Thread::new(EntityId::new(), "provider", "model");
        t2.cancel();
        assert_eq!(t2.status, ThreadStatus::Cancelled);
    }

    #[test]
    fn thread_set_title() {
        let mut t = Thread::new(EntityId::new(), "anthropic", "claude-3");
        t.set_title("Refactoring auth module");
        assert_eq!(t.title.as_deref(), Some("Refactoring auth module"));
    }

    #[test]
    fn thread_increment_turn() {
        let mut t = Thread::new(EntityId::new(), "anthropic", "claude-3");
        t.increment_turn();
        t.increment_turn();
        t.increment_turn();
        assert_eq!(t.turn_count, 3);
    }

    #[test]
    fn thread_set_git_checkpoint() {
        let mut t = Thread::new(EntityId::new(), "anthropic", "claude-3");
        t.set_git_checkpoint("refs/heads/main~5");
        assert_eq!(t.git_checkpoint.as_deref(), Some("refs/heads/main~5"));
    }

    #[test]
    fn thread_serialization_roundtrip() {
        let t = Thread::new(EntityId::new(), "openai", "gpt-4o");
        let json = serde_json::to_string(&t).expect("serialize");
        let back: Thread = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.provider_id, t.provider_id);
        assert_eq!(back.model, t.model);
    }
}

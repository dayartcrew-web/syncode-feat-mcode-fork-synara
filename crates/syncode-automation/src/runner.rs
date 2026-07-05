//! Run lifecycle
//!
//! Tracks the lifecycle of an automation run: pending → running →
//! completed/failed/cancelled/timeout.

use serde::{Deserialize, Serialize};

/// Status of an automation run
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Waiting to execute
    Pending,
    /// Currently executing
    Running,
    /// Completed successfully
    Completed,
    /// Failed (exit code non-zero or error)
    Failed,
    /// Cancelled by user
    Cancelled,
    /// Timed out
    TimedOut,
    /// Retrying after failure
    Retrying,
    /// The underlying turn is blocked waiting for a human approval / user
    /// input. Set by the [`AutomationRunReactor`](crate::run_reactor) when it
    /// observes an approval-requested / user-input-requested lifecycle event
    /// for the run's target thread. Mirrors MCode's
    /// `RunStatus.WaitingForApproval`. Resumes to `Running` once the
    /// approval is responded to.
    WaitingForApproval,
    /// The underlying turn was interrupted (user pressed stop) while still
    /// running. Mirrors MCode's `RunStatus.Interrupted`. Considered terminal
    /// (the run will not silently resume).
    Interrupted,
}

impl RunStatus {
    /// Whether the run is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunStatus::Completed
                | RunStatus::Failed
                | RunStatus::Cancelled
                | RunStatus::TimedOut
                | RunStatus::Interrupted
        )
    }

    /// Whether the run was successful
    pub fn is_success(&self) -> bool {
        matches!(self, RunStatus::Completed)
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunStatus::Pending => write!(f, "pending"),
            RunStatus::Running => write!(f, "running"),
            RunStatus::Completed => write!(f, "completed"),
            RunStatus::Failed => write!(f, "failed"),
            RunStatus::Cancelled => write!(f, "cancelled"),
            RunStatus::TimedOut => write!(f, "timed_out"),
            RunStatus::Retrying => write!(f, "retrying"),
            RunStatus::WaitingForApproval => write!(f, "waiting_for_approval"),
            RunStatus::Interrupted => write!(f, "interrupted"),
        }
    }
}

/// A single automation run record
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomationRun {
    /// Unique run identifier
    pub id: String,
    /// The automation definition ID
    pub automation_id: String,
    /// Current status
    pub status: RunStatus,
    /// Attempt number (0-indexed)
    pub attempt: u32,
    /// Exit code (None if not yet completed)
    pub exit_code: Option<i32>,
    /// Stdout output (truncated)
    pub stdout: String,
    /// Stderr output (truncated)
    pub stderr: String,
    /// Error message if failed
    pub error: Option<String>,
    /// Start timestamp
    pub started_at: Option<String>,
    /// End timestamp
    pub ended_at: Option<String>,
    /// Duration in milliseconds
    pub duration_ms: Option<u64>,
    /// Whether the run is still unread (unseen) by the user.
    /// New runs default to `true`; `automation.markRunRead` flips it to `false`.
    /// Mirrors MCode's `AutomationRunResult.unread` (lifted to the run for the
    /// simpler syncode shape).
    pub unread: bool,
    /// When the run was archived (RFC-3339), or `None` if not archived.
    /// Set by `automation.archiveRun`. Mirrors MCode's
    /// `AutomationRunResult.archivedAt`.
    pub archived_at: Option<String>,
}

impl AutomationRun {
    /// Create a new pending run
    pub fn new(automation_id: String) -> Self {
        Self {
            id: format!("run-{}", uuid::Uuid::new_v4().hyphenated()),
            automation_id,
            status: RunStatus::Pending,
            attempt: 0,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            error: None,
            started_at: None,
            ended_at: None,
            duration_ms: None,
            unread: true,
            archived_at: None,
        }
    }

    /// Mark the run as started
    pub fn mark_started(&mut self) {
        self.status = RunStatus::Running;
        self.started_at = Some(chrono::Utc::now().to_rfc3339());
    }

    /// Mark the run as completed with an exit code
    pub fn mark_completed(&mut self, exit_code: i32, stdout: String, stderr: String) {
        self.exit_code = Some(exit_code);
        self.stdout = stdout;
        self.stderr = stderr;
        self.ended_at = Some(chrono::Utc::now().to_rfc3339());
        self.status = if exit_code == 0 {
            RunStatus::Completed
        } else {
            RunStatus::Failed
        };
        self.compute_duration();
    }

    /// Mark the run as failed with an error message
    pub fn mark_failed(&mut self, error: String) {
        self.error = Some(error);
        self.status = RunStatus::Failed;
        self.ended_at = Some(chrono::Utc::now().to_rfc3339());
        self.compute_duration();
    }

    /// Mark the run as timed out
    pub fn mark_timed_out(&mut self) {
        self.status = RunStatus::TimedOut;
        self.error = Some("Run timed out".to_string());
        self.ended_at = Some(chrono::Utc::now().to_rfc3339());
        self.compute_duration();
    }

    /// Mark the run as cancelled
    pub fn mark_cancelled(&mut self) {
        self.status = RunStatus::Cancelled;
        self.ended_at = Some(chrono::Utc::now().to_rfc3339());
        self.compute_duration();
    }

    /// Mark the run as retrying
    pub fn mark_retrying(&mut self, attempt: u32) {
        self.attempt = attempt;
        self.status = RunStatus::Retrying;
    }

    /// Mark the run as blocked waiting for a human approval / user input on
    /// its target thread. Idempotent — re-marking an already-waiting run is a
    /// no-op. Set by the [`AutomationRunReactor`](crate::run_reactor) on
    /// `ApprovalRequested` / `UserInputRequested` lifecycle events.
    pub fn mark_waiting_for_approval(&mut self) {
        if self.status != RunStatus::WaitingForApproval {
            self.status = RunStatus::WaitingForApproval;
        }
    }

    /// Resume a previously-blocked run back to `Running` after the pending
    /// approval / user input was responded to. Only transitions from
    /// [`RunStatus::WaitingForApproval`]; a no-op for any other status (avoids
    /// clobbering a run that already reached a terminal state).
    pub fn resume_from_approval(&mut self) {
        if self.status == RunStatus::WaitingForApproval {
            self.status = RunStatus::Running;
        }
    }

    /// Mark the run as interrupted (user pressed stop on the underlying turn).
    /// Terminal — a run that was interrupted does not silently resume.
    pub fn mark_interrupted(&mut self) {
        self.status = RunStatus::Interrupted;
        self.ended_at = Some(chrono::Utc::now().to_rfc3339());
        self.compute_duration();
    }

    /// Mark the run as read (seen) by the user. Idempotent — flipping an
    /// already-read run to `unread=false` is a no-op.
    pub fn mark_read(&mut self) {
        self.unread = false;
    }

    /// Archive the run, stamping `archived_at` with the supplied timestamp
    /// (RFC-3339). Idempotent — re-archiving overwrites `archived_at`.
    pub fn archive(&mut self, archived_at: String) {
        self.archived_at = Some(archived_at);
    }

    fn compute_duration(&mut self) {
        if let (Some(start), Some(end)) = (&self.started_at, &self.ended_at)
            && let (Ok(s), Ok(e)) = (
                chrono::DateTime::parse_from_rfc3339(start),
                chrono::DateTime::parse_from_rfc3339(end),
            )
        {
            self.duration_ms = Some((e - s).num_milliseconds() as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_status_is_terminal() {
        assert!(!RunStatus::Pending.is_terminal());
        assert!(!RunStatus::Running.is_terminal());
        assert!(!RunStatus::Retrying.is_terminal());
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Failed.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
        assert!(RunStatus::TimedOut.is_terminal());
    }

    #[test]
    fn run_status_display() {
        assert_eq!(RunStatus::Pending.to_string(), "pending");
        assert_eq!(RunStatus::Running.to_string(), "running");
        assert_eq!(RunStatus::Completed.to_string(), "completed");
        assert_eq!(RunStatus::TimedOut.to_string(), "timed_out");
    }

    #[test]
    fn automation_run_new() {
        let run = AutomationRun::new("auto-123".to_string());
        assert_eq!(run.automation_id, "auto-123");
        assert_eq!(run.status, RunStatus::Pending);
        assert_eq!(run.attempt, 0);
        assert!(run.id.starts_with("run-"));
    }

    #[test]
    fn automation_run_lifecycle_success() {
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        assert_eq!(run.status, RunStatus::Running);
        assert!(run.started_at.is_some());

        run.mark_completed(0, "hello".to_string(), String::new());
        assert_eq!(run.status, RunStatus::Completed);
        assert_eq!(run.exit_code, Some(0));
        assert!(run.ended_at.is_some());
        assert!(run.duration_ms.is_some());
    }

    #[test]
    fn automation_run_lifecycle_failure() {
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        run.mark_completed(1, String::new(), "error msg".to_string());
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.exit_code, Some(1));
    }

    #[test]
    fn automation_run_timeout() {
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        run.mark_timed_out();
        assert_eq!(run.status, RunStatus::TimedOut);
        assert_eq!(run.error.as_deref(), Some("Run timed out"));
    }

    #[test]
    fn automation_run_cancelled() {
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        run.mark_cancelled();
        assert_eq!(run.status, RunStatus::Cancelled);
    }

    #[test]
    fn automation_run_retrying() {
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_started();
        run.mark_retrying(1);
        assert_eq!(run.status, RunStatus::Retrying);
        assert_eq!(run.attempt, 1);
    }

    #[test]
    fn automation_run_serialization() {
        let run = AutomationRun::new("auto-123".to_string());
        let json = serde_json::to_string(&run).unwrap();
        assert!(json.contains("automationId"));
        assert!(json.contains("run-"));
        assert!(json.contains("unread"));
        assert!(json.contains("archivedAt"));
        let back: AutomationRun = serde_json::from_str(&json).unwrap();
        assert_eq!(back.automation_id, "auto-123");
        // Defaults round-trip.
        assert!(back.unread);
        assert!(back.archived_at.is_none());
    }

    #[test]
    fn automation_run_new_defaults_unread_and_archived_at() {
        let run = AutomationRun::new("auto-1".to_string());
        assert!(run.unread, "new runs default to unread=true");
        assert!(
            run.archived_at.is_none(),
            "new runs default to archived_at=None"
        );
    }

    #[test]
    fn automation_run_mark_read_flips_unread() {
        let mut run = AutomationRun::new("auto-1".to_string());
        assert!(run.unread);
        run.mark_read();
        assert!(!run.unread);
        // Idempotent.
        run.mark_read();
        assert!(!run.unread);
    }

    #[test]
    fn automation_run_archive_sets_archived_at() {
        let mut run = AutomationRun::new("auto-1".to_string());
        assert!(run.archived_at.is_none());
        run.archive("2026-07-04T12:00:00+00:00".to_string());
        assert_eq!(
            run.archived_at.as_deref(),
            Some("2026-07-04T12:00:00+00:00")
        );
        // Idempotent overwrite.
        run.archive("2026-07-04T13:00:00+00:00".to_string());
        assert_eq!(
            run.archived_at.as_deref(),
            Some("2026-07-04T13:00:00+00:00")
        );
    }

    #[test]
    fn automation_run_unread_archived_at_round_trip() {
        let mut run = AutomationRun::new("auto-1".to_string());
        run.mark_read();
        run.archive("2026-07-04T12:00:00+00:00".to_string());
        let json = serde_json::to_string(&run).unwrap();
        let back: AutomationRun = serde_json::from_str(&json).unwrap();
        assert!(!back.unread);
        assert_eq!(
            back.archived_at.as_deref(),
            Some("2026-07-04T12:00:00+00:00")
        );
    }

    #[test]
    fn run_status_serialization() {
        let statuses = vec![
            RunStatus::Pending,
            RunStatus::Running,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
            RunStatus::TimedOut,
            RunStatus::Retrying,
        ];
        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let back: RunStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back);
        }
    }
}

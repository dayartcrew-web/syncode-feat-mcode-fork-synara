//! Process-based `RunExecutor` — actually runs the automation's command.
//!
//! The default scheduler executor is [`crate::scheduler::NoopExecutor`], which
//! always errors with "no RunExecutor configured". That keeps the run-record
//! lifecycle tests intact but means runs are *recorded, never executed*. This
//! module provides a real executor that runs the command via `sh -c` (Linux/macOS)
//! or `cmd /C` (Windows) and captures stdout/stderr + the exit code.
//!
//! ## Trait-shape caveat (important)
//!
//! The [`RunExecutor`] port is CQRS-shaped: [`DispatchRequest`] carries a
//! `prompt` (plus provider/model), not a raw command string. [`dispatch_request_for`]
//! populates `prompt` from the def's `prompt_template`, falling back to the
//! legacy `command` field. So when the scheduler dispatches an automation
//! through `ProcessRunExecutor`, `req.prompt` *is* the command (for the legacy
//! `command` path) or the prompt template (for AI-driven automations — which
//! this executor does NOT evaluate; it runs them as shell commands, matching
//! the legacy-field semantics established in [`dispatch_request_for`]).
//!
//! The request does NOT carry `working_dir`/`env`/`timeout_secs` (those live on
//! `AutomationDef`, which is not visible at the port boundary). A follow-up that
//! needs per-run isolation would extend `DispatchRequest` with an optional
//! `command`/`env`/`working_dir` block; for now the executor runs the prompt as
//! a shell command in the host process's CWD with its environment.
//!
//! ## Outcome mapping
//!
//! - **Exit 0** → `Ok(DispatchOutcome { thread_id, turn_id })` with synthesized
//!   ids (the orchestration layer is not involved; this executor is a
//!   standalone runner). `execute_run` records exit 0 → `RunStatus::Completed`.
//! - **Non-zero exit / spawn failure / timeout** → `Err(PortError::Internal(..))`
//!   whose message embeds the exit code + truncated stdout/stderr. `execute_run`
//!   treats this as a dispatch failure and applies the retry policy, ultimately
//!   recording `RunStatus::Failed` with the captured output in the error string.
//!
//! [`dispatch_request_for`]: crate::executor::dispatch_request_for
//! [`RunExecutor`]: syncode_core::ports::RunExecutor

use std::time::Duration;

use syncode_core::ports::{DispatchOutcome, DispatchRequest, PortError, RunExecutor};

/// Default per-command timeout (5 minutes). Overridable via [`ProcessRunExecutor::with_timeout`].
///
/// Kept conservative so a runaway command cannot pin the scheduler tick loop
/// indefinitely; automations needing longer runs should raise this explicitly.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// Real `RunExecutor` that runs `req.prompt` as a shell command.
///
/// See the module docs for the trait-shape caveat and outcome mapping.
#[derive(Debug, Clone)]
pub struct ProcessRunExecutor {
    /// Per-command wall-clock timeout. `None` = no timeout (not recommended).
    timeout: Option<Duration>,
}

impl Default for ProcessRunExecutor {
    fn default() -> Self {
        Self {
            timeout: Some(DEFAULT_TIMEOUT),
        }
    }
}

impl ProcessRunExecutor {
    /// Create a new executor with the default 5-minute timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the per-command timeout. Pass `None` to disable timeouts entirely
    /// (commands may then run unbounded — use with care).
    pub fn with_timeout(timeout: Option<Duration>) -> Self {
        Self { timeout }
    }
}

#[async_trait::async_trait]
impl RunExecutor for ProcessRunExecutor {
    async fn dispatch_turn(
        &self,
        req: DispatchRequest,
    ) -> Result<DispatchOutcome, PortError> {
        // The prompt IS the command (legacy `command` field path; see module docs).
        let command = req.prompt.as_str();
        if command.trim().is_empty() {
            return Err(PortError::Internal(
                "ProcessRunExecutor: empty command (prompt is blank)".into(),
            ));
        }

        // Build the shell invocation. Unix uses `sh -c`; Windows uses `cmd /C`.
        // `kill_on_drop(true)` ensures a timed-out child is reaped when the
        // future is dropped (the timeout wraps the entire spawn+wait).
        let mut cmd = shell_command(command);
        cmd.kill_on_drop(true);

        let fut = async {
            let output = cmd
                .output()
                .await
                .map_err(|e| PortError::Internal(format!("spawn failed: {e}")))?;
            Ok::<_, PortError>(output)
        };

        let output = match self.timeout {
            Some(t) => match tokio::time::timeout(t, fut).await {
                Ok(inner) => inner?,
                Err(_) => {
                    return Err(PortError::Internal(format!(
                        "command timed out after {}s",
                        t.as_secs()
                    )));
                }
            },
            None => fut.await?,
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if output.status.success() {
            // Exit 0 → synthesize thread/turn ids. The orchestration layer is
            // not involved (this executor is a standalone process runner), so
            // fresh opaque ids are sufficient for the run record.
            Ok(DispatchOutcome {
                thread_id: syncode_core::EntityId::new(),
                turn_id: syncode_core::EntityId::new(),
            })
        } else {
            // Non-zero exit (or signal). Embed exit code + truncated output so
            // the failure is diagnosable in the run record.
            let code = output.status.code().unwrap_or(-1);
            Err(PortError::Internal(format!(
                "command exited {code}\n--- stdout ---\n{}\n--- stderr ---\n{}",
                truncate(&stdout, 2048),
                truncate(&stderr, 2048)
            )))
        }
    }
}

/// Build a `tokio::process::Command` that runs `cmd` through the platform shell.
///
/// - Unix: `sh -c "<cmd>"`
/// - Windows: `cmd /C "<cmd>"`
fn shell_command(cmd: &str) -> tokio::process::Command {
    #[cfg(unix)]
    {
        let mut c = tokio::process::Command::new("sh");
        c.arg("-c").arg(cmd);
        c
    }
    #[cfg(not(unix))]
    {
        let mut c = tokio::process::Command::new("cmd");
        c.arg("/C").arg(cmd);
        c
    }
}

/// Truncate `s` to at most `max` chars, appending an ellipsis if cut.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max).collect();
        t.push('…');
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_echo_hello_and_captures_output() {
        // Keystone: the executor actually runs the command. Exit 0 path.
        let exec = ProcessRunExecutor::new();
        let req = DispatchRequest {
            project_id: None,
            target_thread_id: None,
            provider_id: "process".into(),
            model: "local".into(),
            prompt: "echo hello".into(),
        };

        let outcome = exec.dispatch_turn(req).await;
        assert!(outcome.is_ok(), "echo should succeed: {:?}", outcome.err());
        let DispatchOutcome {
            thread_id,
            turn_id,
        } = outcome.unwrap();
        // Synthesized ids are non-empty.
        assert!(!thread_id.to_string().is_empty());
        assert!(!turn_id.to_string().is_empty());
    }

    #[tokio::test]
    async fn non_zero_exit_returns_error_with_output() {
        // A failing command surfaces as a PortError carrying the exit code +
        // stderr. This is what execute_run's retry/fail path consumes.
        let exec = ProcessRunExecutor::new();
        let req = DispatchRequest {
            project_id: None,
            target_thread_id: None,
            provider_id: "process".into(),
            model: "local".into(),
            prompt: "echo oops >&2; exit 7".into(),
        };

        let err = exec.dispatch_turn(req).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("exited 7"), "msg should mention exit 7: {msg}");
        assert!(msg.contains("oops"), "msg should embed stderr: {msg}");
    }

    #[tokio::test]
    async fn empty_command_is_rejected() {
        let exec = ProcessRunExecutor::new();
        let req = DispatchRequest {
            project_id: None,
            target_thread_id: None,
            provider_id: "process".into(),
            model: "local".into(),
            prompt: "   ".into(),
        };
        let err = exec.dispatch_turn(req).await.unwrap_err();
        assert!(err.to_string().contains("empty command"));
    }

    #[tokio::test]
    async fn timeout_kills_long_running_command() {
        // 1s timeout vs a 30s sleep — the timeout arm must fire.
        let exec = ProcessRunExecutor::with_timeout(Some(Duration::from_millis(500)));
        let req = DispatchRequest {
            project_id: None,
            target_thread_id: None,
            provider_id: "process".into(),
            model: "local".into(),
            prompt: "sleep 30".into(),
        };
        let err = exec.dispatch_turn(req).await.unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "expected timeout error: {err}"
        );
    }

    #[tokio::test]
    async fn no_timeout_allows_completion() {
        // Explicit None must not impose a timeout — the command finishes.
        let exec = ProcessRunExecutor::with_timeout(None);
        let req = DispatchRequest {
            project_id: None,
            target_thread_id: None,
            provider_id: "process".into(),
            model: "local".into(),
            prompt: "echo done".into(),
        };
        assert!(exec.dispatch_turn(req).await.is_ok());
    }
}

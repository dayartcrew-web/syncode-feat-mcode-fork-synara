//! Live run-event push seam.
//!
//! The canonical live-push pattern in this codebase is the terminal
//! reader-task (`spawn_terminal_reader` in `syncode-ws/src/rpc.rs`): a
//! long-running task streams progress to subscribers on `push_tx` *during*
//! execution, not just after. PUSH-1 brings the same model to automations.
//!
//! `ProcessRunExecutor` (and the [`crate::executor::execute_run`] retry loop)
//! run inside the `syncode-automation` crate, which cannot depend on the WS
//! layer. This module defines the seam between them: a [`RunEventSink`] port
//! that the WS layer implements (`AutomationPushSink`) and the executor
//! invokes at three lifecycle points — started / progress / completed.
//!
//! ## Design
//!
//! - **`RunEventSink`** is a `Send + Sync` callback trait with one async
//!   method, [`RunEventSink::emit`]. A no-op default is provided so callers
//!   that don't care about live events pay nothing.
//! - **`RunEvent`** carries the run id + automation id + a typed payload
//!   ([`RunEventKind`]). The payload is intentionally cheap to clone — the
//!   sink may forward it across an async boundary.
//! - **`RunContext`** + the `with_run_context` task-local mechanism let the
//!   `ProcessRunExecutor` discover the active sink + run identity for the
//!   *current* `dispatch_turn` call without changing the `RunExecutor` /
//!   `DispatchRequest` port (which is shared with the orchestration layer
//!   and the AI-heartbeat path).
//!
//! This is a pure addition: the synchronous trigger path
//! ([`crate::scheduler::Scheduler::trigger`] /
//! [`crate::executor::execute_run`]) is untouched — it never sets a
//! `RunContext`, so `ProcessRunExecutor` falls back to its current behavior.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

// ─── Event model ───────────────────────────────────────────────────────────

/// The three lifecycle event kinds PUSH-1 emits on `CHANNEL_AUTOMATION`.
///
/// `run-started` fires when the subprocess spawns (or, for non-process
/// executors, at the start of `dispatch_turn`). `run-progress` fires as
/// stdout accumulates (best-effort batching — at least one per run). `run-
/// completed` fires once the run reaches a terminal state (success or
/// failure), carrying the final status + exit code.
#[derive(Debug, Clone)]
pub enum RunEventKind {
    /// Emitted at the start of a run (subprocess spawned / dispatch begun).
    Started {
        /// RFC-3339 timestamp the run began.
        started_at: String,
    },
    /// Emitted during execution as progress accrues (e.g. incremental stdout).
    Progress {
        /// Best-effort progress fraction in `[0.0, 1.0]` (`None` if unknown —
        /// the sink should still forward the event; the subscriber can render
        /// an indeterminate indicator).
        progress: Option<f64>,
        /// Free-form progress message (incremental stdout chunk, stage label, …).
        message: String,
    },
    /// Emitted when the run reaches a terminal state.
    Completed {
        /// Terminal status name (`"completed"` / `"failed"` / …) — matches
        /// [`crate::runner::RunStatus`]'s `Display` impl.
        status: String,
        /// Exit code if known (`None` for failures that didn't yield one).
        exit_code: Option<i32>,
    },
}

/// A single automation run-event destined for the `automation` push channel.
#[derive(Debug, Clone)]
pub struct RunEvent {
    /// The run id (stable across all events for one run).
    pub run_id: String,
    /// The automation definition id the run belongs to.
    pub automation_id: String,
    /// The typed event payload.
    pub kind: RunEventKind,
}

impl RunEvent {
    /// Wire-style `type` discriminator (`"run-started"` / `"run-progress"` /
    /// `"run-completed"`).
    pub fn type_name(&self) -> &'static str {
        match self.kind {
            RunEventKind::Started { .. } => "run-started",
            RunEventKind::Progress { .. } => "run-progress",
            RunEventKind::Completed { .. } => "run-completed",
        }
    }
}

// ─── Sink port ─────────────────────────────────────────────────────────────

/// A best-effort destination for live [`RunEvent`]s.
///
/// The automation crate calls [`RunEventSink::emit`] at lifecycle points
/// *during* a run. Implementations MUST be non-blocking and MUST tolerate
/// being called when there are no subscribers (the WS push bus returns
/// `SendError` in that case — not a failure; the run continues regardless).
///
/// The default implementation is a no-op: callers that don't need live
/// events ([`crate::scheduler::Scheduler::trigger`] /
/// [`crate::executor::execute_run`]) get the historical behavior for free.
pub trait RunEventSink: Send + Sync {
    /// Forward `event` to subscribers. Best-effort: a failure (no receivers,
    /// broken bus) is swallowed — a push failure must never fail a run.
    fn emit(&self, event: RunEvent) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// A no-op sink — the default. Calling `emit` does nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopRunEventSink;

impl RunEventSink for NoopRunEventSink {
    fn emit(&self, _event: RunEvent) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

// ─── Run context (task-local discovery) ────────────────────────────────────

/// Identity + sink for the currently-executing run, scoped to a single
/// `dispatch_turn` call via [`with_run_context`].
///
/// Cloning is cheap (one `Arc` bump for the sink). The context is stored in
/// a task-local so `ProcessRunExecutor` can discover it without threading it
/// through the `RunExecutor` / `DispatchRequest` port (which is shared with
/// the orchestration/AI-heartbeat path and can't carry automation-specific
/// fields).
#[derive(Clone)]
pub struct RunContext {
    /// The run id this context is scoped to.
    pub run_id: String,
    /// The automation definition id the run belongs to.
    pub automation_id: String,
    /// The sink to emit events to (no-op if live push is disabled).
    pub sink: Arc<dyn RunEventSink>,
}

tokio::task_local! {
    /// The active run context for the current task, if any.
    ///
    /// `None` outside of [`with_run_context`] — that's the synchronous-trigger
    /// path, where `ProcessRunExecutor` skips live-event emission entirely.
    pub(crate) static RUN_CONTEXT: Option<RunContext>;
}

/// Run `fut` with `ctx` installed as the active run context for the current
/// task. `ProcessRunExecutor::dispatch_turn` (and any other executor that
/// supports live events) reads it back via [`current_run_context`].
///
/// The future is created by the caller (e.g. `executor.dispatch_turn(req)`)
/// and polled to completion inside the scope; the task-local is set for the
/// entire poll. Nesting replaces the outer context for the duration of `fut`
/// (restored on drop) — a nested `dispatch_turn` (retry loop) re-scopes to
/// the new run id.
pub async fn with_run_context<F, R>(ctx: RunContext, fut: F) -> R
where
    F: Future<Output = R> + Send,
    R: Send,
{
    RUN_CONTEXT.scope(Some(ctx), fut).await
}

/// Read the active run context for the current task, if one is installed.
///
/// Returns `None` on the synchronous trigger path (where live push is not
/// requested) — callers MUST treat that as "no live events, behave as before".
pub fn current_run_context() -> Option<RunContext> {
    RUN_CONTEXT.try_with(|c| c.clone()).ok().flatten()
}

/// Convenience helper: emit a [`RunEvent`] to the current task's sink, if any.
///
/// No-op (returns immediately) when called outside a [`with_run_context`]
/// scope — this is the synchronous-trigger fast path. Errors from the sink
/// are logged and swallowed (live push is best-effort).
pub async fn emit_current(kind: RunEventKind) {
    let Some(ctx) = current_run_context() else {
        return;
    };
    let event = RunEvent {
        run_id: ctx.run_id.clone(),
        automation_id: ctx.automation_id.clone(),
        kind,
    };
    // Best-effort: a sink failure must never propagate out of dispatch_turn.
    ctx.sink.emit(event).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A sink that records every event it receives (test-only).
    struct RecordingSink {
        events: Mutex<Vec<RunEvent>>,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                events: Mutex::new(Vec::new()),
            })
        }
        fn events(&self) -> Vec<RunEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl RunEventSink for RecordingSink {
        fn emit(&self, event: RunEvent) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                self.events.lock().unwrap().push(event);
            })
        }
    }

    #[test]
    fn run_event_type_name_maps_to_wire_strings() {
        let started = RunEvent {
            run_id: "r1".into(),
            automation_id: "a1".into(),
            kind: RunEventKind::Started {
                started_at: "t".into(),
            },
        };
        assert_eq!(started.type_name(), "run-started");

        let progress = RunEvent {
            run_id: "r1".into(),
            automation_id: "a1".into(),
            kind: RunEventKind::Progress {
                progress: Some(0.5),
                message: "halfway".into(),
            },
        };
        assert_eq!(progress.type_name(), "run-progress");

        let completed = RunEvent {
            run_id: "r1".into(),
            automation_id: "a1".into(),
            kind: RunEventKind::Completed {
                status: "completed".into(),
                exit_code: Some(0),
            },
        };
        assert_eq!(completed.type_name(), "run-completed");
    }

    #[tokio::test]
    async fn noop_sink_emit_is_a_no_op() {
        let sink = NoopRunEventSink;
        // Just verifies it doesn't panic / hang.
        sink.emit(RunEvent {
            run_id: "r".into(),
            automation_id: "a".into(),
            kind: RunEventKind::Started {
                started_at: "t".into(),
            },
        })
        .await;
    }

    #[tokio::test]
    async fn emit_current_is_noop_without_context() {
        // Outside any with_run_context scope — must return without panicking.
        emit_current(RunEventKind::Progress {
            progress: None,
            message: "should be dropped".into(),
        })
        .await;
    }

    #[tokio::test]
    async fn emit_current_forwards_to_context_sink() {
        let sink = RecordingSink::new();
        let ctx = RunContext {
            run_id: "run-xyz".into(),
            automation_id: "auto-1".into(),
            sink: sink.clone() as Arc<dyn RunEventSink>,
        };

        with_run_context(ctx, async {
            emit_current(RunEventKind::Started {
                started_at: "t1".into(),
            })
            .await;
            emit_current(RunEventKind::Progress {
                progress: Some(0.25),
                message: "chunk".into(),
            })
            .await;
        })
        .await;

        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].run_id, "run-xyz");
        assert_eq!(events[0].automation_id, "auto-1");
        assert_eq!(events[0].type_name(), "run-started");
        assert_eq!(events[1].type_name(), "run-progress");
    }

    #[tokio::test]
    async fn current_run_context_is_none_outside_scope() {
        assert!(current_run_context().is_none());
    }
}

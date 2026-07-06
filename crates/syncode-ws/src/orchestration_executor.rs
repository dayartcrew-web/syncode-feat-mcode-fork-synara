//! Orchestration-backed `RunExecutor` — dispatches an automation run as a real
//! orchestration turn through the [`ApplicationService`].
//!
//! Mirrors MCode's `AutomationService.dispatchRun`: instead of running a shell
//! command, the automation's prompt is dispatched into the chat pipeline
//! (`ApplicationService::create_thread` for standalone runs +
//! `ApplicationService::start_turn` for the turn). The provider adapter
//! responds, the orchestration reactor maps the provider's
//! `ProviderEvent::Completed` into a `DomainEvent::TurnCompleted` (or
//! `TurnFailed` on error), and this executor captures the finalized assistant
//! output + the real thread/turn ids and returns them in the
//! [`DispatchOutcome`].
//!
//! ## Why lives in `syncode-ws` (not `syncode-automation`)
//!
//! [`ApplicationService`] is defined in `syncode-orchestration`, which depends
//! on `syncode-core`. `syncode-automation` also depends on `syncode-core` only
//! — adding an orchestration dependency would create a layering violation
//! (automation → orchestration, when automation's port surface is meant to be
//! provider/engine-agnostic). The orchestration-backed executor therefore
//! lives one layer up, in the WS host crate, which already depends on both.
//!
//! ## Turn-completion path (PR #131)
//!
//! The chat pipeline fixed a subscribe-too-late race: the
//! [`ProviderCommandReactor::handle_start_turn`] pre-subscribes to the
//! provider adapter's event stream before calling `send_request`, so the
//! synchronous one-shot adapters (claude CLI) cannot drop events on a
//! subscriber-less broadcast bus. After `send_request` returns, the reactor
//! drains the captured events and the pipeline ingests them through
//! [`ingest_provider_event`]. By the time `ApplicationService::start_turn`
//! returns, the [`CommandResult`] carries the terminal `TurnCompleted` (or
//! `TurnFailed`) in `events` / `side_effect_events`. This executor just scans
//! both lists for the terminal event.
//!
//! ## Error mapping
//!
//! - Missing project / thread (`ProjectNotFound` / `ThreadNotFound`) →
//!   `PortError::Internal` with a descriptive message; the run-retry loop
//!   treats this as a dispatch failure.
//! - `TurnFailed` event in the result → `PortError::Internal` carrying the
//!   failure's `error` string; surfaces as a failed run.
//! - Neither `TurnCompleted` nor `TurnFailed` in the result (the turn was
//!   accepted but did not finalize synchronously — async/ACP adapters) →
//!   returns a `DispatchOutcome` with the `TurnStarted` turn id and
//!   `assistant_output = None`. The run is recorded as `Completed` (the
//!   dispatch was accepted) and the [`AutomationRunReactor`] reconciles the
//!   status later from the orchestration domain-event stream.
//!
//! ## Live-event push (PUSH-1 parity)
//!
//! When dispatched through [`Scheduler::trigger_with_events`] (the
//! `automation.runNow` path), a [`RunContext`] is installed on the dispatch
//! task. This executor honors it by emitting `run-started` at the beginning
//! and `run-completed` at the end of `dispatch_turn` — preserving the
//! `automation`-channel live lifecycle that the PUSH-1 contract guarantees
//! (subscribers see automation runs begin and end on the same channel
//! regardless of which executor is wired). Incremental `run-progress` events
//! are NOT emitted here: orchestration drives token-level progress through
//! the orchestration channel (`orchestration` push events like `TokenAppended`
//! / `ToolCallRequested`), which is what mcode parity requires. The shell
//! executor's incremental-stdout `run-progress` events were specific to
//! subprocess capture; the orchestration path surfaces progress via its own
//! event stream.
//!
//! [`Scheduler::trigger_with_events`]: syncode_automation::scheduler::Scheduler::trigger_with_events
//! [`RunContext`]: syncode_automation::events::RunContext
//!
//! [`ProviderCommandReactor::handle_start_turn`]: syncode_orchestration::reactors::ProviderCommandReactor
//! [`ingest_provider_event`]: syncode_orchestration::reactors::ingest_provider_event
//! [`AutomationRunReactor`]: syncode_automation::run_reactor::AutomationRunReactor

use std::sync::Arc;

use syncode_automation::events::{RunEventKind, emit_current};
use syncode_core::EntityId;
use syncode_core::domain::events::DomainEvent;
use syncode_core::ports::{DispatchOutcome, DispatchRequest, PortError, RunExecutor};
use syncode_orchestration::{ApplicationService, CommandResult, OrchestrationError};

/// An orchestration-backed `RunExecutor` that dispatches each automation turn
/// through the [`ApplicationService`] (chat pipeline).
///
/// Construct with [`OrchestrationRunExecutor::new`], passing the shared
/// `ApplicationService` (built once per `WsState` from the orchestrator). The
/// executor is `Clone` (just an `Arc`), so the same instance can be shared
/// between the scheduler and any other dispatch paths.
#[derive(Clone)]
pub struct OrchestrationRunExecutor {
    service: Arc<ApplicationService>,
}

impl OrchestrationRunExecutor {
    /// Construct a new executor wrapping the shared application service.
    pub fn new(service: Arc<ApplicationService>) -> Self {
        Self { service }
    }
}

#[async_trait::async_trait]
impl RunExecutor for OrchestrationRunExecutor {
    async fn dispatch_turn(&self, req: DispatchRequest) -> Result<DispatchOutcome, PortError> {
        // PUSH-1 parity: signal run-started at the beginning of dispatch when
        // a RunContext is installed on this task. Best-effort (no-op outside
        // a with_run_context scope). Mirrors the ProcessRunExecutor's
        // dispatch_turn_live contract for the lifecycle endpoints.
        emit_current(RunEventKind::Started {
            started_at: chrono::Utc::now().to_rfc3339(),
        })
        .await;

        // Drive the orchestration pipeline. Errors from this block become
        // PortError::Internal so the run-retry loop can react; we always
        // follow up with a run-completed event before returning so the
        // PUSH-1 lifecycle terminates cleanly regardless of outcome.
        let outcome_result = self.dispatch_orchestration(req).await;

        // PUSH-1 parity: signal run-completed at the end. Status reflects
        // the dispatch outcome — `completed` for Ok (the turn was accepted
        // and either finalized synchronously or is streaming via the live
        // event path), `failed` for Err. exit_code 0 / None mirror the
        // shell executor's mapping.
        let (status_name, exit_code) = match &outcome_result {
            Ok(_) => ("completed", Some(0)),
            Err(_) => ("failed", None),
        };
        emit_current(RunEventKind::Completed {
            status: status_name.to_string(),
            exit_code,
        })
        .await;

        outcome_result
    }
}

impl OrchestrationRunExecutor {
    /// Inner dispatch: drive the orchestration pipeline for one automation
    /// turn. Separated from the trait method so the PUSH-1 lifecycle emission
    /// can wrap it cleanly (Started before, Completed after — both terminal
    /// outcomes).
    async fn dispatch_orchestration(
        &self,
        req: DispatchRequest,
    ) -> Result<DispatchOutcome, PortError> {
        // 1. Resolve the thread: heartbeat mode uses the request's
        // `target_thread_id`; standalone mode creates a fresh thread in the
        // request's project. Mirrors MCode's `dispatchRun` mode split.
        let thread_id = match req.target_thread_id {
            Some(existing) => existing,
            None => {
                let project_id = req.project_id.ok_or_else(|| {
                    PortError::Internal(
                        "OrchestrationRunExecutor: standalone dispatch requires project_id".into(),
                    )
                })?;
                create_thread_for_run(&self.service, project_id, &req.provider_id, &req.model)
                    .await?
            }
        };

        // 2. Start the turn. The pipeline routes through the decider (TurnStarted),
        //    then the ProviderCommandReactor (pre-subscribe + send_request +
        //    drain + synthesize-terminal), and finally the ingestion reactor
        //    (ProviderEvent → DomainEvent). The CommandResult's events +
        //    side_effect_events together contain TurnStarted + the terminal
        //    TurnCompleted/TurnFailed for synchronous adapters.
        let result = self
            .service
            .start_turn(thread_id, 0, req.prompt.clone())
            .await
            .map_err(map_orchestration_error)?;

        // 3. Extract the turn id from the TurnStarted event.
        let turn_id = result
            .events
            .iter()
            .find_map(|env| {
                if let DomainEvent::TurnStarted { id, .. } = &env.event {
                    Some(*id)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                PortError::Internal(
                    "OrchestrationRunExecutor: StartTurn produced no TurnStarted event".into(),
                )
            })?;

        // 4. Scan both event lists for the terminal outcome. side_effect_events
        //    is where the ingestion reactor's TurnCompleted/TurnFailed lands
        //    for synchronous adapters (the pipeline returns the decider's
        //    events in `events` and the reactor-ingested ones in
        //    `side_effect_events`).
        let (completed, failed) = scan_terminal(&result);
        if let Some(error) = failed {
            return Err(PortError::Internal(format!(
                "automation turn failed: {error}"
            )));
        }

        // 5. Build the outcome. assistant_output is Some when the turn
        //    finalized synchronously (claude); None when the adapter is async
        //    and the turn is still running (the reactor will reconcile the
        //    run status from the live event stream later).
        let outcome = DispatchOutcome::new(thread_id, turn_id);
        Ok(match completed {
            Some(output) => outcome.with_assistant_output(output),
            None => outcome,
        })
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────

/// Create a thread for a standalone automation run. Returns the new thread id
/// (derived from the `ThreadCreated` event) or an error if the project doesn't
/// exist / the command failed / no ThreadCreated event was produced.
async fn create_thread_for_run(
    service: &ApplicationService,
    project_id: EntityId,
    provider_id: &str,
    model: &str,
) -> Result<EntityId, PortError> {
    let result = service
        .create_thread(project_id, provider_id.to_string(), model.to_string())
        .await
        .map_err(map_orchestration_error)?;
    result
        .events
        .iter()
        .find_map(|env| match &env.event {
            DomainEvent::ThreadCreated { id, .. } => Some(*id),
            _ => None,
        })
        .ok_or_else(|| {
            PortError::Internal(
                "OrchestrationRunExecutor: CreateThread produced no ThreadCreated event".into(),
            )
        })
}

/// Map an [`OrchestrationError`] onto a [`PortError`] with a useful message.
///
/// `ProjectNotFound` / `ThreadNotFound` surface the missing id so the operator
/// can correlate; everything else is wrapped as a generic internal error
/// carrying the orchestration error's display string.
fn map_orchestration_error(e: OrchestrationError) -> PortError {
    match e {
        OrchestrationError::ProjectNotFound(id) => {
            PortError::Internal(format!("orchestration: project not found: {id}"))
        }
        OrchestrationError::ThreadNotFound(id) => {
            PortError::Internal(format!("orchestration: thread not found: {id}"))
        }
        other => PortError::Internal(format!("orchestration error: {other}")),
    }
}

/// Scan a [`CommandResult`]'s event lists for the terminal turn outcome.
///
/// Returns `(completed_output, failed_error)`:
/// - `(Some(output), None)` when a `TurnCompleted` event was observed.
/// - `(None, Some(error))` when a `TurnFailed` event was observed.
/// - `(None, None)` when neither was seen (async adapter still running).
///
/// `TurnFailed` wins over `TurnCompleted` when both appear (defensive — the
/// pipeline guarantees one or the other, never both, but a late failure after
/// a synthesized completion would surface as both).
fn scan_terminal(result: &CommandResult) -> (Option<String>, Option<String>) {
    let mut completed: Option<String> = None;
    let mut failed: Option<String> = None;
    for env in result.events.iter().chain(result.side_effect_events.iter()) {
        match &env.event {
            DomainEvent::TurnCompleted {
                assistant_output, ..
            } => {
                completed = Some(assistant_output.clone());
            }
            DomainEvent::TurnFailed { error, .. } => {
                failed = Some(error.clone());
            }
            _ => {}
        }
    }
    if failed.is_some() {
        (None, failed)
    } else {
        (completed, None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use syncode_core::Envelope;
    use syncode_core::domain::primitives::Timestamp;
    use syncode_orchestration::Command;

    /// Verify scan_terminal surfaces TurnCompleted's assistant_output.
    #[test]
    fn scan_terminal_picks_completed_output() {
        let thread = EntityId::new();
        let turn = EntityId::new();
        let result = CommandResult {
            command: Command::CompleteTurn {
                id: turn,
                assistant_output: String::new(),
                duration_ms: 0,
            },
            events: vec![Envelope::new(
                DomainEvent::TurnStarted {
                    id: turn,
                    thread_id: thread,
                    sequence: 0,
                    user_input: "hi".into(),
                    created_at: Timestamp::now(),
                },
                1,
            )],
            side_effect_triggered: true,
            side_effect_events: vec![Envelope::new(
                DomainEvent::TurnCompleted {
                    id: turn,
                    assistant_output: "AUTO_VIA_PROVIDER".into(),
                    duration_ms: 10,
                    completed_at: Timestamp::now(),
                },
                2,
            )],
        };
        let (completed, failed) = scan_terminal(&result);
        assert_eq!(completed.as_deref(), Some("AUTO_VIA_PROVIDER"));
        assert!(failed.is_none());
    }

    /// Verify scan_terminal surfaces TurnFailed's error.
    #[test]
    fn scan_terminal_picks_failed_error() {
        let turn = EntityId::new();
        let result = CommandResult {
            command: Command::FailTurn {
                id: turn,
                error: String::new(),
            },
            events: vec![],
            side_effect_triggered: true,
            side_effect_events: vec![Envelope::new(
                DomainEvent::TurnFailed {
                    id: turn,
                    error: "provider crashed".into(),
                    completed_at: Timestamp::now(),
                },
                1,
            )],
        };
        let (completed, failed) = scan_terminal(&result);
        assert!(completed.is_none());
        assert_eq!(failed.as_deref(), Some("provider crashed"));
    }

    /// Verify scan_terminal returns (None, None) when neither terminal event
    /// is present (async adapter still streaming).
    #[test]
    fn scan_terminal_returns_none_when_no_terminal_event() {
        let result = CommandResult {
            command: Command::CancelTurn {
                id: EntityId::new(),
            },
            events: vec![],
            side_effect_triggered: false,
            side_effect_events: vec![],
        };
        let (completed, failed) = scan_terminal(&result);
        assert!(completed.is_none());
        assert!(failed.is_none());
    }

    /// Verify map_orchestration_error surfaces the missing id for ProjectNotFound.
    #[test]
    fn map_error_surfaces_missing_project_id() {
        let id = EntityId::new();
        let err = map_orchestration_error(OrchestrationError::ProjectNotFound(id));
        let msg = err.to_string();
        assert!(msg.contains("project not found"), "got: {msg}");
        assert!(msg.contains(&id.to_string()), "got: {msg}");
    }
}

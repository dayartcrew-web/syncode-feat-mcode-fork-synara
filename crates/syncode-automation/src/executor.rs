//! Run execution + retry loop.
//!
//! The heart of the automation engine: given a due [`AutomationDef`], dispatch
//! a turn via the injected [`RunExecutor`] port, apply the [`RetryPolicy`] on
//! failure, and record the outcome through the [`AutomationRepository`].
//!
//! Mirrors MCode's `dispatchRun` (which dispatches a CQRS turn — standalone =
//! create thread + start turn; heartbeat = start turn on an existing thread).
//! The retry loop is where the Rust crate is AHEAD of MCode: MCode stubs retry
//! ("retry policies are not supported yet"); here we honor the existing,
//! tested `ExponentialBackoff`/`FixedDelay`/`None` policies.
//!
//! Testability: the retry sleep is behind an injectable [`Delay`] strategy so
//! tests run with zero delay (`Delay::Immediate`) instead of waiting out real
//! backoffs.

use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;

use syncode_core::ports::{AutomationRepository, DispatchRequest, RunExecutor};

use crate::definition::AutomationDef;
use crate::events::{RunContext, RunEventSink, with_run_context};
use crate::policies::{CompletionPolicy, RetryPolicy};
use crate::runner::{AutomationRun, RunStatus};
use crate::schedule;

/// How the retry loop should wait between attempts. Injectable so tests can
/// skip real delays.
#[derive(Debug, Clone, Copy)]
pub enum Delay {
    /// Wait the real backoff duration (production).
    Real,
    /// Skip all delays — retry immediately (tests).
    Immediate,
}

impl Delay {
    /// Apply the delay (no-op for `Immediate`).
    pub async fn wait(self, dur: Duration) {
        if matches!(self, Delay::Real) {
            tokio::time::sleep(dur).await;
        }
    }
}

/// The outcome of executing a run (possibly after retries).
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub final_status: RunStatus,
    pub attempts: u32,
}

/// Derive the [`RetryPolicy`] from an automation def's fields. The def stores
/// `max_retries` + `retry_delay_secs` (simple fixed-style); we expose that as a
/// `FixedDelay`. (The richer `ExponentialBackoff` policy is available for
/// callers that construct policies explicitly.)
pub fn retry_policy_for(def: &AutomationDef) -> RetryPolicy {
    if def.max_retries == 0 {
        RetryPolicy::None
    } else {
        RetryPolicy::FixedDelay {
            max_retries: def.max_retries,
            delay_secs: def.retry_delay_secs,
        }
    }
}

/// Build the dispatch request for an automation def.
///
/// - `target_thread_id` set → **heartbeat**: append a turn to that thread.
/// - otherwise → **standalone**: create a new thread + start a turn.
pub fn dispatch_request_for(def: &AutomationDef) -> DispatchRequest {
    let target_thread_id = def
        .target_thread_id
        .as_deref()
        .and_then(|s| syncode_core::EntityId::parse(s).ok());

    let project_id = def
        .project_id
        .as_deref()
        .and_then(|s| syncode_core::EntityId::parse(s).ok());

    // The prompt: prefer prompt_template (MCode's `prompt`), fall back to the
    // legacy `command` field (divergent from MCode, but populated in tests).
    let prompt = def
        .prompt_template
        .clone()
        .unwrap_or_else(|| def.command.clone());

    let (provider_id, model) = def
        .provider_id
        .as_deref()
        .zip(def.model.as_deref())
        .map(|(p, m)| (p.to_string(), m.to_string()))
        .unwrap_or_else(|| ("default".to_string(), "default".to_string()));

    DispatchRequest {
        project_id,
        target_thread_id,
        provider_id,
        model,
        prompt,
    }
}

/// Execute a single automation run, applying the retry policy on failure.
///
/// Creates a run record, dispatches the turn, and on failure retries per the
/// policy (up to `max_retries`), then records the final outcome + advances
/// `next_run_at`. Returns the final status + attempt count.
///
/// `now` is injected (not read from the system clock) so tests are deterministic.
pub async fn execute_run(
    def: &AutomationDef,
    executor: &dyn RunExecutor,
    repo: &dyn AutomationRepository,
    completion: &CompletionPolicy,
    delay: Delay,
    now: DateTime<Utc>,
) -> RunOutcome {
    let policy = retry_policy_for(def);
    let mut attempt: u32 = 0;
    let req = dispatch_request_for(def);

    // Create + persist the initial run record.
    let mut run = AutomationRun::new(def.id.as_str().to_string());
    run.attempt = attempt;
    run.mark_started();
    persist_run(repo, &run).await;

    loop {
        match executor.dispatch_turn(req.clone()).await {
            Ok(outcome) => {
                // Success signal: a dispatched turn maps to "exit code 0".
                let exit_code = 0;
                run.mark_completed(
                    exit_code,
                    format!("turn {}", outcome.turn_id),
                    String::new(),
                );

                // If the completion policy rejects (e.g. AllowedExitCodes), mark failed.
                if !completion.is_success(exit_code) {
                    run.status = RunStatus::Failed;
                    run.error = Some(format!(
                        "completion policy rejected exit code {}",
                        exit_code
                    ));
                }
                persist_run(repo, &run).await;
                advance_schedule(repo, def, &run.status, now).await;
                return RunOutcome {
                    final_status: run.status.clone(),
                    attempts: attempt + 1,
                };
            }
            Err(err) => {
                // Consult the retry policy for the next delay.
                match policy.delay_for_attempt(attempt) {
                    Some(backoff) if !policy.exhausted(attempt) => {
                        run.mark_retrying(attempt + 1);
                        persist_run(repo, &run).await;
                        delay.wait(backoff).await;
                        attempt += 1;
                        continue;
                    }
                    _ => {
                        // Retries exhausted (or policy is None) → fail permanently.
                        run.mark_failed(format!(
                            "dispatch failed after {} attempt(s): {}",
                            attempt + 1,
                            err
                        ));
                        persist_run(repo, &run).await;
                        advance_schedule(repo, def, &run.status, now).await;
                        return RunOutcome {
                            final_status: RunStatus::Failed,
                            attempts: attempt + 1,
                        };
                    }
                }
            }
        }
    }
}

/// Execute a single automation run **with live event push** (PUSH-1).
///
/// This is the live-push variant of [`execute_run`]: it accepts a
/// [`RunEventSink`] and installs a [`RunContext`] on the current task for the
/// duration of each `dispatch_turn` call, so a participating executor (e.g.
/// [`crate::process_executor::ProcessRunExecutor`]) can emit `run-started` /
/// `run-progress` / `run-completed` events *during* execution — mirroring the
/// terminal reader-task pattern (`spawn_terminal_reader` in
/// `syncode-ws/src/rpc.rs`).
///
/// ## Behavior parity
///
/// The retry loop, run-record lifecycle, and schedule advance are identical
/// to [`execute_run`]. The only addition is the run-context scope around the
/// dispatch call: when `sink` is a [`NoopRunEventSink`] (or the executor
/// doesn't read the context), behavior is indistinguishable from
/// [`execute_run`].
///
/// ## Why a sink argument (not a stored field)
///
/// The sink is per-run, not per-executor: the WS layer supplies a sink wired
/// to `push_tx` for the run triggered by `automation.runNow`, while the
/// synchronous scheduler tick path (which doesn't need live push) calls
/// [`execute_run`] directly. Keeping the sink out of `ProcessRunExecutor`'s
/// fields means a single executor instance serves both paths.
pub async fn execute_run_with_events(
    def: &AutomationDef,
    executor: &dyn RunExecutor,
    repo: &dyn AutomationRepository,
    completion: &CompletionPolicy,
    delay: Delay,
    now: DateTime<Utc>,
    sink: Arc<dyn RunEventSink>,
) -> RunOutcome {
    let policy = retry_policy_for(def);
    let mut attempt: u32 = 0;
    let req = dispatch_request_for(def);

    // Create + persist the initial run record.
    let mut run = AutomationRun::new(def.id.as_str().to_string());
    run.attempt = attempt;
    run.mark_started();
    persist_run(repo, &run).await;

    loop {
        // PUSH-1: scope the run context around this dispatch_turn so the
        // executor can emit live events. The sink + run id are stable across
        // retries (the same run record is retried, not replaced).
        let ctx = RunContext {
            run_id: run.id.clone(),
            automation_id: run.automation_id.clone(),
            sink: sink.clone(),
        };
        let outcome_res = with_run_context(ctx, executor.dispatch_turn(req.clone())).await;

        match outcome_res {
            Ok(outcome) => {
                // Success signal: a dispatched turn maps to "exit code 0".
                let exit_code = 0;
                run.mark_completed(
                    exit_code,
                    format!("turn {}", outcome.turn_id),
                    String::new(),
                );

                // If the completion policy rejects (e.g. AllowedExitCodes), mark failed.
                if !completion.is_success(exit_code) {
                    run.status = RunStatus::Failed;
                    run.error = Some(format!(
                        "completion policy rejected exit code {}",
                        exit_code
                    ));
                }
                persist_run(repo, &run).await;
                advance_schedule(repo, def, &run.status, now).await;
                return RunOutcome {
                    final_status: run.status.clone(),
                    attempts: attempt + 1,
                };
            }
            Err(err) => {
                // Consult the retry policy for the next delay.
                match policy.delay_for_attempt(attempt) {
                    Some(backoff) if !policy.exhausted(attempt) => {
                        run.mark_retrying(attempt + 1);
                        persist_run(repo, &run).await;
                        delay.wait(backoff).await;
                        attempt += 1;
                        continue;
                    }
                    _ => {
                        // Retries exhausted (or policy is None) → fail permanently.
                        run.mark_failed(format!(
                            "dispatch failed after {} attempt(s): {}",
                            attempt + 1,
                            err
                        ));
                        persist_run(repo, &run).await;
                        advance_schedule(repo, def, &run.status, now).await;
                        return RunOutcome {
                            final_status: RunStatus::Failed,
                            attempts: attempt + 1,
                        };
                    }
                }
            }
        }
    }
}

/// Persist a run through the repository (best-effort — a persistence failure
/// is logged, not propagated, since the run outcome is the primary result).
async fn persist_run(repo: &dyn AutomationRepository, run: &AutomationRun) {
    let payload = serde_json::to_value(run);
    if let Ok(p) = payload
        && let Err(e) = repo.save_run(p).await
    {
        tracing::warn!(error = %e, run_id = %run.id, "failed to persist automation run");
    }
}

/// Advance the def's `next_run_at` after a run completes (success or failure).
/// On success: schedule the next fire. On failure: also schedule the next fire
/// (a failed run doesn't disable the automation — that's a separate policy).
/// MCode's `maybeStopLoop` (stopOnError) is a follow-up.
async fn advance_schedule(
    repo: &dyn AutomationRepository,
    def: &AutomationDef,
    _status: &RunStatus,
    now: DateTime<Utc>,
) {
    let next = schedule::next_fire(&def.schedule, now);
    let next_str = next.map(|dt| dt.to_rfc3339());
    let def_id = def.id.as_str();
    if let Err(e) = repo.advance_next_run_at(&def_id, next_str).await {
        tracing::warn!(error = %e, def_id = %def_id, "failed to advance next_run_at");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use syncode_core::ports::{AutomationRepository, DispatchOutcome, PortError};

    // ─── Test doubles ──────────────────────────────────────────────────

    /// A `RunExecutor` that returns a scripted sequence of outcomes, recording
    /// each request. Thread-safe via Mutex.
    struct RecordedExecutor {
        outcomes: Mutex<std::collections::VecDeque<Result<DispatchOutcome, PortError>>>,
        requests: Mutex<Vec<DispatchRequest>>,
    }

    impl RecordedExecutor {
        fn new(outcomes: Vec<Result<DispatchOutcome, PortError>>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into()),
                requests: Mutex::new(Vec::new()),
            }
        }
        fn requests(&self) -> Vec<DispatchRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl RunExecutor for RecordedExecutor {
        async fn dispatch_turn(&self, req: DispatchRequest) -> Result<DispatchOutcome, PortError> {
            self.requests.lock().unwrap().push(req.clone());
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(DispatchOutcome {
                        thread_id: syncode_core::EntityId::new(),
                        turn_id: syncode_core::EntityId::new(),
                    })
                })
        }
    }

    fn ok_outcome() -> Result<DispatchOutcome, PortError> {
        Ok(DispatchOutcome {
            thread_id: syncode_core::EntityId::new(),
            turn_id: syncode_core::EntityId::new(),
        })
    }

    // ─── dispatch_request_for tests ────────────────────────────────────

    #[test]
    fn dispatch_request_standalone_when_no_target() {
        let mut def = AutomationDef::new(
            "t".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.project_id = Some("00000000-0000-0000-0000-000000000001".into());
        def.provider_id = Some("claude".into());
        def.model = Some("sonnet".into());
        def.prompt_template = Some("do thing".into());

        let req = dispatch_request_for(&def);
        assert!(req.target_thread_id.is_none(), "no target = standalone");
        assert!(req.project_id.is_some());
        assert_eq!(req.provider_id, "claude");
        assert_eq!(req.prompt, "do thing");
    }

    #[test]
    fn dispatch_request_heartbeat_when_target_set() {
        let mut def = AutomationDef::new(
            "t".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.target_thread_id = Some("00000000-0000-0000-0000-000000000002".into());

        let req = dispatch_request_for(&def);
        assert!(req.target_thread_id.is_some(), "target set = heartbeat");
    }

    #[test]
    fn dispatch_request_falls_back_to_command_field() {
        let def = AutomationDef::new(
            "t".into(),
            "legacy-cmd".into(),
            crate::definition::ScheduleType::Manual,
        );
        let req = dispatch_request_for(&def);
        assert_eq!(req.prompt, "legacy-cmd");
    }

    #[test]
    fn retry_policy_derives_from_def() {
        let mut def = AutomationDef::new(
            "t".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.max_retries = 0;
        assert_eq!(retry_policy_for(&def), RetryPolicy::None);

        def.max_retries = 3;
        def.retry_delay_secs = 10;
        assert_eq!(
            retry_policy_for(&def),
            RetryPolicy::FixedDelay {
                max_retries: 3,
                delay_secs: 10
            }
        );
    }

    // ─── execute_run tests (need a repo + executor) ────────────────────
    //
    // These use the InMemoryAutomationRepository from the scheduler module.
    // To avoid a circular dep at test time, we define a minimal in-memory
    // repo inline here.

    use std::sync::Arc;

    use std::collections::HashMap;
    use tokio::sync::RwLock;

    struct TestRepo {
        defs: RwLock<HashMap<String, serde_json::Value>>,
        runs: RwLock<Vec<serde_json::Value>>,
        advanced: RwLock<HashMap<String, Option<String>>>,
    }

    impl TestRepo {
        fn new() -> Self {
            Self {
                defs: RwLock::new(HashMap::new()),
                runs: RwLock::new(Vec::new()),
                advanced: RwLock::new(HashMap::new()),
            }
        }
        async fn advance_recorded(&self, id: &str) -> Option<Option<String>> {
            self.advanced.read().await.get(id).cloned()
        }
    }

    #[async_trait::async_trait]
    impl AutomationRepository for TestRepo {
        async fn save_def(&self, _id: &str, payload: serde_json::Value) -> Result<(), PortError> {
            self.defs.write().await.insert(_id.into(), payload);
            Ok(())
        }
        async fn get_def(&self, id: &str) -> Result<Option<serde_json::Value>, PortError> {
            Ok(self.defs.read().await.get(id).cloned())
        }
        async fn list_defs(&self) -> Result<Vec<serde_json::Value>, PortError> {
            Ok(self.defs.read().await.values().cloned().collect())
        }
        async fn delete_def(&self, id: &str) -> Result<bool, PortError> {
            Ok(self.defs.write().await.remove(id).is_some())
        }
        async fn save_run(&self, payload: serde_json::Value) -> Result<(), PortError> {
            self.runs.write().await.push(payload);
            Ok(())
        }
        async fn get_run(&self, _id: &str) -> Result<Option<serde_json::Value>, PortError> {
            Ok(None)
        }
        async fn list_runs(
            &self,
            _automation_id: &str,
        ) -> Result<Vec<serde_json::Value>, PortError> {
            Ok(Vec::new())
        }
        async fn advance_next_run_at(
            &self,
            id: &str,
            next_run_at: Option<String>,
        ) -> Result<(), PortError> {
            self.advanced.write().await.insert(id.into(), next_run_at);
            Ok(())
        }
    }

    fn repo() -> Arc<TestRepo> {
        Arc::new(TestRepo::new())
    }

    fn def_with_retries(max: u32, delay_secs: u64) -> AutomationDef {
        let mut d = AutomationDef::new(
            "t".into(),
            "echo".into(),
            crate::definition::ScheduleType::Interval(60),
        );
        d.max_retries = max;
        d.retry_delay_secs = delay_secs;
        d
    }

    #[tokio::test]
    async fn execute_run_succeeds_first_try() {
        let repo = repo();
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        let def = def_with_retries(3, 1);
        let now = Utc::now();

        let outcome = execute_run(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            now,
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        assert_eq!(outcome.attempts, 1);
        // One dispatch request (no retries).
        assert_eq!(executor.requests().len(), 1);
        // Schedule advanced.
        let def_id = def.id.as_str();
        assert!(repo.advance_recorded(&def_id).await.is_some());
    }

    #[tokio::test]
    async fn execute_run_retries_then_succeeds() {
        let repo = repo();
        // Fail twice, then succeed.
        let executor = RecordedExecutor::new(vec![
            Err(PortError::Internal("boom".into())),
            Err(PortError::Internal("boom".into())),
            ok_outcome(),
        ]);
        let def = def_with_retries(5, 1);

        let outcome = execute_run(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        assert_eq!(outcome.attempts, 3);
        assert_eq!(executor.requests().len(), 3);
    }

    #[tokio::test]
    async fn execute_run_retries_exhausted_then_failed() {
        let repo = repo();
        // Always fail; max_retries=2 → 3 total attempts (0,1,2).
        let executor = RecordedExecutor::new(vec![
            Err(PortError::Internal("boom".into())),
            Err(PortError::Internal("boom".into())),
            Err(PortError::Internal("boom".into())),
        ]);
        let def = def_with_retries(2, 1);

        let outcome = execute_run(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Failed);
        assert_eq!(outcome.attempts, 3);
    }

    #[tokio::test]
    async fn execute_run_no_retry_policy_fails_immediately() {
        let repo = repo();
        let executor = RecordedExecutor::new(vec![Err(PortError::Internal("boom".into()))]);
        let def = def_with_retries(0, 0); // RetryPolicy::None

        let outcome = execute_run(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Failed);
        assert_eq!(outcome.attempts, 1);
        assert_eq!(executor.requests().len(), 1); // no retry
    }

    // ─── execute_run_with_events tests (PUSH-1) ───────────────────────
    //
    // Verifies the live-push variant has the same retry/outcome behavior as
    // execute_run AND that the run context is installed during dispatch.

    use crate::events::{NoopRunEventSink, RunEventSink};

    /// A sink that records events + counts how many were emitted *during* the
    /// dispatch_turn call (i.e. while the context was active).
    struct RecordingSink {
        events: Mutex<Vec<crate::events::RunEvent>>,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                events: Mutex::new(Vec::new()),
            })
        }
    }

    impl RunEventSink for RecordingSink {
        fn emit(
            &self,
            event: crate::events::RunEvent,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                self.events.lock().unwrap().push(event);
            })
        }
    }

    /// An executor that asserts the run context is active when it's called.
    struct ContextCheckingExecutor {
        saw_context: Mutex<bool>,
    }

    #[async_trait::async_trait]
    impl RunExecutor for ContextCheckingExecutor {
        async fn dispatch_turn(&self, _req: DispatchRequest) -> Result<DispatchOutcome, PortError> {
            // The whole point of execute_run_with_events: a context must be
            // installed for the duration of dispatch_turn.
            if crate::events::current_run_context().is_some() {
                *self.saw_context.lock().unwrap() = true;
            }
            ok_outcome()
        }
    }

    #[tokio::test]
    async fn execute_run_with_events_succeeds_and_installs_context() {
        let repo = repo();
        let executor = ContextCheckingExecutor {
            saw_context: Mutex::new(false),
        };
        let def = def_with_retries(3, 1);

        let sink: Arc<dyn RunEventSink> = RecordingSink::new();
        let outcome = execute_run_with_events(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            sink,
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        assert_eq!(outcome.attempts, 1);
        assert!(
            *executor.saw_context.lock().unwrap(),
            "execute_run_with_events must install a RunContext during dispatch_turn"
        );
    }

    #[tokio::test]
    async fn execute_run_with_events_retries_then_succeeds_with_context_each_attempt() {
        let repo = repo();
        // Fail twice, then succeed — and record whether the context was live
        // on each attempt.
        let attempts_with_ctx = Arc::new(Mutex::new(Vec::<bool>::new()));
        struct RetryExecutor {
            outcomes: Mutex<std::collections::VecDeque<Result<DispatchOutcome, PortError>>>,
            log: Arc<Mutex<Vec<bool>>>,
        }
        #[async_trait::async_trait]
        impl RunExecutor for RetryExecutor {
            async fn dispatch_turn(
                &self,
                _req: DispatchRequest,
            ) -> Result<DispatchOutcome, PortError> {
                let has_ctx = crate::events::current_run_context().is_some();
                self.log.lock().unwrap().push(has_ctx);
                match self.outcomes.lock().unwrap().pop_front() {
                    Some(o) => o,
                    None => ok_outcome(),
                }
            }
        }
        let executor = RetryExecutor {
            outcomes: Mutex::new(
                vec![
                    Err(PortError::Internal("boom".into())),
                    Err(PortError::Internal("boom".into())),
                    ok_outcome(),
                ]
                .into(),
            ),
            log: attempts_with_ctx.clone(),
        };

        let def = def_with_retries(5, 1);
        let sink: Arc<dyn RunEventSink> = RecordingSink::new();
        let outcome = execute_run_with_events(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            sink,
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        assert_eq!(outcome.attempts, 3);
        // Every attempt saw a context (retry re-scopes per dispatch_turn).
        let log = attempts_with_ctx.lock().unwrap();
        assert_eq!(log.len(), 3, "one dispatch per attempt");
        assert!(
            log.iter().all(|&x| x),
            "context must be live on every attempt: {log:?}"
        );
    }

    #[tokio::test]
    async fn execute_run_with_events_with_noop_sink_matches_execute_run_behavior() {
        let repo = repo();
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        let def = def_with_retries(3, 1);

        let sink: Arc<dyn RunEventSink> = Arc::new(NoopRunEventSink);
        let outcome = execute_run_with_events(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            sink,
        )
        .await;

        // With a no-op sink, behavior is indistinguishable from execute_run.
        assert_eq!(outcome.final_status, RunStatus::Completed);
        assert_eq!(outcome.attempts, 1);
        assert_eq!(executor.requests().len(), 1);
    }
}

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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use syncode_core::ports::{AutomationRepository, DispatchRequest, RunExecutor};

use crate::completion_harness::{CompletionHarness, CompletionJob, completion_run_context};
use crate::definition::AutomationDef;
use crate::events::{RunContext, RunEventSink, with_run_context};
use crate::policies::{CompletionPolicy, RetryPolicy};
use crate::runner::{AutomationRun, RunStatus};
use crate::schedule;
use crate::worktree::{WorktreeError, WorktreeManager};

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

/// Cross-cutting run dependencies that are OPTIONAL for the basic retry loop.
///
/// Both fields feed follow-up features layered on top of the core dispatch
/// loop and are absent by default (so the historical `execute_run` behavior is
/// preserved when callers pass [`RunDeps::default`]):
///
/// - `repo_root` (P2-8): when `Some` and the def's [`WorktreeMode`] requests
///   isolation, the run executes inside a freshly-created git worktree rooted
///   here. The path is set on [`DispatchRequest::working_dir`] so a
///   participating executor (e.g. `ProcessRunExecutor`) runs the command there.
/// - `completion_harness` (P2-2): when `Some` and the def's completion policy
///   is [`AiEvaluated`](CompletionPolicy::AiEvaluated) and the run succeeded,
///   a [`CompletionJob`] is submitted for off-band LLM evaluation.
///
/// [`WorktreeMode`]: crate::worktree::WorktreeMode
#[derive(Clone, Default)]
pub struct RunDeps {
    /// Repo root for git-worktree isolation (P2-8). `None` = never isolate.
    pub repo_root: Option<PathBuf>,
    /// The completion harness to receive AI-evaluated completion jobs (P2-2).
    /// `None` = completion checks are skipped (the run outcome stands as-is).
    pub completion_harness: Option<Arc<CompletionHarness>>,
}

impl RunDeps {
    /// Builder: set the repo root for worktree isolation.
    pub fn with_repo_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.repo_root = Some(root.into());
        self
    }

    /// Builder: set the completion harness for AI-evaluated completion checks.
    pub fn with_completion_harness(mut self, harness: Arc<CompletionHarness>) -> Self {
        self.completion_harness = Some(harness);
        self
    }
}

/// The outcome of executing a run (possibly after retries).
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub final_status: RunStatus,
    pub attempts: u32,
    /// Whether the automation should be auto-disabled after this run (P2-6).
    ///
    /// Set when `iteration_count` reached `max_iterations`, or when
    /// `stop_on_error` is `true` and the run failed. The caller (scheduler /
    /// host) is responsible for flipping the def's `enabled` flag; the
    /// executor only signals the condition (it does not mutate the stored
    /// def's `enabled` field directly, to avoid racing concurrent edits).
    pub should_disable: bool,
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
        // P2-8: worktree isolation, when active, overrides this after the
        // request is built (see `execute_run_inner`). Default: host CWD.
        working_dir: None,
    }
}

/// Execute a single automation run, applying the retry policy on failure.
///
/// Creates a run record, dispatches the turn, and on failure retries per the
/// policy (up to `max_retries`), then records the final outcome + advances
/// `next_run_at`. Returns the final status + attempt count.
///
/// **P2-6 / P2-7** — the executor now also:
/// - enforces `max_runtime_seconds` (P2-7): the entire dispatch + retry loop
///   is wrapped in a wall-clock timeout; on expiry the run is failed with a
///   timeout error;
/// - tracks `iteration_count` and signals auto-disable via
///   [`RunOutcome::should_disable`] when `max_iterations` is reached or
///   `stop_on_error` fires (P2-6). The updated iteration count is persisted
///   via [`increment_def_iterations`].
///
/// `now` is injected (not read from the system clock) so tests are deterministic.
///
/// This entry point delegates with [`RunDeps::default`] — no worktree isolation
/// and no AI-completion harness. To opt into the P2-2 / P2-8 features, use
/// [`execute_run_with_deps`].
pub async fn execute_run(
    def: &AutomationDef,
    executor: &dyn RunExecutor,
    repo: &dyn AutomationRepository,
    completion: &CompletionPolicy,
    delay: Delay,
    now: DateTime<Utc>,
) -> RunOutcome {
    execute_run_inner(
        def,
        executor,
        repo,
        completion,
        delay,
        now,
        None,
        &RunDeps::default(),
    )
    .await
}

/// Execute a run with the full set of optional cross-cutting [`RunDeps`] — the
/// entry point that activates P2-2 (AI-completion harness) and P2-8 (worktree
/// isolation). Behavior matches [`execute_run`] when `deps` is default.
pub async fn execute_run_with_deps(
    def: &AutomationDef,
    executor: &dyn RunExecutor,
    repo: &dyn AutomationRepository,
    completion: &CompletionPolicy,
    delay: Delay,
    now: DateTime<Utc>,
    deps: &RunDeps,
) -> RunOutcome {
    execute_run_inner(def, executor, repo, completion, delay, now, None, deps).await
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
/// The retry loop, run-record lifecycle, schedule advance, P2-6 auto-disable,
/// and P2-7 runtime timeout are identical to [`execute_run`]. The only
/// addition is the run-context scope around the dispatch call.
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
    execute_run_inner(
        def,
        executor,
        repo,
        completion,
        delay,
        now,
        Some(sink),
        &RunDeps::default(),
    )
    .await
}

/// Execute a run with live event push **and** the full [`RunDeps`] — the
/// live-push variant of [`execute_run_with_deps`]. Activates P2-2 / P2-8.
#[allow(clippy::too_many_arguments)]
pub async fn execute_run_with_events_and_deps(
    def: &AutomationDef,
    executor: &dyn RunExecutor,
    repo: &dyn AutomationRepository,
    completion: &CompletionPolicy,
    delay: Delay,
    now: DateTime<Utc>,
    sink: Arc<dyn RunEventSink>,
    deps: &RunDeps,
) -> RunOutcome {
    execute_run_inner(
        def,
        executor,
        repo,
        completion,
        delay,
        now,
        Some(sink),
        deps,
    )
    .await
}

/// Shared retry loop backing both [`execute_run`] and [`execute_run_with_events`].
///
/// `sink` selects the dispatch path:
/// - `None` → plain `dispatch_turn` (synchronous trigger contract).
/// - `Some` → scopes a [`RunContext`] around each dispatch (live push).
///
/// P2-6: after the loop, `iteration_count` is incremented + persisted and the
/// auto-disable condition is computed into [`RunOutcome::should_disable`].
/// P2-7: the dispatch+retry loop is wrapped in `tokio::time::timeout(
/// max_runtime_seconds)` when the def sets that cap.
#[allow(clippy::too_many_arguments)]
async fn execute_run_inner(
    def: &AutomationDef,
    executor: &dyn RunExecutor,
    repo: &dyn AutomationRepository,
    completion: &CompletionPolicy,
    delay: Delay,
    now: DateTime<Utc>,
    sink: Option<Arc<dyn RunEventSink>>,
    deps: &RunDeps,
) -> RunOutcome {
    let policy = retry_policy_for(def);
    let mut attempt: u32 = 0;
    let mut req = dispatch_request_for(def);

    // Create + persist the initial run record.
    let mut run = AutomationRun::new(def.id.as_str().to_string());
    run.attempt = attempt;
    run.mark_started();
    persist_run(repo, &run).await;

    // P2-8: worktree isolation. When the host supplied a repo_root and the
    // def opts into isolation, create a worktree before dispatch and route the
    // command into it via `req.working_dir`. A creation failure is fatal —
    // isolation was requested and we must NOT silently fall back to the repo
    // root (the command could then mutate the user's working tree).
    let worktree: Option<(PathBuf, PathBuf)> = match deps.repo_root.as_deref() {
        Some(root) => match setup_worktree_for_run(def, root, &run.id).await {
            Some(Ok(path)) => {
                req.working_dir = Some(path.to_string_lossy().into_owned());
                Some((root.to_path_buf(), path))
            }
            Some(Err(e)) => {
                // Isolation requested but unavailable → fail the run with a
                // clear error. Do NOT fall back to the repo root.
                tracing::warn!(
                    automation_id = %def.id,
                    run_id = %run.id,
                    error = %e,
                    "worktree isolation requested but unavailable; failing run"
                );
                run.mark_failed(format!("worktree isolation requested but unavailable: {e}"));
                persist_run(repo, &run).await;
                let final_status = run.status.clone();
                advance_schedule(repo, def, &final_status, now).await;
                let new_count = increment_def_iterations(repo, def).await;
                let should_disable = should_disable_after(def, new_count, &final_status);
                return RunOutcome {
                    final_status,
                    attempts: attempt + 1,
                    should_disable,
                };
            }
            None => None, // Local mode or heartbeat run — no isolation.
        },
        None => None, // No repo_root supplied — isolation impossible.
    };

    // P2-7: optional wall-clock cap on the entire dispatch+retry loop. The
    // retry delays count against the budget — correct, since
    // max_runtime_seconds is the total wall-clock for the run, not per-attempt.
    let runtime_cap = def_max_runtime(def);
    let inputs = DispatchLoopInputs {
        run: &mut run,
        req,
        policy: &policy,
        completion,
        delay,
        repo,
        attempt: &mut attempt,
        executor,
        sink: sink.as_ref(),
    };
    let loop_fut = dispatch_retry_loop(inputs);

    let loop_result = match runtime_cap {
        Some(cap) => match tokio::time::timeout(cap, loop_fut).await {
            Ok(()) => LoopResult::Done,
            Err(_) => LoopResult::TimedOut,
        },
        None => {
            loop_fut.await;
            LoopResult::Done
        }
    };

    // P2-7: if the runtime cap fired, fail the run with a timeout (unless it
    // already reached a terminal state from the dispatch itself).
    let final_status = match loop_result {
        LoopResult::Done => run.status.clone(),
        LoopResult::TimedOut => {
            if !run.status.is_terminal() {
                run.mark_timed_out();
                persist_run(repo, &run).await;
            }
            run.status.clone()
        }
    };

    // P2-8: cleanup the worktree on run end (success OR failure). Best-effort
    // — a failure is logged by `cleanup_worktree`. MCode leaves the worktree
    // in place on success for inspection; we remove unconditionally to avoid
    // accumulating `automation/<slug>/<run-id>` dirs across many runs. Callers
    // that want to preserve a run's worktree can re-`git worktree add` it.
    if let Some((root, path)) = worktree.as_ref() {
        cleanup_worktree(root, path).await;
    }

    advance_schedule(repo, def, &final_status, now).await;

    // P2-6: increment iteration_count + persist, then decide auto-disable.
    let new_count = increment_def_iterations(repo, def).await;
    let should_disable = should_disable_after(def, new_count, &final_status);

    // P2-2: submit an AI-evaluated completion job when the def asks for one and
    // the run reached a success state. Off-band (non-blocking); the harness's
    // worker pool does the slow LLM call + disable. The assistant text is the
    // run's recorded output (the orchestration layer's richer turn text isn't
    // visible at this layer) — adequate for the evaluator's evidence channel.
    if matches!(def.completion_policy, CompletionPolicy::AiEvaluated { .. })
        && final_status.is_success()
        && let Some(harness) = deps.completion_harness.as_ref()
    {
        let assistant_text = assistant_text_for_completion(&run);
        let job = CompletionJob {
            def: Arc::new(def.clone()),
            run: completion_run_context(&run.id, &def.id.as_str()),
            assistant_text,
        };
        let _ = harness.submit(job); // best-effort; outcome logged inside submit
    }

    RunOutcome {
        final_status,
        attempts: attempt + 1,
        should_disable,
    }
}

/// P2-2: derive the assistant text evidence for the completion evaluator from
/// the run record. Uses `stdout` (the dispatch summary / captured output);
/// falls back to `error` and then a placeholder when the run produced no text.
fn assistant_text_for_completion(run: &AutomationRun) -> String {
    if !run.stdout.is_empty() {
        run.stdout.clone()
    } else if let Some(e) = run.error.as_ref().filter(|s| !s.is_empty()) {
        e.clone()
    } else {
        "(run produced no output)".to_string()
    }
}

/// The outcome of the dispatch loop: either it completed naturally (`Done`)
/// or was killed by the P2-7 runtime timeout (`TimedOut`).
#[derive(Debug, Clone, Copy)]
enum LoopResult {
    Done,
    TimedOut,
}

/// Bundled inputs for [`dispatch_retry_loop`] — avoids the clippy
/// `too_many_arguments` lint (9 params > 7 limit) without scattering the
/// state across loose locals.
struct DispatchLoopInputs<'a> {
    run: &'a mut AutomationRun,
    req: DispatchRequest,
    policy: &'a RetryPolicy,
    completion: &'a CompletionPolicy,
    delay: Delay,
    repo: &'a dyn AutomationRepository,
    attempt: &'a mut u32,
    executor: &'a dyn RunExecutor,
    sink: Option<&'a Arc<dyn RunEventSink>>,
}

/// The core retry loop, factored out of [`execute_run_inner`] so it can be
/// wrapped in a runtime timeout (P2-7). Mutates `run` + `attempt` in place.
async fn dispatch_retry_loop(inputs: DispatchLoopInputs<'_>) {
    let DispatchLoopInputs {
        run,
        req,
        policy,
        completion,
        delay,
        repo,
        attempt,
        executor,
        sink,
    } = inputs;

    loop {
        // PUSH-1: when a sink is supplied, scope a RunContext around this
        // dispatch_turn so the executor can emit live events. The sink + run
        // id are stable across retries (the same run record is retried).
        let outcome_res = match sink {
            None => executor.dispatch_turn(req.clone()).await,
            Some(s) => {
                let ctx = RunContext {
                    run_id: run.id.clone(),
                    automation_id: run.automation_id.clone(),
                    sink: Arc::clone(s),
                };
                with_run_context(ctx, executor.dispatch_turn(req.clone())).await
            }
        };

        match outcome_res {
            Ok(outcome) => {
                // Success signal: a dispatched turn maps to "exit code 0".
                let exit_code = 0;
                // Surface the assistant's finalized text in the run record when
                // the executor provided it (e.g. `OrchestrationRunExecutor`
                // driving the provider via `ApplicationService::start_turn`).
                // Fall back to the legacy `turn <id>` placeholder when the
                // executor is process-backed or otherwise has no assistant
                // text — preserves the historical run-record shape.
                let stdout = outcome
                    .assistant_output
                    .clone()
                    .unwrap_or_else(|| format!("turn {}", outcome.turn_id));
                run.mark_completed(exit_code, stdout, String::new());

                // If the completion policy rejects (e.g. AllowedExitCodes), mark failed.
                if !completion.is_success(exit_code) {
                    run.status = RunStatus::Failed;
                    run.error = Some(format!(
                        "completion policy rejected exit code {}",
                        exit_code
                    ));
                }
                persist_run(repo, run).await;
                return;
            }
            Err(err) => {
                // Consult the retry policy for the next delay.
                match policy.delay_for_attempt(*attempt) {
                    Some(backoff) if !policy.exhausted(*attempt) => {
                        run.mark_retrying(*attempt + 1);
                        persist_run(repo, run).await;
                        delay.wait(backoff).await;
                        *attempt += 1;
                        continue;
                    }
                    _ => {
                        // Retries exhausted (or policy is None) → fail permanently.
                        run.mark_failed(format!(
                            "dispatch failed after {} attempt(s): {}",
                            *attempt + 1,
                            err
                        ));
                        persist_run(repo, run).await;
                        return;
                    }
                }
            }
        }
    }
}

/// P2-7: read `max_runtime_seconds` from the def as a `Duration` (if set).
fn def_max_runtime(def: &AutomationDef) -> Option<Duration> {
    def.max_runtime_seconds.map(Duration::from_secs)
}

/// P2-6: increment the def's `iteration_count` in storage and return the new
/// value. Reads the current stored def (so the count is accurate even if the
/// in-memory `def` argument is stale), bumps it, and persists. On failure
/// (def missing / serialization error), falls back to the in-memory count + 1
/// and logs — the run outcome is the primary result.
async fn increment_def_iterations(repo: &dyn AutomationRepository, def: &AutomationDef) -> u32 {
    let id = def.id.as_str();
    // Load the freshest stored def so we increment the persisted count (not a
    // potentially-stale in-memory snapshot).
    let current = match repo.get_def(&id).await {
        Ok(Some(payload)) => serde_json::from_value::<AutomationDef>(payload)
            .map(|d| d.iteration_count)
            .ok(),
        _ => None,
    };
    let new_count = match current {
        Some(c) => c.saturating_add(1),
        None => def.iteration_count.saturating_add(1),
    };

    // Persist by loading the def, bumping the field, and saving. We re-load
    // (not mutate `def`) so we don't clobber concurrent edits to other fields.
    if let Ok(Some(payload)) = repo.get_def(&id).await
        && let Ok(mut stored) = serde_json::from_value::<AutomationDef>(payload)
    {
        stored.iteration_count = new_count;
        if let Ok(p) = serde_json::to_value(&stored)
            && let Err(e) = repo.save_def(&id, p).await
        {
            tracing::warn!(error = %e, %id, count = new_count, "failed to persist iteration_count");
        }
    }
    new_count
}

/// P2-6: decide whether the automation should be auto-disabled after this run.
///
/// - `max_iterations` reached → disable (regardless of success/failure).
/// - `stop_on_error` and the run failed → disable.
pub(crate) fn should_disable_after(
    def: &AutomationDef,
    new_iteration_count: u32,
    final_status: &RunStatus,
) -> bool {
    // Max-iterations cap.
    if let Some(cap) = def.max_iterations
        && new_iteration_count >= cap
    {
        return true;
    }
    // stop_on_error: a failed run disables the automation.
    if def.stop_on_error && matches!(final_status, RunStatus::Failed) {
        return true;
    }
    false
}

/// P2-8: create a git worktree for a standalone run when `worktree_mode`
/// requests isolation, returning the worktree path. Returns `None` when
/// isolation is off (`Local`) or the run is a heartbeat (`target_thread_id`
/// is set — worktrees are for standalone runs only).
///
/// The caller is responsible for removing the worktree on failure (see
/// [`cleanup_worktree_on_failure`]).
pub async fn setup_worktree_for_run(
    def: &AutomationDef,
    repo_root: &std::path::Path,
    run_suffix: &str,
) -> Option<Result<PathBuf, WorktreeError>> {
    if !WorktreeManager::should_isolate(def.worktree_mode) {
        return None;
    }
    // Heartbeat runs (target_thread_id set) append to an existing thread —
    // worktree isolation is for standalone runs.
    if def.target_thread_id.is_some() {
        return None;
    }
    let mgr = WorktreeManager::new(repo_root);
    Some(mgr.create(&def.name, run_suffix).await)
}

/// P2-8: remove a worktree on run failure (cleanup). Best-effort — logs on
/// error. Called by the host/scheduler after a run that used worktree
/// isolation ends in failure.
///
/// Kept for backward compatibility with external callers; the run loop now
/// uses [`cleanup_worktree`] (unconditional) on every run end. This wrapper is
/// a thin alias.
pub async fn cleanup_worktree_on_failure(
    repo_root: &std::path::Path,
    worktree_path: &std::path::Path,
) {
    cleanup_worktree(repo_root, worktree_path).await;
}

/// P2-8: remove a worktree on run end (success OR failure). Best-effort — a
/// removal failure is logged (never propagated). The run loop calls this for
/// every isolated run regardless of outcome; a host that wants to preserve a
/// successful run's worktree should bypass the run-loop isolation and manage
/// the worktree itself.
pub async fn cleanup_worktree(repo_root: &Path, worktree_path: &Path) {
    let mgr = WorktreeManager::new(repo_root);
    if let Err(e) = mgr.remove(worktree_path).await {
        tracing::warn!(error = %e, path = %worktree_path.display(), "failed to clean up worktree");
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
                    Ok(DispatchOutcome::new(
                        syncode_core::EntityId::new(),
                        syncode_core::EntityId::new(),
                    ))
                })
        }
    }

    fn ok_outcome() -> Result<DispatchOutcome, PortError> {
        Ok(DispatchOutcome::new(
            syncode_core::EntityId::new(),
            syncode_core::EntityId::new(),
        ))
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

    // ─── P2-6: maxIterations / stopOnError / iterationCount ────────────

    /// Helper: register a def into the TestRepo so increment_def_iterations
    /// can load + persist it.
    async fn register_def(repo: &Arc<TestRepo>, def: &AutomationDef) {
        let payload = serde_json::to_value(def).unwrap();
        repo.save_def(&def.id.as_str(), payload).await.unwrap();
    }

    /// Read the persisted iteration_count for a def from the TestRepo.
    async fn persisted_iteration_count(repo: &Arc<TestRepo>, def: &AutomationDef) -> u32 {
        let p = repo.get_def(&def.id.as_str()).await.unwrap().unwrap();
        serde_json::from_value::<AutomationDef>(p)
            .unwrap()
            .iteration_count
    }

    #[tokio::test]
    async fn p2_6_max_iterations_signals_disable_when_cap_reached() {
        let repo = repo();
        // Cap at 1 iteration. A single successful run should bump the count
        // to 1 and signal disable.
        let mut def = def_with_retries(3, 1);
        def.max_iterations = Some(1);
        register_def(&repo, &def).await;

        let executor = RecordedExecutor::new(vec![ok_outcome()]);
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
        assert!(
            outcome.should_disable,
            "iteration_count reached max_iterations → should_disable"
        );
        // Persisted count advanced to 1.
        assert_eq!(persisted_iteration_count(&repo, &def).await, 1);
    }

    #[tokio::test]
    async fn p2_6_max_iterations_not_reached_does_not_disable() {
        let repo = repo();
        // Cap at 5; one run leaves us at 1 — under the cap.
        let mut def = def_with_retries(3, 1);
        def.max_iterations = Some(5);
        register_def(&repo, &def).await;

        let executor = RecordedExecutor::new(vec![ok_outcome()]);
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
        assert!(
            !outcome.should_disable,
            "iteration_count (1) < max_iterations (5) → no disable"
        );
        assert_eq!(persisted_iteration_count(&repo, &def).await, 1);
    }

    #[tokio::test]
    async fn p2_6_stop_on_error_disables_after_failed_run() {
        let repo = repo();
        let mut def = def_with_retries(0, 0); // no retries → fails immediately
        def.stop_on_error = true;
        register_def(&repo, &def).await;

        let executor = RecordedExecutor::new(vec![Err(PortError::Internal("boom".into()))]);
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
        assert!(
            outcome.should_disable,
            "stop_on_error=true + failed run → should_disable"
        );
    }

    #[tokio::test]
    async fn p2_6_stop_on_error_false_does_not_disable_on_failure() {
        let repo = repo();
        let mut def = def_with_retries(0, 0);
        def.stop_on_error = false; // default
        register_def(&repo, &def).await;

        let executor = RecordedExecutor::new(vec![Err(PortError::Internal("boom".into()))]);
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
        assert!(
            !outcome.should_disable,
            "stop_on_error=false → a failed run does NOT disable"
        );
    }

    #[tokio::test]
    async fn p2_6_no_max_iterations_no_stop_on_error_never_disables() {
        let repo = repo();
        let def = def_with_retries(0, 0);
        register_def(&repo, &def).await;

        let executor = RecordedExecutor::new(vec![Err(PortError::Internal("boom".into()))]);
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
        assert!(!outcome.should_disable, "no caps → never disables");
    }

    #[test]
    fn p2_6_should_disable_after_unit_logic() {
        let mut def = AutomationDef::new(
            "t".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );

        // No caps → never disable.
        assert!(!should_disable_after(&def, 1, &RunStatus::Failed));
        assert!(!should_disable_after(&def, 100, &RunStatus::Completed));

        // max_iterations reached → disable regardless of status.
        def.max_iterations = Some(3);
        assert!(!should_disable_after(&def, 2, &RunStatus::Completed));
        assert!(should_disable_after(&def, 3, &RunStatus::Completed));
        assert!(should_disable_after(&def, 4, &RunStatus::Failed)); // over cap

        // stop_on_error.
        def.max_iterations = None;
        def.stop_on_error = true;
        assert!(should_disable_after(&def, 1, &RunStatus::Failed));
        assert!(!should_disable_after(&def, 1, &RunStatus::Completed));

        // TimedOut also counts as a failure for stop_on_error? It's a distinct
        // terminal status — we only auto-disable on `Failed`, matching MCode's
        // stopOnError semantics (a timeout is a separate condition).
        assert!(
            !should_disable_after(&def, 1, &RunStatus::TimedOut),
            "TimedOut is not Failed — stop_on_error targets Failed only"
        );
    }

    #[tokio::test]
    async fn p2_6_iteration_count_accumulates_across_runs() {
        let repo = repo();
        let mut def = def_with_retries(0, 0);
        def.max_iterations = Some(2);
        register_def(&repo, &def).await;

        // Run 1: count → 1, not yet at cap.
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        let outcome = execute_run(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
        )
        .await;
        assert_eq!(persisted_iteration_count(&repo, &def).await, 1);
        assert!(!outcome.should_disable);

        // Run 2: count → 2, cap reached → disable.
        let executor2 = RecordedExecutor::new(vec![ok_outcome()]);
        let outcome2 = execute_run(
            &def,
            &executor2,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
        )
        .await;
        assert_eq!(persisted_iteration_count(&repo, &def).await, 2);
        assert!(outcome2.should_disable, "second run reached the cap");
    }

    // ─── P2-7: maxRuntimeSeconds ───────────────────────────────────────

    #[tokio::test]
    async fn p2_7_max_runtime_seconds_times_out_long_running_run() {
        let repo = repo();
        // An executor whose dispatch never resolves (pending forever). The
        // runtime timeout must fire and fail the run as TimedOut.
        struct HangingExecutor;
        #[async_trait::async_trait]
        impl RunExecutor for HangingExecutor {
            async fn dispatch_turn(
                &self,
                _req: DispatchRequest,
            ) -> Result<DispatchOutcome, PortError> {
                // Never resolves within the test's lifetime.
                std::future::pending::<()>().await;
                unreachable!("pending should never resolve")
            }
        }

        // max_runtime_seconds = 1 (the field is u64 seconds). The hanging
        // executor ensures the timeout, not the dispatch, ends the run.
        let mut def = def_with_retries(5, 1);
        def.max_runtime_seconds = Some(1);
        register_def(&repo, &def).await;

        let executor = HangingExecutor;
        let start = std::time::Instant::now();
        let outcome = execute_run(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
        )
        .await;
        let elapsed = start.elapsed();

        assert_eq!(
            outcome.final_status,
            RunStatus::TimedOut,
            "run exceeding max_runtime_seconds must be TimedOut"
        );
        // TimedOut is distinct from Failed, and max_iterations is unset, so
        // the run should NOT signal auto-disable (stop_on_error targets
        // Failed only; a timeout is a separate condition).
        assert!(
            !outcome.should_disable,
            "TimedOut alone (no max_iterations) should not auto-disable"
        );
        // Sanity: the timeout fired roughly around the 1s mark (allow slack).
        assert!(
            elapsed >= std::time::Duration::from_millis(900),
            "timeout should have waited ~1s, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn p2_7_max_runtime_seconds_allows_fast_run() {
        let repo = repo();
        let mut def = def_with_retries(3, 1);
        def.max_runtime_seconds = Some(60); // generous — run completes well under
        register_def(&repo, &def).await;

        let executor = RecordedExecutor::new(vec![ok_outcome()]);
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
        assert!(!outcome.should_disable, "successful run under the cap");
    }

    #[test]
    fn p2_7_def_max_runtime_reads_field() {
        let mut def = AutomationDef::new(
            "t".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        assert_eq!(def_max_runtime(&def), None, "default: no cap");

        def.max_runtime_seconds = Some(30);
        assert_eq!(
            def_max_runtime(&def),
            Some(std::time::Duration::from_secs(30))
        );
    }

    // ─── P2-8: worktree integration helpers ───────────────────────────

    #[tokio::test]
    async fn p2_8_setup_worktree_returns_none_for_local_mode() {
        let mut def = AutomationDef::new(
            "build".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.worktree_mode = crate::worktree::WorktreeMode::Local;
        let result = setup_worktree_for_run(&def, std::path::Path::new("/repo"), "r1").await;
        assert!(result.is_none(), "Local mode → no worktree setup");
    }

    #[tokio::test]
    async fn p2_8_setup_worktree_returns_none_for_heartbeat_run() {
        // Even with Worktree mode, a heartbeat run (target_thread_id set) is
        // not isolated — worktrees are for standalone runs.
        let mut def = AutomationDef::new(
            "build".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.worktree_mode = crate::worktree::WorktreeMode::Worktree;
        def.target_thread_id = Some("00000000-0000-0000-0000-000000000001".into());
        let result = setup_worktree_for_run(&def, std::path::Path::new("/repo"), "r1").await;
        assert!(
            result.is_none(),
            "heartbeat run → no worktree (standalone-only)"
        );
    }

    // ─── P2-8: worktree working-dir wiring (run loop integration) ────
    //
    // These mirror the real-git harness in worktree.rs::tests: they spin up a
    // throwaway git repo, run `execute_run_with_deps` with `repo_root` set, and
    // assert the dispatched request carries the worktree path AND the worktree
    // is cleaned up on run end.

    fn git_is_available() -> bool {
        std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Create a throwaway git repo with one (empty) commit so worktrees can be
    /// attached. Returns the repo path. Caller cleans up.
    fn make_temp_repo() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "syncode-exec-p2-8-{}",
            uuid::Uuid::new_v4().hyphenated()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let out = std::process::Command::new("git")
            .arg("init")
            .current_dir(&dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "git init failed");
        for (k, v) in &[("user.name", "test"), ("user.email", "t@t.test")] {
            let _ = std::process::Command::new("git")
                .args(["config", k, v])
                .current_dir(&dir)
                .output();
        }
        let out = std::process::Command::new("git")
            .args(["commit", "--allow-empty", "-m", "init"])
            .current_dir(&dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        dir
    }

    fn cleanup_repo(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Build a def whose worktree_mode is Worktree (standalone run).
    fn worktree_def() -> AutomationDef {
        let mut def = AutomationDef::new(
            "ci-build".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.worktree_mode = crate::worktree::WorktreeMode::Worktree;
        def
    }

    #[tokio::test]
    async fn p2_8_worktree_mode_run_dispatches_into_worktree_dir() {
        // Standalone Worktree-mode run → the DispatchRequest handed to
        // dispatch_turn carries working_dir == Some(<worktree path>).
        if !git_is_available() {
            eprintln!("skipping: git not on PATH");
            return;
        }
        let repo_root = make_temp_repo();
        let repo = repo();
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        let def = worktree_def();
        register_def(&repo, &def).await;
        let deps = RunDeps::default().with_repo_root(repo_root.clone());

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            &deps,
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        let reqs = executor.requests();
        assert_eq!(reqs.len(), 1, "one dispatch (no retries)");
        let wd = reqs[0]
            .working_dir
            .as_ref()
            .expect("working_dir must be set");
        assert!(
            wd.contains("automation/ci-build/"),
            "working_dir should be the worktree path, got {wd}"
        );
        assert!(
            wd.starts_with(repo_root.to_str().unwrap()),
            "worktree path should be under repo_root, got {wd}"
        );
        // Cleanup ran — the worktree dir is gone after the run ends.
        assert!(
            !std::path::Path::new(wd).exists(),
            "worktree dir should be removed after run end, still exists: {wd}"
        );
        cleanup_repo(&repo_root);
    }

    #[tokio::test]
    async fn p2_8_local_mode_run_leaves_working_dir_unset() {
        // Local-mode def (default) → no worktree; working_dir on the request
        // is None (unchanged), even when a repo_root is supplied.
        if !git_is_available() {
            eprintln!("skipping: git not on PATH");
            return;
        }
        let repo_root = make_temp_repo();
        let repo = repo();
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        // Default def → worktree_mode = Local.
        let def = def_with_retries(0, 0);
        register_def(&repo, &def).await;
        let deps = RunDeps::default().with_repo_root(repo_root.clone());

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            &deps,
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        let reqs = executor.requests();
        assert_eq!(reqs.len(), 1);
        assert!(
            reqs[0].working_dir.is_none(),
            "Local mode must not set working_dir (got {:?})",
            reqs[0].working_dir
        );
        // No automation/ dir created either.
        assert!(
            !repo_root.join("automation").exists(),
            "Local mode must not create any worktree dir"
        );
        cleanup_repo(&repo_root);
    }

    #[tokio::test]
    async fn p2_8_heartbeat_run_leaves_working_dir_unset() {
        // Worktree-mode def BUT target_thread_id set (heartbeat) → no worktree.
        if !git_is_available() {
            eprintln!("skipping: git not on PATH");
            return;
        }
        let repo_root = make_temp_repo();
        let repo = repo();
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        let mut def = worktree_def();
        def.target_thread_id = Some("00000000-0000-0000-0000-000000000001".into());
        register_def(&repo, &def).await;
        let deps = RunDeps::default().with_repo_root(repo_root.clone());

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            &deps,
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        let reqs = executor.requests();
        assert_eq!(reqs.len(), 1);
        assert!(
            reqs[0].working_dir.is_none(),
            "heartbeat run must not isolate (got {:?})",
            reqs[0].working_dir
        );
        assert!(
            !repo_root.join("automation").exists(),
            "heartbeat run must not create a worktree"
        );
        cleanup_repo(&repo_root);
    }

    #[tokio::test]
    async fn p2_8_worktree_cleaned_up_on_failed_run() {
        // A run whose dispatch fails still cleans up the worktree (cleanup is
        // unconditional on run end). With RetryPolicy::None the failure is
        // terminal after one attempt.
        if !git_is_available() {
            eprintln!("skipping: git not on PATH");
            return;
        }
        let repo_root = make_temp_repo();
        let repo = repo();
        let executor = RecordedExecutor::new(vec![Err(PortError::Internal("boom".into()))]);
        let mut def = worktree_def();
        def.max_retries = 0; // RetryPolicy::None → fail immediately
        register_def(&repo, &def).await;
        let deps = RunDeps::default().with_repo_root(repo_root.clone());

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            &deps,
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Failed);
        let reqs = executor.requests();
        assert_eq!(reqs.len(), 1, "one (failed) dispatch");
        let wd = reqs[0].working_dir.as_ref().expect("worktree was set");
        // The worktree WAS created (dispatch ran inside it)...
        assert!(
            wd.contains("automation/ci-build/"),
            "failed run should still have dispatched into the worktree, got {wd}"
        );
        // ...and cleaned up despite the failure.
        assert!(
            !std::path::Path::new(wd).exists(),
            "worktree must be removed on failure too, still exists: {wd}"
        );
        cleanup_repo(&repo_root);
    }

    #[tokio::test]
    async fn p2_8_worktree_creation_failure_fails_run_with_clear_error() {
        // repo_root points somewhere that is NOT a git repo → `git worktree
        // add` fails → the run fails with a clear error mentioning worktree,
        // and dispatch is NEVER called (no silent fallback to repo root).
        let non_repo = std::env::temp_dir().join(format!(
            "syncode-exec-notrepo-{}",
            uuid::Uuid::new_v4().hyphenated()
        ));
        std::fs::create_dir_all(&non_repo).unwrap();

        let repo = repo();
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        let def = worktree_def();
        register_def(&repo, &def).await;
        let deps = RunDeps::default().with_repo_root(non_repo.clone());

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            &deps,
        )
        .await;

        assert_eq!(
            outcome.final_status,
            RunStatus::Failed,
            "worktree creation failure must fail the run"
        );
        // No dispatch happened — the run failed at the isolation step.
        assert!(
            executor.requests().is_empty(),
            "dispatch must NOT run when worktree isolation requested but unavailable"
        );
        // The persisted run record carries a clear error mentioning worktree.
        let runs = repo.runs.read().await.clone();
        let last = runs.last().expect("run persisted");
        let err = last.get("error").and_then(|v| v.as_str()).unwrap_or("");
        assert!(
            err.contains("worktree"),
            "run error should mention worktree isolation, got: {err}"
        );
        cleanup_repo(&non_repo);
    }

    #[tokio::test]
    async fn p2_8_no_repo_root_skips_isolation_entirely() {
        // Default RunDeps (no repo_root) → no isolation attempt even when the
        // def requests Worktree mode. Preserves the historical behavior.
        let repo = repo();
        let executor = RecordedExecutor::new(vec![ok_outcome()]);
        let def = worktree_def();
        register_def(&repo, &def).await;

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &CompletionPolicy::ExitCodeZero,
            Delay::Immediate,
            Utc::now(),
            &RunDeps::default(),
        )
        .await;

        assert_eq!(outcome.final_status, RunStatus::Completed);
        let reqs = executor.requests();
        assert_eq!(reqs.len(), 1);
        assert!(
            reqs[0].working_dir.is_none(),
            "no repo_root → no isolation, working_dir must stay None"
        );
    }

    // ─── P2-2: completion harness integration (run loop) ──────────────

    #[tokio::test]
    async fn p2_2_ai_evaluated_success_submits_completion_job_and_disables() {
        use crate::completion_harness::{
            CompletionDisableFn, CompletionHarness, HarnessConfig, LlmFn,
        };
        use std::time::Duration;

        // End-to-end through the run loop: an AiEvaluated def that succeeds →
        // execute_run_with_deps submits a CompletionJob → the harness's worker
        // evaluates (canned LLM returns a confident Match) → disable_fn fires.
        let mut def = AutomationDef::new(
            "ai-loop".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.completion_policy = CompletionPolicy::AiEvaluated {
            stop_when: "all tests pass".to_string(),
            confidence_threshold: 0.8,
        };
        let def_id = def.id.as_str().to_string();
        let repo = repo();
        register_def(&repo, &def).await;
        let executor = RecordedExecutor::new(vec![ok_outcome()]);

        // Canned LLM that returns a confident Match.
        struct MatchLlm;
        #[async_trait::async_trait]
        impl LlmFn for MatchLlm {
            async fn call(&self, _p: &str) -> Result<String, String> {
                Ok("CONFIDENCE: 0.95".into())
            }
        }
        let llm: Arc<dyn crate::completion_eval::CompletionLlmCall> = Arc::new(
            crate::completion_harness::ProviderCompletionLlm::new(Arc::new(MatchLlm)),
        );

        struct RecDisable {
            ids: Arc<std::sync::Mutex<Vec<String>>>,
        }
        #[async_trait::async_trait]
        impl CompletionDisableFn for RecDisable {
            async fn disable(
                &self,
                def: &AutomationDef,
                _r: &crate::completion_eval::CompletionResult,
            ) {
                self.ids.lock().unwrap().push(def.id.as_str().to_string());
            }
        }
        let ids: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let disable: Arc<dyn CompletionDisableFn> = Arc::new(RecDisable { ids: ids.clone() });

        let harness = Arc::new(CompletionHarness::start_with(
            repo.clone(),
            llm,
            disable,
            HarnessConfig {
                capacity: 8,
                workers: 1,
                eval_timeout: Duration::from_secs(5),
            },
        ));
        let deps = RunDeps::default().with_completion_harness(harness.clone());

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &def.completion_policy,
            Delay::Immediate,
            Utc::now(),
            &deps,
        )
        .await;
        assert_eq!(outcome.final_status, RunStatus::Completed);

        // Poll for the worker to process the job + disable.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if !ids.lock().unwrap().is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("disable_fn never fired — completion job not submitted/processed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(ids.lock().unwrap().clone(), vec![def_id]);
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn p2_2_no_harness_skips_completion_submission() {
        // Default RunDeps (no harness) → even an AiEvaluated def that succeeds
        // does not submit a job. Behavior is unchanged (the run completes).
        let mut def = AutomationDef::new(
            "ai-noharness".into(),
            "echo".into(),
            crate::definition::ScheduleType::Manual,
        );
        def.completion_policy = CompletionPolicy::AiEvaluated {
            stop_when: "done".to_string(),
            confidence_threshold: 0.9,
        };
        let repo = repo();
        register_def(&repo, &def).await;
        let executor = RecordedExecutor::new(vec![ok_outcome()]);

        let outcome = execute_run_with_deps(
            &def,
            &executor,
            repo.as_ref(),
            &def.completion_policy,
            Delay::Immediate,
            Utc::now(),
            &RunDeps::default(),
        )
        .await;
        assert_eq!(outcome.final_status, RunStatus::Completed);
        // No panic / no submission — the run completed normally.
        assert_eq!(executor.requests().len(), 1);
    }
}

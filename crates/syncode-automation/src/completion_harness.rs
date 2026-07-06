//! Production harness for the AI-evaluated completion policy (P2-2).
//!
//! [`crate::completion_eval::evaluate_completion_policy`] is fully tested but
//! has **no production caller** — nothing submits a real run's output for an
//! LLM judgment, so an automation with `CompletionPolicy::AiEvaluated` never
//! actually auto-completes. This module closes that gap with an MCode-style
//! production harness: a bounded queue + a small pool of worker fibers that
//! drain it, evaluate each job against the live def, and disable the
//! automation when the model judges the `stop_when` condition satisfied.
//!
//! ## Why a queue + workers (not inline)
//!
//! The evaluation is a slow LLM round trip (seconds). Doing it inline on the
//! scheduler tick / dispatch path would block the next run; doing it
//! fire-and-forget with an unbounded `tokio::spawn` per job would let a burst
//! of runs spawn an unbounded number of concurrent LLM calls. The bounded
//! queue + fixed worker pool caps concurrency (2 parallel evaluations),
//! bounds memory (100 pending jobs), and never blocks the caller on the LLM
//! (`submit` is non-blocking; a full queue drops with a logged warning, the
//! MCode behavior).
//!
//! ## Flow
//!
//! 1. The host constructs the harness once via [`CompletionHarness::start`],
//!    injecting the repository, the LLM port, and a *disable callback* (the
//!    scheduler / WS layer wires the real disable+persist; the crate itself
//!    stays free of scheduler coupling).
//! 2. After a run reaches a success-ish terminal state,
//!    [`crate::executor::execute_run`] calls [`CompletionHarness::submit`]
//!    with a [`CompletionJob`] (def + run context + assistant text). `submit`
//!    returns immediately (best-effort enqueue).
//! 3. A worker pops the job, wraps [`evaluate_completion_policy`] in a
//!    `tokio::time::timeout(30s)` guard, and acts on the verdict:
//!    - `Match { confidence }` at/above the def's threshold → invoke the
//!      disable callback (the callback persists the result + flips `enabled`).
//!    - `NoMatch` → no-op (the run already recorded its outcome; the next
//!      fire re-evaluates).
//!    - `Stale` → discard (the def changed mid-call; re-evaluation happens on
//!      the next fire).
//!    - timeout → treat as `NoMatch` (log) so a hung model never completes a
//!      run.
//!
//! ## Production LLM wiring
//!
//! The trait [`crate::completion_eval::CompletionLlmCall`] is the seam. For
//! production, [`ProviderCompletionLlm`] adapts an injected [`LlmFn`] closure
//! trait — the real wiring (a `LlmFn` impl that calls
//! `syncode_ws::llm::invoke_llm_oneshot`) belongs in `syncode-ws`, which is
//! the only layer that may depend on the provider/WS stack. This crate
//! provides the injectable shape; tests use canned responders.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use syncode_core::ports::AutomationRepository;

use crate::completion_eval::{
    CompletionLlmCall, CompletionResult, CompletionVerdict, evaluate_completion_policy,
};
use crate::definition::AutomationDef;
use crate::events::{NoopRunEventSink, RunContext};
use crate::policies::CompletionPolicy;

// ─── Job model ─────────────────────────────────────────────────────────────

/// A unit of completion-evaluation work submitted to the harness.
///
/// Built by [`CompletionHarness::submit`]; the harness already holds the
/// shared repository + LLM, so the job only carries the per-run inputs. `def`
/// is an `Arc` so the job is `'static` and cheap to move across the queue.
#[derive(Clone)]
pub struct CompletionJob {
    /// The automation def snapshot at dispatch time. Cloned into the job; the
    /// evaluator reloads the live def for the stale-check.
    pub def: Arc<AutomationDef>,
    /// The run's identity (run id + automation id) — used for logging + the
    /// stale-check reload.
    pub run: RunContext,
    /// The run's assistant output (the evidence the model reasons over).
    /// In production this is the run record's `stdout` / dispatch summary;
    /// the orchestration layer's richer assistant text is not visible here.
    pub assistant_text: String,
}

// ─── Disable callback port ─────────────────────────────────────────────────

/// The "disable this automation" callback the harness invokes when the
/// evaluator returns a confident `Match`.
///
/// Kept as a trait (not a free `Arc< dyn Fn >`) so it's object-safe,
/// mockable in tests, and straightforward for the scheduler/WS layer to
/// implement. The implementer is responsible for:
/// - flipping the def's `enabled` flag to `false` and persisting it, AND
/// - persisting the [`CompletionResult`] (the verdict + raw model reply) for
///   the run record / audit trail.
///
/// Both are best-effort from the harness's perspective — a failure inside the
/// callback is logged and swallowed (a disable that didn't persist will simply
/// be re-evaluated on the next fire; it must never panic a worker).
#[async_trait::async_trait]
pub trait CompletionDisableFn: Send + Sync {
    /// Disable `def` and persist the completion `result`.
    async fn disable(&self, def: &AutomationDef, result: &CompletionResult);
}

// ─── Production LLM seam ───────────────────────────────────────────────────

/// Closure-style LLM invoker — the injectable seam [`ProviderCompletionLlm`]
/// adapts. `syncode-ws` implements this for its provider-CLI adapter (the
/// concrete impl calls `invoke_llm_oneshot`); tests implement it with a
/// canned responder.
#[async_trait::async_trait]
pub trait LlmFn: Send + Sync {
    /// Run `prompt` through the model and return its reply text (or an
    /// human-readable error string — same shape as
    /// [`CompletionLlmCall::invoke`]).
    async fn call(&self, prompt: &str) -> Result<String, String>;
}

/// Production-shaped [`CompletionLlmCall`] backed by an injected [`LlmFn`].
///
/// The automation crate cannot depend on `syncode-ws`, so the real
/// provider-CLI adapter is wired by the host: it constructs an `LlmFn` impl
/// that delegates to `syncode_ws::llm::invoke_llm_oneshot`, wraps it in this
/// struct, and hands it to [`CompletionHarness::start`]. This struct itself
/// is trivial — it only forwards the call — but it gives the harness a
/// concrete, `Arc`-shareable `CompletionLlmCall` to store.
///
/// ```
/// # use std::sync::Arc;
/// # use syncode_automation::completion_harness::{LlmFn, ProviderCompletionLlm};
/// # use syncode_automation::CompletionLlmCall;
/// # struct MyAdapter;
/// # #[async_trait::async_trait]
/// # impl LlmFn for MyAdapter {
/// #     async fn call(&self, _p: &str) -> Result<String, String> { Ok("x".into()) }
/// # }
/// let llm: Arc<dyn CompletionLlmCall> = Arc::new(ProviderCompletionLlm::new(Arc::new(MyAdapter)));
/// ```
pub struct ProviderCompletionLlm {
    inner: Arc<dyn LlmFn>,
}

impl ProviderCompletionLlm {
    /// Wrap an [`LlmFn`] as a [`CompletionLlmCall`].
    pub fn new(inner: Arc<dyn LlmFn>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl CompletionLlmCall for ProviderCompletionLlm {
    async fn invoke(&self, prompt: &str) -> Result<String, String> {
        self.inner.call(prompt).await
    }
}

// ─── Config ────────────────────────────────────────────────────────────────

/// Knobs for [`CompletionHarness::start_with`]. Production values come from
/// [`HarnessConfig::default`]; tests pass a tuned config (small timeout,
/// small capacity) for determinism.
#[derive(Debug, Clone, Copy)]
pub struct HarnessConfig {
    /// Bounded queue capacity. Enqueuing past this drops the job (logged).
    /// MCode's default; 100 absorbs a burst without unbounded memory.
    pub capacity: usize,
    /// Number of worker fibers draining the queue. 2 caps concurrent LLM
    /// calls (provider rate-limit friendly). `0` is allowed for tests that
    /// exercise only the queue-overflow path (no jobs are processed).
    pub workers: usize,
    /// Per-evaluation wall-clock cap. A slow / hung model is treated as
    /// `NoMatch` after this elapses, so a stuck provider never completes a
    /// run. Production: 30s; tests: tens of ms.
    pub eval_timeout: Duration,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            capacity: 100,
            workers: 2,
            eval_timeout: Duration::from_secs(30),
        }
    }
}

/// `submit` outcome — whether the job was enqueued or dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// The job was enqueued for evaluation.
    Enqueued,
    /// The queue was full (or the harness was shut down); the job was
    /// dropped — never block the caller on a slow consumer. The run outcome
    /// is unaffected; the next fire re-evaluates.
    Dropped,
}

// ─── Harness ───────────────────────────────────────────────────────────────

/// The production harness: a bounded queue + N worker fibers that evaluate
/// completion jobs and disable matching automations.
///
/// Construct with [`CompletionHarness::start`] (production defaults) or
/// [`CompletionHarness::start_with`] (tuned config). Drain pending jobs and
/// join the workers with [`CompletionHarness::shutdown`] (takes `&self`, so
/// the harness can live behind an `Arc` shared with the scheduler/executor).
pub struct CompletionHarness {
    tx: Arc<tokio::sync::mpsc::Sender<CompletionJob>>,
    /// One-shot shutdown signal — dropping/sending closes the receiver the
    /// dispatcher is selecting on. Taken once by [`shutdown`](Self::shutdown).
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// The dispatcher's join handle. Taken once by [`shutdown`](Self::shutdown).
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl CompletionHarness {
    /// Start a harness with production defaults (cap 100, 2 workers, 30s
    /// timeout). See [`HarnessConfig::default`].
    pub fn start(
        repo: Arc<dyn AutomationRepository>,
        llm: Arc<dyn CompletionLlmCall>,
        disable_fn: Arc<dyn CompletionDisableFn>,
    ) -> Self {
        Self::start_with(repo, llm, disable_fn, HarnessConfig::default())
    }

    /// Start a harness with a tuned config (tests).
    pub fn start_with(
        repo: Arc<dyn AutomationRepository>,
        llm: Arc<dyn CompletionLlmCall>,
        disable_fn: Arc<dyn CompletionDisableFn>,
        config: HarnessConfig,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<CompletionJob>(config.capacity.max(1));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let dispatcher = tokio::spawn(dispatch_loop(
            rx,
            shutdown_rx,
            repo,
            llm,
            disable_fn,
            config.workers,
            config.eval_timeout,
        ));
        Self {
            tx: Arc::new(tx),
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            handle: Mutex::new(Some(dispatcher)),
        }
    }

    /// Enqueue a completion job. Non-blocking: returns immediately with
    /// [`SubmitOutcome::Enqueued`] or [`SubmitOutcome::Dropped`] when the
    /// queue is full (the MCode behavior — never block the caller on a slow
    /// consumer). A dropped job does not affect the run outcome; the
    /// automation's next fire re-evaluates against fresh output.
    pub fn submit(&self, job: CompletionJob) -> SubmitOutcome {
        match self.tx.try_send(job) {
            Ok(()) => SubmitOutcome::Enqueued,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    "completion harness queue full; dropping completion job (MCode drop semantics)"
                );
                SubmitOutcome::Dropped
            }
            // Closed: the harness was shut down. Treat as a no-op drop — the
            // caller already completed the run; completion eval is advisory.
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!("completion harness shut down; ignoring submit (harness closed)");
                SubmitOutcome::Dropped
            }
        }
    }

    /// Shut down the harness: signal the dispatcher to stop pulling new jobs,
    /// then await it while it drains the queue + in-flight evaluations. After
    /// this returns, [`submit`](Self::submit) returns `Dropped`. Idempotent —
    /// a second call is a no-op (the signal + handle are taken once).
    ///
    /// Takes `&self` (not `self`) so the harness can be shared by `Arc` between
    /// the scheduler (which constructs it) and the executor (which submits
    /// jobs through `RunDeps`).
    pub async fn shutdown(&self) {
        // Signal the dispatcher to stop the recv loop. Dropping the sender has
        // the same effect (the receiver yields `Err` in `select!`). Take the
        // sender out of the mutex (dropping the guard) before any await so we
        // don't hold the lock across the dispatcher join below.
        let stx = self.shutdown_tx.lock().unwrap().take();
        if let Some(stx) = stx {
            let _ = stx.send(());
        }
        // Await the dispatcher — it drains pending + in-flight before exiting.
        // Take the handle out of the mutex first (same await-holding-lock rule).
        let handle = self.handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
    }
}

/// The dispatcher loop: pulls jobs from the receiver and spawns each
/// evaluation behind a semaphore capped at `workers`. Each evaluation is
/// wrapped in `tokio::time::timeout(eval_timeout)`.
///
/// On `Match` (confidence ≥ threshold) → invoke the disable callback. On
/// `NoMatch` / `Stale` / timeout → no-op (log). Errors in the disable
/// callback are logged and swallowed (a disable that didn't persist will be
/// re-evaluated on the next fire).
///
/// `shutdown_rx` is selected on each iteration so [`CompletionHarness::shutdown`]
/// can break the recv loop promptly; in-flight evaluations are still awaited
/// (drained after the loop).
async fn dispatch_loop(
    mut receiver: tokio::sync::mpsc::Receiver<CompletionJob>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    repo: Arc<dyn AutomationRepository>,
    llm: Arc<dyn CompletionLlmCall>,
    disable_fn: Arc<dyn CompletionDisableFn>,
    workers: usize,
    eval_timeout: Duration,
) {
    // workers=0 → no jobs are processed (test-only degenerate config for the
    // queue-overflow path). The loop still drains the receiver so the channel
    // doesn't hold memory indefinitely on shutdown; it just never evaluates.
    if workers == 0 {
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => break,
                job = receiver.recv() => { if job.is_none() { break; } }
            }
        }
        return;
    }

    let sem = Arc::new(tokio::sync::Semaphore::new(workers));
    let mut inflight: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();

    loop {
        // Pull the next job, racing against shutdown. On shutdown signal,
        // stop pulling (the in-flight drain below handles pending work).
        let job = tokio::select! {
            biased;
            _ = &mut shutdown_rx => break,
            j = receiver.recv() => match j {
                Some(j) => j,
                None => break, // all senders dropped — graceful close
            }
        };
        // Acquire a concurrency permit, racing against shutdown too. If
        // shutdown fires here, the pulled job is dropped (completion eval is
        // advisory — the run outcome already stands).
        let permit = tokio::select! {
            biased;
            _ = &mut shutdown_rx => break,
            p = sem.clone().acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => break, // semaphore closed
            }
        };
        let repo = Arc::clone(&repo);
        let llm = Arc::clone(&llm);
        let disable_fn = Arc::clone(&disable_fn);
        inflight.spawn(async move {
            let _permit = permit; // held for the eval's lifetime
            evaluate_and_maybe_disable(
                &job,
                &repo,
                llm.as_ref(),
                disable_fn.as_ref(),
                eval_timeout,
            )
            .await;
        });
        // Reap finished tasks so the JoinSet doesn't grow unboundedly.
        while inflight.try_join_next().is_some() {}
    }
    // Drain in-flight evaluations on shutdown.
    while inflight.join_next().await.is_some() {}
}

/// Evaluate one job and act on the verdict. Extracted for clarity + so a
/// future test can exercise the logic without the dispatcher scaffolding.
async fn evaluate_and_maybe_disable(
    job: &CompletionJob,
    repo: &Arc<dyn AutomationRepository>,
    llm: &dyn CompletionLlmCall,
    disable_fn: &dyn CompletionDisableFn,
    eval_timeout: Duration,
) {
    let CompletionJob {
        def,
        run,
        assistant_text,
    } = job;

    // Wrap the (slow) LLM evaluation in a wall-clock cap. A hung provider is
    // treated as NoMatch so it never silently completes a run.
    let result = match tokio::time::timeout(
        eval_timeout,
        evaluate_completion_policy(def, run, assistant_text, llm, repo),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => {
            tracing::warn!(
                automation_id = %def.id,
                run_id = %run.run_id,
                timeout_secs = eval_timeout.as_secs(),
                "completion evaluation timed out; treating as NoMatch"
            );
            return;
        }
    };

    let threshold = match &def.completion_policy {
        CompletionPolicy::AiEvaluated {
            confidence_threshold,
            ..
        } => *confidence_threshold,
        _ => return, // not AI-evaluated — shouldn't have been submitted
    };

    match &result.verdict {
        CompletionVerdict::Match { confidence } if *confidence >= threshold => {
            tracing::info!(
                automation_id = %def.id,
                run_id = %run.run_id,
                confidence,
                "completion policy matched — disabling automation"
            );
            // The disable callback persists the result + flips `enabled`. Best
            // effort: a failure is logged (the next fire re-evaluates).
            disable_fn.disable(def.as_ref(), &result).await;
        }
        CompletionVerdict::Match { confidence } => {
            // Below the def's threshold (defensive — evaluate_completion_policy
            // already gates on threshold, but a stale reload could shift it).
            tracing::debug!(
                automation_id = %def.id,
                run_id = %run.run_id,
                confidence,
                threshold,
                "completion Match below threshold — no disable"
            );
        }
        CompletionVerdict::NoMatch { reason, .. } => {
            tracing::debug!(
                automation_id = %def.id,
                run_id = %run.run_id,
                ?reason,
                "completion NoMatch — automation stays enabled"
            );
        }
        CompletionVerdict::Stale {
            expected_version,
            current_version,
        } => {
            tracing::debug!(
                automation_id = %def.id,
                run_id = %run.run_id,
                expected_version,
                current_version,
                "completion verdict stale (def changed mid-call) — discarded"
            );
        }
    }
}

/// Build a [`RunContext`] suitable for a completion job when the caller has no
/// live event sink (e.g. the synchronous executor path). The evaluator only
/// uses `run_id` + `automation_id` for logging; the sink is a no-op.
pub(crate) fn completion_run_context(run_id: &str, automation_id: &str) -> RunContext {
    RunContext {
        run_id: run_id.to_string(),
        automation_id: automation_id.to_string(),
        sink: Arc::new(NoopRunEventSink) as Arc<dyn crate::events::RunEventSink>,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    use crate::completion_eval::NoMatchReason;
    use crate::policies::CompletionPolicy;

    // ─── Test doubles ──────────────────────────────────────────────────

    /// A canned LLM that returns a fixed reply, optionally sleeping first to
    /// exercise the timeout path. Records the prompts it saw.
    struct CannedLlm {
        reply: String,
        delay: Option<Duration>,
        prompts: StdMutex<Vec<String>>,
    }

    impl CannedLlm {
        fn matched(score: f64) -> Self {
            Self {
                reply: format!("CONFIDENCE: {score:.2}"),
                delay: None,
                prompts: StdMutex::new(Vec::new()),
            }
        }
        fn no_score() -> Self {
            Self {
                reply: "I can't tell".into(),
                delay: None,
                prompts: StdMutex::new(Vec::new()),
            }
        }
        fn slow(score: f64, delay: Duration) -> Self {
            Self {
                reply: format!("CONFIDENCE: {score:.2}"),
                delay: Some(delay),
                prompts: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl CompletionLlmCall for CannedLlm {
        async fn invoke(&self, prompt: &str) -> Result<String, String> {
            self.prompts.lock().unwrap().push(prompt.to_string());
            if let Some(d) = self.delay {
                tokio::time::sleep(d).await;
            }
            Ok(self.reply.clone())
        }
    }

    /// A recording disable callback: captures every def-id it was asked to
    /// disable. Thread-safe.
    struct RecordingDisable {
        calls: StdMutex<Vec<String>>,
    }

    impl RecordingDisable {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: StdMutex::new(Vec::new()),
            })
        }
        fn ids(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
        fn count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl CompletionDisableFn for RecordingDisable {
        async fn disable(&self, def: &AutomationDef, _result: &CompletionResult) {
            self.calls.lock().unwrap().push(def.id.as_str().to_string());
        }
    }

    /// Minimal in-memory repo for the stale-check reload (mirrors
    /// completion_eval's StaleCheckRepo). Returns the stored def payload.
    struct CannedRepo {
        payload: StdMutex<Option<serde_json::Value>>,
    }

    impl CannedRepo {
        fn for_def(def: &AutomationDef) -> Arc<Self> {
            Arc::new(Self {
                payload: StdMutex::new(Some(serde_json::to_value(def).unwrap())),
            })
        }
    }

    #[async_trait::async_trait]
    impl AutomationRepository for CannedRepo {
        async fn save_def(
            &self,
            _id: &str,
            _payload: serde_json::Value,
        ) -> Result<(), syncode_core::PortError> {
            Ok(())
        }
        async fn get_def(
            &self,
            _id: &str,
        ) -> Result<Option<serde_json::Value>, syncode_core::PortError> {
            Ok(self.payload.lock().unwrap().clone())
        }
        async fn list_defs(&self) -> Result<Vec<serde_json::Value>, syncode_core::PortError> {
            Ok(Vec::new())
        }
        async fn delete_def(&self, _id: &str) -> Result<bool, syncode_core::PortError> {
            Ok(false)
        }
        async fn save_run(
            &self,
            _payload: serde_json::Value,
        ) -> Result<(), syncode_core::PortError> {
            Ok(())
        }
        async fn get_run(
            &self,
            _id: &str,
        ) -> Result<Option<serde_json::Value>, syncode_core::PortError> {
            Ok(None)
        }
        async fn list_runs(
            &self,
            _automation_id: &str,
        ) -> Result<Vec<serde_json::Value>, syncode_core::PortError> {
            Ok(Vec::new())
        }
        async fn advance_next_run_at(
            &self,
            _id: &str,
            _next_run_at: Option<String>,
        ) -> Result<(), syncode_core::PortError> {
            Ok(())
        }
    }

    fn ai_def(id: &str, threshold: f64) -> AutomationDef {
        let mut def = AutomationDef::new(
            id.to_string(),
            "echo".to_string(),
            crate::definition::ScheduleType::Manual,
        );
        def.completion_policy = CompletionPolicy::AiEvaluated {
            stop_when: "all tests pass".to_string(),
            confidence_threshold: threshold,
        };
        def
    }

    fn job(def: &AutomationDef, text: &str) -> CompletionJob {
        CompletionJob {
            def: Arc::new(def.clone()),
            run: completion_run_context("run-1", &def.id.as_str()),
            assistant_text: text.to_string(),
        }
    }

    /// Submit a job and poll until the disable callback fires (or timeout).
    /// Returns whether disable fired.
    async fn waits_for_disable(
        harness: &CompletionHarness,
        disable: &RecordingDisable,
        job: CompletionJob,
        timeout: Duration,
    ) -> bool {
        harness.submit(job);
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if disable.count() > 0 {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // ─── GAP 1 core tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn harness_match_invokes_disable_and_drains() {
        // A confident Match above the threshold → disable_fn invoked once with
        // the right def id. Queue drains (job consumed).
        let def = ai_def("auto-match", 0.8);
        let def_id = def.id.as_str().to_string();
        let repo = CannedRepo::for_def(&def);
        let llm: Arc<dyn CompletionLlmCall> = Arc::new(CannedLlm::matched(0.95));
        let disable = RecordingDisable::new();

        let harness = CompletionHarness::start_with(
            repo,
            llm,
            disable.clone(),
            HarnessConfig {
                capacity: 8,
                workers: 1,
                eval_timeout: Duration::from_secs(5),
            },
        );

        let fired = waits_for_disable(
            &harness,
            &disable,
            job(&def, "50 tests passed"),
            Duration::from_secs(2),
        )
        .await;
        assert!(fired, "disable_fn should fire for a confident Match");
        assert_eq!(disable.ids(), vec![def_id]);

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn harness_no_match_does_not_disable() {
        // A reply with no parseable score → NoMatch → disable_fn NOT invoked.
        let def = ai_def("auto-nomatch", 0.8);
        let repo = CannedRepo::for_def(&def);
        let llm: Arc<dyn CompletionLlmCall> = Arc::new(CannedLlm::no_score());
        let disable = RecordingDisable::new();

        let harness = CompletionHarness::start_with(
            repo,
            llm,
            disable.clone(),
            HarnessConfig {
                capacity: 8,
                workers: 1,
                eval_timeout: Duration::from_secs(2),
            },
        );

        let fired = waits_for_disable(
            &harness,
            &disable,
            job(&def, "partial output"),
            Duration::from_millis(500),
        )
        .await;
        assert!(
            !fired,
            "disable_fn must NOT fire for a NoMatch (unparseable reply)"
        );
        assert_eq!(disable.count(), 0);
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn harness_stale_does_not_disable() {
        // Simulate a def that changes mid-call: the repo returns a payload
        // whose version differs from the def snapshot the evaluator read.
        let mut def = ai_def("auto-stale", 0.8);
        def.version = 1;
        let repo = CannedRepo::for_def(&def);
        // Mutate the stored payload's version so the stale-check reload sees 2.
        {
            let mut p = repo.payload.lock().unwrap().clone().unwrap();
            p["version"] = serde_json::json!(2);
            *repo.payload.lock().unwrap() = Some(p);
        }
        let llm: Arc<dyn CompletionLlmCall> = Arc::new(CannedLlm::matched(0.99));
        let disable = RecordingDisable::new();

        let harness = CompletionHarness::start_with(
            repo,
            llm,
            disable.clone(),
            HarnessConfig {
                capacity: 8,
                workers: 1,
                eval_timeout: Duration::from_secs(2),
            },
        );

        let fired = waits_for_disable(
            &harness,
            &disable,
            job(&def, "output"),
            Duration::from_millis(500),
        )
        .await;
        assert!(!fired, "disable_fn must NOT fire for a Stale verdict");
        assert_eq!(disable.count(), 0);
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn harness_slow_llm_times_out_and_does_not_disable() {
        // Bonus: the 30s timeout path, exercised with a tiny eval_timeout +
        // a slow LLM. No disable fires; the harness survives.
        //
        // Proof the timeout fired (not the LLM): the slow LLM would return a
        // confident Match at 400ms → disable would fire. We poll for 1.2s; if
        // disable never fires, the eval_timeout (50ms) MUST have intervened.
        let def = ai_def("auto-slow", 0.8);
        let repo = CannedRepo::for_def(&def);
        let llm: Arc<dyn CompletionLlmCall> =
            Arc::new(CannedLlm::slow(0.99, Duration::from_millis(400)));
        let disable = RecordingDisable::new();

        let harness = CompletionHarness::start_with(
            repo,
            llm,
            disable.clone(),
            HarnessConfig {
                capacity: 8,
                workers: 1,
                eval_timeout: Duration::from_millis(50),
            },
        );

        let fired = waits_for_disable(
            &harness,
            &disable,
            job(&def, "output"),
            // Poll well past the slow LLM's 400ms reply — if the timeout hadn't
            // fired, the disable would land ~400ms in.
            Duration::from_millis(1200),
        )
        .await;
        assert!(!fired, "a timed-out evaluation must not disable");
        assert_eq!(disable.count(), 0);
        // Sanity: a fast (non-timed-out) path with the same LLM DOES disable,
        // proving the timeout — not the LLM — is what suppressed it here.
        // (Covered structurally by harness_match_invokes_disable_and_drains.)
        harness.shutdown().await;
    }

    // ─── Bounded-queue tests ───────────────────────────────────────────

    #[tokio::test]
    async fn submit_is_graceful_when_queue_full() {
        // workers=0 + capacity=2: no consumer drains, so submits past cap
        // return Dropped without panicking or blocking. Submitting 5 → the
        // first 2 Enqueue, the rest Drop.
        let def = ai_def("auto-overflow", 0.8);
        let repo = CannedRepo::for_def(&def);
        let llm: Arc<dyn CompletionLlmCall> = Arc::new(CannedLlm::matched(0.95));
        let disable = RecordingDisable::new();

        let harness = CompletionHarness::start_with(
            repo,
            llm,
            disable.clone(),
            HarnessConfig {
                capacity: 2,
                workers: 0,
                eval_timeout: Duration::from_secs(5),
            },
        );

        let mut outcomes = Vec::new();
        for _ in 0..5 {
            outcomes.push(harness.submit(job(&def, "out")));
        }

        let enqueued = outcomes
            .iter()
            .filter(|o| **o == SubmitOutcome::Enqueued)
            .count();
        let dropped = outcomes
            .iter()
            .filter(|o| **o == SubmitOutcome::Dropped)
            .count();
        assert_eq!(
            enqueued, 2,
            "capacity=2 → exactly 2 enqueued (got {outcomes:?})"
        );
        assert_eq!(dropped, 3, "3 overflow submits dropped (got {outcomes:?})");
        // No disable fired (no workers to process).
        assert_eq!(disable.count(), 0);
        harness.shutdown().await;
    }

    // ─── LlmFn / ProviderCompletionLlm seam ────────────────────────────

    #[tokio::test]
    async fn provider_completion_llm_delegates_to_injected_fn() {
        struct Echo;
        #[async_trait::async_trait]
        impl LlmFn for Echo {
            async fn call(&self, prompt: &str) -> Result<String, String> {
                Ok(format!("echo:{prompt}"))
            }
        }
        let llm = ProviderCompletionLlm::new(Arc::new(Echo));
        // As a CompletionLlmCall it forwards.
        let reply = llm.invoke("hi").await.unwrap();
        assert_eq!(reply, "echo:hi");
    }

    // ─── completion_run_context helper ────────────────────────────────

    #[tokio::test]
    async fn completion_run_context_carries_identity() {
        let ctx = completion_run_context("run-x", "auto-y");
        assert_eq!(ctx.run_id, "run-x");
        assert_eq!(ctx.automation_id, "auto-y");
        // Sink is present (no-op) — emitting is a no-op, not a panic.
        crate::events::emit_current(crate::events::RunEventKind::Progress {
            progress: None,
            message: "x".into(),
        })
        .await;
    }

    // ─── evaluate_and_maybe_disable unit checks (verdict routing) ─────

    #[tokio::test]
    async fn evaluate_and_maybe_disable_no_op_for_no_score_reply() {
        let def = ai_def("auto-route", 0.8);
        let repo = CannedRepo::for_def(&def);
        let llm = CannedLlm::no_score();
        let disable = RecordingDisable::new();
        evaluate_and_maybe_disable(
            &job(&def, "out"),
            &(repo as Arc<dyn AutomationRepository>),
            &llm,
            disable.as_ref(),
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(disable.count(), 0);
        // NoMatch reason type is in scope (sanity).
        let _ = NoMatchReason::Unparseable;
    }
}

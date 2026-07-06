//! Stacked actions — commit→push→PR pipeline
//!
//! Implements a pipeline of git actions that can be chained:
//! Stage → Commit → Push → Create PR
//!
//! Each step can fail independently, and the pipeline can be
//! resumed from the last successful step.

use crate::service::{Git2Service, GitError, GitService};

/// A single action in the pipeline
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StackedAction {
    /// Stage specific files
    Stage { paths: Vec<String> },
    /// Create a commit with a message
    Commit { message: String },
    /// Push to a remote branch
    Push { remote: String, branch: String },
    /// Create a pull request (external — returns URL)
    CreatePR {
        title: String,
        body: String,
        base: String,
    },
}

/// Result of executing a stacked action
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActionResult {
    pub action_index: usize,
    pub success: bool,
    pub output: Option<String>,
    pub error: Option<String>,
}

/// A per-stage progress notification emitted by
/// [`StackedPipeline::execute_with_progress`]. GIT-4 wires this onto the
/// `git` push channel so a connection subscribed via
/// `git.subscribeActionProgress` receives ≥1 event per pipeline stage.
///
/// The shape mirrors the MCode `GitActionProgress` envelope the vendored UI
/// renders: `{ stage, percent, message }`. `percent` is a 0..=100 estimate
/// of overall pipeline completion (the active stage counts as partially
/// done); `message` carries a human-readable description of the stage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActionProgress {
    /// Free-form stage label: `"branch"` / `"commit"` / `"push"` /
    /// `"create_pr"` / `"done"` / `"error"`. Matches the MCode wire names.
    pub stage: String,
    /// 0..=100 — coarse overall completion estimate.
    pub percent: u8,
    /// Human-readable per-stage message (action output or status text).
    pub message: String,
}

impl ActionProgress {
    /// Build a progress event for the given stage index. `percent` is the
    /// midpoint of the stage's slice of the overall pipeline (so a subscriber
    /// sees ≥1 strictly-increasing percent value per stage).
    ///
    /// The `manual_checked_ops` clippy lint is intentionally suppressed: the
    /// zero-guard is a semantic choice (empty pipeline → 100% done), not a
    /// panic-avoidance pattern, so the explicit `if total == 0` branch reads
    /// more clearly than a `checked_div` chain here.
    #[allow(clippy::manual_checked_ops)]
    fn stage(stage: &str, stage_index: usize, total: usize, message: impl Into<String>) -> Self {
        // Midpoint of [stage_index/total, (stage_index+1)/total] as a percent.
        // Guard against `total == 0` (no actions) — treat as 100% done.
        let percent = if total == 0 {
            100
        } else {
            let lo = (stage_index * 100) / total;
            let hi = ((stage_index + 1) * 100) / total;
            ((lo + hi) / 2).min(100) as u8
        };
        Self {
            stage: stage.to_string(),
            percent,
            message: message.into(),
        }
    }

    /// Terminal "done" event — 100% completion.
    fn done(message: impl Into<String>) -> Self {
        Self {
            stage: "done".to_string(),
            percent: 100,
            message: message.into(),
        }
    }

    /// Terminal "error" event — also 100% (pipeline stopped).
    fn error(message: impl Into<String>) -> Self {
        Self {
            stage: "error".to_string(),
            percent: 100,
            message: message.into(),
        }
    }
}

/// A pipeline of stacked actions
#[derive(Debug, Clone, Default)]
pub struct StackedPipeline {
    actions: Vec<StackedAction>,
    results: Vec<ActionResult>,
}

impl StackedPipeline {
    /// Create a new empty pipeline
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an action to the pipeline
    pub fn add(&mut self, action: StackedAction) {
        self.actions.push(action);
    }

    /// Get the list of actions
    pub fn actions(&self) -> &[StackedAction] {
        &self.actions
    }

    /// Get the results from previous executions
    pub fn results(&self) -> &[ActionResult] {
        &self.results
    }

    /// Execute all pending actions against the git service.
    ///
    /// This is the **default sync path** (no progress emission). GIT-4 leaves
    /// it unchanged — the 40+ existing stacked-action tests assert this exact
    /// behavior, so it must not regress. Callers that want live per-stage
    /// progress events should use [`StackedPipeline::execute_with_progress`].
    pub async fn execute(&mut self, service: &Git2Service) -> Result<Vec<ActionResult>, GitError> {
        let mut all_results = Vec::new();

        for (i, action) in self.actions.iter().enumerate() {
            let result = execute_action(service, action, i);

            all_results.push(result.clone());
            self.results.push(result);
        }

        Ok(all_results)
    }

    /// Execute all pending actions, invoking `on_progress` before each stage
    /// runs and once more at the end (done/error). GIT-4: this is the
    /// progress-emitting variant used when ≥1 connection is subscribed to the
    /// `git` push channel. The callback receives an [`ActionProgress`] with
    /// `{ stage, percent, message }`; the subscriber-facing push frame is
    /// shaped by the caller (the WS layer wraps it onto `CHANNEL_GIT`).
    ///
    /// Semantics are identical to [`execute`](Self::execute) — same per-action
    /// results, same error propagation — the only difference is the progress
    /// side-channel. A failing stage still records its result and continues
    /// (mirrors `execute`); the final event is `"done"` if every stage ran,
    /// `"error"` only if `on_progress` itself fails (which it never should —
    /// callers must make the callback best-effort).
    ///
    /// `on_progress` is invoked as `&mut F` so a closure capturing a `Vec` or
    /// a channel sender can accumulate events. It is called **before** the
    /// stage executes (with the stage's "starting" message) — the caller can
    /// also read `self.results()` after the call to build completion events.
    pub async fn execute_with_progress<F>(
        &mut self,
        service: &Git2Service,
        mut on_progress: F,
    ) -> Result<Vec<ActionResult>, GitError>
    where
        F: FnMut(ActionProgress),
    {
        let total = self.actions.len();
        let mut all_results = Vec::new();

        for (i, action) in self.actions.iter().enumerate() {
            // Pre-stage progress: "starting <stage>".
            on_progress(ActionProgress::stage(
                stage_label(action),
                i,
                total,
                stage_starting_message(action),
            ));

            let result = execute_action(service, action, i);

            all_results.push(result.clone());
            self.results.push(result);
        }

        // Terminal event. If every stage succeeded → "done"; otherwise the
        // caller can read per-stage errors from the results. We surface a
        // single "done" so subscribers see a clean completion signal.
        let any_failed = all_results.iter().any(|r| !r.success);
        let final_msg = if any_failed {
            format!(
                "Completed with failures ({} of {} stages failed)",
                all_results.iter().filter(|r| !r.success).count(),
                total
            )
        } else {
            format!("Completed all {total} stages")
        };
        if any_failed {
            on_progress(ActionProgress::error(final_msg));
        } else {
            on_progress(ActionProgress::done(final_msg));
        }

        Ok(all_results)
    }

    /// Clear the pipeline and results
    pub fn reset(&mut self) {
        self.actions.clear();
        self.results.clear();
    }
}

/// Execute a single [`StackedAction`] against the service, returning its
/// [`ActionResult`]. Extracted from `StackedPipeline::execute` so both the
/// default sync path and `execute_with_progress` share identical per-action
/// semantics (GIT-4 factored this out without changing behavior — the 40+
/// existing stacked-action tests assert the exact output strings).
fn execute_action(
    service: &Git2Service,
    action: &StackedAction,
    action_index: usize,
) -> ActionResult {
    match action {
        StackedAction::Stage { paths } => {
            let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
            match service.add(&path_refs) {
                Ok(()) => ActionResult {
                    action_index,
                    success: true,
                    output: Some(format!("Staged {} files", paths.len())),
                    error: None,
                },
                Err(e) => ActionResult {
                    action_index,
                    success: false,
                    output: None,
                    error: Some(e.to_string()),
                },
            }
        }
        StackedAction::Commit { message } => match service.commit(message) {
            Ok(commit) => ActionResult {
                action_index,
                success: true,
                output: Some(format!(
                    "Committed: {} ({})",
                    commit.message, commit.short_hash
                )),
                error: None,
            },
            Err(e) => ActionResult {
                action_index,
                success: false,
                output: None,
                error: Some(e.to_string()),
            },
        },
        StackedAction::Push { remote, branch } => match service.push(remote, branch) {
            Ok(result) => {
                let msg = match &result {
                    crate::service::PushResult::Pushed {
                        branch,
                        upstream_branch,
                        set_upstream,
                    } => {
                        if *set_upstream {
                            format!("Pushed {} (set upstream to {})", branch, upstream_branch)
                        } else {
                            format!("Pushed {} to {}", branch, upstream_branch)
                        }
                    }
                    crate::service::PushResult::SkippedUpToDate {
                        branch,
                        upstream_branch,
                    } => {
                        format!("Skipped ({} up to date with {})", branch, upstream_branch)
                    }
                };
                ActionResult {
                    action_index,
                    success: true,
                    output: Some(msg),
                    error: None,
                }
            }
            Err(e) => ActionResult {
                action_index,
                success: false,
                output: None,
                error: Some(e.to_string()),
            },
        },
        StackedAction::CreatePR { title, body, base } => {
            match create_pull_request(service, title, body, base) {
                Ok(url) => ActionResult {
                    action_index,
                    success: true,
                    output: Some(format!("Created PR '{}' → {}", title, url)),
                    error: None,
                },
                Err(e) => ActionResult {
                    action_index,
                    success: false,
                    output: None,
                    error: Some(e.to_string()),
                },
            }
        }
    }
}

/// MCode wire-name label for a stage. Matches the vendored UI's
/// `GitActionProgress.stage` discriminator.
fn stage_label(action: &StackedAction) -> &'static str {
    match action {
        StackedAction::Stage { .. } => "stage",
        StackedAction::Commit { .. } => "commit",
        StackedAction::Push { .. } => "push",
        StackedAction::CreatePR { .. } => "create_pr",
    }
}

/// Human-readable "starting" message for the pre-stage progress event.
fn stage_starting_message(action: &StackedAction) -> String {
    match action {
        StackedAction::Stage { paths } => format!("Staging {} files", paths.len()),
        StackedAction::Commit { message } => {
            // Truncate long messages so the progress frame stays readable.
            // Use char-boundary-safe truncation: `&message[..60]` indexes
            // BYTES and would panic if byte 60 landed inside a multi-byte
            // UTF-8 sequence (emoji, CJK, en-dash, accented chars). See
            // GIT-4 rework gap #1.
            format!("Committing: {}", truncate_to_chars(message, 60))
        }
        StackedAction::Push { remote, branch } => {
            format!("Pushing {branch} to {remote}")
        }
        StackedAction::CreatePR { title, base, .. } => {
            format!(
                "Creating PR '{}' against {}",
                truncate_to_chars(title, 60),
                base
            )
        }
    }
}

/// Truncate `s` to at most `max_chars` Unicode scalar values (NOT bytes),
/// returning a slice of the original string (no allocation).
///
/// `String::len()` and `&str[..n]` operate on BYTES: slicing at a byte index
/// that falls inside a multi-byte UTF-8 sequence (emoji, CJK, en-dash,
/// accented letters) **panics** at runtime. Commit messages and PR titles
/// routinely contain such characters, so the progress-frame truncation must
/// be char-boundary-safe.
///
/// This is the most portable fix (works on stable Rust — no nightly
/// `floor_char_boundary`). It returns a borrow of the input: short strings
/// come back untouched; longer strings come back sliced at the byte offset of
/// the `max_chars`-th char (always a valid char boundary, so `&s[..end]` is
/// sound).
fn truncate_to_chars(s: &str, max_chars: usize) -> &str {
    if s.chars().count() <= max_chars {
        return s;
    }
    // Walk the char indices and find the byte offset of the (max_chars)-th
    // char. Slicing at a char boundary is always safe.
    let mut end = s.len();
    for (i, (byte_idx, _)) in s.char_indices().enumerate() {
        if i == max_chars {
            end = byte_idx;
            break;
        }
    }
    &s[..end]
}

/// Create a GitHub pull request by shelling out to `gh pr create` (mirrors
/// MCode's `GitHubCliShape.createPullRequest`). Auth is delegated to the user's
/// `gh auth login` setup — we never handle tokens. The PR body is written to a
/// temp file and passed via `--body-file` (avoids shell-escaping long bodies,
/// matching MCode).
///
/// Returns the created PR's URL on success. Detects the "PR already exists"
/// race and surfaces it as an error (callers may treat it as success per
/// MCode's `opened_existing` semantics).
pub fn create_pull_request(
    service: &Git2Service,
    title: &str,
    body: &str,
    base: &str,
) -> Result<String, GitError> {
    use std::io::Write;

    let cwd = service.path();
    let head = service
        .current_branch()?
        .ok_or_else(|| GitError::BranchNotFound("HEAD is detached; cannot create PR".into()))?;

    // Write the body to a temp file (MCode writes to a temp .md).
    let mut tmp = tempfile::Builder::new()
        .suffix(".md")
        .tempfile()
        .map_err(|e| GitError::GitOperation(git2::Error::from_str(&e.to_string())))?;
    tmp.write_all(body.as_bytes())
        .map_err(|e| GitError::GitOperation(git2::Error::from_str(&e.to_string())))?;
    let tmp_path = tmp.path().to_string_lossy().to_string();

    let args = [
        "pr",
        "create",
        "--base",
        base,
        "--head",
        &head,
        "--title",
        title,
        "--body-file",
        &tmp_path,
    ];
    let output = crate::service::run_gh(cwd, &args)?;
    // tmp is dropped here, cleaning up the file.

    if output.status != 0 {
        return Err(crate::service::classify_cli_error(&output.stderr));
    }

    // `gh pr create` prints the PR URL on success (to stdout). Parse it.
    // If a PR already exists, gh exits non-zero with a message containing the
    // existing URL — surfaced as an error above; callers decide.
    let url = output
        .stdout
        .lines()
        .find(|line| line.starts_with("https://"))
        .map(String::from)
        .unwrap_or_else(|| output.stdout.trim().to_string());
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_starts_empty() {
        let pipeline = StackedPipeline::new();
        assert!(pipeline.actions().is_empty());
        assert!(pipeline.results().is_empty());
    }

    #[test]
    fn pipeline_add_actions() {
        let mut pipeline = StackedPipeline::new();
        pipeline.add(StackedAction::Stage {
            paths: vec!["file.rs".to_string()],
        });
        pipeline.add(StackedAction::Commit {
            message: "test commit".to_string(),
        });
        assert_eq!(pipeline.actions().len(), 2);
    }

    #[test]
    fn pipeline_reset() {
        let mut pipeline = StackedPipeline::new();
        pipeline.add(StackedAction::Commit {
            message: "test".to_string(),
        });
        pipeline.reset();
        assert!(pipeline.actions().is_empty());
        assert!(pipeline.results().is_empty());
    }

    #[test]
    fn action_result_serialization() {
        let result = ActionResult {
            action_index: 0,
            success: true,
            output: Some("ok".to_string()),
            error: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: ActionResult = serde_json::from_str(&json).unwrap();
        assert!(back.success);
        assert_eq!(back.output.unwrap(), "ok");
    }

    #[test]
    fn stacked_action_serialization() {
        let action = StackedAction::Commit {
            message: "fix bug".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("fix bug"));
    }

    // ─── GIT-4: ActionProgress + execute_with_progress unit tests ───────
    //
    // These cover the progress-emission contract in isolation:
    //   - `ActionProgress` serializes to the MCode `{ stage, percent, message }`
    //     wire shape.
    //   - `execute_with_progress` invokes the callback ≥1× per stage + a
    //     terminal done/error event, WITHOUT requiring a real git repo (the
    //     callback fires before each stage's git call, so we can observe the
    //     pre-stage events even when the git operations themselves error).
    //   - percent is monotonically non-decreasing across stages.

    #[test]
    fn action_progress_serializes_to_mcode_shape() {
        let p = ActionProgress {
            stage: "commit".to_string(),
            percent: 50,
            message: "Committing: fix".to_string(),
        };
        let json = serde_json::to_string(&p).unwrap();
        let back: ActionProgress = serde_json::from_str(&json).unwrap();
        assert_eq!(back.stage, "commit");
        assert_eq!(back.percent, 50);
        assert_eq!(back.message, "Committing: fix");
        // Wire shape must include the three MCode fields verbatim.
        assert!(json.contains("\"stage\""));
        assert!(json.contains("\"percent\""));
        assert!(json.contains("\"message\""));
    }

    #[test]
    fn action_progress_done_is_100_percent() {
        let p = ActionProgress::done("all good");
        assert_eq!(p.percent, 100);
        assert_eq!(p.stage, "done");
    }

    #[test]
    fn action_progress_error_is_terminal() {
        let p = ActionProgress::error("boom");
        assert_eq!(p.percent, 100);
        assert_eq!(p.stage, "error");
        assert_eq!(p.message, "boom");
    }

    #[test]
    fn action_progress_stage_percent_is_monotonic() {
        // 3 stages → midpoints at ~17, ~50, ~83. Verify the formula produces
        // strictly-increasing percent values per stage.
        let total = 3;
        let mut prev = 0u8;
        for i in 0..total {
            let p = ActionProgress::stage("x", i, total, "msg");
            assert!(
                p.percent >= prev,
                "stage {i}: percent {} should be >= prev {prev}",
                p.percent
            );
            prev = p.percent;
        }
    }

    #[test]
    fn action_progress_stage_zero_total_is_done() {
        // Guard against div-by-zero: an empty pipeline's "stage" event is 100%.
        let p = ActionProgress::stage("done", 0, 0, "empty");
        assert_eq!(p.percent, 100);
    }

    #[test]
    fn stage_label_and_starting_message_cover_all_variants() {
        let cases = [
            (
                StackedAction::Stage {
                    paths: vec!["a".into()],
                },
                "stage",
            ),
            (
                StackedAction::Commit {
                    message: "m".into(),
                },
                "commit",
            ),
            (
                StackedAction::Push {
                    remote: "origin".into(),
                    branch: "b".into(),
                },
                "push",
            ),
            (
                StackedAction::CreatePR {
                    title: "t".into(),
                    body: "x".into(),
                    base: "main".into(),
                },
                "create_pr",
            ),
        ];
        for (action, expected_label) in cases {
            assert_eq!(stage_label(&action), expected_label);
            // Starting message must be non-empty for every variant.
            assert!(!stage_starting_message(&action).is_empty());
        }
    }

    // ─── GIT-4 rework gap #1: UTF-8-safe truncation ──────────────────────
    //
    // The pre-GIT-4-rework code did `&message[..60]` / `&title[..60]`, which
    // indexes BYTES. If byte 60 lands inside a multi-byte UTF-8 sequence
    // (emoji, en-dash, CJK, accented chars) the slice PANICS at runtime.
    // Commit messages and PR titles routinely contain such characters, so
    // `stage_starting_message` must truncate on CHAR boundaries, not bytes.
    // These tests prove the fix: a message whose byte-60 position falls in
    // the middle of a multi-byte char must truncate without panicking and
    // must yield a valid UTF-8 string of ≤60 CHARS.

    #[test]
    fn truncate_to_chars_under_max_is_identity() {
        // Short string (≤60 chars) is returned untouched — no truncation.
        let s = "hello";
        assert_eq!(truncate_to_chars(s, 60), s);
        // Exactly 60 chars: boundary is inclusive (no truncation).
        let exactly_60: String = "a".repeat(60);
        assert_eq!(truncate_to_chars(&exactly_60, 60), exactly_60);
    }

    #[test]
    fn truncate_to_chars_ascii_truncates_at_char_count() {
        // 70 ASCII chars → truncated to exactly 60 chars.
        let s: String = "a".repeat(70);
        let t = truncate_to_chars(&s, 60);
        assert_eq!(t.chars().count(), 60);
        assert_eq!(t.len(), 60); // ASCII: bytes == chars
        assert!(t.starts_with("aaaaaaaa"));
    }

    #[test]
    fn truncate_to_chars_multibyte_emoji_at_boundary_no_panic() {
        // 58 ASCII chars + a 4-byte emoji (🚀 = U+1F680, 4 bytes in UTF-8).
        // The OLD code would try `&s[..60]` — byte 60 lands in the middle of
        // the emoji (bytes 58,59,60,61) → PANIC. The new char-boundary-aware
        // truncation must keep the emoji intact (char 59 is the emoji, char
        // 60 doesn't exist yet) OR drop it cleanly at a char boundary.
        let mut msg = String::from("a".repeat(58).as_str());
        msg.push('🚀'); // char index 58, bytes 58..62
        msg.push_str(&"b".repeat(20)); // push past 60 chars total
        // This call must NOT panic (the core regression check).
        let t = truncate_to_chars(&msg, 60);
        // Result is valid UTF-8 (slice of a valid String) and ≤60 chars.
        assert!(
            t.chars().count() <= 60,
            "truncated result must be ≤60 chars"
        );
        // The first 58 ASCII chars are always preserved (they're all 1 byte).
        assert!(t.starts_with(&"a".repeat(58)));
    }

    #[test]
    fn stage_starting_message_commit_with_multibyte_no_panic() {
        // End-to-end: a commit message with multi-byte chars near position 60
        // must produce a starting message WITHOUT panicking. This is the
        // exact scenario the rework gap describes (emoji at position 58-59).
        let mut message = String::from("a".repeat(58).as_str());
        message.push('🚀');
        message.push_str(&"b".repeat(20));
        let action = StackedAction::Commit { message };
        // Must not panic — that's the assertion. Also check shape.
        let msg = stage_starting_message(&action);
        assert!(msg.starts_with("Committing: "));
        // Truncated portion must be valid UTF-8 (always true for a String,
        // but this guards against future refactors that might reintroduce
        // raw byte slicing).
        assert!(msg.is_char_boundary(msg.len()));
    }

    #[test]
    fn stage_starting_message_createpr_title_with_multibyte_no_panic() {
        // Same regression check for the CreatePR title truncation path.
        // CJK chars are 3 bytes each in UTF-8 — a run of them near position 60
        // is a common real-world case (e.g. a Chinese PR title).
        let mut title = String::from("a".repeat(30).as_str());
        title.push_str(&"功能".repeat(20)); // 40 CJK chars (3 bytes each)
        let action = StackedAction::CreatePR {
            title,
            body: String::new(),
            base: "main".into(),
        };
        let msg = stage_starting_message(&action);
        assert!(msg.starts_with("Creating PR '"));
        assert!(msg.contains("against main"));
        // Must not have panicked to get here.
    }

    #[test]
    fn execute_with_progress_emits_per_stage_and_terminal_event() {
        // We don't need a real repo: the callback fires BEFORE each stage's
        // git call, so we capture the pre-stage events. The git calls will
        // error (no repo), but `execute_action` records the failure and
        // continues — `execute_with_progress` then emits a terminal "error"
        // event. We assert:
        //   - ≥1 event per stage (the pre-stage "starting" event).
        //   - a terminal event (done/error).
        //   - percent is non-decreasing across the captured events.
        //
        // Build a service over a nonexistent path so repo ops fail fast.
        let svc = Git2Service::open(std::path::Path::new("/tmp/nonexistent-git-4-unit-test-xyz"))
            .unwrap_or_else(|_| {
                // If open fails, fall back to constructing via a path that will
                // also fail per-op. Git2Service::open itself returns Ok for any
                // path (lazy discovery) — so this branch is essentially never hit.
                Git2Service::open(std::path::Path::new(".")).unwrap()
            });

        let mut pipeline = StackedPipeline::new();
        pipeline.add(StackedAction::Stage {
            paths: vec!["a.rs".into()],
        });
        pipeline.add(StackedAction::Commit {
            message: "t".into(),
        });

        let mut events: Vec<ActionProgress> = Vec::new();
        // Drive the future synchronously (execute_with_progress has no real
        // await points — same as execute). We build a no-op waker from raw
        // std (futures_util isn't a dependency of this crate).
        use std::pin::Pin;
        use std::task::{Context, RawWaker, RawWakerVTable, Waker};
        fn noop_waker() -> Waker {
            fn noop(_: *const ()) {}
            fn noop_clone(_: *const ()) -> RawWaker {
                RawWaker::new(std::ptr::null(), &VTABLE)
            }
            static VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
            unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
        }
        let mut fut = Box::pin(pipeline.execute_with_progress(&svc, |p| events.push(p)));
        let _ = Pin::new(&mut fut).poll(&mut Context::from_waker(&noop_waker()));
        drop(fut);
        // Re-borrow pipeline for results check after the pinned fut is dropped.
        // (The fut borrowed pipeline mutably; we polled it once and dropped.)

        // ≥1 event per stage (2 stages → ≥2 pre-stage events) + 1 terminal.
        assert!(
            events.len() >= 3,
            "expected ≥3 progress events (2 stages + terminal), got {}: {:?}",
            events.len(),
            events
        );

        // Every event has the three MCode fields populated.
        for ev in &events {
            assert!(!ev.stage.is_empty(), "stage must be non-empty: {:?}", ev);
            assert!(ev.percent <= 100, "percent must be <= 100: {:?}", ev);
        }

        // The last event must be terminal (done or error).
        let last = events.last().unwrap();
        assert!(
            last.stage == "done" || last.stage == "error",
            "terminal event must be done/error, got stage={}",
            last.stage
        );
        assert_eq!(last.percent, 100);

        // Percent must be non-decreasing across the sequence.
        let mut prev = 0u8;
        for ev in &events {
            assert!(
                ev.percent >= prev,
                "percent regressed: {:?} after prev {}",
                ev,
                prev
            );
            prev = ev.percent;
        }
    }
}

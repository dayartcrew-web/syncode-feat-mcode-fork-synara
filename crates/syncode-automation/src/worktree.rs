//! Git worktree isolation for automation runs (P2-8).
//!
//! When an automation's [`WorktreeMode`] is `Worktree` (or `Auto`), each
//! standalone run executes inside a freshly-created git worktree rooted at
//! `automation/<slugified-name>/<suffix>`. This gives every run a pristine,
//! isolated checkout — divergent changes in one run cannot leak into another.
//!
//! ## Lifecycle
//!
//! 1. **Before dispatch** — [`WorktreeManager::create`] runs
//!    `git worktree add <path>` (creating a new working tree at `HEAD`).
//! 2. **On failure** — [`WorktreeManager::remove`] runs
//!    `git worktree remove --force <path>` (cleanup).
//! 3. **On success** — the worktree is left in place (available for
//!    inspection / follow-up commits).
//!
//! ## Port-shape limitation
//!
//! The current [`syncode_core::ports::DispatchRequest`] does not carry a
//! `working_dir`, so the command is not yet *executed* inside the worktree by
//! [`crate::process_executor::ProcessRunExecutor`]. The worktree is created +
//! cleaned up around the run (the isolation contract's setup/teardown); wiring
//! the path into dispatch is a follow-up that extends the port.

use std::path::{Path, PathBuf};

use tokio::process::Command;

/// How (or whether) an automation run should be isolated in a git worktree.
///
/// Stored on [`crate::definition::AutomationDef`] as `worktree_mode` with a
/// serde default of [`WorktreeMode::Local`] (backward compatible — existing
/// stored defs deserialize to the no-worktree behavior).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeMode {
    /// Run in the host process's working directory (no worktree). The default —
    /// preserves the historical behavior for automations that don't opt in.
    #[default]
    Local,
    /// Always create a dedicated git worktree per standalone run.
    Worktree,
    /// Automatically decide: behaves like [`WorktreeMode::Worktree`] when the
    /// repository supports it (a follow-up may downgrade to `Local` when no git
    /// repo is detected). For now it is equivalent to `Worktree`.
    Auto,
}

impl WorktreeMode {
    /// Whether this mode requests worktree isolation (i.e. is `Worktree` or
    /// `Auto`). `Local` returns `false`.
    pub fn uses_worktree(self) -> bool {
        matches!(self, WorktreeMode::Worktree | WorktreeMode::Auto)
    }
}

/// Errors that can occur during worktree management.
#[derive(Debug, thiserror::Error)]
pub enum WorktreeError {
    /// A `git` invocation failed (non-zero exit or spawn error).
    #[error("git command failed: {0}")]
    Git(String),
    /// The path contained characters that cannot appear in a worktree path
    /// after slugification (should not happen — slugify strips them).
    #[error("invalid worktree path: {0}")]
    InvalidPath(String),
}

/// Manages the create/remove lifecycle of git worktrees for automation runs.
///
/// Construct with an explicit `repo_root` (the path to the git repository's
/// top-level directory). Worktrees are created under
/// `<repo_root>/automation/<slug>/<suffix>`.
///
/// ```
/// # use syncode_automation::worktree::{WorktreeManager, WorktreeMode};
/// let mgr = WorktreeManager::new("/repo");
/// let path = mgr.worktree_path("Nightly Build", "run-abc");
/// assert!(path.ends_with("automation/nightly-build/run-abc"));
/// ```
#[derive(Debug, Clone)]
pub struct WorktreeManager {
    repo_root: PathBuf,
}

impl WorktreeManager {
    /// Create a manager rooted at `repo_root`.
    pub fn new(repo_root: impl Into<PathBuf>) -> Self {
        Self {
            repo_root: repo_root.into(),
        }
    }

    /// Compute the worktree path for a given automation name + run suffix,
    /// without creating anything. The name is slugified (lowercased, spaces →
    /// hyphens, non-alphanumerics stripped) so arbitrary automation names
    /// produce valid directory names.
    ///
    /// Format: `<repo_root>/automation/<slugified-name>/<suffix>`
    pub fn worktree_path(&self, automation_name: &str, suffix: &str) -> PathBuf {
        self.repo_root
            .join("automation")
            .join(slugify(automation_name))
            .join(suffix)
    }

    /// Whether the given def's [`WorktreeMode`] requests worktree isolation.
    /// Convenience wrapper around [`WorktreeMode::uses_worktree`] so callers
    /// don't need to import the enum.
    pub fn should_isolate(mode: WorktreeMode) -> bool {
        mode.uses_worktree()
    }

    /// Create a git worktree for `automation_name` tagged with `suffix`.
    /// Returns the absolute path of the new working tree on success.
    ///
    /// Runs `git worktree add <path>` (checking out `HEAD`). The parent
    /// directory tree is created if needed.
    pub async fn create(
        &self,
        automation_name: &str,
        suffix: &str,
    ) -> Result<PathBuf, WorktreeError> {
        let path = self.worktree_path(automation_name, suffix);
        // Ensure the parent directory exists so `git worktree add` can place
        // the working tree there.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| WorktreeError::Git(format!("create_dir_all failed: {e}")))?;
        }

        let output = Command::new("git")
            .arg("worktree")
            .arg("add")
            .arg(&path)
            .current_dir(&self.repo_root)
            .output()
            .await
            .map_err(|e| WorktreeError::Git(format!("spawn failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(WorktreeError::Git(format!(
                "git worktree add exited {}: {}",
                output.status.code().unwrap_or(-1),
                stderr.trim()
            )));
        }
        Ok(path)
    }

    /// Remove a previously-created worktree (cleanup on failure). Runs
    /// `git worktree remove --force <path>`. Best-effort — logs on failure.
    pub async fn remove(&self, path: &Path) -> Result<(), WorktreeError> {
        let output = Command::new("git")
            .arg("worktree")
            .arg("remove")
            .arg("--force")
            .arg(path)
            .current_dir(&self.repo_root)
            .output()
            .await
            .map_err(|e| WorktreeError::Git(format!("spawn failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(WorktreeError::Git(format!(
                "git worktree remove exited {}: {}",
                output.status.code().unwrap_or(-1),
                stderr.trim()
            )));
        }
        Ok(())
    }
}

/// Slugify a name for use as a directory component: lowercase ASCII, spaces →
/// hyphens, other non-alphanumeric characters stripped, collapse repeated
/// hyphens. Empty input yields `"unnamed"`.
fn slugify(name: &str) -> String {
    let slug: String = name
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else if c.is_whitespace() || c == '_' || c == '-' {
                '-'
            } else {
                '\0' // stripped below
            }
        })
        .filter(|c| *c != '\0')
        .collect();
    // Collapse runs of hyphens + trim leading/trailing.
    let collapsed: String = collapse_hyphens(&slug);
    if collapsed.is_empty() {
        "unnamed".to_string()
    } else {
        collapsed
    }
}

/// Collapse consecutive hyphens and trim leading/trailing hyphens.
fn collapse_hyphens(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_hyphen = false;
    for c in s.chars() {
        if c == '-' {
            if !prev_hyphen && !out.is_empty() {
                out.push('-');
            }
            prev_hyphen = true;
        } else {
            out.push(c);
            prev_hyphen = false;
        }
    }
    // Trim trailing hyphen.
    if out.ends_with('-') {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── WorktreeMode ──────────────────────────────────────────────────

    #[test]
    fn worktree_mode_default_is_local() {
        assert_eq!(WorktreeMode::default(), WorktreeMode::Local);
    }

    #[test]
    fn worktree_mode_uses_worktree() {
        assert!(!WorktreeMode::Local.uses_worktree());
        assert!(WorktreeMode::Worktree.uses_worktree());
        assert!(WorktreeMode::Auto.uses_worktree());
    }

    #[test]
    fn worktree_mode_serialization() {
        // snake_case serde representation.
        let json = serde_json::to_string(&WorktreeMode::Worktree).unwrap();
        assert_eq!(json, "\"worktree\"");
        let json = serde_json::to_string(&WorktreeMode::Local).unwrap();
        assert_eq!(json, "\"local\"");
        let json = serde_json::to_string(&WorktreeMode::Auto).unwrap();
        assert_eq!(json, "\"auto\"");

        // Round-trip.
        let back: WorktreeMode = serde_json::from_str("\"auto\"").unwrap();
        assert_eq!(back, WorktreeMode::Auto);
    }

    #[test]
    fn worktree_mode_missing_field_defaults_to_local() {
        // A legacy payload without worktreeMode deserializes to Local.
        #[derive(serde::Deserialize)]
        struct Wrapper {
            #[serde(default)]
            mode: WorktreeMode,
        }
        let w: Wrapper = serde_json::from_str("{}").unwrap();
        assert_eq!(w.mode, WorktreeMode::Local);
    }

    // ─── slugify ───────────────────────────────────────────────────────

    #[test]
    fn slugify_basic() {
        assert_eq!(slugify("Nightly Build"), "nightly-build");
        assert_eq!(slugify("  hello   world  "), "hello-world");
        assert_eq!(slugify("test_auto"), "test-auto");
    }

    #[test]
    fn slugify_strips_special_chars() {
        assert_eq!(slugify("build & deploy!"), "build-deploy");
        assert_eq!(slugify("café"), "caf"); // non-ASCII stripped
    }

    #[test]
    fn slugify_empty_becomes_unnamed() {
        assert_eq!(slugify(""), "unnamed");
        assert_eq!(slugify("   "), "unnamed");
        assert_eq!(slugify("!!!"), "unnamed");
    }

    // ─── WorktreeManager path computation ──────────────────────────────

    #[test]
    fn worktree_path_format() {
        let mgr = WorktreeManager::new("/repo");
        let path = mgr.worktree_path("Nightly Build", "run-abc123");
        assert!(path.starts_with("/repo"));
        assert!(path.ends_with("automation/nightly-build/run-abc123"));
    }

    #[test]
    fn worktree_path_unique_per_suffix() {
        let mgr = WorktreeManager::new("/repo");
        let p1 = mgr.worktree_path("build", "run-1");
        let p2 = mgr.worktree_path("build", "run-2");
        assert_ne!(p1, p2);
    }

    // ─── WorktreeManager create/remove lifecycle (real git) ────────────
    //
    // These tests create a throwaway git repository in the system temp dir,
    // then exercise the real `git worktree add` / `remove` round-trip.
    // Skipped if `git` is not on PATH.

    fn git_is_available() -> bool {
        std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Create a throwaway git repo with one (empty) commit so worktrees can be
    /// attached. Returns the repo path. The caller is responsible for cleanup
    /// (best-effort — the temp dir may be reaped by the OS).
    fn make_temp_repo() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "syncode-wt-test-{}",
            uuid::Uuid::new_v4().hyphenated()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // git init
        let out = std::process::Command::new("git")
            .arg("init")
            .current_dir(&dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "git init failed");

        // Set a local identity (CI environments may not have one).
        for (k, v) in &[("user.name", "test"), ("user.email", "t@t.test")] {
            let _ = std::process::Command::new("git")
                .args(["config", k, v])
                .current_dir(&dir)
                .output();
        }

        // Create an initial commit (worktree needs HEAD to exist).
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

    /// Best-effort recursive directory removal for test cleanup.
    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn worktree_create_and_remove_roundtrip() {
        if !git_is_available() {
            eprintln!("skipping: git not on PATH");
            return;
        }
        let repo = make_temp_repo();
        let mgr = WorktreeManager::new(&repo);

        // Create a worktree.
        let path = mgr
            .create("test-build", "run-001")
            .await
            .expect("worktree create should succeed");
        assert!(path.is_dir(), "worktree dir should exist after create");
        assert!(
            path.ends_with("automation/test-build/run-001"),
            "unexpected path: {}",
            path.display()
        );

        // Remove it.
        mgr.remove(&path)
            .await
            .expect("worktree remove should succeed");
        assert!(!path.exists(), "worktree dir should be gone after remove");

        cleanup(&repo);
    }

    #[tokio::test]
    async fn worktree_create_isolates_checkouts() {
        if !git_is_available() {
            eprintln!("skipping: git not on PATH");
            return;
        }
        let repo = make_temp_repo();
        let mgr = WorktreeManager::new(&repo);

        // Two distinct runs → two distinct worktree directories.
        let p1 = mgr.create("ci", "run-a").await.unwrap();
        let p2 = mgr.create("ci", "run-b").await.unwrap();
        assert_ne!(p1, p2);
        assert!(p1.is_dir());
        assert!(p2.is_dir());

        // Writing a file in one does not appear in the other.
        std::fs::write(p1.join("marker.txt"), "from-a").unwrap();
        assert!(p1.join("marker.txt").exists());
        assert!(
            !p2.join("marker.txt").exists(),
            "worktrees must be isolated"
        );

        // Cleanup both.
        mgr.remove(&p1).await.unwrap();
        mgr.remove(&p2).await.unwrap();

        cleanup(&repo);
    }

    #[test]
    fn should_isolate_reflects_mode() {
        assert!(!WorktreeManager::should_isolate(WorktreeMode::Local));
        assert!(WorktreeManager::should_isolate(WorktreeMode::Worktree));
        assert!(WorktreeManager::should_isolate(WorktreeMode::Auto));
    }
}

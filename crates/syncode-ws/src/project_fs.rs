//! Project filesystem primitives — sandboxed read/write/list/search.
//!
//! All entry points take a `root` directory (the project root) plus a
//! caller-supplied *relative* path and resolve the latter under the former.
//! The resolution step ([`resolve_within_root`]) is the **security-critical**
//! piece: it canonicalizes both paths and rejects any result that escapes the
//! project root. This blocks the standard traversal vectors:
//!
//! - `../` escapes (`../../etc/passwd`)
//! - absolute paths (`/etc/passwd`, `C:\Windows\system32`)
//! - symlinks inside the root that point outside it (canonicalize follows the
//!   link, so the resolved path lands outside the root and is rejected)
//!
//! The guard pattern mirrors `read_plugin` (`crates/syncode-ws/src/rpc.rs`),
//! which canonicalizes the requested path and asserts a `.plugins` ancestor is
//! present. Here the assertion is "the canonical path starts with the
//! canonical root" — a stricter, project-scoped variant.
//!
//! PROJ-1 ships the primitives + guard; PROJ-2/3/4 wire them into the
//! `project.*` JSON-RPC handlers (read_file / write_file / list_directory /
//! search_files / …).

use std::path::{Path, PathBuf};

use syncode_core::util::path as core_path;
use thiserror::Error;

/// Error returned by the project filesystem primitives.
#[derive(Debug, Error)]
pub enum ProjectFsError {
    /// The supplied project root does not exist or cannot be canonicalized.
    #[error("invalid project root")]
    InvalidRoot,
    /// The resolved path escapes the project root — traversal blocked.
    #[error("path traversal detected")]
    PathTraversal,
    /// The target path (or its parent, for a write) does not exist.
    #[error("path not found")]
    NotFound,
    /// A read was attempted against a non-file entry (e.g. a directory).
    #[error("not a file")]
    NotAFile,
    /// An underlying OS I/O error (permissions, disk, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// A single directory entry surfaced by [`list_directory`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    /// File/dir name (no path).
    pub name: String,
    /// `true` when this entry is a directory.
    pub is_dir: bool,
    /// File size in bytes (0 for directories).
    pub size: u64,
}

/// Resolve a caller-supplied relative path under `root`, enforcing that the
/// canonical result stays inside the (canonicalized) project root.
///
/// Empty `relative` returns the canonical root itself (listing the project
/// root is allowed). Absolute paths and `../` escapes are rejected with
/// [`ProjectFsError::PathTraversal`]. Symlinks are followed by `canonicalize`;
/// a link whose target is outside the root is rejected the same way.
///
/// For non-existent leaves (the write-a-new-file case), the leaf's *parent*
/// is canonicalized and the file name re-appended; the parent must itself be
/// inside the root, so `../newfile` under a root whose parent is outside is
/// still blocked.
pub fn resolve_within_root(root: &Path, relative: &str) -> Result<PathBuf, ProjectFsError> {
    // Delegate the canonicalize step to syncode-core (uses dunce on Windows to
    // avoid \\?\ prefix pollution; passthrough on Unix).
    let canonical_root = core_path::canonicalize_existing(root)
        .map_err(|_| ProjectFsError::InvalidRoot)?;

    // Empty relative → the root itself.
    if relative.is_empty() {
        return Ok(canonical_root);
    }

    let rel_path = Path::new(relative);

    // Reject absolute paths outright (Windows `C:\…` and Unix `/…`).
    if rel_path.is_absolute() {
        return Err(ProjectFsError::PathTraversal);
    }

    // ─── Defense layer 1: lexical pre-check (delegated to core) ──
    if core_path::relative_goes_above_root(relative) {
        return Err(ProjectFsError::PathTraversal);
    }

    let candidate = canonical_root.join(rel_path);

    // ─── Defense layer 2: canonicalize + containment ──────────────
    let canonical = match core_path::canonicalize_existing(&candidate) {
        Ok(c) => c,
        Err(_) => {
            let parent = match candidate.parent() {
                Some(p) if !p.as_os_str().is_empty() => p,
                _ => return Err(ProjectFsError::PathTraversal),
            };
            let canonical_parent = core_path::canonicalize_existing(parent)
                .map_err(|_| ProjectFsError::NotFound)?;
            if !canonical_parent.starts_with(&canonical_root) {
                return Err(ProjectFsError::PathTraversal);
            }
            match candidate.file_name() {
                Some(name) => canonical_parent.join(name),
                None => return Err(ProjectFsError::PathTraversal),
            }
        }
    };

    if !canonical.starts_with(&canonical_root) {
        return Err(ProjectFsError::PathTraversal);
    }
    Ok(canonical)
}

// Note: `relative_goes_above_root` and `canonicalize_longest_existing_ancestor`
// were extracted to `syncode_core::util::path` (canonicalize_lexical /
// canonicalize_hybrid / relative_goes_above_root / is_within_root). Call sites
// in this module now use the core implementations directly.

#[cfg(test)]
mod lexical_tests {
    // Tests exercise the shared core implementation directly.
    use syncode_core::util::path::relative_goes_above_root;

    #[test]
    fn empty_path_does_not_escape() {
        assert!(!relative_goes_above_root(""));
    }
    #[test]
    fn simple_relative_does_not_escape() {
        assert!(!relative_goes_above_root("a/b/c"));
    }
    #[test]
    fn dotdot_at_start_escapes() {
        assert!(relative_goes_above_root("../b"));
    }
    #[test]
    fn dotdot_chain_escapes() {
        assert!(relative_goes_above_root("../../etc/passwd"));
        assert!(relative_goes_above_root("a/../../../b"));
    }
    #[test]
    fn balanced_dotdot_stays_at_root() {
        // `a/..` → depth 0; ok.
        assert!(!relative_goes_above_root("a/../b"));
        assert!(!relative_goes_above_root("a/b/../../c"));
    }
}

/// List the entries of `root / relative` (or `root` itself when `relative` is
/// empty). Entries are sorted by name for deterministic output. Symlinks are
/// reported with `is_dir` reflecting their target (canonicalize-based).
pub async fn list_directory(root: &Path, relative: &str) -> Result<Vec<DirEntry>, ProjectFsError> {
    let path = resolve_within_root(root, relative)?;
    let mut reader = tokio::fs::read_dir(&path).await?;
    let mut entries = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        let metadata = entry.metadata().await?;
        entries.push(DirEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            is_dir: metadata.is_dir(),
            size: if metadata.is_file() { metadata.len() } else { 0 },
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

/// Read a file under `root / relative` as bytes. Returns
/// [`ProjectFsError::NotAFile`] if the resolved path is a directory.
pub async fn read_file(root: &Path, relative: &str) -> Result<Vec<u8>, ProjectFsError> {
    let path = resolve_within_root(root, relative)?;
    let metadata = tokio::fs::metadata(&path).await?;
    if !metadata.is_file() {
        return Err(ProjectFsError::NotAFile);
    }
    let bytes = tokio::fs::read(&path).await?;
    Ok(bytes)
}

/// Write `content` to `root / relative`, creating parent directories as
/// needed (inside the root only — the resolution step guarantees this).
///
/// Unlike [`read_file`] / [`list_directory`], this does **not** require the
/// target (or its parents) to pre-exist: [`resolve_for_write`] runs the lexical
/// pre-check, then canonicalizes the longest *existing* ancestor of the
/// candidate and requires it to stay inside the canonical root — so any
/// symlinked directory along the existing path is dereferenced and rejected if
/// it points outside. Non-existent trailing components are then re-appended
/// lexically; since they don't yet exist they cannot be symlinks, and
/// `create_dir_all` only ever creates paths derived from guard-approved
/// components, so it cannot escape the sandbox.
pub async fn write_file(root: &Path, relative: &str, content: &[u8]) -> Result<(), ProjectFsError> {
    let path = resolve_for_write(root, relative)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&path, content).await?;
    Ok(())
}

/// Variant of [`resolve_within_root`] for the write-a-new-file case: doesn't
/// require the leaf (or its parents) to pre-exist.
///
/// Runs the lexical pre-check (defense layer 1), then a **canonicalize-the-
/// longest-existing-ancestor** step (defense layer 2). The reason this can't
/// just delegate to [`resolve_within_root`] is that `write_file` supports
/// creating missing parents via `create_dir_all` (e.g. writing
/// `nested/dir/deep.txt` when only `root` exists). `resolve_within_root`
/// canonicalizes only the immediate parent and returns [`ProjectFsError::NotFound`]
/// when that parent doesn't yet exist — which would block the nested-create use
/// case. Here we instead walk up from the candidate until we hit an existing
/// ancestor, canonicalize *that*, and require the result to stay inside the
/// root. Any symlinked directory in the path is an existing component, so it
/// gets canonicalized and (if it points outside) caught. Non-existent trailing
/// components are then re-appended lexically; since they don't exist they
/// cannot themselves be symlinks yet.
fn resolve_for_write(root: &Path, relative: &str) -> Result<PathBuf, ProjectFsError> {
    let canonical_root = core_path::canonicalize_existing(root)
        .map_err(|_| ProjectFsError::InvalidRoot)?;
    if relative.is_empty() {
        return Err(ProjectFsError::NotAFile);
    }
    let rel_path = Path::new(relative);
    if rel_path.is_absolute() {
        return Err(ProjectFsError::PathTraversal);
    }
    if core_path::relative_goes_above_root(relative) {
        return Err(ProjectFsError::PathTraversal);
    }
    let candidate = canonical_root.join(rel_path);
    // Canonicalize the longest existing ancestor (delegated to syncode-core).
    // This follows any symlinks among the *existing* components.
    let canonical = core_path::canonicalize_hybrid(&candidate)?;
    if !canonical.starts_with(&canonical_root) {
        return Err(ProjectFsError::PathTraversal);
    }
    Ok(canonical)
}

/// Recursively search `root` for files whose name contains `query`
/// (case-sensitive substring). Returns paths relative to `root`, using `/`
/// separators. Common noisy directories (`.git`, `node_modules`) are skipped.
///
/// PROJ-1 ships substring matching; PROJ-4 will upgrade to glob/regex.
pub async fn search_files(root: &Path, query: &str) -> Result<Vec<String>, ProjectFsError> {
    let canonical_root = resolve_within_root(root, "")?;
    let mut matches = Vec::new();
    walk_dir(&canonical_root, &canonical_root, query, &mut matches).await?;
    matches.sort();
    Ok(matches)
}

/// Recursive directory walk helper. `dir` is the current position, `root` is
/// the immutable project root (for relative-path computation). The recursion
/// is bounded by the filesystem itself; large trees are not a concern for the
/// skeleton (PROJ-4 will add pagination/limits).
///
/// **Symlink safety (REWORK round 1):** uses [`DirEntry::symlink_metadata`]
/// (not `metadata()`, which follows links) and **skips symlink entries
/// entirely** — neither matching them as files nor recursing through them as
/// directories. This prevents the walk from (a) leaving the project root via a
/// symlinked directory that points outside it, and (b) entering symlink loops.
/// PROJ-4 may introduce an opt-in follow-links mode; for the skeleton the
/// safe default is to treat links as opaque.
///
/// Recursion is `Box::pin`-pinned because async fns cannot be directly
/// recursive without an indirection (their generated futures are sized
/// assuming non-recursive calls).
async fn walk_dir(
    root: &Path,
    dir: &Path,
    query: &str,
    out: &mut Vec<String>,
) -> Result<(), ProjectFsError> {
    let mut reader = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = reader.next_entry().await? {
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        let entry_path = entry.path();
        // Use symlink_metadata (free fn on tokio::fs; does NOT follow the
        // link, unlike DirEntry::metadata) so we can detect symlinks and skip
        // them, rather than recursing through a link that points outside the
        // root or into a loop.
        let metadata = tokio::fs::symlink_metadata(&entry_path).await?;
        if metadata.is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            // Skip the well-known noisy dirs to keep searches useful.
            if file_name_str == ".git" || file_name_str == "node_modules" {
                continue;
            }
            // Recursion is indirect (async fn); pin to avoid the borrow cycle.
            Box::pin(walk_dir(root, &entry_path, query, out)).await?;
        } else if metadata.is_file()
            && file_name_str.contains(query)
            && let Ok(rel) = entry_path.strip_prefix(root)
        {
            out.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

// ─── PROJ-3: script discovery + execution ─────────────────────────────────
//
// `discover_scripts` returns the union of:
//   1. `scripts` entries from `package.json` (Node.js), and
//   2. target lines from a GNU/BSD `Makefile`.
// Each entry carries its `source` ("package.json" or "Makefile") so the UI
// can badge origin and `run_script` can pick the right launcher.
//
// `run_script` shells out via `tokio::process::Command`:
//   - `package.json` scripts  → `npm run <name>` (the canonical way to run
//     an npm script — works whether the project uses npm, pnpm, or yarn
//     via `packageManager` field since each tool honors `npm run`).
//   - `Makefile` targets      → `make <target>`
//   - unknown source / direct → run `command` verbatim through the shell
//     (`sh -c` on Unix, `cmd /C` on Windows) so callers can pass a raw
//     command string.
// In every case the process runs in the project root (`cwd / relative`),
// applies the traversal guard to `cwd`/`relative`, and returns
// `{ stdout, stderr, exitCode }`.

/// Where a [`DiscoveredScript`] originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptSource {
    /// `package.json` `scripts.<name>`.
    PackageJson,
    /// `Makefile` target.
    Makefile,
}

impl ScriptSource {
    /// Stable string used in the JSON-RPC response (`source` field).
    pub fn as_str(self) -> &'static str {
        match self {
            ScriptSource::PackageJson => "package.json",
            ScriptSource::Makefile => "Makefile",
        }
    }
}

impl std::fmt::Display for ScriptSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single runnable script/target discovered in the project root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredScript {
    /// Script/target name (e.g. `test`, `build`, `lint`).
    pub name: String,
    /// The raw command string (`package.json` value or `Makefile` recipe
    /// placeholder for the target). For Makefile targets the command is
    /// the first recipe line (trimmed) — purely informational; the actual
    /// run is delegated to `make`.
    pub command: String,
    /// Where the script came from.
    pub source: ScriptSource,
}

/// Result of [`run_script`]: captured process output + exit status.
#[derive(Debug, Clone)]
pub struct ScriptRunResult {
    /// Decoded stdout (lossy utf-8).
    pub stdout: String,
    /// Decoded stderr (lossy utf-8).
    pub stderr: String,
    /// Exit code. `None` if the process was killed by a signal (Unix).
    pub exit_code: Option<i32>,
}

/// Discover runnable scripts/targets under `root / relative`.
///
/// Parses `package.json` (the `scripts` object, if present) and a `Makefile`
/// (target lines — those of the form `name:` at column 0, optionally followed
/// by prerequisites; recipe lines begin with a TAB and are ignored for
/// discovery). Returns the union sorted by `(source, name)` for deterministic
/// output. If neither file exists, returns an empty list.
pub async fn discover_scripts(root: &Path, relative: &str) -> Result<Vec<DiscoveredScript>, ProjectFsError> {
    let base = resolve_within_root(root, relative)?;
    let mut out = Vec::new();

    // ── package.json ──
    let pkg_path = base.join("package.json");
    if tokio::fs::metadata(&pkg_path).await.is_ok()
        && let Ok(bytes) = tokio::fs::read(&pkg_path).await
    {
        // Parse defensively: a malformed package.json must not break
        // discovery (we just skip it and continue to the Makefile).
        if let Ok(serde_json::Value::Object(map)) = serde_json::from_slice::<serde_json::Value>(&bytes)
            && let Some(serde_json::Value::Object(scripts)) = map.get("scripts")
        {
            for (name, val) in scripts {
                if let serde_json::Value::String(cmd) = val {
                    out.push(DiscoveredScript {
                        name: name.clone(),
                        command: cmd.clone(),
                        source: ScriptSource::PackageJson,
                    });
                }
            }
        }
    }

    // ── Makefile ──
    // GNU/BSD make also honors `makefile` (lowercase) and `GNUmakefile`;
    // the canonical MCode project convention is `Makefile`, so we stick to
    // that to avoid surprise cross-listings (matches the MCode reference UI).
    let makefile_path = base.join("Makefile");
    if tokio::fs::metadata(&makefile_path).await.is_ok()
        && let Ok(text) = tokio::fs::read_to_string(&makefile_path).await
    {
        for line in text.lines() {
            // Recipe lines start with TAB — skip. Comments start with '#'.
            // A target line has the shape `name: prereqs` with `name`
            // containing no whitespace (other than the trailing colon).
            // Pattern rules (`%.o: %.c`) are included; `.PHONY` etc.
            // carry a leading dot but are still valid targets.
            if line.starts_with('\t') || line.starts_with(' ') || line.starts_with('#') {
                continue;
            }
            let line = line.trim_end_matches('\\').trim_end();
            if line.is_empty() {
                continue;
            }
            // Must contain a colon and have a non-empty LHS.
            let Some((lhs, _rhs)) = line.split_once(':') else {
                continue;
            };
            let lhs = lhs.trim();
            if lhs.is_empty() || lhs.contains(|c: char| c.is_whitespace()) {
                continue;
            }
            out.push(DiscoveredScript {
                name: lhs.to_string(),
                command: format!("make {lhs}"),
                source: ScriptSource::Makefile,
            });
        }
    }

    out.sort_by(|a, b| match a.source.as_str().cmp(b.source.as_str()) {
        std::cmp::Ordering::Equal => a.name.cmp(&b.name),
        other => other,
    });
    Ok(out)
}

/// Run a script by name (or raw command) inside `root / relative`.
///
/// Dispatch keys (`source` param):
/// - `"package.json"` → `npm run <name>`
/// - `"Makefile"`     → `make <name>`
/// - any other / absent → run `command` (or `name`) through the shell.
///
/// The `cwd` for the child process is `root / relative` (traversal-guarded).
/// Returns captured stdout/stderr + exit code. The process inherits the
/// server's environment (so `npm`, `make`, `PATH`, etc. resolve).
pub async fn run_script(
    root: &Path,
    relative: &str,
    name: &str,
    source: Option<&str>,
    command: Option<&str>,
) -> Result<ScriptRunResult, ProjectFsError> {
    let cwd = resolve_within_root(root, relative)?;

    // Build the argv. We deliberately use the platform-native launcher rather
    // than spawning `npm`/`make` by bare name in some cases — but the run is
    // always via `tokio::process::Command` (the cwd is already validated).
    #[cfg(unix)]
    let (shell, shell_flag): (&str, &str) = ("sh", "-c");
    #[cfg(windows)]
    let (shell, shell_flag): (&str, &str) = ("cmd", "/C");

    let mut cmd = tokio::process::Command::new(shell);
    cmd.arg(shell_flag).current_dir(&cwd);

    let cmdline = match source {
        Some("package.json") => {
            // `npm run` resolves the script from package.json; we pass only
            // the name (the command itself lives in package.json).
            // Quote the name defensively — `npm run` handles one argv token.
            format!("npm run --silent {name}")
        }
        Some("Makefile") => {
            format!("make {name}")
        }
        _ => {
            // Unknown / no source: run the raw `command` if given, else `name`.
            // If `command` looks like it already names a script, the caller
            // is responsible for quoting. This is the "direct" path.
            command.unwrap_or(name).to_string()
        }
    };
    cmd.arg(&cmdline);

    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let output = cmd.output().await?;
    let exit_code = output.status.code();
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    Ok(ScriptRunResult {
        stdout,
        stderr,
        exit_code,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an empty tempdir and return its path.
    fn temp_root() -> tempfile::TempDir {
        tempfile::tempdir().expect("create tempdir")
    }

    // ─── Smoke tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn list_directory_smoke_on_tempdir() {
        // AC: ≥1 smoke test (list_directory on a tempdir).
        let root = temp_root();
        let root_path = root.path();

        // Seed: two files + one subdir.
        tokio::fs::write(root_path.join("a.txt"), b"aaa")
            .await
            .unwrap();
        tokio::fs::write(root_path.join("b.txt"), b"bb")
            .await
            .unwrap();
        tokio::fs::create_dir(root_path.join("sub"))
            .await
            .unwrap();

        let entries = list_directory(root_path, "").await.expect("list ok");
        assert_eq!(entries.len(), 3, "three entries expected");
        // Sorted: a.txt, b.txt, sub.
        assert_eq!(entries[0].name, "a.txt");
        assert!(!entries[0].is_dir);
        assert_eq!(entries[0].size, 3);
        assert_eq!(entries[1].name, "b.txt");
        assert_eq!(entries[1].size, 2);
        assert_eq!(entries[2].name, "sub");
        assert!(entries[2].is_dir);
    }

    #[tokio::test]
    async fn list_subdir_via_relative_path() {
        let root = temp_root();
        let root_path = root.path();
        tokio::fs::create_dir_all(root_path.join("d1/d2"))
            .await
            .unwrap();
        tokio::fs::write(root_path.join("d1/d2/inner.md"), b"x")
            .await
            .unwrap();

        let entries = list_directory(root_path, "d1/d2").await.expect("list ok");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "inner.md");
    }

    #[tokio::test]
    async fn read_write_roundtrip() {
        let root = temp_root();
        let root_path = root.path();
        write_file(root_path, "out.txt", b"hello").await.expect("write ok");
        // create_dir_all on parent of nested write.
        write_file(root_path, "nested/dir/deep.txt", b"deep")
            .await
            .expect("nested write ok");
        let a = read_file(root_path, "out.txt").await.expect("read ok");
        assert_eq!(a, b"hello");
        let b = read_file(root_path, "nested/dir/deep.txt").await.expect("read ok");
        assert_eq!(b, b"deep");
    }

    #[tokio::test]
    async fn read_directory_returns_not_a_file() {
        let root = temp_root();
        let root_path = root.path();
        tokio::fs::create_dir(root_path.join("d")).await.unwrap();
        let err = read_file(root_path, "d").await.expect_err("must error");
        assert!(matches!(err, ProjectFsError::NotAFile), "got {err:?}");
    }

    #[tokio::test]
    async fn search_files_finds_by_substring() {
        let root = temp_root();
        let root_path = root.path();
        tokio::fs::write(root_path.join("foo.rs"), b"").await.unwrap();
        tokio::fs::write(root_path.join("bar.rs"), b"").await.unwrap();
        tokio::fs::write(root_path.join("README.md"), b"")
            .await
            .unwrap();
        tokio::fs::create_dir_all(root_path.join("src")).await.unwrap();
        tokio::fs::write(root_path.join("src/foo.rs"), b"").await.unwrap();

        let rs_files = search_files(root_path, ".rs").await.expect("search ok");
        assert_eq!(rs_files.len(), 3, "got {rs_files:?}");
        assert!(rs_files.contains(&"foo.rs".to_string()));
        assert!(rs_files.contains(&"bar.rs".to_string()));
        assert!(rs_files.contains(&"src/foo.rs".to_string()));
    }

    // ─── Path-traversal guard tests ─────────────────────────────────

    #[tokio::test]
    async fn traversal_dotdot_escape_is_blocked() {
        // AC: attempt to escape project root → error.
        let root = temp_root();
        let root_path = root.path();
        // Seed a file *outside* the root (sibling dir) to ensure canonicalize
        // would otherwise succeed.
        let outside = root_path.parent().unwrap();
        tokio::fs::write(outside.join("secret.txt"), b"topsecret")
            .await
            .ok(); // may already exist; ignore.

        let err = list_directory(root_path, "..").await.expect_err("must block");
        assert!(
            matches!(err, ProjectFsError::PathTraversal),
            "got {err:?}"
        );

        let err = read_file(root_path, "../secret.txt").await.expect_err("must block");
        assert!(
            matches!(err, ProjectFsError::PathTraversal),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn traversal_absolute_path_is_blocked() {
        // AC: absolute paths → error.
        let root = temp_root();
        let root_path = root.path();

        // Unix-style and Windows-style absolute targets.
        for abs in ["/etc/passwd", "C:/Windows/system32/drivers/etc/hosts"] {
            let err = resolve_within_root(root_path, abs).expect_err("absolute blocked");
            assert!(
                matches!(err, ProjectFsError::PathTraversal),
                "for {abs}: got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn traversal_dotdot_chain_escape_is_blocked() {
        let root = temp_root();
        let root_path = root.path();
        let err = resolve_within_root(root_path, "../../../../etc/passwd")
            .expect_err("must block");
        assert!(matches!(err, ProjectFsError::PathTraversal), "got {err:?}");
    }

    #[tokio::test]
    async fn write_dotdot_escape_is_blocked() {
        let root = temp_root();
        let root_path = root.path();
        let err = write_file(root_path, "../escape.txt", b"x")
            .await
            .expect_err("must block");
        assert!(matches!(err, ProjectFsError::PathTraversal), "got {err:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn traversal_symlink_escape_is_blocked() {
        // AC: symlink to outside → error (Unix-only; Windows requires admin
        // perms to create symlinks).
        use std::os::unix::fs::symlink;

        let root = temp_root();
        let root_path = root.path();

        // Create a file outside the root, in a sibling tempdir.
        let outside = tempfile::tempdir().expect("outside tempdir");
        let outside_file = outside.path().join("outside.txt");
        tokio::fs::write(&outside_file, b"outside")
            .await
            .unwrap();

        // Plant a symlink inside root pointing at the outside file.
        let link_path = root_path.join("escape_link.txt");
        symlink(&outside_file, &link_path).expect("create symlink");

        // resolve_via_root must reject: the link's canonical target is outside.
        let err = read_file(root_path, "escape_link.txt")
            .await
            .expect_err("symlink escape must block");
        assert!(
            matches!(err, ProjectFsError::PathTraversal),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_inside_root_is_allowed() {
        // Sanity: a symlink whose target *is* inside the root resolves fine.
        use std::os::unix::fs::symlink;

        let root = temp_root();
        let root_path = root.path();
        tokio::fs::write(root_path.join("real.txt"), b"real")
            .await
            .unwrap();
        symlink("real.txt", root_path.join("link.txt")).expect("create symlink");

        let bytes = read_file(root_path, "link.txt").await.expect("read ok");
        assert_eq!(bytes, b"real");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_symlink_directory_escape_is_blocked() {
        // Gap-2 regression test (REWORK round 1): a symlinked directory
        // *inside* the root pointing OUTSIDE the root must not let write_file
        // escape the sandbox. Before the fix, `resolve_for_write` only ran
        // lexical + absolute checks and returned `canonical_root.join(rel)`
        // without canonicalizing intermediate components, so writing
        // `linkdir/x` dereferenced the symlink and landed outside the root.
        //
        // This mirrors `traversal_symlink_escape_is_blocked` (the READ path)
        // but exercises the WRITE path. Unix-only — Windows requires admin
        // privileges to create symlinks, so this runs in CI on Linux.
        use std::os::unix::fs::symlink;

        let root = temp_root();
        let root_path = root.path();

        // A directory outside the root (sibling tempdir).
        let outside = tempfile::tempdir().expect("outside tempdir");

        // Plant a symlinked directory inside the root pointing at the outside
        // directory. The outside dir must exist so the symlink target resolves.
        let link_dir = root_path.join("linkdir");
        symlink(outside.path(), &link_dir).expect("create symlinked dir");

        // Writing through the symlinked dir must be rejected: canonicalizing
        // the existing ancestor (`linkdir`) dereferences it to a path outside
        // the root, which fails containment.
        let err = write_file(root_path, "linkdir/x", b"content")
            .await
            .expect_err("write through symlinked dir must block");
        assert!(
            matches!(err, ProjectFsError::PathTraversal),
            "got {err:?}"
        );

        // Defense-in-depth: the outside directory must remain empty — the
        // blocked write must not have created `outside/x`.
        let leaked = outside.path().join("x");
        assert!(
            !leaked.exists(),
            "write leaked through symlink to {leaked:?}"
        );
    }

    #[tokio::test]
    async fn write_empty_relative_path_returns_not_a_file() {
        // Gap-4 (REWORK round 1): write_file with an empty relative path must
        // surface a clean `NotAFile` error rather than an opaque IO failure
        // from `tokio::fs::write(root_dir, …)`.
        let root = temp_root();
        let root_path = root.path();
        let err = write_file(root_path, "", b"content")
            .await
            .expect_err("empty path must error");
        assert!(
            matches!(err, ProjectFsError::NotAFile),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn search_files_skips_symlinked_directories() {
        // Gap-3 regression test (REWORK round 1): search_files must NOT follow
        // symlinked directories — otherwise a link pointing outside the root
        // (or into a loop) would let the walk escape the sandbox or hang.
        // Uses symlink_metadata and skips any symlink entry outright.
        use std::os::unix::fs::symlink;

        let root = temp_root();
        let root_path = root.path();

        // A real file inside the root — must be found.
        tokio::fs::write(root_path.join("real.rs"), b"")
            .await
            .unwrap();

        // A directory OUTSIDE the root containing a matching file.
        let outside = tempfile::tempdir().expect("outside tempdir");
        tokio::fs::write(outside.path().join("leaked.rs"), b"")
            .await
            .unwrap();

        // Symlink the outside dir into the root.
        symlink(outside.path(), root_path.join("linkdir")).expect("create symlink");

        let matches = search_files(root_path, ".rs").await.expect("search ok");
        // Only the real file should match — the symlinked dir must be skipped.
        assert_eq!(matches.len(), 1, "got {matches:?}");
        assert_eq!(matches[0], "real.rs");
        assert!(
            !matches.iter().any(|m| m.contains("leaked.rs")),
            "search escaped via symlink: {matches:?}"
        );
    }

    #[tokio::test]
    async fn invalid_root_is_rejected() {
        let err = resolve_within_root(Path::new("/nonexistent/path/that/does/not/exist"), "x")
            .expect_err("invalid root");
        assert!(matches!(err, ProjectFsError::InvalidRoot), "got {err:?}");
    }

    #[tokio::test]
    async fn empty_relative_returns_root() {
        let root = temp_root();
        let root_path = root.path();
        let resolved = resolve_within_root(root_path, "").expect("empty ok");
        // Should equal the canonicalized root. Use the same canonicalize path
        // as resolve_within_root (dunce on Windows, std on Unix) so the
        // comparison is consistent across platforms — std::fs::canonicalize
        // returns \\?\-prefixed paths on Windows which would never compare
        // equal to the dunce-stripped result.
        let canonical = syncode_core::util::path::canonicalize_existing(root_path).unwrap();
        assert_eq!(resolved, canonical);
    }

    // ─── PROJ-3: discover_scripts + run_script ────────────────────────────

    #[tokio::test]
    async fn discover_scripts_returns_union_of_package_json_and_makefile() {
        // AC: discoverScripts returns union of package.json scripts + Makefile
        // targets. Seed both files in a tempdir and assert all 4 entries
        // surface, sorted by (source, name).
        let root = temp_root();
        let root_path = root.path();

        // package.json with 2 scripts.
        tokio::fs::write(
            root_path.join("package.json"),
            br#"{
                "name": "demo",
                "scripts": {
                    "build": "tsc",
                    "test": "vitest run"
                }
            }"#,
        )
        .await
        .unwrap();

        // Makefile with 2 targets (+ a recipe line + comment to verify they
        // are NOT surfaced as targets).
        tokio::fs::write(
            root_path.join("Makefile"),
            b"# top-level comment\n\
\ntest:\n\
\tcargo test\n\
\n\
lint:\n\
\tcargo clippy\n",
        )
        .await
        .unwrap();

        let scripts = discover_scripts(root_path, "").await.expect("discover ok");
        // 2 package.json + 2 Makefile = 4.
        assert_eq!(scripts.len(), 4, "got {scripts:?}");

        // Sorted by (source.as_str(), name): "Makefile" < "package.json"
        // (capital 'M' = 0x4D < lowercase 'p' = 0x70 in ASCII), so Makefile
        // entries come first. Within each source, entries are name-sorted.
        assert_eq!(scripts[0].name, "lint");
        assert_eq!(scripts[0].source, ScriptSource::Makefile);

        assert_eq!(scripts[1].name, "test");
        assert_eq!(scripts[1].source, ScriptSource::Makefile);

        assert_eq!(scripts[2].name, "build");
        assert_eq!(scripts[2].source, ScriptSource::PackageJson);
        assert_eq!(scripts[2].command, "tsc");

        assert_eq!(scripts[3].name, "test");
        assert_eq!(scripts[3].source, ScriptSource::PackageJson);
        assert_eq!(scripts[3].command, "vitest run");
    }

    #[tokio::test]
    async fn discover_scripts_empty_when_neither_file_exists() {
        // A bare tempdir with no package.json and no Makefile must return
        // an empty list (not an error) — the discovery is best-effort.
        let root = temp_root();
        let scripts = discover_scripts(root.path(), "").await.expect("discover ok");
        assert!(scripts.is_empty(), "got {scripts:?}");
    }

    #[tokio::test]
    async fn run_script_returns_stdout_stderr_exit_code() {
        // AC: runScript shells out and returns stdout/stderr/exit-code. Use
        // a cross-platform command via the shell (`sh -c` / `cmd /C`): print
        // to stdout and exit 0.
        let root = temp_root();
        let root_path = root.path();
        // The "direct" path (no source) — pass a raw command.
        #[cfg(unix)]
        let cmd = "echo hello-script";
        #[cfg(windows)]
        let cmd = "echo hello-script";

        let result = run_script(root_path, "", cmd, None, Some(cmd))
            .await
            .expect("run ok");
        assert_eq!(result.exit_code, Some(0), "stderr={}", result.stderr);
        assert!(
            result.stdout.contains("hello-script"),
            "stdout was: {}",
            result.stdout
        );
    }

    #[tokio::test]
    async fn run_script_reports_nonzero_exit_code() {
        // AC: non-zero exits surface as a non-None exit code (the call still
        // succeeds — we don't treat exit≠0 as a Rust error, just report it).
        let root = temp_root();
        let root_path = root.path();
        // `exit 7` — works on both sh and cmd.
        let result = run_script(root_path, "", "exit 7", None, Some("exit 7"))
            .await
            .expect("run ok");
        assert_eq!(result.exit_code, Some(7));
    }

    #[tokio::test]
    async fn run_script_blocks_path_traversal() {
        // Defense-in-depth: the cwd path must be inside the root. A `..`
        // relative must be rejected before any subprocess is spawned.
        let root = temp_root();
        let err = run_script(root.path(), "..", "echo hi", None, Some("echo hi"))
            .await
            .expect_err("must block traversal");
        assert!(matches!(err, ProjectFsError::PathTraversal), "got {err:?}");
    }
}

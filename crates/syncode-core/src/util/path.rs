//! Cross-platform canonical path generator with dynamic OS awareness.
//!
//! Three layers of path handling, each with a distinct contract:
//!
//! - **Layer 1 — Pure string ops** ([`normalize_separators`], [`canonicalize_lexical`],
//!   [`to_absolute`]): no filesystem I/O, infallible (or only fail on a missing CWD).
//!   Mirror Go's `filepath.Clean` / Python's `os.path.normpath`. Use these for
//!   display, comparison, or as a TOCTOU-free fast-reject before the real check.
//!
//! - **Layer 2 — Filesystem canonicalization** ([`canonicalize_existing`],
//!   [`canonicalize_hybrid`]): touch the disk. `existing` requires the path to
//!   exist and resolves all symlinks (POSIX `realpath`); `hybrid` tolerates
//!   non-existent leaves by canonicalizing the longest existing ancestor —
//!   needed for write-before-create flows.
//!
//! - **Layer 3 — Sandbox containment** ([`is_within_root`], [`relative_goes_above_root`]):
//!   the security boundary. Canonicalizes both sides before the containment
//!   check so symlinks, macOS `/tmp` → `/private/tmp` rewriting, and Windows
//!   drive-letter quirks are all handled consistently.
//!
//! ## OS awareness
//!
//! On Windows, [`canonicalize_existing`] and [`canonicalize_hybrid`] use
//! [`dunce::canonicalize`] to strip the `\\?\` verbatim prefix that
//! `std::fs::canonicalize` adds (see <https://github.com/rust-lang/rust/issues/42869>).
//! That prefix breaks Node.js, many CLI tools, and string equality with
//! non-canonical paths. `dunce` only strips it when safe (no UNC host, no
//! reserved name, under `MAX_PATH`); genuinely verbatim paths are left alone.
//! On Unix, `dunce` is a no-op passthrough.
//!
//! ## When to use which function
//!
//! | Use case | Function |
//! |----------|----------|
//! | Display a path cleanly | `normalize_separators` + `canonicalize_lexical` |
//! | Compare two paths lexically | `canonicalize_lexical` |
//! | Resolve symlinks for an existing file | `canonicalize_existing` |
//! | Resolve for a file about to be created | `canonicalize_hybrid` |
//! | Sandbox check | `is_within_root` (canonicalizes both sides) |
//! | Fast pre-reject of `../` escapes | `relative_goes_above_root` |

use std::io;
use std::path::{Component, Path, PathBuf};

/// The path-separator style of the current operating system.
///
/// Returned by [`current_path_style`] for runtime OS detection; useful for
/// cross-compilation-aware code and tests that need to assert OS-specific
/// behavior without `cfg` blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathStyle {
    /// POSIX style: forward slash `/` separator, single root, no drive letters.
    /// Linux, macOS, FreeBSD, etc.
    Unix,
    /// Windows style: backslash `\` separator (forward slash also accepted by
    /// most APIs), drive letters (`C:\`), UNC shares (`\\server\share`).
    Windows,
}

impl PathStyle {
    /// The native separator character for this style.
    #[must_use]
    pub const fn separator(self) -> char {
        match self {
            PathStyle::Unix => '/',
            PathStyle::Windows => '\\',
        }
    }

    /// The non-native separator character (the one to normalize away).
    #[must_use]
    pub const fn other_separator(self) -> char {
        match self {
            PathStyle::Unix => '\\',
            PathStyle::Windows => '/',
        }
    }

    /// Returns `true` if this style is [`PathStyle::Windows`].
    #[must_use]
    pub const fn is_windows(self) -> bool {
        matches!(self, PathStyle::Windows)
    }
}

/// Returns the [`PathStyle`] of the operating system we're compiled for.
///
/// This is a compile-time constant — use it when the OS is known at build time.
/// For runtime detection (e.g. cross-platform test fixtures), this is still
/// correct since the binary itself is platform-specific.
#[must_use]
pub const fn current_path_style() -> PathStyle {
    #[cfg(windows)]
    {
        PathStyle::Windows
    }
    #[cfg(not(windows))]
    {
        PathStyle::Unix
    }
}

/// Returns `true` if running on Windows. Sugar for `cfg!(windows)` at runtime
/// — useful in test bodies that need to branch on OS without separate
/// `#[cfg]`-gated functions.
#[must_use]
pub fn is_windows() -> bool {
    cfg!(windows)
}

// ---------------------------------------------------------------------------
// Layer 1: Pure string operations (no I/O, infallible)
// ---------------------------------------------------------------------------

/// Normalize path separators to the current OS convention.
///
/// On Windows, replaces forward slashes `/` with backslashes `\`.
/// On Unix, replaces backslashes `\` with forward slashes `/`.
///
/// Does not collapse `.` or `..` — use [`canonicalize_lexical`] for that.
/// This is a pure string operation, no filesystem I/O.
#[must_use]
pub fn normalize_separators(path: &Path) -> PathBuf {
    let style = current_path_style();
    let s = path.to_string_lossy();
    let normalized = s.replace(
        style.other_separator(),
        style.separator().to_string().as_str(),
    );
    PathBuf::from(normalized)
}

/// Lexical path normalization — collapse `.`, resolve `..` where possible,
/// and remove redundant separators. Mirrors Go's `filepath.Clean` and
/// Python's `os.path.normpath`.
///
/// This is a **pure string operation**: it does not touch the filesystem and
/// therefore cannot resolve symlinks. Use [`canonicalize_existing`] when you
/// need symlink resolution.
///
/// # Examples
///
/// ```
/// # use std::path::{Path, PathBuf};
/// # use syncode_core::util::path::canonicalize_lexical;
/// assert_eq!(canonicalize_lexical(Path::new("a/./b")), PathBuf::from("a/b"));
/// assert_eq!(canonicalize_lexical(Path::new("a/b/../c")), PathBuf::from("a/c"));
/// assert_eq!(canonicalize_lexical(Path::new("a//b")), PathBuf::from("a/b"));
/// ```
///
/// # Leading `..`
///
/// `..` components at the start of a relative path cannot be resolved without
/// knowing the root, so they are preserved: `../x` → `../x`. Once a `Normal`
/// component appears, subsequent `..` can cancel it: `a/../..` → `..`.
#[must_use]
pub fn canonicalize_lexical(path: &Path) -> PathBuf {
    use std::ffi::OsString;

    let mut stack: Vec<OsString> = Vec::new();
    let mut prefix: Option<OsString> = None; // Windows drive letter / UNC prefix
    let mut root: bool = false; // Absolute path has a leading root component

    for component in path.components() {
        match component {
            Component::Prefix(p) => {
                // Windows only: C: or \\?\C: or \\server\share — keep the
                // prefix as-is; it cannot be collapsed.
                prefix = Some(p.as_os_str().to_os_string());
            }
            Component::RootDir => {
                root = true;
                stack.clear();
            }
            Component::CurDir => {
                // `.` is a no-op — skip.
            }
            Component::ParentDir => {
                if let Some(last) = stack.last() {
                    // If the last pushed component is a normal name, pop it
                    // (`a/..` → ``). We don't pop the root or a prior `..`.
                    // `last` came from `Component::Normal` so it's always a
                    // real name, never `..` itself.
                    if last != ".." {
                        stack.pop();
                        continue;
                    }
                }
                // Either stack is empty, or top is already `..`: push `..`.
                // When the path is absolute and the stack is empty, `..`
                // at the root is a no-op (can't go above root).
                if root && stack.is_empty() {
                    // `/.` → `/`; `/..` → `/` (root has no parent).
                } else {
                    stack.push(OsString::from(".."));
                }
            }
            Component::Normal(name) => {
                stack.push(name.to_os_string());
            }
        }
    }

    // Reassemble. On Windows a `Prefix` + `RootDir` pair (e.g. `C:\`) must be
    // reconstructed carefully — `PathBuf::push("")` after a prefix doesn't add
    // the backslash the way it does on Unix.
    let has_prefix = prefix.is_some();
    let mut result = PathBuf::new();
    if let Some(p) = prefix {
        result.push(p);
    }
    if root {
        if has_prefix {
            // Windows: `C:` + `\` = `C:\`. Append the separator to the prefix.
            let sep = if cfg!(windows) { "\\" } else { "/" };
            let mut s = result.into_os_string();
            s.push(std::ffi::OsStr::new(sep));
            result = PathBuf::from(s);
        } else {
            // Unix absolute root: start from "/" so the leading separator is
            // preserved. `PathBuf::push("")` after `PathBuf::new()` yields an
            // empty path (no separator), which drops the root on Linux.
            result = PathBuf::from(if cfg!(windows) { "\\" } else { "/" });
        }
    }
    for part in stack {
        result.push(part);
    }

    // Empty relative path normalizes to "." (matches Go filepath.Clean("") = ".").
    if result.as_os_str().is_empty() {
        return PathBuf::from(".");
    }
    result
}

/// Make a path absolute by joining it with the current working directory.
///
/// Does **not** resolve symlinks or canonicalize separators — this is purely
/// `cwd.join(path)`. Mirrors the unstable `std::path::absolute`. Fails only if
/// the current directory cannot be read.
///
/// For full canonicalization (symlink resolution), use [`canonicalize_existing`].
pub fn to_absolute(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = std::env::current_dir()?;
    Ok(cwd.join(path))
}

// ---------------------------------------------------------------------------
// Layer 2: Filesystem canonicalization
// ---------------------------------------------------------------------------

/// Full `realpath` — resolves all symlinks, collapses `.`/`..`, returns the
/// canonical absolute path. **Requires the path to exist** (returns
/// [`io::ErrorKind::NotFound`] otherwise).
///
/// Uses [`dunce::canonicalize`] internally on Windows to avoid the `\\?\`
/// verbatim-prefix pollution; passthrough on Unix (dunce is a no-op there).
pub fn canonicalize_existing(path: &Path) -> io::Result<PathBuf> {
    dunce::canonicalize(path)
}

/// Hybrid canonicalize: canonicalize the longest existing ancestor, then
/// re-append the missing trailing components lexically.
///
/// This tolerates non-existent paths — essential for the write-before-create
/// case (`write_file("nested/dir/new.txt")` where `nested/dir` exists but
/// `new.txt` doesn't). The canonical step follows any symlinks among the
/// *existing* components, so a symlinked directory pointing outside the root
/// is dereferenced and can be rejected by the caller's containment check.
///
/// # Algorithm
///
/// 1. Fast path: if the whole path canonicalizes, return it.
/// 2. Walk up collecting non-existent trailing components until an ancestor
///    canonicalizes.
/// 3. Re-append the missing components in shallowest-first order.
///
/// # Errors
///
/// Returns [`io::Error`] only if even the filesystem root cannot be
/// canonicalized (in practice: never, for real paths).
pub fn canonicalize_hybrid(path: &Path) -> io::Result<PathBuf> {
    use std::ffi::OsString;

    // Fast path: whole path exists.
    if let Ok(c) = dunce::canonicalize(path) {
        return Ok(c);
    }

    // Walk up collecting non-existent trailing components.
    let mut missing: Vec<OsString> = Vec::new();
    let mut current = PathBuf::from(path);
    loop {
        let Some(parent) = current.parent() else {
            // Reached filesystem root without a canonicalizable ancestor.
            return Ok(path.to_path_buf());
        };
        if parent.as_os_str().is_empty() {
            // Relative root — defensive, callers pass absolute candidates.
            return Ok(path.to_path_buf());
        }
        if let Ok(canonical_parent) = dunce::canonicalize(parent) {
            // Found the longest existing ancestor. Record current's name
            // (it's the first missing component) then re-append the rest
            // in reverse (shallowest-first).
            if let Some(name) = current.file_name() {
                missing.push(name.to_os_string());
            }
            let mut result = canonical_parent;
            for name in missing.into_iter().rev() {
                result.push(name);
            }
            return Ok(result);
        }
        // Parent also doesn't exist — record current's name and climb.
        if let Some(name) = current.file_name() {
            missing.push(name.to_os_string());
        }
        current = PathBuf::from(parent);
    }
}

// ---------------------------------------------------------------------------
// Layer 3: Sandbox containment (security boundary)
// ---------------------------------------------------------------------------

/// Returns `true` if `path` resolves to a location inside `root`.
///
/// **Both `path` and `root` are canonicalized** before the containment check.
/// This is the single rule that makes the check sound against:
///
/// - Symlinks inside `root` pointing outside it.
/// - macOS `/tmp` → `/private/tmp` rewriting (rust-lang/rust#99608).
/// - Windows drive-letter case variations.
///
/// Returns `false` (not an error) if either path cannot be canonicalized
/// (e.g. doesn't exist). For a stricter version that surfaces the error, call
/// [`canonicalize_existing`] on each side manually.
#[must_use]
pub fn is_within_root(path: &Path, root: &Path) -> bool {
    let Ok(canonical_path) = canonicalize_existing(path) else {
        return false;
    };
    let Ok(canonical_root) = canonicalize_existing(root) else {
        return false;
    };
    canonical_path.starts_with(canonical_root)
}

/// Pure lexical depth counter — returns `true` if `relative` would escape
/// the root via `..` components **without touching the filesystem**.
///
/// This is the TOCTOU-safe fast-reject used before the canonicalize-based
/// containment check. It scans [`Path::components()`] left-to-right tracking
/// an `i32` depth: `..` decrements, `Normal` increments, `.` and empty are
/// no-ops. Depth going negative → escapes. Absolute components (`RootDir`,
/// `Prefix`) → escapes.
///
/// # Examples
///
/// ```
/// # use std::path::Path;
/// # use syncode_core::util::path::relative_goes_above_root;
/// assert!(!relative_goes_above_root("a/b/c"));
/// assert!(!relative_goes_above_root("a/../b"));           // balanced
/// assert!(relative_goes_above_root("../b"));              // escapes at start
/// assert!(relative_goes_above_root("a/../../b"));         // escapes mid-path
/// assert!(relative_goes_above_root("/etc/passwd"));       // absolute
/// ```
///
/// # Security note
///
/// This function alone is **not sufficient** for sandbox security — it cannot
/// detect a symlinked *directory* inside the root that points outside (the
/// lexical path stays inside, but the resolved target escapes). Always pair
/// it with [`is_within_root`] or [`canonicalize_hybrid`] + `starts_with`.
#[must_use]
pub fn relative_goes_above_root(relative: &str) -> bool {
    let mut depth: i32 = 0;
    for comp in Path::new(relative).components() {
        match comp {
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return true;
                }
            }
            Component::CurDir | Component::Normal(_) => {
                if matches!(comp, Component::Normal(_)) {
                    depth += 1;
                }
            }
            Component::RootDir | Component::Prefix(_) => return true,
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod lexical_tests {
    use super::*;

    // --- normalize_separators ---

    #[cfg(unix)]
    #[test]
    fn normalize_separators_unix_replaces_backslash() {
        assert_eq!(
            normalize_separators(Path::new("a\\b\\c")),
            PathBuf::from("a/b/c")
        );
    }

    #[cfg(windows)]
    #[test]
    fn normalize_separators_windows_replaces_forward_slash() {
        assert_eq!(
            normalize_separators(Path::new("a/b/c")),
            PathBuf::from("a\\b\\c")
        );
    }

    #[test]
    fn normalize_separators_preserves_already_native() {
        let native = Path::new(if cfg!(windows) { "a\\b" } else { "a/b" });
        assert_eq!(normalize_separators(native), native.to_path_buf());
    }

    // --- canonicalize_lexical ---

    #[test]
    fn canonicalize_lexical_collapses_curdir() {
        assert_eq!(
            canonicalize_lexical(Path::new("a/./b")),
            PathBuf::from("a/b")
        );
        assert_eq!(canonicalize_lexical(Path::new("./a")), PathBuf::from("a"));
        assert_eq!(canonicalize_lexical(Path::new("a/.")), PathBuf::from("a"));
    }

    #[test]
    fn canonicalize_lexical_resolves_parentdir() {
        assert_eq!(
            canonicalize_lexical(Path::new("a/b/../c")),
            PathBuf::from("a/c")
        );
        assert_eq!(
            canonicalize_lexical(Path::new("a/../b")),
            PathBuf::from("b")
        );
    }

    #[test]
    fn canonicalize_lexical_collapses_redundant_separators() {
        assert_eq!(
            canonicalize_lexical(Path::new("a//b")),
            PathBuf::from("a/b")
        );
        assert_eq!(
            canonicalize_lexical(Path::new("a///b")),
            PathBuf::from("a/b")
        );
    }

    #[test]
    fn canonicalize_lexical_preserves_leading_parentdir() {
        // `../x` — can't resolve without root, preserved.
        assert_eq!(
            canonicalize_lexical(Path::new("../x")),
            PathBuf::from("../x")
        );
        assert_eq!(
            canonicalize_lexical(Path::new("../../x")),
            PathBuf::from("../../x")
        );
    }

    #[test]
    fn canonicalize_lexical_parentdir_after_normal_cancels() {
        // `a/../..` → `..` (a is cancelled, then .. escapes)
        assert_eq!(
            canonicalize_lexical(Path::new("a/../..")),
            PathBuf::from("..")
        );
    }

    #[test]
    fn canonicalize_lexical_empty_returns_dot() {
        assert_eq!(canonicalize_lexical(Path::new("")), PathBuf::from("."));
    }

    #[cfg(unix)]
    #[test]
    fn canonicalize_lexical_absolute_unix_preserves_root() {
        assert_eq!(
            canonicalize_lexical(Path::new("/a/b/../c")),
            PathBuf::from("/a/c")
        );
        // `..` at root is a no-op.
        assert_eq!(canonicalize_lexical(Path::new("/..")), PathBuf::from("/"));
        assert_eq!(
            canonicalize_lexical(Path::new("/a/../..")),
            PathBuf::from("/")
        );
    }

    #[cfg(windows)]
    #[test]
    fn canonicalize_lexical_absolute_windows_preserves_prefix() {
        assert_eq!(
            canonicalize_lexical(Path::new("C:\\a\\b\\..\\c")),
            PathBuf::from("C:\\a\\c")
        );
        assert_eq!(
            canonicalize_lexical(Path::new("C:\\")),
            PathBuf::from("C:\\")
        );
    }

    // --- relative_goes_above_root ---

    #[test]
    fn relative_goes_above_root_empty_is_safe() {
        assert!(!relative_goes_above_root(""));
    }

    #[test]
    fn relative_goes_above_root_simple_relative_is_safe() {
        assert!(!relative_goes_above_root("a/b/c"));
        assert!(!relative_goes_above_root("a"));
    }

    #[test]
    fn relative_goes_above_root_dotdot_at_start_escapes() {
        assert!(relative_goes_above_root("../b"));
        assert!(relative_goes_above_root("../"));
    }

    #[test]
    fn relative_goes_above_root_chained_dotdot_escapes() {
        assert!(relative_goes_above_root("../../etc/passwd"));
        assert!(relative_goes_above_root("a/../../../b"));
    }

    #[test]
    fn relative_goes_above_root_balanced_dotdot_is_safe() {
        // `a/..` → depth 0; ok.
        assert!(!relative_goes_above_root("a/../b"));
        assert!(!relative_goes_above_root("a/b/../../c"));
    }

    #[test]
    fn relative_goes_above_root_absolute_escapes() {
        assert!(relative_goes_above_root("/etc/passwd"));
        assert!(relative_goes_above_root("/"));
        // Windows-style absolute paths are only recognized as absolute on
        // Windows (the component parser produces Component::Prefix). On Unix,
        // `C:/Windows` parses as Normal("C:") + Normal("Windows") — a relative
        // path — so we only assert this where the parser agrees with us.
        #[cfg(windows)]
        {
            assert!(relative_goes_above_root("C:/Windows"));
            assert!(relative_goes_above_root("C:\\Windows"));
        }
    }

    #[test]
    fn relative_goes_above_root_curdir_is_noop() {
        assert!(!relative_goes_above_root("./a"));
        assert!(!relative_goes_above_root("a/./b"));
    }

    // --- PathStyle + current_path_style ---

    #[test]
    fn current_path_style_matches_cfg() {
        let style = current_path_style();
        assert_eq!(style.is_windows(), cfg!(windows));
    }

    #[test]
    fn path_style_separator_consistency() {
        let s = current_path_style();
        assert_ne!(s.separator(), s.other_separator());
    }

    // --- to_absolute ---

    #[test]
    fn to_absolute_already_absolute_is_idempotent() {
        let abs = if cfg!(windows) {
            Path::new("C:\\Users\\test")
        } else {
            Path::new("/usr/local")
        };
        assert_eq!(to_absolute(abs).unwrap(), abs.to_path_buf());
    }

    #[test]
    fn to_absolute_relative_joins_cwd() {
        let cwd = std::env::current_dir().unwrap();
        let result = to_absolute(Path::new("foo/bar")).unwrap();
        assert!(result.is_absolute());
        assert!(result.starts_with(&cwd));
        assert!(result.ends_with("foo/bar"));
    }
}

#[cfg(test)]
mod fs_tests {
    use super::*;

    fn temp_root() -> tempfile::TempDir {
        tempfile::tempdir().expect("create tempdir")
    }

    // --- canonicalize_existing ---

    #[test]
    fn canonicalize_existing_resolves_real_file() {
        let dir = temp_root();
        let file = dir.path().join("real.txt");
        std::fs::write(&file, b"x").unwrap();
        let c = canonicalize_existing(&file).unwrap();
        assert_eq!(c.file_name().unwrap(), "real.txt");
        assert!(c.is_absolute());
    }

    #[test]
    fn canonicalize_existing_missing_returns_not_found() {
        let dir = temp_root();
        let missing = dir.path().join("nope.txt");
        let err = canonicalize_existing(&missing).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[cfg(unix)]
    #[test]
    fn canonicalize_existing_resolves_symlink() {
        let dir = temp_root();
        let target = dir.path().join("target.txt");
        let link = dir.path().join("link.txt");
        std::fs::write(&target, b"x").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let c = canonicalize_existing(&link).unwrap();
        assert_eq!(c, target.canonicalize().unwrap());
    }

    // --- canonicalize_hybrid ---

    #[test]
    fn canonicalize_hybrid_existing_path_matches_canonicalize() {
        let dir = temp_root();
        let file = dir.path().join("exists.txt");
        std::fs::write(&file, b"x").unwrap();
        let hybrid = canonicalize_hybrid(&file).unwrap();
        let existing = canonicalize_existing(&file).unwrap();
        assert_eq!(hybrid, existing);
    }

    #[test]
    fn canonicalize_hybrid_handles_nonexistent_leaf() {
        let dir = temp_root();
        // Canonicalize the tempdir for a stable comparison base.
        let canonical_dir = canonicalize_existing(dir.path()).unwrap();
        let path = dir.path().join("new.txt"); // doesn't exist
        let hybrid = canonicalize_hybrid(&path).unwrap();
        assert!(hybrid.starts_with(&canonical_dir));
        assert_eq!(hybrid.file_name().unwrap(), "new.txt");
    }

    #[test]
    fn canonicalize_hybrid_handles_nested_nonexistent() {
        let dir = temp_root();
        let canonical_dir = canonicalize_existing(dir.path()).unwrap();
        // Only `dir` exists; `a/b/c.txt` are all missing.
        let path = dir.path().join("a/b/c.txt");
        let hybrid = canonicalize_hybrid(&path).unwrap();
        assert!(hybrid.starts_with(&canonical_dir));
        assert!(hybrid.ends_with("a/b/c.txt"));
    }

    #[cfg(unix)]
    #[test]
    fn canonicalize_hybrid_resolves_symlinked_ancestor() {
        // The critical security test: a symlinked directory inside the root
        // pointing OUTSIDE. The hybrid canonicalize must follow the symlink
        // so the caller's `starts_with(root)` check fails.
        let inside = temp_root();
        let outside = temp_root();
        // Create a symlink `inside/link` → `outside`
        let link = inside.path().join("link");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();
        // `inside/link/x.txt` — link exists (points outside), x.txt doesn't.
        let candidate = link.join("x.txt");
        let hybrid = canonicalize_hybrid(&candidate).unwrap();
        // The resolved path must be under `outside`, NOT under `inside`.
        let canonical_inside = canonicalize_existing(inside.path()).unwrap();
        assert!(
            !hybrid.starts_with(&canonical_inside),
            "hybrid {:?} should NOT be within inside {:?} (symlink escape detected)",
            hybrid,
            canonical_inside
        );
    }

    // --- is_within_root ---

    #[test]
    fn is_within_root_true_for_nested_path() {
        let dir = temp_root();
        let nested = dir.path().join("sub/deep/file.txt");
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
        std::fs::write(&nested, b"x").unwrap();
        assert!(is_within_root(&nested, dir.path()));
    }

    #[test]
    fn is_within_root_true_for_root_itself() {
        let dir = temp_root();
        assert!(is_within_root(dir.path(), dir.path()));
    }

    #[test]
    fn is_within_root_false_for_outside_path() {
        let dir_a = temp_root();
        let dir_b = temp_root();
        let file_b = dir_b.path().join("file.txt");
        std::fs::write(&file_b, b"x").unwrap();
        assert!(!is_within_root(&file_b, dir_a.path()));
    }

    #[test]
    fn is_within_root_false_for_missing_path() {
        let dir = temp_root();
        let missing = dir.path().join("nope.txt");
        assert!(!is_within_root(&missing, dir.path()));
    }

    #[cfg(unix)]
    #[test]
    fn is_within_root_false_for_symlink_escape() {
        let inside = temp_root();
        let outside = temp_root();
        let outside_file = outside.path().join("secret.txt");
        std::fs::write(&outside_file, b"x").unwrap();
        // Symlink inside pointing outside.
        let link = inside.path().join("escape");
        std::os::unix::fs::symlink(&outside_file, &link).unwrap();
        // Naive lexical check would say inside (path stays under `inside`),
        // but canonicalize-based check must catch the escape.
        assert!(!is_within_root(&link, inside.path()));
    }
}

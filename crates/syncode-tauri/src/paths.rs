//! Filesystem locations the desktop shell writes to (logs, crash dumps, etc.).
//!
//! Currently only exposes [`log_dir`]: the directory under the per-OS app-data
//! root where the panic hook in `main.rs` writes `panic.log`. Kept tiny and
//! dependency-free so the panic hook itself can't fail because of a missing
//! directory-helper crate.

use std::path::PathBuf;

/// App subdirectory used for logs / crash dumps, relative to the per-OS
/// app-data root. Constant so log files land in a stable location across
/// versions.
const LOG_SUBDIR: &str = "syncode";

/// Resolve the log directory under the platform-appropriate app-data root.
///
/// Returns `None` only when the OS doesn't expose an app-data directory (very
/// rare — typically only broken CI sandboxes). Callers should treat `None` as
/// "skip file logging" rather than fatal.
///
/// # Platform behavior
/// - **Windows**: `%APPDATA%\syncode` (e.g. `C:\Users\Alice\AppData\Roaming\syncode`)
/// - **macOS**: `~/Library/Application Support/syncode`
/// - **Linux**: `$XDG_DATA_HOME/syncode` or `~/.local/share/syncode`
pub fn log_dir() -> Option<PathBuf> {
    data_dir_root().map(|d| d.join(LOG_SUBDIR))
}

/// Per-OS app-data root (the part above the app-specific subdir). Kept private
/// — callers should use [`log_dir`] which appends the syncode-specific name.
fn data_dir_root() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        // %APPDATA% is always set on modern Windows; fall back to LOCALAPPDATA
        // then to %USERPROFILE%\.syncode for degraded environments.
        std::env::var_os("APPDATA")
            .or_else(|| std::env::var_os("LOCALAPPDATA"))
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("USERPROFILE").map(|p| PathBuf::from(p).join(".syncode-data"))
            })
    }
    #[cfg(target_os = "macos")]
    {
        home_dir().map(|h| h.join("Library").join("Application Support"))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
            Some(PathBuf::from(xdg))
        } else {
            home_dir().map(|h| h.join(".local").join("share"))
        }
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
    {
        None
    }
}

/// `$HOME` / `%USERPROFILE%` lookup shared by the unix + macOS branches above.
#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("USERPROFILE").map(PathBuf::from))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_dir_ends_with_syncode_subdir() {
        let dir = log_dir();
        // Most CI runners set HOME / APPDATA; some bare sandboxes don't —
        // accept either outcome rather than skipping the test entirely.
        if let Some(d) = dir {
            assert!(
                d.ends_with(LOG_SUBDIR),
                "log_dir() should end with `{LOG_SUBDIR}`, got {d:?}"
            );
        }
    }

    #[test]
    fn log_subdir_is_stable_string() {
        // Catch accidental rename — panic.log consumers hardcode `syncode/panic.log`.
        assert_eq!(LOG_SUBDIR, "syncode");
    }
}

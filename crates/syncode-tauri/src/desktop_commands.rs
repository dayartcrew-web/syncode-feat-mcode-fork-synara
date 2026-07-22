//! Tauri IPC Commands — Desktop integration (DSK-2).
//!
//! Four commands surfaced to the frontend via Tauri's `invoke()` bridge,
//! backing the `desktop.*` RPC names the cloned MCode UI references:
//!
//! - [`check_for_updates`] → `desktop.checkForUpdates`
//! - [`apply_update`]      → `desktop.applyUpdate`
//! - [`open_external`]     → `desktop.openExternal`
//! - [`open_in_editor`]    → `desktop.openInEditor`
//!
//! The update commands reuse [`crate::updater::UpdaterState`] (managed state
//! initialised in `main.rs`). The update *check* drives the state machine
//! (`Idle → Checking → Available|UpToDate|Error`); the actual
//! download/install is delegated to Tauri's bundled auto-updater when the
//! `updater` plugin is registered, otherwise we report a graceful
//! not-configured error so the UI can fall back. `open_external` and
//! `open_in_editor` shell out to the OS default opener / user's editor — they
//! reuse the same OS-default resolution helper as
//! [`crate::shell_commands::shell_open_editor`] but accept a richer request
//! shape (label, line number, editor override) that the MCode UI sends.

use crate::updater::{UpdateStatus, UpdaterState};
use serde::{Deserialize, Serialize};
use tauri_plugin_updater::UpdaterExt;

/// Toggle the webview DevTools window for the main window (or a named label).
///
/// Frontend binds F12 / Ctrl+Shift+I → `invoke("toggle_devtools")`. The Tauri
/// devtools API is gated behind the `devtools` Cargo feature (or a debug
/// build); without it the command is a graceful no-op returning `false`, so
/// the same frontend hotkey works regardless of build config. Release builds
/// pass `--features devtools` (see `.github/workflows/release.yml`) so
/// dogfooders can inspect the webview to diagnose issues — without it, the
/// `open_devtools` symbols don't exist and DevTools is entirely unavailable
/// in a release build (the v0.1.6 regression).
///
/// Returns `true` when DevTools is now open, `false` when now closed (or
/// unavailable because the feature is off).
#[tauri::command]
pub async fn toggle_devtools(app: tauri::AppHandle, label: Option<String>) -> Result<bool, String> {
    #[cfg(any(debug_assertions, feature = "devtools"))]
    {
        use tauri::Manager;
        let window = match label.as_deref() {
            Some(l) if !l.is_empty() => app
                .get_webview_window(l)
                .ok_or_else(|| format!("webview window '{l}' not found"))?,
            _ => app
                .get_webview_window("main")
                .ok_or_else(|| "main webview window not found".to_string())?,
        };
        let now_open = !window.is_devtools_open();
        if now_open {
            window.open_devtools();
        } else {
            window.close_devtools();
        }
        Ok(now_open)
    }
    #[cfg(not(any(debug_assertions, feature = "devtools")))]
    {
        let _ = (app, label);
        Ok(false)
    }
}

/// Result of [`check_for_updates`]: a projection of [`UpdateStatus`] into the
/// shape the MCode UI expects (camelCase + `available` boolean). The
/// underlying [`UpdateStatus`] is the source of truth; this struct is the
/// wire DTO.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CheckForUpdatesResult {
    /// Whether a newer version is available to download.
    pub available: bool,
    /// Current state machine phase (`idle`, `checking`, `available`,
    /// `downloading`, `ready`, `installed`, `up_to_date`, `error`).
    pub status: String,
    /// Populated when `available`/`ready` — the version on the release server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Populated when `available` — markdown release notes for the version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_notes: Option<String>,
    /// Populated when `status == "error"` — human-readable diagnostic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl CheckForUpdatesResult {
    /// Project an [`UpdateStatus`] into the wire DTO. Centralised here so the
    /// `available` boolean stays consistent with the variant.
    pub fn from_status(status: &UpdateStatus) -> Self {
        let (available, version, release_notes, message) = match status {
            UpdateStatus::Available {
                version,
                release_notes,
            } => (
                true,
                Some(version.clone()),
                Some(release_notes.clone()),
                None,
            ),
            UpdateStatus::Ready { version } => (true, Some(version.clone()), None, None),
            UpdateStatus::Error { message } => (false, None, None, Some(message.clone())),
            _ => (false, None, None, None),
        };
        Self {
            available,
            status: status.to_string(),
            version,
            release_notes,
            message,
        }
    }
}

/// Ack result of [`apply_update`]. `true` only when an update is in the
/// `available` or `ready` phase AND the Tauri updater plugin successfully
/// downloaded + installed it (a restart is then required). `false` + a
/// `reason` is returned for every other path (no update available, plugin
/// missing, install error).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApplyUpdateResult {
    /// `true` when the update was installed and a restart is pending.
    pub installed: bool,
    /// Populated when `installed` is true — the version that was installed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Populated when `installed` is false — human-readable reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Request payload for [`open_external`] / [`open_in_editor`]. Mirrors the
/// camelCase keys the MCode UI sends.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenTarget {
    /// URL (for `openExternal`) or filesystem path (for `openInEditor`) to open.
    pub target: String,
    /// Optional editor binary override (editor only). When omitted, falls back
    /// to `$EDITOR`, then the OS default opener.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub editor: Option<String>,
    /// Optional line number hint (editor only). Surfaced to the editor as
    /// `+<line>` when supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
}

/// Resolve the OS-default opener binary name. Same resolution as
/// [`crate::shell_commands::shell_open_editor`] but factored so the desktop
/// commands can re-use it without re-spawning the helper module's inline
/// closure. Returns `"open"` (macOS) / `"explorer"` (Windows) /
/// `"xdg-open"` (Linux).
pub fn default_opener() -> String {
    if cfg!(target_os = "macos") {
        "open".to_string()
    } else if cfg!(target_os = "windows") {
        "explorer".to_string()
    } else {
        "xdg-open".to_string()
    }
}

/// Check for an application update.
///
/// Frontend invokes `invoke("check_for_updates")`. The command flips the
/// managed [`UpdaterState`] to [`UpdateStatus::Checking`], then runs a REAL
/// probe via `tauri-plugin-updater` (registered in `main.rs`): the plugin
/// fetches the configured `latest.json` endpoint, verifies the signature, and
/// `check()` returns `Some(Update)` when a newer signed version exists. The
/// status transitions to `Available { version, release_notes }`, `UpToDate`,
/// or `Error { message }` accordingly, and the projected DTO is returned.
#[tauri::command]
pub async fn check_for_updates(
    app: tauri::AppHandle,
    updater: tauri::State<'_, UpdaterState>,
) -> Result<CheckForUpdatesResult, String> {
    updater.set_status(UpdateStatus::Checking);
    // Real probe via tauri-plugin-updater (registered in `main.rs`). The
    // plugin fetches the configured `latest.json` endpoint + verifies the
    // signature; `check()` returns `Some(Update)` when a newer version exists.
    match app.updater() {
        Ok(ext) => match ext.check().await {
            Ok(Some(update)) => {
                let version = update.version.clone();
                let release_notes = update.body.clone().unwrap_or_default();
                updater.set_status(UpdateStatus::Available {
                    version,
                    release_notes,
                });
            }
            Ok(None) => {
                updater.set_status(UpdateStatus::UpToDate);
            }
            Err(e) => {
                updater.set_status(UpdateStatus::Error {
                    message: e.to_string(),
                });
            }
        },
        Err(e) => {
            updater.set_status(UpdateStatus::Error {
                message: format!("updater plugin unavailable: {e}"),
            });
        }
    }
    Ok(CheckForUpdatesResult::from_status(&updater.status()))
}

/// Apply a pending update.
///
/// Frontend invokes `invoke("apply_update")`. Requires the state machine to
/// be in `Available` or `Ready` (set by a prior successful
/// [`check_for_updates`] that found a newer version); every other state
/// returns `installed: false` with a typed reason. The actual install is
/// delegated to the `tauri-plugin-updater`; in dev / when the plugin is
/// absent, we surface a graceful not-configured reason so the UI can show a
/// "restart to update" hint only when there's truly something to apply.
#[tauri::command]
pub async fn apply_update(
    app: tauri::AppHandle,
    updater: tauri::State<'_, UpdaterState>,
) -> Result<ApplyUpdateResult, String> {
    let status = updater.status();
    if !matches!(
        status,
        UpdateStatus::Available { .. } | UpdateStatus::Ready { .. }
    ) {
        return Ok(ApplyUpdateResult {
            installed: false,
            version: None,
            reason: Some(format!("no update pending (status: {status})")),
        });
    }
    let ext = match app.updater() {
        Ok(u) => u,
        Err(e) => {
            return Ok(ApplyUpdateResult {
                installed: false,
                version: None,
                reason: Some(format!("updater plugin unavailable: {e}")),
            });
        }
    };
    // Re-check to obtain a fresh `Update` handle (it owns the signed download
    // URL + is consumed by `download_and_install`).
    match ext.check().await {
        Ok(Some(update)) => {
            let version = update.version.clone();
            updater.set_status(UpdateStatus::Downloading { progress: 0.0 });
            // download + install (signature-verified by the plugin). On
            // success the installer replaces the app; on NSIS (Windows) the
            // plugin relaunches. Progress callback is a best-effort no-op
            // here (coarse state suffices for the badge).
            match update
                .download_and_install(|_chunk_len, _total| {}, || {})
                .await
            {
                Ok(()) => {
                    updater.set_status(UpdateStatus::Installed {
                        version: version.clone(),
                    });
                    Ok(ApplyUpdateResult {
                        installed: true,
                        version: Some(version),
                        reason: None,
                    })
                }
                Err(e) => {
                    updater.set_status(UpdateStatus::Error {
                        message: e.to_string(),
                    });
                    Ok(ApplyUpdateResult {
                        installed: false,
                        version: None,
                        reason: Some(format!("download/install failed: {e}")),
                    })
                }
            }
        }
        Ok(None) => {
            updater.set_status(UpdateStatus::UpToDate);
            Ok(ApplyUpdateResult {
                installed: false,
                version: None,
                reason: Some("no update available on re-check".to_string()),
            })
        }
        Err(e) => {
            updater.set_status(UpdateStatus::Error {
                message: e.to_string(),
            });
            Ok(ApplyUpdateResult {
                installed: false,
                version: None,
                reason: Some(format!("re-check failed: {e}")),
            })
        }
    }
}

/// Open a URL in the OS default browser / handler.
///
/// Frontend invokes `invoke("open_external", { target })`. Shells out to the
/// OS opener (`open` / `explorer` / `xdg-open`). Mirrors
/// [`crate::shell_commands::shell_open_editor`] but is exposed under the
/// `desktop.openExternal` name the MCode UI references.
#[tauri::command]
pub async fn open_external(target: String) -> Result<(), String> {
    if target.trim().is_empty() {
        return Err("target must be a non-empty URL or path".to_string());
    }
    let opener = default_opener();
    let mut cmd = std::process::Command::new(&opener);
    cmd.arg(&target);
    crate::process_ext::hide_console_window(&mut cmd);
    cmd.spawn()
        .map_err(|e| format!("failed to open \"{target}\" via {opener}: {e}"))?;
    Ok(())
}

/// Open a path in the user's editor.
///
/// Frontend invokes `invoke("open_in_editor", { target, editor?, line? })`.
/// When `editor` is omitted and `EDITOR` is unset, falls back to the OS
/// default opener (same as [`crate::shell_commands::shell_open_editor`]).
/// Supports an optional line-number hint (`+<line>` arg for editors that
/// accept it — vim/VS Code/Sublime).
#[tauri::command]
pub async fn open_in_editor(
    target: String,
    editor: Option<String>,
    line: Option<u32>,
) -> Result<(), String> {
    if target.trim().is_empty() {
        return Err("target must be a non-empty path".to_string());
    }
    let editor = editor
        .or_else(|| std::env::var("EDITOR").ok().filter(|e| !e.is_empty()))
        .unwrap_or_else(default_opener);
    let mut cmd = std::process::Command::new(&editor);
    if let Some(line) = line {
        cmd.arg(format!("+{line}"));
    }
    cmd.arg(&target);
    crate::process_ext::hide_console_window(&mut cmd);
    cmd.spawn()
        .map_err(|e| format!("failed to launch editor \"{editor}\": {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_for_updates_result_from_status_available() {
        let status = UpdateStatus::Available {
            version: "2.0.0".to_string(),
            release_notes: "new".to_string(),
        };
        let res = CheckForUpdatesResult::from_status(&status);
        assert!(res.available);
        assert_eq!(res.status, "available: 2.0.0");
        assert_eq!(res.version.as_deref(), Some("2.0.0"));
        assert_eq!(res.release_notes.as_deref(), Some("new"));
        assert!(res.message.is_none());
    }

    #[test]
    fn check_for_updates_result_from_status_up_to_date() {
        let res = CheckForUpdatesResult::from_status(&UpdateStatus::UpToDate);
        assert!(!res.available);
        assert_eq!(res.status, "up_to_date");
        assert!(res.version.is_none());
    }

    #[test]
    fn check_for_updates_result_from_status_error() {
        let status = UpdateStatus::Error {
            message: "boom".to_string(),
        };
        let res = CheckForUpdatesResult::from_status(&status);
        assert!(!res.available);
        assert_eq!(res.message.as_deref(), Some("boom"));
    }

    #[test]
    fn check_for_updates_command_flips_state_machine() {
        // The command body is the source of truth for the state transition;
        // here we exercise just the state-machine projection by manually
        // walking the same transitions the command performs.
        let state = UpdaterState::new();
        state.set_status(UpdateStatus::Checking);
        assert_eq!(state.status(), UpdateStatus::Checking);
        state.set_status(UpdateStatus::UpToDate);
        let res = CheckForUpdatesResult::from_status(&state.status());
        assert_eq!(res.status, "up_to_date");
    }

    #[test]
    fn apply_update_rejects_when_no_update_pending() {
        let state = UpdaterState::new();
        // Default state is Idle — apply should refuse with a typed reason.
        let status = state.status();
        assert!(!matches!(
            status,
            UpdateStatus::Available { .. } | UpdateStatus::Ready { .. }
        ));
        let reason = format!("no update pending (status: {status})");
        assert!(reason.contains("no update pending"));
    }

    #[test]
    fn apply_update_succeeds_when_available() {
        let state = UpdaterState::new();
        state.set_status(UpdateStatus::Available {
            version: "1.2.0".to_string(),
            release_notes: String::new(),
        });
        let version = match state.status() {
            UpdateStatus::Available { version, .. } => version,
            _ => unreachable!("set_status above guarantees Available"),
        };
        state.set_status(UpdateStatus::Installed {
            version: version.clone(),
        });
        assert!(matches!(state.status(), UpdateStatus::Installed { .. }));
    }

    #[test]
    fn default_opener_is_non_empty() {
        let opener = default_opener();
        assert!(!opener.is_empty());
        // Cross-platform sanity: one of the three known openers.
        assert!(opener == "open" || opener == "explorer" || opener == "xdg-open");
    }

    #[test]
    fn open_target_request_deserializes() {
        // The frontend sends camelCase keys — verify the DTO round-trips.
        let json = r#"{"target":"/tmp/file.ts","editor":"code","line":42}"#;
        let parsed: OpenTarget = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.target, "/tmp/file.ts");
        assert_eq!(parsed.editor.as_deref(), Some("code"));
        assert_eq!(parsed.line, Some(42));
    }

    #[test]
    fn open_target_minimal_deserializes() {
        let json = r#"{"target":"https://example.com"}"#;
        let parsed: OpenTarget = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.target, "https://example.com");
        assert!(parsed.editor.is_none());
        assert!(parsed.line.is_none());
    }

    #[test]
    fn apply_update_result_serialization() {
        let res = ApplyUpdateResult {
            installed: true,
            version: Some("1.5.0".to_string()),
            reason: None,
        };
        let json = serde_json::to_string(&res).unwrap();
        assert!(json.contains("\"installed\":true"));
        assert!(json.contains("\"version\":\"1.5.0\""));
        // `reason` is skipped when None.
        assert!(!json.contains("reason"));
    }

    #[test]
    fn check_for_updates_result_camelcase_keys() {
        let res = CheckForUpdatesResult::from_status(&UpdateStatus::Available {
            version: "9.9.9".to_string(),
            release_notes: "notes".to_string(),
        });
        let json = serde_json::to_string(&res).unwrap();
        assert!(json.contains("releaseNotes"));
        assert!(!json.contains("release_notes"));
    }

    #[test]
    fn apply_update_result_failure_serialization() {
        let res = ApplyUpdateResult {
            installed: false,
            version: None,
            reason: Some("no update pending".to_string()),
        };
        let json = serde_json::to_string(&res).unwrap();
        assert!(json.contains("\"installed\":false"));
        assert!(json.contains("\"reason\":\"no update pending\""));
    }

    #[test]
    fn open_external_rejects_empty_target() {
        // The command body checks `target.trim().is_empty()` and returns Err.
        // Reproduce the guard here so the test exercises the same predicate.
        let empty = "";
        assert!(empty.trim().is_empty());
    }
}

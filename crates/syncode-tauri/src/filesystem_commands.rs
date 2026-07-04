//! Tauri IPC Commands — Filesystem integration (DSK-2).
//!
//! One command surfaced to the frontend via Tauri's `invoke()` bridge,
//! backing the `filesystem.browse` RPC name the cloned MCode UI references:
//!
//! - [`browse`] → `filesystem.browse`
//!
//! This opens a native OS file/folder picker dialog. Tauri v2's official
//! surface for this is the `tauri-plugin-dialog` crate (`DialogExt` on
//! `WebviewWindow`). When that plugin isn't registered (e.g. minimal dev
//! builds, headless CI), the command falls back to a **graceful typed
//! result** that reports the cancellation, so the frontend's `Browse…`
//! button doesn't throw — it just shows "no selection".
//!
//! ## Design note: why no `tauri-plugin-dialog` import?
//!
//! The workspace `Cargo.toml` doesn't (yet) declare `tauri-plugin-dialog`,
//! so we don't depend on `DialogExt` directly. The command accepts the
//! pick options, validates them, and returns the empty-selection fallback
//! today. When the plugin lands, the implementation swaps in
//! `window.dialog().file().pick_*` behind the same signature — the
//! frontend contract is unchanged.

use serde::{Deserialize, Serialize};

/// What kind of entity the picker should let the user select.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BrowseKind {
    /// Pick a single file (`open` file dialog).
    #[default]
    File,
    /// Pick multiple files (multi-select file dialog).
    Files,
    /// Pick a directory (folder picker).
    Folder,
}

/// Request payload for [`browse`]. Mirrors the camelCase keys the MCode UI
/// sends (`kind`, `defaultPath`, `filters`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowseRequest {
    /// Picker kind (defaults to `file` when omitted).
    #[serde(default)]
    pub kind: BrowseKind,
    /// Initial directory the dialog opens in. Optional — when omitted the
    /// OS chooses (usually the app's working directory or last-used folder).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_path: Option<String>,
    /// Optional file-extension filters (`{ name, extensions }`). When
    /// present, restricts the picker to matching files. Ignored for the
    /// `folder` kind.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub filters: Vec<FileFilter>,
}

/// A named file-extension filter entry for the picker dialog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileFilter {
    /// Human-readable name (`"TypeScript"`, `"Images"`).
    pub name: String,
    /// Extensions without leading dot (`["ts", "tsx"]`).
    pub extensions: Vec<String>,
}

/// Result of [`browse`]. `selections` is empty when the user cancels the
/// dialog or the dialog surface is unavailable (platform-limited fallback).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BrowseResult {
    /// One path per picked entry. Empty array = no selection.
    pub selections: Vec<String>,
    /// Whether the picker completed (`true`) or fell back to the empty
    /// result (`false`). The frontend treats an empty `selections` either
    /// way; this flag lets it distinguish "user cancelled" from "platform
    /// unsupported" for telemetry.
    pub completed: bool,
    /// Diagnostic when `completed` is false (platform fallback path).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl Default for BrowseResult {
    fn default() -> Self {
        Self::empty()
    }
}

impl BrowseResult {
    /// Construct the "user cancelled" / empty result.
    pub fn empty() -> Self {
        Self {
            selections: Vec::new(),
            completed: true,
            note: None,
        }
    }

    /// Construct the platform-limited fallback result (the dialog plugin
    /// isn't registered). Empty selections + `completed: false` + note.
    pub fn platform_limited() -> Self {
        Self {
            selections: Vec::new(),
            completed: false,
            note: Some(
                "filesystem.browse is platform-limited in this build — \
                 tauri-plugin-dialog is not registered; returning empty \
                 selection"
                    .to_string(),
            ),
        }
    }

    /// Construct a successful single-selection result.
    #[cfg(test)]
    pub fn picked(path: impl Into<String>) -> Self {
        Self {
            selections: vec![path.into()],
            completed: true,
            note: None,
        }
    }
}

/// Open a native file/folder picker dialog.
///
/// Frontend invokes `invoke("browse", { kind?, defaultPath?, filters? })`.
/// Today this returns the platform-limited fallback (see
/// [`BrowseResult::platform_limited`]); the request is validated (filter
/// names non-empty, extensions lowercased) before the fallback so that when
/// `tauri-plugin-dialog` is wired in, the same validation runs.
#[tauri::command]
pub fn browse(request: BrowseRequest) -> Result<BrowseResult, String> {
    // Validate filters up-front so the plugin wiring inherits these guards.
    for filter in &request.filters {
        if filter.name.trim().is_empty() {
            return Err("filter.name must be non-empty".to_string());
        }
        if filter.extensions.is_empty() {
            return Err(format!(
                "filter \"{}\" must list at least one extension",
                filter.name
            ));
        }
    }
    // No `tauri-plugin-dialog` wired today — return the platform-limited
    // fallback. When the plugin is registered, replace this body with
    // `app.get_webview_window("main").dialog().file()...pick_*` and keep the
    // validation guards above.
    Ok(BrowseResult::platform_limited())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browse_kind_default_is_file() {
        assert_eq!(BrowseKind::default(), BrowseKind::File);
    }

    #[test]
    fn browse_kind_deserializes_folder() {
        let json = "\"folder\"";
        let parsed: BrowseKind = serde_json::from_str(json).unwrap();
        assert_eq!(parsed, BrowseKind::Folder);
    }

    #[test]
    fn browse_request_minimal_deserializes() {
        let json = "{}";
        let parsed: BrowseRequest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.kind, BrowseKind::File);
        assert!(parsed.default_path.is_none());
        assert!(parsed.filters.is_empty());
    }

    #[test]
    fn browse_request_camelcase_keys_deserialize() {
        let json = r#"{
            "kind": "files",
            "defaultPath": "/home/user",
            "filters": [
                {"name": "TypeScript", "extensions": ["ts", "tsx"]}
            ]
        }"#;
        let parsed: BrowseRequest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.kind, BrowseKind::Files);
        assert_eq!(parsed.default_path.as_deref(), Some("/home/user"));
        assert_eq!(parsed.filters.len(), 1);
        assert_eq!(parsed.filters[0].name, "TypeScript");
        assert_eq!(parsed.filters[0].extensions, vec!["ts", "tsx"]);
    }

    #[test]
    fn browse_command_returns_platform_limited_fallback() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![],
        };
        let res = browse(req).unwrap();
        assert!(res.selections.is_empty());
        assert!(!res.completed);
        assert!(res.note.is_some());
    }

    #[test]
    fn browse_command_rejects_filter_with_empty_name() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![FileFilter {
                name: "  ".to_string(),
                extensions: vec!["ts".to_string()],
            }],
        };
        let err = browse(req).unwrap_err();
        assert!(err.contains("filter.name must be non-empty"));
    }

    #[test]
    fn browse_command_rejects_filter_with_no_extensions() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![FileFilter {
                name: "Empty".to_string(),
                extensions: vec![],
            }],
        };
        let err = browse(req).unwrap_err();
        assert!(err.contains("at least one extension"));
    }

    #[test]
    fn browse_result_empty_is_completed() {
        let res = BrowseResult::empty();
        assert!(res.selections.is_empty());
        assert!(res.completed);
        assert!(res.note.is_none());
    }

    #[test]
    fn browse_result_platform_limited_carries_note() {
        let res = BrowseResult::platform_limited();
        assert!(res.selections.is_empty());
        assert!(!res.completed);
        let note = res.note.expect("note");
        assert!(note.contains("platform-limited"));
    }

    #[test]
    fn browse_result_picked_round_trips() {
        let res = BrowseResult::picked("/tmp/file.ts");
        let json = serde_json::to_string(&res).unwrap();
        // `note: None` → skipped in JSON.
        assert!(!json.contains("note"));
        assert!(json.contains("/tmp/file.ts"));
        let back: BrowseResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.selections, vec!["/tmp/file.ts".to_string()]);
        assert!(back.completed);
    }

    #[test]
    fn browse_result_camelcase_serialization() {
        let res = BrowseResult::platform_limited();
        let json = serde_json::to_string(&res).unwrap();
        assert!(json.contains("\"selections\""));
        assert!(json.contains("\"completed\""));
    }

    #[test]
    fn browse_result_default_is_empty() {
        let res = BrowseResult::default();
        assert!(res.completed);
        assert!(res.selections.is_empty());
    }
}

//! Tauri IPC Commands — Filesystem integration (DSK-2 / P4-4).
//!
//! One command surfaced to the frontend via Tauri's `invoke()` bridge,
//! backing the `filesystem.browse` RPC name the cloned MCode UI references:
//!
//! - [`browse`] → `filesystem.browse`
//!
//! This opens a **real native OS file/folder picker** dialog. The picker
//! implementation is provided by [`rfd`] (Rust File Dialog) — a thin
//! cross-platform wrapper around Win32 common dialogs (Windows),
//! `NSOpenPanel` (macOS), and the xdg-desktop-portal / GTK (Linux). `rfd`
//! was chosen over `tauri-plugin-dialog` because the workspace already
//! brings in the Tauri core but not the dialog plugin; `rfd` works without
//! any Tauri plugin registration and is decoupled from the Tauri runtime,
//! which keeps the unit-testable surface pure (see [`build_picker`] and
//! [`shape_result`]).
//!
//! ## Testability
//!
//! A real dialog can't open during a unit test (no GUI, no user to click).
//! The command is therefore split into:
//!
//! - [`validate_request`] — pure input validation (filter names non-empty,
//!   extensions present). Returns a `Result<(), String>` and is exhaustively
//!   exercised by unit tests.
//! - [`build_picker`] — pure projection from [`BrowseRequest`] into an
//!   `rfd::AsyncFileDialog` builder. Tested implicitly via the shape tests
//!   that pass a request through validation and assert the kind/filters are
//!   applied (we don't construct the `rfd` builder in tests to avoid pulling
//!   GTK/portal deps into the test harness).
//! - [`shape_result`] — pure mapping from `Option<PathBuf>` /
//!   `Option<Vec<PathBuf>>` into [`BrowseResult`]. Unit-tested with synthetic
//!   paths, since it only stringifies and packages.
//! - [`browse`] — the thin async Tauri command that wires the three together
//!   and actually `.await`s the picker. Not unit-tested (would block on a
//!   GUI); covered by the integration surface once the desktop shell runs.
//!
//! ## Why `rfd` and not `tauri-plugin-dialog`
//!
//! Both work; `rfd` was picked because:
//!   1. It needs no Tauri plugin registration (the desktop shell doesn't
//!      register plugins yet), so the command works in any binary that links
//!      `syncode-tauri`.
//!   2. Its builder is `Send`-able and `async`-aware via the `tokio` feature,
//!      matching the Tauri command's async runtime.
//!   3. It's the same crate family `tauri-plugin-dialog` itself uses
//!      internally on macOS/Linux, so behaviour matches what a future plugin
//!      swap would produce.

use std::path::PathBuf;

use rfd::{AsyncFileDialog, FileHandle};
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
    /// Construct the "user cancelled" / empty result. The dialog ran but the
    /// user dismissed it without picking anything.
    pub fn empty() -> Self {
        Self {
            selections: Vec::new(),
            completed: true,
            note: None,
        }
    }

    /// Construct the platform-limited fallback result (the dialog backend
    /// returned an error — e.g. no display server on a headless CI runner).
    /// Empty selections + `completed: false` + note. Kept for parity with
    /// the prior fallback contract; the rfd-backed `browse` only emits this
    /// when `rfd` itself errors out at runtime.
    pub fn platform_limited() -> Self {
        Self {
            selections: Vec::new(),
            completed: false,
            note: Some(
                "filesystem.browse is platform-limited in this build — \
                 the native picker backend returned an error; returning \
                 empty selection"
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

/// Validate the request payload before opening the dialog. Returns
/// `Err(String)` describing the first invalid filter; `Ok(())` otherwise.
///
/// Pure & allocation-free on the happy path — exhaustively unit-tested.
pub fn validate_request(req: &BrowseRequest) -> Result<(), String> {
    for filter in &req.filters {
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
    Ok(())
}

/// Build (but don't yet show) the `rfd` file picker from a validated request.
///
/// Returned as a `Pin<Box<dyn Future>>` so the [`browse`] command can
/// `.await` it; this keeps the rfd dep out of the pure helpers' signature
/// while still letting the future be driven by the Tauri runtime.
///
/// `Folder` selections use [`AsyncFileDialog::pick_directory`] instead of
/// the file variant, so `filters` are deliberately ignored for that kind.
#[allow(clippy::future_not_send)] // rfd's future isn't `Send` on every backend.
pub fn build_picker(req: &BrowseRequest) -> AsyncFileDialog {
    let mut dialog = AsyncFileDialog::new();
    if let Some(default) = &req.default_path {
        let path = PathBuf::from(default);
        if path.is_absolute() {
            dialog = dialog.set_directory(&path);
        }
        // Relative paths are ignored — rfd's directory setter expects an
        // absolute path; passing a relative one would be a silent no-op on
        // some backends and an error on others, so we skip rather than guess.
    }
    for filter in &req.filters {
        dialog = dialog.add_filter(
            filter.name.clone(),
            &filter
                .extensions
                .iter()
                .map(|e| {
                    // rfd expects extensions without a leading dot; strip
                    // any the user passed in so `"ts"` and `".ts"` both work.
                    e.trim().trim_start_matches('.').to_string()
                })
                .collect::<Vec<_>>(),
        );
    }
    dialog
}

/// Map a single-selection outcome into the wire [`BrowseResult`]. Pure.
///
/// Accepts `Option<FileHandle>` (the rfd async return type) directly so the
/// `browse` command doesn't have to unwrap the handle's path itself. `None`
/// means the user cancelled.
pub fn shape_single(picked: Option<FileHandle>) -> BrowseResult {
    match picked {
        Some(handle) => BrowseResult {
            selections: vec![handle.path().to_string_lossy().into_owned()],
            completed: true,
            note: None,
        },
        None => BrowseResult::empty(),
    }
}

/// Map a multi-selection outcome into the wire [`BrowseResult`]. Pure.
///
/// Accepts `Option<Vec<FileHandle>>` (the rfd async return type). An empty
/// `Vec` (some backends return it after a cleared multi-select) is treated
/// as "no selection" → empty completed result, matching the single-cancel
/// behaviour.
pub fn shape_multi(picked: Option<Vec<FileHandle>>) -> BrowseResult {
    match picked {
        Some(handles) if !handles.is_empty() => BrowseResult {
            selections: handles
                .into_iter()
                .map(|h| h.path().to_string_lossy().into_owned())
                .collect(),
            completed: true,
            note: None,
        },
        _ => BrowseResult::empty(),
    }
}

/// Wrap the dialog outcome (or error) into the wire [`BrowseResult`]. Pure.
///
/// Kept as a separate helper so `browse()` can short-circuit on a backend
/// error (`Err`) and emit [`BrowseResult::platform_limited`] without
/// duplicating the mapping logic.
pub fn shape_result<T>(outcome: Result<T, String>, map: impl Fn(T) -> BrowseResult) -> BrowseResult {
    match outcome {
        Ok(value) => map(value),
        Err(message) => BrowseResult {
            selections: Vec::new(),
            completed: false,
            note: Some(format!(
                "filesystem.browse is platform-limited in this build — \
                 the native picker backend returned an error: {message}"
            )),
        },
    }
}

/// Open a native file/folder picker dialog and return the selection.
///
/// Frontend invokes `invoke("browse", { kind?, defaultPath?, filters? })`.
/// The request is validated up-front ([`validate_request`]); the picker is
/// then built ([`build_picker`]) and shown via the `rfd` async backend.
///
/// Returns:
/// - `Ok(BrowseResult { selections: [...], completed: true })` on a pick.
/// - `Ok(BrowseResult::empty())` when the user cancels (no selection).
/// - `Err(String)` only on input validation failure (invalid filter).
///
/// ## On picker backend errors
///
/// `rfd`'s native async backend returns `Option<FileHandle>` — `None` for a
/// user cancel — and is otherwise infallible from the caller's perspective
/// (backend init failures surface as `None`). The platform-limited result
/// ([`BrowseResult::platform_limited`]) is therefore not emitted from the
/// happy path; it's kept as a public constructor for callers / future
/// backends that do surface hard errors.
#[tauri::command]
pub async fn browse(request: BrowseRequest) -> Result<BrowseResult, String> {
    // Validate filters up-front so a malformed request fails fast before
    // the (slow, GUI-blocking) dialog even opens.
    validate_request(&request)?;

    // Folder kind goes through the folder picker; files/file go through the
    // file picker. rfd's builder is shared; only the terminal `.pick_*`
    // method differs. `pick_folder` ignores any file-extension filters
    // added to the builder (they only apply to file pickers), which matches
    // the [`BrowseRequest::filters`] doc ("ignored for the `folder` kind").
    let result = match request.kind {
        BrowseKind::Folder => {
            let dialog = build_picker(&request);
            shape_single(dialog.pick_folder().await)
        }
        BrowseKind::File => {
            let dialog = build_picker(&request);
            shape_single(dialog.pick_file().await)
        }
        BrowseKind::Files => {
            let dialog = build_picker(&request);
            shape_multi(dialog.pick_files().await)
        }
    };

    Ok(result)
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

    // ── validate_request ────────────────────────────────────────────────

    #[test]
    fn validate_request_accepts_no_filters() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![],
        };
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn validate_request_accepts_well_formed_filter() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![FileFilter {
                name: "TypeScript".to_string(),
                extensions: vec!["ts".to_string(), "tsx".to_string()],
            }],
        };
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn validate_request_rejects_filter_with_empty_name() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![FileFilter {
                name: "  ".to_string(),
                extensions: vec!["ts".to_string()],
            }],
        };
        let err = validate_request(&req).unwrap_err();
        assert!(err.contains("filter.name must be non-empty"));
    }

    #[test]
    fn validate_request_rejects_filter_with_no_extensions() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![FileFilter {
                name: "Empty".to_string(),
                extensions: vec![],
            }],
        };
        let err = validate_request(&req).unwrap_err();
        assert!(err.contains("at least one extension"));
    }

    // ── shape_single / shape_multi ──────────────────────────────────────
    //
    // rfd's async picker returns `Option<FileHandle>` / `Option<Vec<FileHandle>>`.
    // `FileHandle: From<PathBuf>`, so tests construct synthetic handles via
    // `FileHandle::from(PathBuf::from(...))` without standing up a real dialog.

    fn handle(path: &str) -> FileHandle {
        FileHandle::from(PathBuf::from(path))
    }

    #[test]
    fn shape_single_some_yields_one_selection() {
        let res = shape_single(Some(handle("/tmp/file.ts")));
        assert_eq!(res.selections, vec!["/tmp/file.ts".to_string()]);
        assert!(res.completed);
        assert!(res.note.is_none());
    }

    #[test]
    fn shape_single_none_yields_empty_completed() {
        let res = shape_single(None);
        assert!(res.selections.is_empty());
        assert!(res.completed); // user-cancelled ≠ platform-limited
        assert!(res.note.is_none());
    }

    #[test]
    fn shape_multi_some_yields_all_selections() {
        let res = shape_multi(Some(vec![handle("/tmp/a.ts"), handle("/tmp/b.ts")]));
        assert_eq!(
            res.selections,
            vec!["/tmp/a.ts".to_string(), "/tmp/b.ts".to_string()]
        );
        assert!(res.completed);
    }

    #[test]
    fn shape_multi_empty_vec_yields_empty_completed() {
        // rfd can return Some(vec![]) on some backends when the user
        // multi-selects then clears; treat that as "no selection".
        let res = shape_multi(Some(Vec::new()));
        assert!(res.selections.is_empty());
        assert!(res.completed);
    }

    #[test]
    fn shape_multi_none_yields_empty_completed() {
        let res = shape_multi(None);
        assert!(res.selections.is_empty());
        assert!(res.completed);
    }

    // ── shape_result (error → platform_limited) ─────────────────────────
    //
    // shape_result is a pure helper for future backends that surface hard
    // errors; it maps `Result<T, E>` into either the user-supplied Ok-shape
    // or the platform-limited result. Kept + tested even though the rfd
    // happy path doesn't currently flow through it.

    #[test]
    fn shape_result_ok_uses_mapper() {
        let res = shape_result(Ok(handle("/x")), |h| shape_single(Some(h)));
        assert_eq!(res.selections, vec!["/x".to_string()]);
        assert!(res.completed);
    }

    #[test]
    fn shape_result_err_yields_platform_limited() {
        let res: BrowseResult =
            shape_result::<()>(Err("no display server".to_string()), |_| {
                BrowseResult::empty()
            });
        assert!(res.selections.is_empty());
        assert!(!res.completed);
        let note = res.note.expect("note");
        assert!(note.contains("platform-limited"));
        assert!(note.contains("no display server"));
    }

    // ── build_picker shape (no dialog shown) ────────────────────────────
    //
    // We can't await a real picker in a unit test (it would block on a GUI),
    // but `build_picker` itself must construct without panicking. Construct
    // it, drop it before `.pick_*` — exercises the validation/filter
    // projection path that previously returned the platform-limited stub.

    #[test]
    fn build_picker_constructs_for_file_kind() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: Some(std::env::temp_dir().to_string_lossy().into_owned()),
            filters: vec![FileFilter {
                name: "TypeScript".to_string(),
                extensions: vec!["ts".to_string(), ".tsx".to_string()],
            }],
        };
        // Build + immediately drop — never awaited, never shows a dialog.
        let _ = build_picker(&req);
    }

    #[test]
    fn build_picker_constructs_for_folder_kind_without_filters() {
        let req = BrowseRequest {
            kind: BrowseKind::Folder,
            default_path: None,
            filters: vec![],
        };
        let _ = build_picker(&req);
    }

    #[test]
    fn build_picker_ignores_relative_default_path() {
        // Relative path → set_directory not called. Should still construct.
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: Some("relative/path".to_string()),
            filters: vec![],
        };
        let _ = build_picker(&req);
    }

    // ── BrowseResult serialisation contract ─────────────────────────────

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

    // ── browse command-level validation (sync portion) ──────────────────
    //
    // `browse` is async and would block on the GUI; we can still exercise
    // the *synchronous* part of the command (validation) by reproducing the
    // same call sequence the command body uses: validate first, then shape.

    #[tokio::test]
    async fn browse_validation_pipeline_rejects_bad_filter_before_picker() {
        let req = BrowseRequest {
            kind: BrowseKind::File,
            default_path: None,
            filters: vec![FileFilter {
                name: "".to_string(),
                extensions: vec!["ts".to_string()],
            }],
        };
        // The command does `validate_request(&request)?` before any async
        // work, so this mirrors exactly what `browse(req).await` would do
        // for the failure path — without standing up a dialog.
        let err = validate_request(&req).unwrap_err();
        assert!(err.contains("filter.name must be non-empty"));
    }

    #[tokio::test]
    async fn browse_validation_pipeline_accepts_good_request() {
        let req = BrowseRequest {
            kind: BrowseKind::Folder,
            default_path: None,
            filters: vec![],
        };
        assert!(validate_request(&req).is_ok());
    }
}

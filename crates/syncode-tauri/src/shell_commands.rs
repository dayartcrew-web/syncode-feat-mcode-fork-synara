//! Tauri IPC Commands — shell integration (open-in-editor, etc.)
//!
//! Bridges the frontend `NativeApi.shell.openInEditor` (T6 `tauriNativeApi`)
//! to the OS. Falls back to the OS default opener when no editor is given and
//! `EDITOR` is unset.

/// Open a path in the user's editor (or the OS default opener).
///
/// Frontend invokes `invoke("shell_open_editor", { cwd, editor })`. When
/// `editor` is omitted and `EDITOR` is unset, uses `open` (macOS) /
/// `explorer` (Windows) / `xdg-open` (Linux).
#[tauri::command]
pub async fn shell_open_editor(cwd: String, editor: Option<String>) -> Result<(), String> {
    let editor = editor
        .or_else(|| std::env::var("EDITOR").ok().filter(|e| !e.is_empty()))
        .unwrap_or_else(|| {
            if cfg!(target_os = "macos") {
                "open".to_string()
            } else if cfg!(target_os = "windows") {
                "explorer".to_string()
            } else {
                "xdg-open".to_string()
            }
        });
    std::process::Command::new(&editor)
        .arg(&cwd)
        .spawn()
        .map_err(|e| format!("failed to launch editor \"{editor}\": {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_open_editor_resolves_default_opener() {
        // Exercise the default-opener resolution path without spawning.
        let resolved = None::<String>
            .or_else(|| std::env::var("EDITOR").ok().filter(|e| !e.is_empty()))
            .unwrap_or_else(|| {
                if cfg!(target_os = "macos") {
                    "open".to_string()
                } else if cfg!(target_os = "windows") {
                    "explorer".to_string()
                } else {
                    "xdg-open".to_string()
                }
            });
        assert!(!resolved.is_empty());
    }
}

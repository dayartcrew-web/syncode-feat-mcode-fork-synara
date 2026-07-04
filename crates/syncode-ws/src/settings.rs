//! In-memory server settings (T6c-18) — REAL persistence for the server
//! session.
//!
//! The cloned MCode UI persists user edits via `server.setConfig` /
//! `updateSettings` / `patchSettings` / `updateProvider` /
//! `upsertKeybinding`. Syncode has no on-disk settings file, but the
//! in-memory store defined here makes those edits durable for the lifetime
//! of the WebSocket server: reads return the stored value, writes merge into
//! it, and push events fan out the new state to subscribed connections.
//!
//! The store holds two top-level JSON documents:
//!   - `config`   — the MCode `ServerConfig` shape
//!     (`frontend/src/contracts/tier3/server.ts`).
//!   - `settings` — the MCode `ServerSettings` shape (same source).
//!
//! Both are initialized from the canonical default builders
//! (`build_default_server_config` / `build_default_server_settings`) at
//! `WsState` construction time. The `auth_mode` string is captured then so
//! the config's `authMode` field stays consistent across the session (the
//! UI doesn't read it today, but it's a cheap accurate signal).
//!
//! `merge_json` is a recursive JSON deep-merge used by `patchSettings` /
//! `updateSettings` to apply a partial patch: objects are merged key-by-key,
//! arrays and scalars are replaced wholesale (the MCode
//! `ServerSettingsPatch` semantics — e.g. `skills.disabled` is replaced, not
//! appended).

use serde_json::{Map, Value};

/// In-memory server settings — persists during the server session.
///
/// Stored as opaque `serde_json::Value` rather than typed structs because the
/// MCode `ServerConfig`/`ServerSettings` schemas are large and partially
/// optional; the handlers below touch a handful of fields each, and a Value
/// avoids drifting from the contracts layer when MCode evolves. The shapes
/// are validated structurally at the handler boundary (reject non-object
/// patches with `-32602`).
#[derive(Debug, Clone)]
pub struct ServerSettingsState {
    /// `ServerConfig` document. Initialized from `build_default_server_config`.
    pub config: Value,
    /// `ServerSettings` document. Initialized from `build_default_server_settings`.
    pub settings: Value,
}

impl ServerSettingsState {
    /// Build the default state. `auth_mode` is the syncode `WsAuthConfig` mode
    /// string (`unsafe-no-auth` | `remote-reachable` | …) surfaced in the
    /// config's `authMode` field. Kept here (rather than reading `WsState` at
    /// materialize time) so the store can be built before `WsState` is fully
    /// assembled.
    pub fn new(auth_mode: String) -> Self {
        Self {
            config: build_default_server_config(&auth_mode),
            settings: build_default_server_settings(),
        }
    }
}

/// Build the minimal valid `ServerConfig` shape (MCode
/// `frontend/src/contracts/tier3/server.ts`). Shared by the read-side
/// `server.getConfig` handler, the write-side `server.setConfig` handler, and
/// the in-memory store initialization (`ServerSettingsState::new`).
///
/// Top-level fields returned:
/// - `cwd`: process cwd (non-empty)
/// - `worktreesDir`: `<cwd>/.synara/worktrees` (non-empty)
/// - `keybindingsConfigPath`: `<home>/.synara/keybindings.json` (non-empty)
/// - `keybindings`: empty array (no resolved rules; UI tolerates empty)
/// - `issues`: empty array (no keybinding-config validation runs)
/// - `providers`: empty array (no provider-availability probe)
/// - `availableEditors`: empty array (no editor detection)
/// - `homeDir`: `Option<HOME>` (omitted when unset; optional in schema)
/// - `authMode`: syncode auth mode surfaced from `WsAuthConfig`
///   (`unsafe-no-auth` | `remote-reachable` | ...). Not part of the MCode
///   `ServerConfig` schema, but harmless as an extra field and useful for
///   the UI to display the active auth policy.
pub fn build_default_server_config(auth_mode: &str) -> Value {
    let cwd = server_cwd();
    let home = server_home_dir();
    let worktrees_dir = format!("{}/.synara/worktrees", cwd.trim_end_matches('/'));
    let keybindings_path = format!(
        "{}/.synara/keybindings.json",
        home.as_deref().unwrap_or(&cwd)
    );
    // Use the supplied mode verbatim — the UI doesn't read this field today,
    // but it's a cheap, accurate signal of the active policy. Fall back to the
    // no-auth default only if the caller passed an empty string.
    let auth_mode_str = if auth_mode.trim().is_empty() {
        "unsafe-no-auth".to_string()
    } else {
        auth_mode.to_string()
    };

    let mut cfg = serde_json::json!({
        "cwd": cwd,
        "worktreesDir": worktrees_dir,
        "keybindingsConfigPath": keybindings_path,
        "keybindings": [],
        "issues": [],
        "providers": [],
        "availableEditors": [],
        "authMode": auth_mode_str,
    });
    // Insert `homeDir` only when HOME was resolvable (the field is optional in
    // the MCode schema; absence deserializes as `undefined`). Single-level
    // guard — clippy-clean (no collapsible-if nesting).
    if let (Some(h), Some(obj)) = (home, cfg.as_object_mut()) {
        obj.insert("homeDir".into(), Value::String(h));
    }
    cfg
}

/// Build the MCode `DEFAULT_SERVER_SETTINGS` literal. Shared by the read-side
/// `server.getSettings` handler, the write-side `server.updateSettings` /
/// `patchSettings` handlers, and the in-memory store initialization. The
/// vendored UI references this exact shape for state initialization (see
/// `frontend/src/contracts/tier3/server.ts` `DEFAULT_SERVER_SETTINGS`).
/// Each provider is enabled with its conventional binary name and empty
/// `customModels`; the text-generation model selection defaults to
/// `{ provider: "codex", model: "gpt-5.4-mini" }` (matches the literal).
pub fn build_default_server_settings() -> Value {
    serde_json::json!({
        "enableAssistantStreaming": false,
        "defaultThreadEnvMode": "local",
        "addProjectBaseDirectory": "",
        "textGenerationModelSelection": {
            "provider": "codex",
            "model": "gpt-5.4-mini",
        },
        "providers": {
            "codex": { "enabled": true, "binaryPath": "codex", "customModels": [], "homePath": "" },
            "claudeAgent": { "enabled": true, "binaryPath": "claude", "customModels": [], "launchArgs": "" },
            "cursor": { "enabled": true, "binaryPath": "cursor-agent", "customModels": [], "apiEndpoint": "" },
            "gemini": { "enabled": true, "binaryPath": "gemini", "customModels": [] },
            "grok": { "enabled": true, "binaryPath": "grok", "customModels": [] },
            "kilo": { "enabled": true, "binaryPath": "kilo", "customModels": [], "serverUrl": "", "serverPassword": "" },
            "opencode": {
                "enabled": true, "binaryPath": "opencode", "customModels": [],
                "serverUrl": "", "serverPassword": "", "experimentalWebSockets": false,
            },
            "pi": { "enabled": true, "binaryPath": "pi", "customModels": [], "agentDir": "" },
        },
        "skills": { "disabled": [] },
    })
}

/// Resolve the server's process cwd as a non-empty string. Falls back to
/// `"/"` (POSIX root) when `std::env::current_dir` fails or yields an empty
/// string — the MCode `ServerConfig.cwd` is `TrimmedNonEmptyString`, so we
/// must always return a non-empty value.
pub(crate) fn server_cwd() -> String {
    std::env::current_dir()
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "/".to_string())
}

/// Resolve the user's home directory from `$HOME` (POSIX) or `$USERPROFILE`
/// (Windows). Returns `None` when neither is set or both are empty/blank —
/// the `homeDir` field is optional in the MCode schema and is omitted in
/// that case.
pub(crate) fn server_home_dir() -> Option<String> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Recursively deep-merge `patch` into `target` (in place).
///
/// Semantics (mirrors MCode `ServerSettingsPatch`):
///   - For each key in `patch`:
///     - If both `target[key]` and `patch[key]` are objects, recurse.
///     - Otherwise, replace `target[key]` with `patch[key]` (scalars, arrays,
///       null, and object-vs-non-object mismatches all overwrite).
///   - Keys absent from `patch` are left untouched in `target`.
///   - A non-object `target` or `patch` is a no-op (the caller validates
///     object shape before calling; defensive guard for safety).
///
/// Arrays are replaced wholesale (not concatenated) — the MCode patch
/// semantics treat `skills.disabled` and provider `customModels` as full
/// replacements, not append operations.
pub fn merge_json(target: &mut Value, patch: &Value) {
    // Deep-merge only applies when both sides are objects. Any other shape
    // (scalar/array/null on either side) falls through: the caller has
    // already validated the patch is an object, so a non-object target is
    // the only realistic path here, and replacing it with the patch is the
    // correct outcome (the field was previously missing or wrong-typed).
    let (Some(target_obj), Some(patch_obj)) = (target.as_object_mut(), patch.as_object()) else {
        // Replace wholesale — preserves patch semantics for scalar/array
        // patches against a non-object target.
        *target = patch.clone();
        return;
    };
    merge_objects(target_obj, patch_obj);
}

/// Object-level deep-merge: walks `patch` keys and recurses into matching
/// object-valued entries. Extracted so the top-level `merge_json` can take
/// `&mut Value` while the recursion works on `&mut Map`.
fn merge_objects(target: &mut Map<String, Value>, patch: &Map<String, Value>) {
    for (key, patch_value) in patch {
        match target.get_mut(key) {
            Some(existing) => {
                // Both object → recurse. Anything else → replace.
                if let (Some(existing_obj), Some(patch_obj)) =
                    (existing.as_object_mut(), patch_value.as_object())
                {
                    merge_objects(existing_obj, patch_obj);
                } else {
                    *existing = patch_value.clone();
                }
            }
            None => {
                target.insert(key.clone(), patch_value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_has_config_and_settings() {
        let state = ServerSettingsState::new("unsafe-no-auth".into());
        assert!(state.config.is_object());
        assert!(state.settings.is_object());
        assert_eq!(state.config["authMode"], "unsafe-no-auth");
        assert_eq!(state.settings["defaultThreadEnvMode"], "local");
        // Provider map should be populated by the default.
        assert!(state.settings["providers"]["codex"].is_object());
    }

    #[test]
    fn default_config_non_empty_cwd() {
        let cfg = build_default_server_config("unsafe-no-auth");
        assert!(!cfg["cwd"].as_str().unwrap().is_empty());
        assert!(cfg["worktreesDir"].as_str().unwrap().contains(".synara"));
    }

    #[test]
    fn default_settings_has_all_providers() {
        let s = build_default_server_settings();
        let providers = s["providers"].as_object().unwrap();
        for key in [
            "codex",
            "claudeAgent",
            "cursor",
            "gemini",
            "grok",
            "kilo",
            "opencode",
            "pi",
        ] {
            assert!(providers.contains_key(key), "missing provider: {key}");
            assert_eq!(providers[key]["enabled"], true);
        }
    }

    #[test]
    fn merge_replaces_scalar() {
        let mut target = serde_json::json!({ "enableAssistantStreaming": false });
        let patch = serde_json::json!({ "enableAssistantStreaming": true });
        merge_json(&mut target, &patch);
        assert_eq!(target["enableAssistantStreaming"], true);
    }

    #[test]
    fn merge_recurses_into_objects() {
        let mut target = serde_json::json!({
            "textGenerationModelSelection": { "provider": "codex", "model": "gpt-5.4-mini" }
        });
        let patch = serde_json::json!({ "textGenerationModelSelection": { "model": "claude-4" } });
        merge_json(&mut target, &patch);
        // Untouched sibling key preserved.
        assert_eq!(target["textGenerationModelSelection"]["provider"], "codex");
        // Patched key replaced.
        assert_eq!(target["textGenerationModelSelection"]["model"], "claude-4");
    }

    #[test]
    fn merge_replaces_arrays_wholesale() {
        let mut target = serde_json::json!({ "skills": { "disabled": ["a"] } });
        let patch = serde_json::json!({ "skills": { "disabled": ["b", "c"] } });
        merge_json(&mut target, &patch);
        let disabled = target["skills"]["disabled"].as_array().unwrap();
        assert_eq!(disabled.len(), 2);
        assert_eq!(disabled[0], "b");
        assert_eq!(disabled[1], "c");
    }

    #[test]
    fn merge_adds_new_keys() {
        let mut target = serde_json::json!({ "keep": 1 });
        let patch = serde_json::json!({ "added": 2 });
        merge_json(&mut target, &patch);
        assert_eq!(target["keep"], 1);
        assert_eq!(target["added"], 2);
    }

    #[test]
    fn merge_object_into_scalar_replaces() {
        // If the target is a scalar but the patch is an object, the patch
        // replaces wholesale — there's no object to merge into.
        let mut target = serde_json::json!(42);
        let patch = serde_json::json!({ "a": 1 });
        merge_json(&mut target, &patch);
        assert_eq!(target["a"], 1);
    }

    #[test]
    fn merge_nested_provider_field() {
        let mut target = serde_json::json!({
            "providers": { "codex": { "enabled": true, "binaryPath": "codex", "customModels": [] } }
        });
        let patch = serde_json::json!({ "providers": { "codex": { "enabled": false } } });
        merge_json(&mut target, &patch);
        assert_eq!(target["providers"]["codex"]["enabled"], false);
        // Untouched sibling keys preserved.
        assert_eq!(target["providers"]["codex"]["binaryPath"], "codex");
    }
}

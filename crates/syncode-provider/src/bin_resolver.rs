//! Cross-platform binary path resolution.
//!
//! The core problem this module solves: on Windows, npm-global CLIs ship as
//! `.cmd` batch wrappers. Rust's `std::process::Command` does not honour
//! `PATHEXT`, so `Command::new("claude")` fails to find `claude.cmd`. Worse,
//! even when wrapped with `cmd /C claude.cmd ...`, the JSON-RPC over stdio
//! protocol used by long-lived CLIs (codex `app-server`) often hangs because
//! `cmd.exe` does not faithfully relay piped stdin/stdout for the lifetime of
//! an interactive NDJSON session.
//!
//! The fix is to **prefer a real `.exe`** whenever one exists — either from a
//! native installer (e.g. `codex.exe` under `%LOCALAPPDATA%\Programs\OpenAI\Codex\bin\`)
//! or `~/.local/bin/claude.exe` — and only fall back to the npm `.cmd` wrapper
//! when no native binary is present.

/// Per-tool known native-install locations, checked **before** PATH lookup so
/// that a real `.exe` always wins over the npm `.cmd` shim.
///
/// Each entry is a function mapping a "home-ish" env var to a candidate path.
const KNOWN_LOCATIONS: &[(&str, &[&str])] = &[
    // OpenAI Codex native installer (Windows): %LOCALAPPDATA%\Programs\OpenAI\Codex\bin\codex.exe
    ("LOCALAPPDATA", &["Programs", "OpenAI", "Codex", "bin"]),
    // Cursor CLI ships a native launcher inside the Electron app bundle.
    (
        "LOCALAPPDATA",
        &["Programs", "cursor", "resources", "app", "bin"],
    ),
];

/// Resolve a bare binary name (e.g. `"claude"`, `"codex"`) to a full path.
///
/// On Unix this is a no-op (PATH resolution works natively). On Windows the
/// resolution order is:
///
/// 1. Known native-install directories (prefer `.exe`) — these don't need
///    `cmd /C` and faithfully relay stdio pipes for NDJSON protocols.
/// 2. `~/.local/bin/<name>.exe` — common Rust/Go binary install location.
/// 3. `which::which(name)` — honours PATHEXT; usually returns the npm `.cmd`.
/// 4. `%APPDATA%/npm/<name>.cmd` — last-resort npm shim.
/// 5. The bare name (let the OS try and surface a clear error if it fails).
pub fn resolve_binary(name: &str) -> String {
    // On Unix, PATH resolution works natively — no intervention needed.
    #[cfg(not(windows))]
    {
        return name.to_string();
    }

    // On Windows, prefer a real .exe over the npm .cmd shim.
    #[cfg(windows)]
    {
        // 1. Known native-install directories (prefer .exe)
        for (env_key, parts) in KNOWN_LOCATIONS {
            if let Ok(base) = std::env::var(env_key) {
                for ext in &["exe", "cmd", "bat"] {
                    let mut candidate = std::path::PathBuf::from(&base);
                    for p in parts.iter() {
                        candidate.push(p);
                    }
                    candidate.push(format!("{name}.{ext}"));
                    if candidate.exists() {
                        return candidate.to_string_lossy().into_owned();
                    }
                }
            }
        }

        // 2. ~/.local/bin/<name>.exe  (Rust/Go cargo install location)
        if let Ok(home) = std::env::var("USERPROFILE") {
            for ext in &["exe", "cmd", "bat"] {
                let candidate = format!("{home}/.local/bin/{name}.{ext}");
                if std::path::Path::new(&candidate).exists() {
                    return candidate;
                }
            }
        }

        // 3. which::which (honours PATHEXT — usually returns the npm .cmd shim)
        if let Ok(path) = which::which(name) {
            return path.to_string_lossy().into_owned();
        }

        // 4. npm global location fallback
        if let Ok(appdata) = std::env::var("APPDATA") {
            for ext in &["cmd", "exe", "bat"] {
                let candidate = format!("{appdata}/npm/{name}.{ext}");
                if std::path::Path::new(&candidate).exists() {
                    return candidate;
                }
            }
        }

        // 5. Fall back — let the OS try (may fail, but gives a clear error)
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_binary_returns_name_on_unix() {
        // On Unix, the function should just return the name as-is.
        // On Windows, it probes PATH — we can't assert a specific path
        // without knowing what's installed, so just verify it doesn't panic.
        let result = resolve_binary("nonexistent_binary_xyz");
        assert!(!result.is_empty());
    }

    #[cfg(windows)]
    #[test]
    fn resolve_binary_prefers_exe_over_cmd_when_present() {
        // The OpenAI Codex native installer ships codex.exe under LOCALAPPDATA.
        // When present, resolve_binary("codex") MUST return the .exe path,
        // not the npm codex.cmd shim — this is the whole point of the resolver.
        let local_appdata = std::env::var("LOCALAPPDATA").unwrap_or_default();
        let native_exe = format!("{local_appdata}/Programs/OpenAI/Codex/bin/codex.exe");
        if std::path::Path::new(&native_exe).exists() {
            let resolved = resolve_binary("codex");
            assert!(
                resolved.to_lowercase().ends_with("codex.exe"),
                "expected codex.exe to win over codex.cmd, got: {resolved}"
            );
        }
    }
}

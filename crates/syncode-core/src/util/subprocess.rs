//! Cross-crate subprocess helpers — the single chokepoint for hiding child
//! console windows on Windows.
//!
//! Centralising `CREATE_NO_WINDOW` here means every spawn site
//! (`syncode-provider`, `syncode-git`, `syncode-ws`, `syncode-automation`,
//! `syncode-tauri`) hides its console window through ONE place — mirroring
//! synara's shared `prepareWindowsSafeProcess` (`packages/shared/windowsProcess.ts`).
//! Without a shared helper, the per-crate spawns leak: on Windows every
//! `claude`/`codex`/`opencode`/`git`/`gh`/`npm`/automation `cmd /C`/… child
//! pops a visible `cmd` window each time the app does work
//! ("cmd muncul saat syncode bekerja").
//!
//! `CREATE_NO_WINDOW` (0x0800_0000) is the Windows process-creation flag that
//! prevents a console being allocated for the child. `tokio::process::Command`
//! exposes it as an inherent method on Windows; `std::process::Command`
//! exposes it via the `CommandExt` trait.
//!
//! Prefer [`hidden_command`] / [`hidden_std_command`] over `Command::new` so
//! the window-hide can never be forgotten at a call site.

/// Hide the console window for a `tokio::process::Command` on Windows
/// (`CREATE_NO_WINDOW` = 0x0800_0000). No-op on non-Windows. Call before
/// `.spawn()` / `.output()`.
#[cfg(windows)]
pub fn hide_console_window(cmd: &mut tokio::process::Command) {
    cmd.creation_flags(0x0800_0000);
}

#[cfg(not(windows))]
#[allow(unused_variables)]
pub fn hide_console_window(cmd: &mut tokio::process::Command) {}

/// Hide the console window for a `std::process::Command` on Windows. No-op on
/// non-Windows. `creation_flags` is a trait method (`CommandExt`) on std, so
/// the import is scoped to the Windows branch.
#[cfg(windows)]
pub fn hide_console_window_std(cmd: &mut std::process::Command) {
    use std::os::windows::process::CommandExt;
    cmd.creation_flags(0x0800_0000);
}

#[cfg(not(windows))]
#[allow(unused_variables)]
pub fn hide_console_window_std(cmd: &mut std::process::Command) {}

/// Construct a `tokio::process::Command` with the console window hidden.
/// Prefer this over `Command::new` so the window-hide is baked in.
pub fn hidden_command<S: AsRef<str>>(program: S) -> tokio::process::Command {
    let mut c = tokio::process::Command::new(program.as_ref());
    hide_console_window(&mut c);
    c
}

/// Construct a `std::process::Command` with the console window hidden.
pub fn hidden_std_command<S: AsRef<str>>(program: S) -> std::process::Command {
    let mut c = std::process::Command::new(program.as_ref());
    hide_console_window_std(&mut c);
    c
}

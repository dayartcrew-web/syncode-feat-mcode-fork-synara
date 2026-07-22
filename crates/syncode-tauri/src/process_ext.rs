//! Windows console-hiding helper for `std::process::Command`.
//!
//! Without `CREATE_NO_WINDOW` (0x0800_0000), spawning `.cmd`/`.bat`/console
//! binaries from a `windows_subsystem = "windows"` app pops a visible `cmd`
//! window in front of the GUI — the "terminal runs in front when the app is
//! running" desktop bug. The Tauri binary itself is built with
//! `windows_subsystem = "windows"` (see `main.rs`), so the main process has no
//! console; this guard is for the child processes it launches.
//!
//! This mirrors `syncode_provider::subprocess::hide_console_window` (which
//! covers `tokio::process::Command`) for the synchronous `std::process::Command`
//! spawns in `shell_commands.rs` / `desktop_commands.rs`. No-op on non-Windows.

#[cfg(windows)]
pub fn hide_console_window(cmd: &mut std::process::Command) {
    use std::os::windows::process::CommandExt;
    cmd.creation_flags(0x0800_0000);
}

#[cfg(not(windows))]
#[allow(unused_variables)]
pub fn hide_console_window(cmd: &mut std::process::Command) {}

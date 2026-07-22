//! Windows console-hiding helper for `std::process::Command`.
//!
//! Without `CREATE_NO_WINDOW` (0x0800_0000), spawning `.cmd`/`.bat`/console
//! binaries from a `windows_subsystem = "windows"` app pops a visible `cmd`
//! window in front of the GUI — the "terminal runs in front when the app is
//! running" desktop bug. The Tauri binary itself is built with
//! `windows_subsystem = "windows"` (see `main.rs`), so the main process has no
//! console; this guard is for the child processes it launches.
//!
//! Re-exports the shared implementation from `syncode_core::util::subprocess`
//! so there is ONE `CREATE_NO_WINDOW` chokepoint across all crates
//! (`syncode-provider` tokio spawns, `syncode-ws`/`syncode-git`/`syncode-automation`
//! spawns, and these synchronous `std::process::Command` spawns here). No-op on
//! non-Windows.

pub use syncode_core::util::subprocess::hide_console_window_std as hide_console_window;

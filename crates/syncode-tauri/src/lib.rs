//! Syncode Tauri — Desktop Integration
//!
//! Tauri app entry, IPC commands, auto-updater, system tray,
//! and native window management.

pub mod browser_commands;
pub mod commands;
pub mod desktop_commands;
pub mod filesystem_commands;
pub mod paths;
pub mod process_ext;
pub mod shell_commands;
pub mod terminal_commands;
pub mod tray;
pub mod updater;
/// IPC commands exposing the booted WS endpoint to the frontend (DSK-1).
pub mod ws_commands;
/// WS server spawn inside the Tauri shell (DSK-1).
pub mod ws_setup;

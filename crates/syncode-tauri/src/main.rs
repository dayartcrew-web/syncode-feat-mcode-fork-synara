//! Syncode Desktop — Tauri v2 Application Entry
//!
//! Main binary entry point for the Syncode desktop application.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use syncode_tauri::{commands, git_commands, shell_commands, terminal_commands};
use tauri::Manager;

fn main() {
    tauri::Builder::default()
        .manage(commands::ProviderRegistryState::new())
        .manage(commands::SessionStoreState::new())
        .invoke_handler(tauri::generate_handler![
            // app / providers / sessions
            commands::get_app_info,
            commands::get_version,
            commands::list_providers,
            commands::get_provider_status,
            commands::list_sessions,
            commands::create_session,
            // shell
            shell_commands::shell_open_editor,
            // git
            git_commands::git_status,
            git_commands::git_diff,
            git_commands::git_log,
            git_commands::git_branches,
            git_commands::git_add,
            git_commands::git_commit,
            git_commands::git_create_branch,
            git_commands::git_delete_branch,
            git_commands::git_checkout,
            // terminal
            terminal_commands::terminal_create_session,
            terminal_commands::terminal_write,
            terminal_commands::terminal_ack,
            terminal_commands::terminal_resize,
            terminal_commands::terminal_read_output,
            terminal_commands::terminal_destroy_session,
            terminal_commands::terminal_list_sessions,
        ])
        .setup(|app| {
            // Initialize tracing subscriber
            tracing_subscriber::fmt()
                .with_max_level(if cfg!(debug_assertions) {
                    tracing::Level::DEBUG
                } else {
                    tracing::Level::INFO
                })
                .init();

            tracing::info!("Syncode desktop starting — PID: {}", std::process::id());

            #[cfg(debug_assertions)]
            {
                let window = app.get_webview_window("main").unwrap();
                window.open_devtools();
            }

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running Syncode Tauri application");
}

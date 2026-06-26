//! Syncode Desktop — Tauri v2 Application Entry
//!
//! Main binary entry point for the Syncode desktop application.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use syncode_tauri::commands;
use tauri::Manager;

fn main() {
    tauri::Builder::default()
        .manage(commands::ProviderRegistryState::new())
        .manage(commands::SessionStoreState::new())
        .invoke_handler(tauri::generate_handler![
            commands::get_app_info,
            commands::get_version,
            commands::list_providers,
            commands::get_provider_status,
            commands::list_sessions,
            commands::create_session,
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

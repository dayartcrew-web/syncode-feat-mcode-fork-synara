//! Syncode Desktop — Tauri v2 Application Entry
//!
//! Main binary entry point for the Syncode desktop application.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use syncode_tauri::{
    browser_commands, commands, desktop_commands, filesystem_commands, git_commands,
    shell_commands, terminal_commands, ws_commands, ws_setup,
};
use tauri::Manager;

fn main() {
    tauri::Builder::default()
        .manage(commands::ProviderRegistryState::new())
        .manage(commands::SessionStoreState::new())
        // Managed updater state — desktop commands (DSK-2) read/mutate this
        // to drive the check-for-updates / apply-update flow.
        .manage(syncode_tauri::updater::UpdaterState::new())
        // Holds the WS server handle once `.setup()` boots it. Managed here
        // (before setup) so the WS commands can `try_state` it from the very
        // first invoke — they'll return "WS unavailable" until setup finishes,
        // rather than panicking on a missing state.
        .manage(ws_setup::WsRuntimeState::new())
        .invoke_handler(tauri::generate_handler![
            // app / providers / sessions
            commands::get_app_info,
            commands::get_version,
            commands::list_providers,
            commands::get_provider_status,
            commands::list_sessions,
            commands::create_session,
            // ws (DSK-1) — exposes the in-process WS endpoint to the frontend
            ws_commands::get_ws_endpoint,
            // shell
            shell_commands::shell_open_editor,
            // desktop (DSK-2) — checkForUpdates / applyUpdate / openExternal /
            // openInEditor. Back the `desktop.*` RPC names the MCode UI calls
            // via Tauri invoke().
            desktop_commands::check_for_updates,
            desktop_commands::apply_update,
            desktop_commands::open_external,
            desktop_commands::open_in_editor,
            // browser (DSK-2) — captureScreenshot / listTabs. Graceful
            // platform-limited stubs (no portable webview-capture / tab-list
            // API in Tauri v2 today); return typed fallbacks.
            browser_commands::capture_screenshot,
            browser_commands::list_tabs,
            // filesystem (DSK-2) — browse. Native file/folder picker dialog;
            // falls back to an empty-selection result when the dialog plugin
            // isn't registered.
            filesystem_commands::browse,
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

            // Boot the in-process WS server (DSK-1). The web UI connects to
            // this endpoint instead of an external standalone binary, so the
            // desktop shell is self-contained. `boot` runs on the Tauri app's
            // existing tokio runtime — we just block on it here. Failures
            // (port already taken, etc.) are surfaced to the user via the
            // returned error and abort startup; SQLite/provider init failures
            // degrade gracefully (in-memory / inert) and never reach this
            // error path.
            let ws_config = ws_setup::WsConfig::from_env();
            tracing::info!(
                host = %ws_config.host,
                port = ws_config.port,
                db_path = %ws_config.db_path,
                default_provider = %ws_config.default_provider,
                "Booting in-process WS server",
            );
            let ws_handle = tauri::async_runtime::block_on(ws_setup::boot(&ws_config))
                .map_err(|e| {
                    // Wrap as a Box<dyn Error> — Tauri's setup expects that.
                    Box::<dyn std::error::Error>::from(e)
                })?;
            tracing::info!(
                endpoint = %ws_handle.endpoint,
                "In-process WS server booted",
            );

            // Store the handle so WS commands and the shared WsState are
            // reachable from Tauri commands. `WsRuntimeState::set` panics on a
            // double-set — setup runs once, so that's the correct invariant.
            app.state::<ws_setup::WsRuntimeState>().set(ws_handle);

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

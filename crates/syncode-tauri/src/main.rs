//! Syncode Desktop — Tauri v2 Application Entry
//!
//! Main binary entry point for the Syncode desktop application.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use syncode_tauri::{
    browser_commands, commands, desktop_commands, filesystem_commands, shell_commands,
    terminal_commands, ws_commands, ws_setup,
};
use tauri::Manager;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
    install_panic_hook();
    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_dialog::init());
    // Embedded WebDriver server + WebdriverIO backend access — ONLY in
    // test/dogfood binaries built with `--features webdriver`. Never ship in a
    // production release (the embedded driver can drive the webview). The
    // `let builder` shadowing pattern keeps `#[cfg]` off the method chain
    // (attribute-on-call-in-chain is unreliable).
    #[cfg(feature = "webdriver")]
    let builder = builder
        .plugin(tauri_plugin_wdio_webdriver::init())
        .plugin(tauri_plugin_wdio::init());
    builder
        .manage(commands::ProviderRegistryState::new())
        .manage(commands::SessionStoreState::new())
        // Shared terminal PTY session manager. The `terminal_*` commands take
        // `State<SharedSessionManager>`; without this every invoke rejects with
        // "state not managed" and the terminal panel renders dead.
        .manage(terminal_commands::shared_session_manager())
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
            desktop_commands::toggle_devtools,
            // browser (DSK-2) — captureScreenshot / listTabs. Graceful
            // platform-limited stubs (no portable webview-capture / tab-list
            // API in Tauri v2 today); return typed fallbacks.
            browser_commands::capture_screenshot,
            browser_commands::list_tabs,
            // filesystem (DSK-2) — browse. Native file/folder picker dialog;
            // falls back to an empty-selection result when the dialog plugin
            // isn't registered.
            filesystem_commands::browse,
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
            // Initialize tracing. The desktop binary sets
            // `windows_subsystem = "windows"` (line 5) in release builds, which
            // detaches stderr — so a plain `fmt().init()` silently discards
            // every `tracing::info!` call in `ws_setup::boot`. Layer a file
            // writer under `%APPDATA%\syncode\syncode.log` (mirrors panic.log)
            // so users can share logs when filing issues. The file is truncated
            // on each launch to keep it focused on the most recent session.
            let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new(if cfg!(debug_assertions) {
                    "debug"
                } else {
                    "info"
                })
            });
            let registry = tracing_subscriber::registry().with(env_filter);
            let stderr_layer = fmt::layer().with_writer(std::io::stderr);

            if let Some(dir) = syncode_tauri::paths::log_dir() {
                let _ = std::fs::create_dir_all(&dir);
                let _ = std::fs::write(dir.join("syncode.log"), ""); // truncate on launch
                let file_appender = tracing_appender::rolling::never(&dir, "syncode.log");
                let file_layer = fmt::layer().with_writer(file_appender).with_ansi(false); // ANSI escapes don't render in Notepad
                registry.with(stderr_layer).with(file_layer).init();
            } else {
                registry.with(stderr_layer).init();
            }

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
            let ws_handle =
                tauri::async_runtime::block_on(ws_setup::boot(&ws_config)).map_err(|e| {
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
        .build(tauri::generate_context!())
        .expect("error while building Syncode Tauri application")
        .run(|app_handle, event| {
            // v0.1.5: flush provider SessionManager resume cursors to disk
            // before the process tears down. Without this, closing the window
            // mid-conversation drops the session (the next launch can't
            // reattach). The standalone binary handles this via SIGINT; the
            // desktop shell needs the same flush on ExitRequested. Best-effort
            // with a 2s budget so a stuck write can't hang exit.
            if let tauri::RunEvent::ExitRequested { .. } = event {
                flush_resume_cursors(app_handle);
            }
        });
}

/// Graceful-shutdown helper: flush the provider `SessionManager` resume
/// cursors to disk before the desktop process exits (v0.1.5).
///
/// Why: if the desktop is closed mid-conversation, the in-memory cursors
/// that let the next launch reattach to in-flight provider sessions are
/// lost without this flush. The standalone server has the same handler
/// keyed off Ctrl-C / SIGINT (see `crates/syncode-ws/src/bin/server.rs`);
/// until v0.1.5 the Tauri shell had no equivalent, so closing the window
/// during an active turn dropped the session.
///
/// Caps the flush at 2s so a stuck write can't hang the exit; worst case
/// (timeout exceeded) the next start replays one extra message — acceptable
/// per the v0.1.5 plan.
///
/// **Runtime-tear-down safety (v0.1.5 post-smoke fix):** when the desktop
/// is force-killed (Ctrl-C in dev, task manager, OS shutdown), Tauri's
/// `ExitRequested` can fire after the Tokio runtime has already begun
/// tearing down. Calling `block_on(...)` at that point panics with
/// `"there is no reactor running"`. The handler detects the no-runtime
/// case via [`tokio::runtime::Handle::try_current`] and skips the flush
/// cleanly — the documented worst-case (one message replayed on next
/// start) is exactly the same as a timeout, so the behavior contract holds.
fn flush_resume_cursors(app: &tauri::AppHandle) {
    let Some(ws_state) = app.try_state::<ws_setup::WsRuntimeState>() else {
        tracing::info!("no WsRuntimeState — skipping cursor flush");
        return;
    };
    let runtime = ws_state.0.lock().expect("WsRuntimeState poisoned");
    let Some(handle) = runtime.as_ref() else {
        tracing::info!("WsRuntimeState has no handle — skipping cursor flush");
        return;
    };
    let Some(reactor) = handle.state.orchestrator.command_reactor() else {
        tracing::info!("no command reactor configured — skipping cursor flush");
        return;
    };
    // Detect "Tokio runtime already torn down" before attempting block_on —
    // force-kill paths (Ctrl-C, OS shutdown) can reach ExitRequested after
    // the runtime is gone, in which case block_on would panic.
    let Ok(rt_handle) = tokio::runtime::Handle::try_current() else {
        tracing::info!("no Tokio runtime at flush time — skipping cursor flush");
        return;
    };
    let mgr = reactor.session_manager();
    let store = syncode_provider::FileResumeCursorStore::new();
    let flush = async {
        let n = mgr.persist_sessions(&store).await;
        tracing::info!(persisted = n, "resume cursors persisted on shutdown");
    };
    // 2s timeout — see doc comment. Best-effort: a timeout logs + continues.
    let _ = rt_handle.block_on(tokio::time::timeout(
        std::time::Duration::from_secs(2),
        flush,
    ));
}

/// Install a panic hook that writes the panic payload + location to a log
/// file in the user's data directory.
///
/// Why: in release mode `windows_subsystem = "windows"` is set (line 5 above),
/// which detaches stdout/stderr — a `setup()` failure (e.g. WS port already
/// taken) panics silently and the app just disappears. The hook preserves the
/// default behavior (printing to stderr when one exists) and additionally
/// writes the panic to `%APPDATA%\syncode\panic.log` (Windows) or
/// `~/.local/share/syncode/panic.log` (Linux/macOS) so users have a breadcrumb
/// to share when filing an issue. The file is overwritten each launch so it
/// always reflects the most recent panic.
fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let full_msg = format!(
            "Syncode desktop panicked at {location}\n  payload: {msg}\n  utc:    {ts}\n",
            ts = chrono::Utc::now().to_rfc3339()
        );
        // Best-effort file write — never let logging itself panic.
        if let Some(dir) = syncode_tauri::paths::log_dir() {
            let _ = std::fs::create_dir_all(&dir);
            let _ = std::fs::write(dir.join("panic.log"), &full_msg);
        }
        default_hook(info);
    }));
}

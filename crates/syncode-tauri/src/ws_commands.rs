//! Tauri IPC commands for the in-process WebSocket server (DSK-1).
//!
//! These commands let the frontend discover the WS endpoint that `.setup()`
//! booted inside the desktop shell, so it can open a JSON-RPC connection to
//! the same backend the standalone `syncode-ws` binary normally exposes. The
//! shared [`syncode_ws::WsState`] is reachable through
//! [`crate::ws_setup::WsRuntimeState`] — IPC commands and WS handlers see the
//! same instance.

use crate::ws_setup::WsRuntimeState;
use serde::{Deserialize, Serialize};

/// WS endpoint info returned by [`get_ws_endpoint`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsEndpoint {
    /// Full `ws://host:port/ws` URL the frontend should connect to.
    pub endpoint: String,
    /// Whether the WS server is booted and accepting connections. `false`
    /// means `.setup()` either hasn't run yet or boot failed (frontend should
    /// back off / show an error).
    pub available: bool,
}

/// Return the in-process WS server's endpoint URL, or `available: false` if
/// the server isn't booted.
///
/// The frontend uses this to know where to point its JSON-RPC transport — it
/// cannot assume a fixed port because `SYNCODE_WS_PORT` may override the
/// default and a port collision would cause `.setup()` to fail. Reading it
/// from managed state is the single source of truth.
#[tauri::command]
pub fn get_ws_endpoint(ws: tauri::State<'_, WsRuntimeState>) -> WsEndpoint {
    match ws.endpoint() {
        Some(endpoint) => WsEndpoint {
            endpoint,
            available: true,
        },
        None => WsEndpoint {
            endpoint: String::new(),
            available: false,
        },
    }
}

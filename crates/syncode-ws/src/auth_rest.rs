//! Stateless REST shims for the 10 `/api/auth/*` endpoints (v0.1.5).
//!
//! The frontend (`frontend/src/wsNativeApi.ts`) calls these via
//! `requestAuthJson`. The JSON-RPC versions live in [`crate::rpc`]; these
//! handlers translate axum requests into RPC-shaped calls and JSON-RPC
//! responses back into REST JSON.
//!
//! # Why these live inside `syncode-ws` (not `syncode-http`)
//!
//! `syncode-http` is a leaf crate (no `syncode-ws` dependency). The auth RPC
//! handlers in [`crate::rpc`] take `&WsState` + `ConnectionId`, so any code
//! that delegates to them must live where it can see `WsState` — i.e. inside
//! `syncode-ws`. Putting the REST shims here also keeps the auth surface
//! (REST + WS) in one place; future refactors can extract a shared `auth`
//! crate if the surface grows.
//!
//! # ConnectionId handling
//!
//! REST is stateless, but several RPC handlers (bootstrap, getSessionState,
//! getWebSocketToken) take a `ConnectionId` because the WS model binds
//! principals to live connections. For REST we mint a **synthetic** conn_id
//! per request (via `state.next_connection_id.fetch_add`). After the handler
//! runs, we clear the synthetic `conn_auth` entry to avoid unbounded growth.
//!
//! # Authentication model for v0.1.5
//!
//! In the default `UnsafeNoAuth` mode, the server treats every request as
//! authenticated (local-first trust boundary). In `RemoteReachable` mode,
//! REST endpoints that create/modify auth state still work (they're public
//! in the RPC authz table too — `auth/bootstrap`, `auth/status`); endpoints
//! that require a bound principal surface `authenticated: false` because
//! HTTP requests carry no Authorization header in the current frontend.
//! Proper bearer-token middleware for REST is deferred (tracked separately).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::Json;
use axum::Router;
use axum::extract::{Json as AxumJson, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};

use crate::rpc::{
    handle_auth_bootstrap, handle_auth_create_pairing_credential, handle_auth_get_session_state,
    handle_auth_get_web_socket_token, handle_auth_list_client_sessions,
    handle_auth_list_pairing_links, handle_auth_revoke_client_session,
    handle_auth_revoke_pairing_link,
};
use crate::{ConnectionId, JsonRpcResponse, WsState};

/// Build the axum router for the 10 auth REST routes. Mount via
/// [`crate::server::build_app`]'s `.merge(auth_rest::auth_rest_router(state))`.
pub fn auth_rest_router(state: Arc<WsState>) -> Router {
    Router::new()
        .route("/api/auth/session", get(auth_session))
        .route("/api/auth/bootstrap", post(auth_bootstrap))
        .route("/api/auth/bootstrap/bearer", post(auth_bootstrap_bearer))
        .route("/api/auth/ws-token", post(auth_ws_token))
        .route("/api/auth/pairing-token", post(auth_pairing_token))
        .route("/api/auth/pairing-links", get(auth_list_pairing_links))
        .route(
            "/api/auth/pairing-links/revoke",
            post(auth_revoke_pairing_link),
        )
        .route("/api/auth/clients", get(auth_list_clients))
        .route("/api/auth/clients/revoke", post(auth_revoke_client))
        .route(
            "/api/auth/clients/revoke-others",
            post(auth_revoke_other_clients),
        )
        .with_state(state)
}

/// Mint a synthetic conn_id for a stateless HTTP request.
///
/// Reuses `state.next_connection_id` so HTTP-originated ids never collide
/// with WS connection ids (both increment the same atomic). HTTP requests
/// never register a `connections` entry, so cleanup of that map is a no-op;
/// the `conn_auth` entry (populated by bootstrap) is cleared explicitly by
/// the caller.
fn next_http_conn_id(state: &WsState) -> ConnectionId {
    state.next_connection_id.fetch_add(1, Ordering::Relaxed)
}

/// Convert a [`JsonRpcResponse`] into an axum response.
///
/// - Success → `200 OK` with the `result` payload as JSON.
/// - Error → mapped HTTP status (401 / 403 / 400 / 500) with `{"error": msg}`.
///
/// The error mapping uses the JSON-RPC error code bands defined in
/// [`crate::auth::auth_error_codes`] and [`crate::error_codes`].
fn rpc_to_response(resp: JsonRpcResponse) -> Response {
    if let Some(err) = resp.error {
        let status = match err.code {
            crate::auth::auth_error_codes::UNAUTHORIZED => StatusCode::UNAUTHORIZED,
            crate::auth::auth_error_codes::FORBIDDEN => StatusCode::FORBIDDEN,
            crate::error_codes::INVALID_PARAMS => StatusCode::BAD_REQUEST,
            crate::error_codes::PARSE_ERROR => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        // Surface the message; auth errors are intentionally user-facing so
        // the frontend can show "Invalid credential". Internal-error paths
        // already redact secrets upstream before constructing the message.
        let body = Json(json!({ "error": err.message }));
        (status, body).into_response()
    } else {
        let body = Json(resp.result.unwrap_or(Value::Null));
        (StatusCode::OK, body).into_response()
    }
}

// ─── Handlers ────────────────────────────────────────────────────────────

/// `GET /api/auth/session` — current request's auth state.
async fn auth_session(State(state): State<Arc<WsState>>) -> Response {
    let conn_id = next_http_conn_id(&state);
    let resp = handle_auth_get_session_state(&state, conn_id, Value::Null).await;
    state.conn_auth.clear(conn_id).await;
    rpc_to_response(resp)
}

/// `POST /api/auth/bootstrap` — exchange a credential for a session token.
///
/// Body: `{ "credential": string }`. In no-auth mode the credential is ignored
/// and the call returns a synthetic `authenticated: true` result.
async fn auth_bootstrap(
    State(state): State<Arc<WsState>>,
    AxumJson(body): AxumJson<Value>,
) -> Response {
    let conn_id = next_http_conn_id(&state);
    let params = json!({ "credential": body.get("credential").cloned().unwrap_or(Value::Null) });
    let resp = handle_auth_bootstrap(&state, conn_id, Value::Null, &params).await;
    state.conn_auth.clear(conn_id).await;
    rpc_to_response(resp)
}

/// `POST /api/auth/bootstrap/bearer` — same as [`auth_bootstrap`] but tags
/// the response with `sessionMethod: "bearer-session-token"` so the frontend
/// can distinguish the bearer-token flow from the cookie/session flow.
async fn auth_bootstrap_bearer(
    State(state): State<Arc<WsState>>,
    AxumJson(body): AxumJson<Value>,
) -> Response {
    let conn_id = next_http_conn_id(&state);
    let params = json!({ "credential": body.get("credential").cloned().unwrap_or(Value::Null) });
    let resp = handle_auth_bootstrap(&state, conn_id, Value::Null, &params).await;
    state.conn_auth.clear(conn_id).await;
    // Augment success result with the bearer session-method marker. Errors
    // pass through unchanged.
    if resp.error.is_none() {
        let mut result = resp.result.clone().unwrap_or(Value::Null);
        if let Some(obj) = result.as_object_mut() {
            obj.insert(
                "sessionMethod".into(),
                Value::String("bearer-session-token".into()),
            );
        }
        let augmented = JsonRpcResponse::success(Value::Null, result);
        return rpc_to_response(augmented);
    }
    rpc_to_response(resp)
}

/// `POST /api/auth/ws-token` — mint a fresh WS bearer token bound to the
/// calling principal. Body (all optional): `{ "ttlMinutes": number }`.
async fn auth_ws_token(
    State(state): State<Arc<WsState>>,
    AxumJson(body): AxumJson<Value>,
) -> Response {
    let conn_id = next_http_conn_id(&state);
    let resp = handle_auth_get_web_socket_token(&state, conn_id, Value::Null, &body).await;
    state.conn_auth.clear(conn_id).await;
    rpc_to_response(resp)
}

/// `POST /api/auth/pairing-token` — mint a short-TTL pairing credential.
/// Body (all optional): `{ "role": "owner"|"client", "ttlMinutes": number }`.
async fn auth_pairing_token(
    State(state): State<Arc<WsState>>,
    AxumJson(body): AxumJson<Value>,
) -> Response {
    // Empty body is allowed (handler applies defaults); pass an empty object
    // in that case so the handler's `params.get(...)` calls all return None.
    let params = if body.is_null() { json!({}) } else { body };
    let resp = handle_auth_create_pairing_credential(&state, Value::Null, &params).await;
    rpc_to_response(resp)
}

/// `GET /api/auth/pairing-links` — enumerate live pairing links.
async fn auth_list_pairing_links(State(state): State<Arc<WsState>>) -> Response {
    let resp = handle_auth_list_pairing_links(&state, Value::Null).await;
    // The RPC returns `{ links: [...] }`; the frontend treats the response
    // as a bare array (`ReadonlyArray<AuthPairingLink>`), so unwrap it.
    let unwrapped = match resp.error {
        None => JsonRpcResponse::success(
            Value::Null,
            resp.result
                .and_then(|v| v.get("links").cloned())
                .unwrap_or(Value::Array(vec![])),
        ),
        Some(_) => resp,
    };
    rpc_to_response(unwrapped)
}

/// `POST /api/auth/pairing-links/revoke` — invalidate a pairing link by id.
/// Body: `{ "id": string }`.
async fn auth_revoke_pairing_link(
    State(state): State<Arc<WsState>>,
    AxumJson(body): AxumJson<Value>,
) -> Response {
    let resp = handle_auth_revoke_pairing_link(&state, Value::Null, &body).await;
    rpc_to_response(resp)
}

/// `GET /api/auth/clients` — enumerate authenticated WS sessions.
async fn auth_list_clients(State(state): State<Arc<WsState>>) -> Response {
    let resp = handle_auth_list_client_sessions(&state, Value::Null).await;
    // Same unwrap as auth_list_pairing_links — frontend wants a bare array.
    let unwrapped = match resp.error {
        None => JsonRpcResponse::success(
            Value::Null,
            resp.result
                .and_then(|v| v.get("sessions").cloned())
                .unwrap_or(Value::Array(vec![])),
        ),
        Some(_) => resp,
    };
    rpc_to_response(unwrapped)
}

/// `POST /api/auth/clients/revoke` — invalidate one session by id.
/// Body: `{ "connectionId": number }`.
async fn auth_revoke_client(
    State(state): State<Arc<WsState>>,
    AxumJson(body): AxumJson<Value>,
) -> Response {
    let resp = handle_auth_revoke_client_session(&state, Value::Null, &body).await;
    rpc_to_response(resp)
}

/// `POST /api/auth/clients/revoke-others` — revoke all sessions except the
/// calling one. For REST, "calling one" is ambiguous (no bound principal);
/// this v0.1.5 implementation revokes ALL sessions. The frontend contract
/// returns `{ revokedCount: number }`.
async fn auth_revoke_other_clients(State(state): State<Arc<WsState>>) -> Response {
    let sessions = state.conn_auth.list_sessions().await;
    let count = sessions.len();
    for (conn_id, _) in sessions {
        state.conn_auth.clear(conn_id).await;
    }
    Json(json!({ "revokedCount": count })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use syncode_auth::WsAuthConfig;
    use tower::util::ServiceExt;

    fn test_state() -> Arc<WsState> {
        Arc::new(WsState::new_in_memory(16))
    }

    fn authed_state() -> Arc<WsState> {
        let mut state = WsState::new_in_memory(16);
        state.auth_config = WsAuthConfig::default();
        Arc::new(state)
    }

    async fn body_json(body: Body) -> Value {
        let bytes = body
            .collect()
            .await
            .expect("body collect")
            .to_bytes()
            .to_vec();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    #[tokio::test]
    async fn session_returns_authenticated_field_in_no_auth_mode() {
        // Default mode is UnsafeNoAuth → getSessionState returns a synthesized
        // Owner principal because the no-auth branch sets one. The REST shim
        // surfaces whatever the RPC handler returns.
        let state = test_state();
        let app = auth_rest_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp.into_body()).await;
        assert!(
            body.get("authenticated").is_some(),
            "session response should include `authenticated` field: {body}"
        );
    }

    #[tokio::test]
    async fn list_pairing_links_returns_array_envelope() {
        let state = test_state();
        let app = auth_rest_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/pairing-links")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp.into_body()).await;
        // Frontend expects a bare array, not `{ links: [...] }`.
        assert!(
            body.is_array(),
            "pairing-links response should be a bare array: {body}"
        );
    }

    #[tokio::test]
    async fn list_clients_returns_array_envelope() {
        let state = test_state();
        let app = auth_rest_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/clients")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp.into_body()).await;
        assert!(
            body.is_array(),
            "clients response should be a bare array: {body}"
        );
    }

    #[tokio::test]
    async fn pairing_token_creates_link() {
        let state = test_state();
        let app = auth_rest_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/pairing-token")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp.into_body()).await;
        assert!(
            body.get("credential").is_some(),
            "pairing-token should return credential: {body}"
        );
    }

    #[tokio::test]
    async fn revoke_others_returns_count() {
        let state = authed_state();
        let app = auth_rest_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/clients/revoke-others")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp.into_body()).await;
        assert!(
            body.get("revokedCount").is_some(),
            "revoke-others should return revokedCount: {body}"
        );
    }
}

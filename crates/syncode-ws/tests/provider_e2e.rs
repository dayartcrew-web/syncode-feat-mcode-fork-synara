//! Real-provider end-to-end test — boots a WS server armed with a REAL
//! provider adapter (claude by default) and drives the full chat flow
//! end-to-end through JSON-RPC over WebSocket.
//!
//! Gating: `SYNCODE_PROVIDER_E2E=1`. Off by default (requires the provider
//! CLI to be installed + authenticated). Run with:
//!
//! ```sh
//! SYNCODE_PROVIDER_E2E=1 \
//!   cargo test -p syncode-ws --test provider_e2e -- --nocapture --test-threads=1
//! ```
//!
//! The server-side build mirrors `crates/syncode-ws/src/bin/server.rs`'s
//! `build_orchestrator`: a `SqliteEventRepository` (temp file), a
//! `ProviderCommandReactor`, a shared read-model handle, and a real provider
//! adapter from `syncode_provider::registry::create_by_id`. The orchestrator
//! is then wrapped by `WsState::new`, which attaches the push bus so
//! provider-stream events (tokens, completion) fan out to subscribed clients.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use syncode_core::ports::EventRepository;
use syncode_provider::registry::create_by_id;
use syncode_provider::{ProviderConfig, SessionManager};
use tokio_tungstenite::tungstenite::Message;

/// Gate env var. Test is a no-op skip unless set to `1`.
const E2E_VAR: &str = "SYNCODE_PROVIDER_E2E";

fn e2e_enabled() -> bool {
    std::env::var(E2E_VAR).ok().as_deref() == Some("1")
}

/// Default provider id (overridable via `SYNCODE_DEFAULT_PROVIDER`, mirroring
/// the production server binary).
fn default_provider() -> String {
    std::env::var("SYNCODE_DEFAULT_PROVIDER").unwrap_or_else(|_| "claude".to_string())
}

/// Default model (overridable via `SYNCODE_DEFAULT_MODEL`).
fn default_model() -> String {
    std::env::var("SYNCODE_DEFAULT_MODEL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "claude-sonnet-4".to_string())
}

/// Build the orchestrator with a real provider adapter — mirrors
/// `bin/server.rs::build_orchestrator` minus session rehydration (not needed
/// for a fresh test run).
async fn build_orchestrator_with_real_provider() -> syncode_orchestration::Orchestrator {
    use std::path::PathBuf;
    use syncode_persistence::{adapters::SqliteEventRepository, init_database};

    let db_path = PathBuf::from(format!("/tmp/provider-e2e-{}.db", std::process::id()));
    // Clean any stale DB from a previous run.
    let _ = std::fs::remove_file(&db_path);

    let pool = init_database(&db_path)
        .await
        .expect("init_database for provider e2e");
    let repo: Arc<dyn EventRepository> = Arc::new(SqliteEventRepository::new(pool));

    let read_model: Arc<tokio::sync::RwLock<syncode_orchestration::ReadModelStore>> = Arc::new(
        tokio::sync::RwLock::new(syncode_orchestration::ReadModelStore::new()),
    );

    let session_manager = SessionManager::new();
    let reactor = Arc::new(
        syncode_orchestration::ProviderCommandReactor::new(session_manager)
            .with_read_model(Arc::clone(&read_model)),
    );

    let provider_id = default_provider();
    let adapter = create_by_id(&provider_id)
        .unwrap_or_else(|| panic!("provider `{provider_id}` not available — install the CLI"));

    // Spawn the adapter (launches claude CLI / codex app-server / etc.).
    {
        let mut guard = adapter.write().await;
        let config = ProviderConfig {
            provider_id: provider_id.clone(),
            model: default_model(),
            api_key: None,
            base_url: None,
            max_tokens: Some(4096),
            extra: HashMap::new(),
        };
        guard
            .spawn(config)
            .await
            .unwrap_or_else(|e| panic!("failed to spawn provider `{provider_id}`: {e}"));
    }

    syncode_orchestration::Orchestrator::with_reactor_adapter_and_read_model(
        repo, reactor, adapter, read_model,
    )
}

/// Boot a WS server with the real-provider orchestrator. Returns the WS URL
/// and the accept-loop task handle.
async fn boot_server() -> (String, tokio::task::JoinHandle<()>) {
    let orchestrator = build_orchestrator_with_real_provider().await;
    let state = Arc::new(syncode_ws::WsState::new(1024, orchestrator));
    let app = syncode_ws::server::build_app(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let url = format!("ws://127.0.0.1:{}/ws", port);

    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    // Let the accept loop arm before we connect.
    tokio::time::sleep(Duration::from_millis(150)).await;
    (url, handle)
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(url: &str) -> WsStream {
    let (stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("connect ws");
    stream
}

/// Send a JSON-RPC request and await its response (matched by `id`). Push
/// notifications (no `id`) are skipped.
async fn rpc_call(stream: &mut WsStream, method: &str, params: Value) -> Value {
    let id = json!(uuid::Uuid::new_v4().to_string());
    let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    stream
        .send(Message::Text(request.to_string().into()))
        .await
        .expect("send rpc");

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    if v.get("id").is_some() {
                        return v;
                    }
                    // Push notification — skip.
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("ws error mid-rpc: {e}"),
                None => panic!("stream closed without response for `{method}`"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout waiting for response to `{method}`"))
}

/// Pull text content out of a push event envelope. Tolerant of wire-format
/// variance: looks for `params.data.content`, falls back to walking any string
/// under `params.data` that looks like text.
fn extract_push_text(msg: &Value) -> Option<String> {
    // Common shape: { params: { type: "turn.token_received", data: { content } } }
    if let Some(content) = msg.pointer("/params/data/content").and_then(Value::as_str) {
        return Some(content.to_string());
    }
    if let Some(text) = msg.pointer("/params/data/text").and_then(Value::as_str) {
        return Some(text.to_string());
    }
    None
}

/// Classification helper — what kind of push event is this?
fn push_event_type(msg: &Value) -> Option<String> {
    msg.pointer("/params/type")
        .and_then(Value::as_str)
        .or_else(|| msg.pointer("/params/eventType").and_then(Value::as_str))
        .map(str::to_string)
}

#[tokio::test]
async fn real_provider_chat_e2e() {
    if !e2e_enabled() {
        eprintln!("[skip] set {E2E_VAR}=1 to run real provider e2e");
        return;
    }

    let provider_id = default_provider();
    let model = default_model();
    eprintln!("[provider-e2e] booting server with provider `{provider_id}` model `{model}`");

    let (url, handle) = boot_server().await;
    let mut stream = connect(&url).await;

    // 1. provider/list-models — provider is spawned, models must be non-empty.
    let models_resp = rpc_call(&mut stream, "provider/list-models", json!({})).await;
    assert!(
        models_resp.get("error").is_none(),
        "provider/list-models error: {:?}",
        models_resp["error"]
    );
    let models = models_resp["result"]["models"]
        .as_array()
        .expect("models array");
    assert!(!models.is_empty(), "models list should be non-empty");
    eprintln!("[provider-e2e] {} models advertised", models.len());

    // 2. provider/get-composer-capabilities — claude supports skill discovery.
    let caps_resp = rpc_call(
        &mut stream,
        "provider/get-composer-capabilities",
        json!({ "providerId": provider_id, "model": model }),
    )
    .await;
    assert!(
        caps_resp.get("error").is_none(),
        "capabilities error: {:?}",
        caps_resp["error"]
    );
    let supports_skill = caps_resp["result"]["supportsSkillDiscovery"].as_bool();
    eprintln!("[provider-e2e] supportsSkillDiscovery = {supports_skill:?} (claude expected true)");

    // 3. provider/list-options — reasoningEffort should be present for claude.
    let opts_resp = rpc_call(
        &mut stream,
        "provider/list-options",
        json!({ "providerId": provider_id, "model": model }),
    )
    .await;
    assert!(
        opts_resp.get("error").is_none(),
        "list-options error: {:?}",
        opts_resp["error"]
    );
    let has_reasoning = opts_resp["result"]["reasoningEffort"]
        .get("options")
        .is_some();
    eprintln!(
        "[provider-e2e] reasoningEffort options present = {has_reasoning} (claude expected true)"
    );

    // 4. project/create — get a project id + working directory.
    let project_name = format!("provider-e2e-{}", std::process::id());
    let project_resp = rpc_call(
        &mut stream,
        "project/create",
        json!({ "name": project_name }),
    )
    .await;
    assert!(
        project_resp.get("error").is_none(),
        "project/create error: {:?}",
        project_resp["error"]
    );
    let project_id = project_resp["result"]["project"]["id"]
        .as_str()
        .or_else(|| project_resp["result"]["id"].as_str())
        .expect("project id")
        .to_string();
    eprintln!("[provider-e2e] created project {project_id}");

    // 5. thread/create — bind a thread to the provider + model.
    let thread_resp = rpc_call(
        &mut stream,
        "thread/create",
        json!({
            "projectId": project_id,
            "providerId": provider_id,
            "model": model,
        }),
    )
    .await;
    assert!(
        thread_resp.get("error").is_none(),
        "thread/create error: {:?}",
        thread_resp["error"]
    );
    let thread_id = thread_resp["result"]["thread"]["id"]
        .as_str()
        .or_else(|| thread_resp["result"]["id"].as_str())
        .expect("thread id")
        .to_string();
    eprintln!("[provider-e2e] created thread {thread_id}");

    // 6. push/subscribe — opt in to ALL channels so token + completion events arrive.
    let sub_resp = rpc_call(&mut stream, "push/subscribe", json!({ "channels": ["*"] })).await;
    assert!(
        sub_resp.get("error").is_none(),
        "push/subscribe error: {:?}",
        sub_resp["error"]
    );

    // 7. turn/start — minimal prompt, instructs the model to be terse.
    let prompt = "Say 'hello world' and nothing else.";
    let turn_resp = rpc_call(
        &mut stream,
        "turn/start",
        json!({
            "threadId": thread_id,
            "projectId": project_id,
            "prompt": prompt,
        }),
    )
    .await;
    assert!(
        turn_resp.get("error").is_none(),
        "turn/start error: {:?}",
        turn_resp["error"]
    );
    let turn_id = turn_resp["result"]["turn"]["id"]
        .as_str()
        .or_else(|| turn_resp["result"]["id"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("turn-for-{thread_id}"));
    eprintln!("[provider-e2e] started turn {turn_id}, awaiting streamed tokens...");

    // 8. Collect push events until a completion signal (or 30s timeout).
    let mut collected_text = String::new();
    let mut saw_completion = false;
    let collect_deadline = Duration::from_secs(30);

    let collect_result = tokio::time::timeout(collect_deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    // Skip RPC responses (have `id`) — we want push notifications.
                    if v.get("id").is_some() {
                        continue;
                    }

                    if let Some(t) = extract_push_text(&v) {
                        collected_text.push_str(&t);
                    }

                    if let Some(etype) = push_event_type(&v) {
                        let lower = etype.to_ascii_lowercase();
                        if lower.contains("completed")
                            || lower.contains("finished")
                            || lower.contains("done")
                            || lower.contains("turn.completed")
                            || lower.contains("turn_completed")
                        {
                            saw_completion = true;
                            eprintln!(
                                "[provider-e2e] completion signal `{etype}` after {} chars",
                                collected_text.len()
                            );
                            return;
                        }
                    }
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    eprintln!("[provider-e2e] ws error while collecting: {e}");
                    return;
                }
                None => {
                    eprintln!("[provider-e2e] stream closed mid-collection");
                    return;
                }
            }
        }
    })
    .await;

    if collect_result.is_err() {
        eprintln!(
            "[provider-e2e] collection timed out after {collect_deadline:?} (saw_completion={saw_completion}, collected={} chars)",
            collected_text.len()
        );
    }

    // 9. Assertions.
    assert!(
        !collected_text.trim().is_empty(),
        "expected non-empty streamed text, got: `{collected_text}`"
    );
    eprintln!(
        "[provider-e2e] streamed response ({} chars): {:?}",
        collected_text.len(),
        collected_text.chars().take(200).collect::<String>()
    );

    // 10. turn/get — final status should be completed.
    let get_resp = rpc_call(
        &mut stream,
        "turn/get",
        json!({ "threadId": thread_id, "turnId": turn_id }),
    )
    .await;
    let status = get_resp["result"]["turn"]["status"]
        .as_str()
        .or_else(|| get_resp["result"]["status"].as_str())
        .unwrap_or("<missing>");
    eprintln!("[provider-e2e] turn status = `{status}`");
    // Tolerate both `completed` and any terminal state naming.
    let lower_status = status.to_ascii_lowercase();
    assert!(
        lower_status.contains("complete")
            || lower_status.contains("finish")
            || lower_status == "done",
        "expected terminal turn status, got `{status}`"
    );

    let _ = stream.close(None).await;
    handle.abort();

    // Clean up the temp DB.
    let _ = std::fs::remove_file(format!("/tmp/provider-e2e-{}.db", std::process::id()));
    eprintln!("[provider-e2e] PASS — real provider chat flow end-to-end");
}

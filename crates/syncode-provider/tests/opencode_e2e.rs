//! Real-binary E2E for the OpenCode CLI HTTP/SSE provider.
//!
//! Spawns the actual `opencode` CLI (`opencode serve`) via the standard
//! [`syncode_provider::adapters::opencode::OpenCodeAdapter`]: spawn →
//! `start_session` → `send_request`, asserting that streamed
//! [`ProviderEvent::Token`] chunks and a terminal [`ProviderEvent::Completed`]
//! arrive over the server's SSE channel. This validates real-binary OpenCode
//! interop — not just the SSE→event decoders, which are covered by the in-process
//! fake-SSE tests in `opencode_server` and the adapter glue tests in
//! `adapters::opencode`.
//!
//! # Gating
//!
//! Off by default so `cargo test` is a no-op in environments without the CLI
//! (and without model-provider credentials). Runs only when BOTH hold:
//! - env `SYNICODE_OPENCODE_E2E=1` is set, AND
//! - the `opencode` binary is reachable on `PATH`.
//!
//! Run with:
//! ```text
//! SYNICODE_OPENCODE_E2E=1 cargo test -p syncode-provider --test opencode_e2e -- --nocapture --test-threads=1
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use syncode_core::EntityId;
use syncode_provider::adapters::opencode::{OpenCodeAdapter, OpenCodeConfig};
use syncode_provider::{
    PROVIDER_OPENCODE, ProviderAdapter, ProviderConfig, ProviderEvent, ProviderRequest,
    SessionContext,
};
use tokio_stream::StreamExt;

/// Gate: only run when the operator opted in AND the `opencode` CLI is on PATH.
///
/// `Command::status` succeeds (returns `Ok`) whenever the process *spawns*,
/// regardless of its exit code — so an unknown `--version` flag still confirms
/// the binary exists, while a missing binary yields `Err(NotFound)` → skip.
fn e2e_enabled() -> bool {
    if std::env::var("SYNICODE_OPENCODE_E2E").as_deref() != Ok("1") {
        return false;
    }
    let resolved = syncode_provider::resolve_binary("opencode");
    std::process::Command::new(&resolved)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

#[tokio::test]
async fn opencode_real_binary_one_turn_completes() {
    if !e2e_enabled() {
        eprintln!(
            "[skip] opencode E2E: set SYNICODE_OPENCODE_E2E=1 and install the `opencode` CLI to run"
        );
        return;
    }

    let cwd = std::env::temp_dir().to_string_lossy().to_string();
    let mut provider = OpenCodeAdapter::with_opencode_config(OpenCodeConfig::default());
    let mut extra = HashMap::new();
    extra.insert("cwd".to_string(), serde_json::json!(cwd));
    provider
        .spawn(ProviderConfig {
            provider_id: PROVIDER_OPENCODE.to_string(),
            model: String::new(), // let the server pick its configured default model
            api_key: None,
            base_url: None,
            max_tokens: None,
            extra,
        })
        .await
        .expect("opencode spawn");

    let session_id = provider
        .start_session(SessionContext {
            thread_id: EntityId::new(),
            turn_id: EntityId::new(),
            working_dir: cwd.clone(),
            system_prompt: Some("You are a terse responder.".to_string()),
            user_input: "Reply with exactly the word: PONG".to_string(),
            context_files: vec![],
        })
        .await
        .expect("opencode start_session");

    // Subscribe BEFORE send_request so streamed events are buffered for us
    // (broadcast only delivers to receivers that exist at send time).
    let stream = provider.event_stream(&session_id).expect("event_stream");
    tokio::pin!(stream);

    let req = ProviderRequest::new(
        "chat",
        Some(serde_json::json!({
            "input": "Reply with exactly the word: PONG",
            "session_id": session_id,
        })),
    );
    provider
        .send_request(req)
        .await
        .expect("opencode turn completed");

    // Drain the event stream until Completed (or a generous deadline). A real
    // OpenCode turn should emit >=1 Token then a Completed; an Error event fails
    // the test loudly so CLI/server quirks surface immediately.
    let mut tokens = 0u32;
    let mut completed = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(ev))) => match ev {
                ProviderEvent::Token { content, .. } => {
                    tokens += 1;
                    eprintln!("[opencode-e2e] token: {content}");
                }
                ProviderEvent::Completed { output, usage, .. } => {
                    eprintln!("[opencode-e2e] completed: usage={usage:?} output={output}");
                    completed = true;
                    break;
                }
                ProviderEvent::Error { message, code, .. } => {
                    panic!("opencode error event (code={code:?}): {message}");
                }
                other => eprintln!("[opencode-e2e] event: {other:?}"),
            },
            _ => break, // stream closed or timed out
        }
    }

    assert!(
        completed,
        "expected a terminal Completed event from opencode (tokens observed: {tokens})"
    );

    // Best-effort teardown — never fail the test on shutdown.
    let _ = provider.stop_session(&session_id).await;
    let _ = provider.shutdown().await;
}

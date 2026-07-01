//! Real-binary E2E for the Kilo CLI HTTP/SSE provider.
//!
//! Kilo speaks the same OpenCode-compatible local-server protocol as OpenCode
//! (`kilo serve` ≡ `opencode serve`). This test drives the real `kilo` binary
//! via [`syncode_provider::adapters::kilo::KiloAdapter`] exactly like the
//! OpenCode E2E (`tests/opencode_e2e.rs`): spawn → `start_session` →
//! `send_request`, asserting streamed [`ProviderEvent::Token`] chunks and a
//! terminal [`ProviderEvent::Completed`] arrive over the SSE channel. The
//! protocol + decoding paths are shared with OpenCode (see
//! `opencode_server`); this test isolates Kilo-specific spawn/identity.
//!
//! # Gating
//!
//! Off by default so `cargo test` is a no-op in environments without the CLI.
//! Runs only when BOTH hold:
//! - env `SYNICODE_KILO_E2E=1` is set, AND
//! - the `kilo` binary is reachable on `PATH`.
//!
//! Run with:
//! ```text
//! SYNICODE_KILO_E2E=1 cargo test -p syncode-provider --test kilo_e2e -- --nocapture --test-threads=1
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use syncode_core::EntityId;
use syncode_provider::adapters::kilo::{KiloAdapter, KiloConfig};
use syncode_provider::{
    PROVIDER_KILO, ProviderAdapter, ProviderConfig, ProviderEvent, ProviderRequest, SessionContext,
};
use tokio_stream::StreamExt;

/// Gate: only run when the operator opted in AND the `kilo` CLI is on PATH.
fn e2e_enabled() -> bool {
    if std::env::var("SYNICODE_KILO_E2E").as_deref() != Ok("1") {
        return false;
    }
    std::process::Command::new("kilo")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

#[tokio::test]
async fn kilo_real_binary_one_turn_completes() {
    if !e2e_enabled() {
        eprintln!("[skip] kilo E2E: set SYNICODE_KILO_E2E=1 and install the `kilo` CLI to run");
        return;
    }

    let cwd = std::env::temp_dir().to_string_lossy().to_string();
    let mut provider = KiloAdapter::with_kilo_config(KiloConfig::default());
    let mut extra = HashMap::new();
    extra.insert("cwd".to_string(), serde_json::json!(cwd));
    provider
        .spawn(ProviderConfig {
            provider_id: PROVIDER_KILO.to_string(),
            model: String::new(), // let the server pick its configured default model
            api_key: None,
            base_url: None,
            max_tokens: None,
            extra,
        })
        .await
        .expect("kilo spawn");

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
        .expect("kilo start_session");

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
        .expect("kilo turn completed");

    // Drain the event stream until Completed (or a generous deadline).
    let mut tokens = 0u32;
    let mut completed = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(ev))) => match ev {
                ProviderEvent::Token { content, .. } => {
                    tokens += 1;
                    eprintln!("[kilo-e2e] token: {content}");
                }
                ProviderEvent::Completed { output, usage, .. } => {
                    eprintln!("[kilo-e2e] completed: usage={usage:?} output={output}");
                    completed = true;
                    break;
                }
                ProviderEvent::Error { message, code, .. } => {
                    panic!("kilo error event (code={code:?}): {message}");
                }
                other => eprintln!("[kilo-e2e] event: {other:?}"),
            },
            _ => break, // stream closed or timed out
        }
    }

    assert!(
        completed,
        "expected a terminal Completed event from kilo (tokens observed: {tokens})"
    );

    // Best-effort teardown — never fail the test on shutdown.
    let _ = provider.stop_session(&session_id).await;
    let _ = provider.shutdown().await;
}

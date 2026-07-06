//! Real-binary E2E for the Pi CLI RPC-mode provider.
//!
//! Pi (@earendil-works/pi-coding-agent) is driven in headless RPC mode
//! (`pi --mode rpc`): JSON commands on stdin, streamed events + responses on
//! stdout. This test drives the real `pi` binary via
//! [`syncode_provider::adapters::pi::PiAdapter`] exactly like the other CLI
//! providers: spawn → `start_session` → `send_request`, asserting streamed
//! [`ProviderEvent::Token`] chunks and a terminal [`ProviderEvent::Completed`].
//! The protocol + event-mapping paths are covered by `pi_rpc` unit tests
//! (in-process duplex fake); this test isolates real-binary interop.
//!
//! # Gating
//!
//! Off by default so `cargo test` is a no-op in environments without the CLI.
//! Runs only when BOTH hold:
//! - env `SYNICODE_PI_E2E=1` is set, AND
//! - the `pi` binary is reachable on `PATH`.
//!
//! Run with:
//! ```text
//! SYNICODE_PI_E2E=1 cargo test -p syncode-provider --test pi_e2e -- --nocapture --test-threads=1
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use syncode_core::EntityId;
use syncode_provider::adapters::pi::{PiAdapter, PiConfig};
use syncode_provider::{
    PROVIDER_PI, ProviderAdapter, ProviderConfig, ProviderEvent, ProviderRequest, SessionContext,
};
use tokio_stream::StreamExt;

/// Gate: only run when the operator opted in AND the `pi` CLI is on PATH.
fn e2e_enabled() -> bool {
    if std::env::var("SYNICODE_PI_E2E").as_deref() != Ok("1") {
        return false;
    }
    let resolved = syncode_provider::resolve_binary("pi");
    std::process::Command::new(&resolved)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

#[tokio::test]
async fn pi_real_binary_one_turn_completes() {
    if !e2e_enabled() {
        eprintln!("[skip] pi E2E: set SYNICODE_PI_E2E=1 and install the `pi` CLI to run");
        return;
    }

    let cwd = std::env::temp_dir().to_string_lossy().to_string();
    let mut provider = PiAdapter::with_pi_config(PiConfig::default());
    let mut extra = HashMap::new();
    extra.insert("cwd".to_string(), serde_json::json!(cwd));
    provider
        .spawn(ProviderConfig {
            provider_id: PROVIDER_PI.to_string(),
            model: String::new(), // let pi use its settings.json default model
            api_key: None,
            base_url: None,
            max_tokens: None,
            extra,
        })
        .await
        .expect("pi spawn");

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
        .expect("pi start_session");

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
    provider.send_request(req).await.expect("pi turn completed");

    // Drain the event stream until Completed (or a generous deadline).
    let mut tokens = 0u32;
    let mut completed = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(ev))) => match ev {
                ProviderEvent::Token { content, .. } => {
                    tokens += 1;
                    eprintln!("[pi-e2e] token: {content}");
                }
                ProviderEvent::Completed { output, usage, .. } => {
                    eprintln!("[pi-e2e] completed: usage={usage:?} output={output}");
                    completed = true;
                    break;
                }
                ProviderEvent::Error { message, code, .. } => {
                    panic!("pi error event (code={code:?}): {message}");
                }
                other => eprintln!("[pi-e2e] event: {other:?}"),
            },
            _ => break, // stream closed or timed out
        }
    }

    assert!(
        completed,
        "expected a terminal Completed event from pi (tokens observed: {tokens})"
    );

    // Best-effort teardown — never fail the test on shutdown.
    let _ = provider.stop_session(&session_id).await;
    let _ = provider.shutdown().await;
}

//! Shared orchestrator construction (DSK-2 / v0.1.5).
//!
//! Both the standalone `syncode-ws` binary and the Tauri desktop shell need to
//! build an [`Orchestrator`] wired with a [`ProviderCommandReactor`], a spawned
//! default adapter, a per-thread adapter registry, a rehydrated
//! [`SessionManager`], and a replayed read model. Before this module the two
//! builds had divergent `build_orchestrator` functions — the standalone had the
//! full 200-line pipeline, the Tauri shell had a 28-line stub that skipped
//! `adapter.spawn`, the adapter registry, and `rehydrate_sessions`, which is
//! why providers did not work in Tauri.
//!
//! [`build_orchestrator`] is now the single source of truth. Pass `Some(pool)`
//! for SQLite-backed persistence (loads `textGenerationModelSelection` from
//! `server_settings`, attaches the pool to the workflow-state provider so
//! thread → workflow links survive a restart). Pass `None` for in-memory mode
//! (env-only provider selection, no workflow-state sidecar).
//!
//! # Provider id precedence
//!
//! 1. `server_settings.textGenerationModelSelection.provider` — the Settings
//!    panel's picker (loaded fresh from `pool`, when provided).
//! 2. `SYNCODE_DEFAULT_PROVIDER` env var — operator override.
//! 3. `DEFAULT_PROVIDER` (`"opencode"`) — backwards-compatible default.
//!
//! Per-provider extras (`binaryPath`, `serverUrl`, `launchArgs`, …) are pulled
//! from the same persisted settings when available, so adapters launch with the
//! user-configured CLI path and credentials.
//!
//! When the resolved provider's CLI is unavailable the orchestrator falls back
//! to [`Orchestrator::new`] — turns are recorded but no AI response is
//! generated, and the server still boots (graceful degradation, logged at
//! `WARN`).

use std::collections::HashMap;
use std::sync::Arc;

use syncode_core::ports::EventRepository;
use syncode_orchestration::Orchestrator;
use syncode_persistence::SqlitePool;
use syncode_provider::{FileResumeCursorStore, SessionManager};

use crate::settings::{
    extract_provider_extras, resolve_default_model, resolve_default_provider,
};

/// Build the orchestrator with a [`ProviderCommandReactor`] + a provider
/// adapter, so turns actually invoke a provider and AI responses stream back.
///
/// Pass `Some(pool)` on the SQLite-success path so settings load from disk and
/// the workflow-state preamble has a store to read thread → workflow links
/// from. Pass `None` for the in-memory / SQLite-failure fallback — env-only
/// provider resolution, no preamble sidecar.
///
/// See the module docs for provider-id precedence and the full pipeline.
pub async fn build_orchestrator(
    repo: Arc<dyn EventRepository>,
    settings_pool: Option<&SqlitePool>,
) -> Orchestrator {
    let settings = match settings_pool {
        Some(pool) => match syncode_persistence::settings_store::load_settings(pool).await {
            Ok(Some(value)) => value,
            Ok(None) => serde_json::Value::Null,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to load persisted ServerSettings — falling back to env-only provider selection"
                );
                serde_json::Value::Null
            }
        },
        None => serde_json::Value::Null,
    };

    let env_provider = std::env::var("SYNCODE_DEFAULT_PROVIDER").ok();
    let env_model = std::env::var("SYNCODE_DEFAULT_MODEL").ok();
    let default_provider = resolve_default_provider(&settings, env_provider.as_deref());
    let default_model = resolve_default_model(&settings, env_model.as_deref());
    let provider_extras = extract_provider_extras(&default_provider, &settings);

    // PR-1-2: construct the shared read model handle first so the reactor and
    // the orchestrator can both see it. The reactor uses it to resolve a
    // thread's project root path as the session working directory; the
    // orchestrator's projector writes to it as commands are handled. Sharing
    // the Arc (not cloning the store) keeps them in lock-step.
    let read_model: Arc<tokio::sync::RwLock<syncode_orchestration::ReadModelStore>> = Arc::new(
        tokio::sync::RwLock::new(syncode_orchestration::ReadModelStore::new()),
    );

    let session_manager = SessionManager::new();
    // C2: attach a workflow-state provider so freshly started chat sessions
    // carry syncode's workflow context (phase, current task, constraints) as
    // a leading block in their system prompt. Backed by the
    // `thread_workflow_links` sidecar — None when no SQLite pool is attached
    // (in-memory mode) → identical to prior behavior.
    let workflow_state: Arc<dyn syncode_orchestration::workflow_state::WorkflowStateProvider> =
        Arc::new(crate::thread_workflow_bridge::ThreadWorkflowPreamble::new(
            settings_pool.cloned(),
        ));
    let reactor = Arc::new(
        syncode_orchestration::ProviderCommandReactor::new(session_manager)
            .with_read_model(Arc::clone(&read_model))
            .with_workflow_state(workflow_state),
    );

    let orchestrator = match syncode_provider::registry::create_by_id(&default_provider) {
        Some(adapter) => {
            {
                let mut guard = adapter.write().await;
                let config = syncode_provider::ProviderConfig {
                    provider_id: default_provider.clone(),
                    model: default_model.clone(),
                    api_key: None,
                    base_url: None,
                    max_tokens: Some(4096),
                    extra: provider_extras,
                };
                match guard.spawn(config).await {
                    Ok(()) => {
                        tracing::info!(provider = %default_provider, "provider adapter spawned")
                    }
                    Err(e) => {
                        tracing::error!(provider = %default_provider, error = %e, "failed to spawn provider adapter — turns will fail")
                    }
                }
            }

            tracing::info!(
                provider = %default_provider,
                model = %default_model,
                "chat pipeline armed: turns will dispatch to the provider"
            );

            // P0-4: rehydrate persisted sessions before the orchestrator takes
            // ownership of the adapter — pass a clone of the SharedAdapter so
            // the rehydrate path can call `resume_session` without an extra
            // lock dance.
            let store = FileResumeCursorStore::new();
            let rehydrated = reactor
                .session_manager()
                .rehydrate_sessions(&store, &adapter)
                .await;
            let reattached = rehydrated
                .iter()
                .filter(|r| matches!(r.outcome, syncode_provider::RehydrationOutcome::Reattached))
                .count();
            let failed = rehydrated.len() - reattached;
            tracing::info!(
                rehydrated = rehydrated.len(),
                reattached,
                failed,
                "session resume cursors rehydrated"
            );

            let mut orchestrator = Orchestrator::with_reactor_adapter_and_read_model(
                repo, reactor, adapter, read_model,
            );

            // Per-thread provider dispatch: spawn + register adapters for
            // every AVAILABLE provider (not just the default), so threads
            // created with a different provider dispatch to the correct
            // adapter instead of the global default.
            let mut registry_entries: Vec<(String, syncode_provider::registry::SharedAdapter)> =
                Vec::new();
            registry_entries.push((
                default_provider.clone(),
                orchestrator.adapter().cloned().unwrap(),
            ));
            for &pid in syncode_provider::ALL_PROVIDERS {
                if pid == default_provider.as_str()
                    || pid == syncode_provider::PROVIDER_ANTHROPIC
                    || pid == syncode_provider::PROVIDER_OPENAI
                {
                    continue;
                }
                if let Some(extra_adapter) = syncode_provider::registry::create_by_id(pid) {
                    let cfg = syncode_provider::ProviderConfig {
                        provider_id: pid.to_string(),
                        model: String::new(),
                        api_key: None,
                        base_url: None,
                        max_tokens: Some(4096),
                        extra: HashMap::new(),
                    };
                    {
                        let mut guard = extra_adapter.write().await;
                        if let Err(e) = guard.spawn(cfg).await {
                            tracing::warn!(
                                provider = pid,
                                error = %e,
                                "non-default provider adapter spawn failed — \
                                 threads using this provider will fail at turn time"
                            );
                            continue;
                        }
                    }
                    tracing::info!(
                        provider = pid,
                        "provider adapter spawned for per-thread dispatch"
                    );
                    registry_entries.push((pid.to_string(), extra_adapter));
                }
            }
            orchestrator = orchestrator.with_adapter_registry(registry_entries);
            orchestrator
        }
        None => {
            tracing::warn!(
                provider = %default_provider,
                "provider adapter not available — chat will be inert \
                 (turns recorded but no AI response). Install the provider CLI \
                 or set SYNCODE_DEFAULT_PROVIDER to an available provider id."
            );
            Orchestrator::new(repo)
        }
    };

    // Replay the read model from the event store so threads/projects from
    // previous sessions appear in the shell snapshot on startup.
    match orchestrator.replay_read_model().await {
        Ok((snapshots, events)) => {
            tracing::info!(
                snapshots_loaded = snapshots,
                events_replayed = events,
                "read model replayed from event store"
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to replay read model — starting with empty store");
        }
    }

    orchestrator
}

# Provider Adapters

`syncode-provider` abstracts AI coding agents behind the `ProviderAdapter` trait.
This document records which providers are **real** (functional) versus
**deferred**, and why.

## Status summary

| Provider | Status | Transport | Entry point |
|----------|--------|-----------|-------------|
| **cursor** | ✅ Real (ACP) | stdio NDJSON JSON-RPC | `adapters::cursor::create()` |
| **grok** | ✅ Real (ACP) | stdio NDJSON JSON-RPC | `adapters::grok::create()` |
| **gemini** | ✅ Real (ACP) | stdio NDJSON JSON-RPC | `adapters::gemini::create()` |
| anthropic | ✅ Real (HTTP) | reqwest POST | `adapters::AnthropicAdapter` |
| openai | ✅ Real (HTTP) | reqwest POST | `adapters::OpenAIAdapter` |
| claude | 🟡 Stub | — | `adapters::ClaudeAdapter` |
| codex | 🟡 Stub | — | `adapters::CodexAdapter` |
| opencode | 🟡 Stub | — | `adapters::OpenCodeAdapter` |
| kilo | 🟡 Stub | — | `adapters::KiloAdapter` |
| pi | 🟡 Stub | — | `adapters::PiAdapter` |

## Real ACP providers (cursor, grok, gemini)

These three speak the [Agent Client Protocol](https://agentclientprotocol.com)
(v0.11.3, `PROTOCOL_VERSION = 1`): JSON-RPC 2.0 over stdio with NDJSON framing.
They share one Rust implementation — [`AcpProvider`](src/acp_provider.rs) over
the [`AcpClient`](src/acp.rs) — and differ only in **spawn configuration**:

| Provider | Spawn form | Optional flags (env) |
|----------|------------|----------------------|
| cursor | `cursor-agent [-e <endpoint>] acp` | `SYNICODE_CURSOR_ENDPOINT` |
| grok | `grok agent [--always-approve] [-m <model>] [--reasoning-effort <effort>] --no-leader stdio` | `SYNICODE_GROK_ALWAYS_APPROVE`, `SYNICODE_GROK_MODEL`, `SYNICODE_GROK_REASONING_EFFORT` |
| gemini | `gemini --acp` | — |

Lifecycle mapping (trait → ACP): `spawn` = launch + `initialize`; `start_session`
= `session/new`; `send_request` = `session/prompt` (streams `session/update` →
`ProviderEvent`); `interrupt` = `session/cancel`; `health_check` = child
liveness; `shutdown` = kill child.

### Verifying with real binaries

Real-subprocess spawn requires the CLI installed **and** speaking ACP, so it is
gated behind `SYNICODE_ACP_E2E=1` in the integration tests. Without the gate,
the tests assert only the environment-independent pre-spawn invariant
(`Disconnected`). The full protocol lifecycle is covered by `acp` / `acp_provider`
unit tests using an in-process duplex fake agent (no real binary needed).

> **Gemini caveat:** mcode drives Gemini with a bespoke adapter (manual
> `child_process` + JSON-RPC) rather than its shared ACP runtime, hinting at
> possible wire quirks. syncode routes Gemini through the standard `AcpClient`;
> real-binary E2E validation is required to confirm interop and surface any
> provider-specific handling.

## Deferred providers (claude, codex, opencode, kilo, pi)

These remain as **non-functional stubs** (`spawn` sets flags; `send_request`
echoes `{"stub": true}`). They are **not feasible** to port faithfully to Rust
today because each depends on a TS-only SDK or managed HTTP app-server with no
Rust equivalent — the ACP foundation does not help them.

| Provider | mcode mechanism | Blocker |
|----------|-----------------|---------|
| claude | in-process `@anthropic-ai/claude-agent-sdk` (AsyncIterable) | No Rust SDK; spawns the `claude` CLI internally |
| codex | HTTP/WebSocket to a managed Codex app-server (SSE+JSON) | No public Rust client for the app-server protocol |
| opencode | HTTP/SSE to a spawned `opencode` app-server (OpenCode SDK) | TS-only SDK; app-server contract undocumented for Rust |
| kilo | shares the OpenCode adapter (same SDK/path) | Same as opencode |
| pi | in-process `@earendil-works/pi-coding-agent` SDK | No Rust SDK |

Restoring any of these requires either an official Rust client or a faithful
reimplementation of the SDK/app-server contract, which is out of scope for this
workflow.

## Factory

[`registry::create_by_id`](src/registry.rs) / [`registry::acp_config_for`]
construct the three ACP providers by id and return them as `SharedAdapter`
(`Arc<RwLock<dyn ProviderAdapter>>`); call `register_shared` to add them to a
`ProviderRegistry`. HTTP and stub adapters are constructed directly by their
owners.

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
| claude | ✅ Real (CLI stream-json) | subprocess NDJSON (SDKMessage) | `adapters::ClaudeAdapter` |
| codex | ✅ Real (app-server) | stdio NDJSON JSON-RPC | `adapters::CodexAdapter` |
| opencode | ✅ Real (HTTP+SSE) | local server REST+SSE | `adapters::OpenCodeAdapter` |
| kilo | ✅ Real (HTTP+SSE) | local server REST+SSE | `adapters::KiloAdapter` |
| pi | ✅ Real (RPC) | stdio JSON (`pi --mode rpc`) | `adapters::PiAdapter` |

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

## Real app-server provider (codex)

The OpenAI Codex CLI exposes a thread/turn JSON-RPC app-server distinct from
ACP. It shares the same protocol-agnostic foundation —
[`JsonRpcTransport`](src/subprocess.rs) — as the ACP providers, but speaks
Codex's own surface via [`CodexAppServerClient`](src/codex_app_server.rs),
wrapped by [`CodexAdapter`](src/adapters/codex.rs):

| Provider | Spawn form | Optional flags (config) |
|----------|------------|-------------------------|
| codex | `codex app-server` | `CodexConfig.full_auto`, `CodexConfig.sandbox` |

Lifecycle mapping (trait → Codex): `spawn` = launch + `initialize` (no protocol
version; `capabilities.experimentalApi`) + `initialized`; `start_session` =
`thread/start`; `send_request` = `turn/start` (streams `item/*` deltas →
`ProviderEvent`, ends on `turn/completed`/`turn/aborted`); `interrupt` =
`turn/interrupt`; `health_check` = child liveness; `shutdown` = kill child.

By default `CodexConfig.full_auto` runs with `approvalPolicy: "never"` +
`sandbox: "workspace-write"` and auto-approves every command/file-change approval
Codex requests mid-turn, so a headless adapter never deadlocks on a prompt.

### Verifying with real binaries

Codex spawn requires the `codex` CLI installed, so it is gated behind
`SYNICODE_CODEX_E2E=1` in `tests/codex_e2e.rs` (skips without the gate). The full
protocol lifecycle is covered by `codex_app_server` / `adapters::codex` unit
tests using an in-process duplex fake server (no real binary needed).

## Real stream-json provider (claude)

The Anthropic Claude Code CLI is driven in `stream-json` mode. Unlike the ACP and
codex providers (long-lived JSON-RPC subprocesses), the Claude CLI is a
**one-shot streaming producer**: each turn spawns
`claude -p <prompt> --output-format stream-json` and emits one JSON `SDKMessage`
per line on stdout until it terminates with a `result` message. There is no
request/response correlation, so [`ClaudeAdapter`](src/adapters/claude.rs) does
**not** use `JsonRpcTransport` — it spawns a fresh
[`tokio::process::Command`] per turn and decodes the NDJSON stream directly
(path 1 of the feasibility verdict in `.masday/research/custom-providers.md` §2).

| Provider | Spawn form | Optional flags (config) |
|----------|------------|-------------------------|
| claude | `claude -p <prompt> --output-format stream-json` | `ClaudeConfig.full_auto` (`--dangerously-skip-permissions`), `ClaudeConfig.model` (`--model`), `--append-system-prompt` |

Lifecycle mapping (trait → Claude): `spawn` = store config (no subprocess yet);
`start_session` = record `(working_dir, system_prompt)`; `send_request` = spawn
the CLI rooted at the session cwd and stream `SDKMessage` deltas →
`ProviderEvent`, ending on the terminal `result` (`success` → `Completed`,
`error_during_execution`/`is_error` → `Error`); `interrupt` = `start_kill()` the
in-flight turn subprocess (stream-only: killing the CLI is the only interrupt
path — there is no mid-turn RPC, and a mid-turn interrupt therefore restarts the
turn); `health_check` = spawned flag; `shutdown` = kill any in-flight turns.

By default `ClaudeConfig.full_auto` passes `--dangerously-skip-permissions` so a
headless adapter never deadlocks on the first permission prompt. Auth flows via
`ANTHROPIC_API_KEY` (injected from config/env) or the CLI's own OAuth login.

### Verifying with real binaries

Claude spawn requires the `claude` CLI installed (and Anthropic credentials), so
it is gated behind `SYNICODE_CLAUDE_E2E=1` in `tests/claude_e2e.rs` (skips
without the gate). The full stream-decoding path is covered by the
`adapters::claude` unit tests, which drive a fake `SDKMessage` reader through
`run_turn` and exercise every mapping branch (text deltas, tool-use starts,
assistant tool blocks, terminal result variants, malformed lines) without any
real binary.

## Real HTTP/SSE providers (opencode, kilo)

OpenCode and Kilo are driven through a **local HTTP/SSE server**: the adapter
spawns `{opencode|kilo} serve --hostname 127.0.0.1 --port <ephemeral>`, waits for
the `<prefix> server listening on http://…` ready line, then talks REST
(`POST /session`, `POST /session/{id}/prompt_async`, `POST /session/{id}/abort`,
`POST /permission/{id}/reply`) and consumes the streaming `GET /event` SSE
channel. There is no JSON-RPC over stdio here — the two providers share one
Rust transport, [`OpenCodeServerClient`](src/opencode_server.rs), parameterized
by an [`OpenCodeCompatibleCliSpec`] so Kilo is a single registration that
differs from OpenCode only in spawn form and identity (binary, ready-line prefix,
default agent).

| Provider | Spawn form | Default agent | Optional flags (config) |
|----------|------------|---------------|-------------------------|
| opencode | `opencode serve --hostname 127.0.0.1 --port <p>` | `build` | `OpenCodeConfig.full_auto`, `OpenCodeConfig.agent`, `OpenCodeConfig.bin_path`, `OpenCodeConfig.extra_args` |
| kilo | `kilo serve --hostname 127.0.0.1 --port <p>` | `code` | `KiloConfig.full_auto`, `KiloConfig.agent`, `KiloConfig.bin_path`, `KiloConfig.extra_args` |

Lifecycle mapping (trait → OpenCode session/turn model, identical for both):
`spawn` = launch + wait for ready line; `start_session` = `POST /session` rooted
at the spawn cwd → session id; `send_request` = `prompt_async` + drain SSE
(`message.part.delta` / `session.next.text.delta` → `Token`, tool lifecycle →
`ToolCall`/`ToolResult`, ends on `session.status` idle / `session.idle`);
`interrupt` = `POST /session/{id}/abort`; `health_check` = spawned-server
liveness; `shutdown` = kill the spawned server.

By default `OpenCodeConfig::full_auto`/`KiloConfig::full_auto` creates the
session with a blanket `*/* → allow` permission rule, and the SSE drain
auto-approves any `permission.asked` request mid-turn (`"once"`), so a headless
adapter never deadlocks on the first permission prompt. A locally-spawned server
runs **without** auth (mcode never sends a password to a server it started); HTTP
Basic auth is applied only for an externally-managed server (see `OpenCodeAuth`).

### Verifying with real binaries

Spawn requires the `opencode` / `kilo` CLI installed (and model-provider
credentials), so the E2E tests are gated behind `SYNICODE_OPENCODE_E2E=1` /
`SYNICODE_KILO_E2E=1` in `tests/opencode_e2e.rs` / `tests/kilo_e2e.rs` (skip
without the gate). The full SSE→`ProviderEvent` decoding path is covered by the
`opencode_server` unit tests, which drive pure decoder functions with fake SSE
bytes — no binary and no live HTTP server required — and the adapter glue
(session/model resolution, forwarder, turn→response mapping) is covered by the
`adapters::opencode` / `adapters::kilo` unit tests.

## Real RPC provider (pi)

The `@earendil-works/pi-coding-agent` CLI ships a first-class headless RPC
mode (`pi --mode rpc`) — a JSON-over-stdio protocol explicitly designed for
non-Node embedding (documented at the SDK's `docs/rpc.md`, with a reference TS
client). mcode drives pi via the in-process TS SDK, but that is mcode's choice,
not pi's limitation; the RPC mode is the recommended path for non-Node drivers.

Because pi speaks its own `{"type":"<cmd>"}` / `{"type":"response"}` / event
framing (NOT JSON-RPC 2.0), it does **not** reuse `JsonRpcTransport` — the
existing reader classifies by `id`+`method` and would silently drop pi frames.
Instead it has a dedicated [`PiClient`](src/pi_rpc.rs) with a `type`-keyed
reader: responses route to awaiters by string id; everything else forwards as
an agent event. The NDJSON framing (one JSON object per `\n`, LF-only) is
identical.

| Provider | Spawn form | Optional flags (config) |
|----------|------------|-------------------------|
| pi | `pi --mode rpc [--provider <p>] [--model <m>]` | `PiConfig.provider`, `PiConfig.model`, `PiConfig.extra_args`, `PiConfig.bin_path` |

Lifecycle mapping (trait → pi RPC): `spawn` = launch `pi --mode rpc`;
`start_session` = mint a local session id (pi's default session is implicit);
`send_request` = `prompt` (submit, then drain events to terminal `agent_end`);
`interrupt` = `abort`; `health_check` = child liveness; `shutdown` = kill child.

Event mapping: `message_update` text/thinking deltas → `ProviderEvent::Token`;
`tool_execution_start/end` → `ToolCall`/`ToolResult`; `agent_end` (stopReason
`stop`/`length`/`toolUse`) → `Completed`, else → `Error`. The terminal signal
is `agent_end` (NOT `turn_end` — a multi-step turn emits multiple `turn_end`
before the single `agent_end`).

Auth: pi owns its credentials (`~/.pi/agent/auth.json` or `pi` run once
interactively); the spawned process inherits them. No auth on the stdio channel.

### Verifying with real binaries

Spawn requires the `pi` CLI installed (and model-provider credentials), so the
E2E test is gated behind `SYNICODE_PI_E2E=1` in `tests/pi_e2e.rs` (skips
without the gate). The full protocol lifecycle is covered by `pi_rpc` unit
tests, which drive the client over an in-process `tokio::io::duplex` with a
fake pi peer emitting canned event sequences (text deltas → agent_end; tool
cycle; error stop-reason) — no real binary required.

## MCP server forwarding (PR #205)

ACP-speaking providers (cursor/grok/gemini) receive the `mcpServers` array at
`session/new` time. The wire point is `extract_provider_extras()` in
`crates/syncode-ws/src/settings.rs` (line ~745). The ACP provider shape is
unchanged — only the source of the `mcpServers` vec grew from settings extraction.

## Factory

[`registry::create_by_id`](src/registry.rs) / [`registry::acp_config_for`]
construct the three ACP providers by id and return them as `SharedAdapter`
(`Arc<RwLock<dyn ProviderAdapter>>`); call `register_shared` to add them to a
`ProviderRegistry`. HTTP and stub adapters are constructed directly by their
owners.

# Mock-Data Cleanup Plan (frontend panels showing non-real data)

**Context:** Several frontend settings panels render data that looks "mock" — either
hardcoded defaults or empty stubs. Root cause: the T6c backend phases wired RPCs that
return static defaults instead of probing the real environment. This plan audits each
panel and prescribes the fix.

## Update 2026-07-09 — progress
- ✅ **settings/provider** → **PR #145** (real CLI detection via `which`).
- ✅ **settings/usage** → **PR #146** (`TurnCompleted` now carries `usage`; a ws
  `spawn_usage_reactor` records chat-turn usage into `UsageStore`). Note: opencode's
  `--format json` does NOT report token usage (CLI limitation), so opencode turns
  record nothing until the CLI reports usage; claude (`extract_usage`) + ACP adapters
  parse `result.usage` and DO record.

## Status survey (2026-07-09)

| Panel / RPC | Source | Status |
|---|---|---|
| **settings/provider** (`server/getConfig` → `providers[]`) | `ServerSettingsState` default | ✅ **FIXED (PR #145)** — now probes CLI on PATH (`which::which`); cursor/grok/kilo/pi report `not_installed` |
| **settings/usage** (`server/getUsage`, `listProviderUsage`) | `UsageStore` (in-memory) | ⚠️ **EMPTY** — only LLM-op path records; **chat turns drop usage** (see fix below) |
| **diagnostics** (`server/getDiagnostics`) | `/proc` RSS + child rollup | ✅ REAL (T6c-26) |
| **profile stats** (`stats/getProfileStats`, `tokenStats`) | activity/usage aggregation | ✅ REAL (aggregates from store) |
| **server/welcome, getEnvironment** | platform/version | ✅ REAL |

So the two actionable gaps are **settings/provider** (done) and **settings/usage**.

---

## Fix: settings/usage (chat turns don't record token usage)

### Root cause
`ProviderEvent::Completed { output, usage, .. }` carries the provider's token usage.
The reactor maps it to `DomainEvent::TurnCompleted` in
`crates/syncode-orchestration/src/reactors/ingestion.rs:120-140`, but **`TurnCompleted`
has no `usage` field** — `usage` is read only for a `duration_ms` heuristic (line 126)
then **discarded**. So chat-turn usage never reaches `UsageStore`, and `server/getUsage`
returns empty (only `invoke_llm_oneshot` LLM ops record usage).

### Fix (cross-crate, 4 edits)
1. **`crates/syncode-core/src/domain/events.rs:267`** — add a `usage` field to the
   `TurnCompleted` variant. Core can't import `syncode_provider::UsageInfo`, so define a
   minimal core-local `TurnUsage { input_tokens, output_tokens, total_tokens }` (or reuse
   an existing core numeric type) — `Option<TurnUsage>`.
2. **`crates/syncode-orchestration/src/reactors/ingestion.rs:132`** — pass `usage`
   through: `usage: usage.map(into_turn_usage)`.
3. **`crates/syncode-orchestration/src/decider.rs:1375`** — the other `TurnCompleted`
   construction (synchronous path): `usage: None`.
4. **`crates/syncode-ws/src/` projection** — when projecting `TurnCompleted` (read-model
   projector / push delivery), `state.usage.write().record(UsageEntry { provider_id,
   model, input_tokens, output_tokens })` so `server/getUsage` reflects chat turns.

### Blast radius
`TurnCompleted` is matched in projections/projector/ingestion/pipeline/decider/command/ws
(~37 sites). Most use `{ .. }` so adding a field is non-breaking; only the 2 construction
sites need the new field. + tests.

### Verify
Run a chat turn (opencode/claude) → `server/getUsage` returns a non-empty entry for that
provider with the turn's token counts.

---

## Other panels — audit checklist (verify before trusting)
Most are already REAL. When in doubt, check the handler in `crates/syncode-ws/src/rpc.rs`:
- If it reads from a store/probe → real.
- If it returns a hardcoded `json!({...})` literal → mock (fix like PR #145).

Panels to spot-check in a future pass:
- `server/getConfig` other slices (`availableEditors`, `authMode`) — editors probe REAL;
  verify the rest.
- `provider/list-skills` / `list-skills-catalog` — filesystem scan (real, may be empty).
- `provider/skills/plugins/commands` discovery — no subsystem (served-but-empty by design).
- `server/getProviderUsageSnapshot` — single-provider slice of UsageStore (real, empty
  until the usage fix above lands).

## Priority
1. **settings/usage** fix above (cross-crate) — the remaining real "mock-looking" gap.
2. Spot-check pass for any remaining hardcoded `json!` defaults in server RPCs.

## PRs this effort (2026-07-08/09)
- #140 events.ts threadMetaUpdated · #141 message.text · #142 opencode part.text ·
  #143 opencode picker z.ai · #144 ACP Completed output · **#145 settings provider CLI detection**

# Research: synara vs syncode — cmd-window / "built-in app" gap

**Date:** 2026-07-23
**Question:** Why does syncode (Tauri) pop visible `cmd` windows while working, while synara (Electron) does not? Is syncode "not a built-in app" like synara?
**Method:** Parallel read-only audit of both repos (subprocess spawn + window-hide handling).

## TL;DR

- **syncode is actually MORE "built-in" than synara**: its core is in-process (Rust inside Tauri) — it spawns NO backend subprocess at all. synara spawns its Node backend as a child process.
- The cmd-window leak is **NOT an architecture gap — it's a helper-isolation gap**. synara hides child windows via a **shared helper** (`prepareWindowsSafeProcess`) in `packages/shared`, used by most call sites. syncode's `hide_console_window` helper lived in **one crate** (`syncode-provider`), so git/gh/npm/automation/mcp/voice spawns in *other* crates leaked.
- synara is **not fully clean either** — its ACP/opencode providers go through an Effect `ChildProcessSpawner` that **drops `windowsHide`** (the field isn't propagated). Only Claude (its main provider) uses the correct raw-spawn path. syncode can match and exceed synara with one shared helper applied everywhere.

## A. How synara avoids cmd windows

### A1. Backend spawn — the GUI-subsystem trick (no flag needed)
`apps/desktop/src/main.ts:2763` spawns the Node backend (`apps/server`) using `process.execPath` (the **Electron binary**) with `ELECTRON_RUN_AS_NODE=1`:
```ts
const child = ChildProcess.spawn(process.execPath, [...backendNodeArgs(), backendEntry], {
  env: { ...backendEnv(), ELECTRON_RUN_AS_NODE: "1", SYNARA_SERVER_ENTRY: backendEntry },
  stdio: captureBackendLogs ? ["ignore","pipe","pipe"] : "inherit",
});
```
- `electron.exe` has PE subsystem `WINDOWS` (GUI), so Windows allocates **no console** for the child.
- `ELECTRON_RUN_AS_NODE=1` runs the GUI binary as pure Node — no Electron window either.
- Net: backend runs hidden **by design**, with zero hide-flags. (Not applicable to Rust/Tauri — syncode has no backend subprocess at all.)

### A2. Provider CLI spawn — shared helper + `windowsHide: true`
`packages/shared/src/windowsProcess.ts:161` — `prepareWindowsSafeProcess()` is the **single chokepoint**: returns `windowsHide: true` on Windows, wraps `.cmd`/`.bat` shims via `cmd.exe /d /s /v:off /c call "…"` with `windowsVerbatimArguments: true`. Used by:
- **Claude** (`apps/server/src/provider/Layers/ClaudeAdapter.ts:399-432`) — raw `spawn` + `windowsHide: true` ✓
- **Pi** (`PiAdapter.ts:223`) ✓
- **MCP bridge** (`externalMcp/bridge.ts:58`), **editor discovery**, **processRunner**, **codex app-server**, **open(url/file)** ✓

### A3. synara's own gap (ACP/opencode path)
Cursor/Grok/Gemini/Droid (`AcpSessionRuntime.ts:689-716`), opencode (`opencodeRuntime.ts:880`), and `ProviderHealth.ts:653` spawn via the Effect `ChildProcessSpawner`. They call `prepareWindowsSafeProcess` but **only spread `windowsVerbatimArguments` — `windowsHide` is dropped**. Verified in Effect source: `NodeChildProcessSpawner` (`packages/platform-node-shared/src/NodeChildProcessSpawner.ts:474-477`) forwards only `{ cwd, env, stdio, detached, shell }`, and `CommandOptions` has **no `windowsHide` field**. → those providers CAN flash a cmd window on Windows. Unnoticed because Claude (main provider) is clean.

### A4. No global monkey-patch
synara has no runtime-global `windowsHide`. Its "global"-ness is **convention discipline**: one shared helper called at most call sites. No `child_process.spawn` override.

## B. How syncode leaks (the gap)

`hide_console_window` (CREATE_NO_WINDOW = 0x0800_0000) existed but was **isolated to `crates/syncode-provider/src/subprocess.rs`**. Verified: `creation_flags` appeared ONLY in `syncode-provider`. So:

| Layer | Status |
|---|---|
| Backend | In-process Rust — no subprocess, no leak (cleaner than synara) |
| Provider CLI (claude/codex/opencode/gemini) | Fixed in v0.1.9 (5 sites) — verified hidden |
| **git/gh/npm/automation/mcp/voice/worktree/local-server** | **LEAKED** — spawn sites in `syncode-ws`, `syncode-git`, `syncode-automation`, `syncode-tauri` had no hide |

**Two GUARANTEED-window sites** (literal `cmd /C <arbitrary>` on Windows, run on every script/automation):
- `crates/syncode-automation/src/process_executor.rs:333` (`shell_command`, `cmd /C`)
- `crates/syncode-ws/src/project_fs.rs:666` (`run_script`, `shell="cmd"`)

**The in-app terminal panel is NOT the leak** — `syncode-terminal` uses `portable_pty` (ConPTY on Windows), which renders into the embedded panel with no visible window. "cmd tampil di depan" = external subprocess cmd window.

## C. The fix (mirrors synara's shared-helper approach, then exceeds it)

1. **Promote the helper to a shared crate** — `crates/syncode-core/src/util/subprocess.rs`:
   - `hide_console_window(&mut tokio::process::Command)` + `hide_console_window_std(&mut std::process::Command)` + constructors `hidden_command()` / `hidden_std_command()`.
   - `syncode-core` is the universal dep (every crate depends on it).
2. **`syncode-provider/subprocess.rs` re-exports** the shared one (DRY; the 5 fixed sites unchanged).
3. **Apply to all leak sites** (this PR): process_executor `cmd /C`, project_fs `cmd /C`, rpc.rs git/gh (3), provider_versions, local_server, mcp_catalog, voice, worktree (2), syncode-git `run_cli` (chokepoint for git/gh), shell_commands, desktop_commands (2).
4. *(Optional follow-up)* CI lint banning raw `Command::new` outside the helper — prevents future leaks (the discipline synara gets from convention, syncode gets from enforcement).

**Net:** syncode becomes cleaner than synara (no backend subprocess + every child hidden via one enforced chokepoint, including the ACP/opencode-equivalent path synara still leaks).

## D. Key evidence (path:line)

**synara**
- Backend GUI-subsystem trick: `apps/desktop/src/main.ts:2763-2773`
- Shared helper: `packages/shared/src/windowsProcess.ts:161-189`
- Claude (clean): `apps/server/src/provider/Layers/ClaudeAdapter.ts:399-432`
- Effect gap (drops windowsHide): `apps/server/src/provider/acp/AcpSessionRuntime.ts:689-716`; Effect `NodeChildProcessSpawner.ts:474-477`

**syncode (pre-fix)**
- Helper isolated: `crates/syncode-provider/src/subprocess.rs` (only crate with `creation_flags`)
- Guaranteed-window sites: `crates/syncode-automation/src/process_executor.rs:333`, `crates/syncode-ws/src/project_fs.rs:666`
- Terminal uses ConPTY (no window): `crates/syncode-terminal/src/pty.rs`

**syncode (post-fix, this PR)**
- Shared helper: `crates/syncode-core/src/util/subprocess.rs`
- Re-export: `crates/syncode-provider/src/subprocess.rs` (`pub use syncode_core::util::subprocess::hide_console_window`)

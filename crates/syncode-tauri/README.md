# syncode-tauri

Tauri v2 desktop shell for Syncode. Embeds the in-process WebSocket server
(`syncode-ws`) and exposes native capabilities (filesystem picker, updater,
shell-open, terminal PTY) to the webview via Tauri IPC commands.

## Architecture

The desktop shell is **self-contained**: it boots the same `axum` WS server
the standalone binary (`crates/syncode-ws/src/bin/server.rs`) exposes, so the
browser-mode UI and the desktop shell run identical backend code. Both use
the shared `orchestrator_setup::build_orchestrator` helper introduced in
v0.1.5 — see [crates/syncode-ws/src/orchestrator_setup.rs](../syncode-ws/src/orchestrator_setup.rs).

Configuration (environment):
- `SYNCODE_WS_HOST` — bind host (default `127.0.0.1`).
- `SYNCODE_WS_PORT` — bind port (default `33101`; the standalone binary uses
  `3000` so the two can run concurrently).
- `SYNCODE_DB` — SQLite path (default `syncode.db` in cwd). Empty → in-memory.
- `SYNCODE_DEFAULT_PROVIDER` — provider id armed on the chat pipeline
  (default `claude`). Falls back to inert mode if the CLI is absent.

## Known Issues

### CSP `inline-style` console warnings (v0.1.5 cosmetic, not functional)

**Symptom:** opening the webview devtools console shows warnings like
`[Report Only] Refused to apply inline style because it violates the
following Content Security Policy directive: "style-src 'self' ..."` when
the UI mutates a DOM element's `style` attribute at runtime.

**Root cause:** Tauri v2's auto-nonce injection on `style-src` disables
`'unsafe-inline'` for `<style>` blocks and `style` attributes. Any code
path that sets `element.style.x = ...` or assigns a `style="..."` attribute
at runtime trips the warning. The desktop shell's CSP is set in
[tauri.conf.json](tauri.conf.json) (`app.security.csp`) and the upstream
behavior is documented in [Tauri v2 security#Content Security Policy](https://v2.tauri.app/security/csp/).

**Functional impact:**
- CSS-class-driven styles apply correctly (the dominant pattern in the
  frontend).
- `style="..."` attribute assignments are honored in browsers that support
  `style-src-attr 'unsafe-inline'` (the directive is currently not
  separately set in `tauri.conf.json`; the `style-src 'self'` rule
  supersedes it).
- The warnings are console-only — they do **not** break rendering or block
  interaction. Closing the devtools hides them entirely.

**Future fix (not in v0.1.5 scope):**
1. Migrate runtime inline-style mutations to CSS classes (cleanest path —
   the styles are already enumerated; `style.cssText` becomes a class name
   swap).
2. Or add `'unsafe-hashes'` per-style declarations to the CSP for the
   specific inline-style fingerprints we want to allow. Use the
   [CSP hashing tool of your choice](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy/Sources#unsafe-hashes)
   to compute `'sha256-...'` values.
3. Or split the CSP into `style-src` (for `<style>` blocks) and
   `style-src-attr` (for inline attributes) with per-directive policies.

### Provider CLI missing on first launch

If `claude`, `cursor`, or another provider CLI is not on `PATH`, the
orchestrator falls back to **inert mode** at startup (turns are recorded
but no AI response is generated, and the server still boots). The fix is
to install the provider CLI and restart the desktop shell — the shared
`build_orchestrator` helper re-resolves the provider on each launch from
`server_settings.textGenerationModelSelection`.

## Release checklist (v0.1.5+)

1. Bump version in [Cargo.toml](../../Cargo.toml) (`workspace.package.version`).
2. Bump version in [tauri.conf.json](tauri.conf.json) (`version`).
3. Update [CHANGELOG.md](../../CHANGELOG.md) via `git-cliff` (auto-generated
   from conventional commits — keep `feat(tauri):` / `fix(tauri):` prefixes
   consistent so the changelog groups correctly).
4. Run the full verification: `cargo build --release`, `cargo test`,
   `cargo clippy --all-targets --all-features -- -D warnings`.
5. Automated E2E smoke (`cargo test -p syncode-tauri --test v015_smoke`) —
   boots the desktop WS/HTTP server on an ephemeral port and drives the
   project/thread/settings cycle + every HTTP route + the auth REST surface
   headlessly. **This is the closest to "Tauri shell works" you can verify
   without a display.**
6. Manual UI smoke — the steps below cover surface area a headless test
   cannot reach (webview rendering, IPC commands, real provider dispatch).

## Manual UI smoke (requires a display)

These steps verify surface area a headless test cannot reach: webview
rendering, Tauri IPC commands, real provider dispatch. Each step pairs the
action with the contract it pins so a reviewer can decide depth of coverage.

1. **Boot** — `cargo tauri dev` (or run a built installer). Confirm the
   window opens with no console errors except the documented CSP
   `inline-style` warnings.
2. **Project cycle** — File → New Project, give it a name + path. Confirm
   the project list panel updates; close + reopen Tauri; confirm the
   project reappears (proves SQLite persistence wired through the unified
   `build_orchestrator`).
3. **Thread cycle** — Open a project, click New Thread, pick `claude` +
   `sonnet`, type a message. The thread should appear in the list; pause +
   resume the thread; confirm state transitions.
4. **Provider dispatch** — With the `claude` CLI on `PATH`, send a message
   and confirm an AI response streams back. If the CLI is missing, expect
   inert mode (turn recorded, no AI reply) — that's the documented fallback.
5. **Settings cycle** — Open Settings, change a theme, close + reopen
   Tauri; confirm the theme persists (proves `attach_pool(Some(pool))`
   fired inside the shared orchestrator helper).
6. **HTTP routes** — Open thread picker; confirm editor icons render (or
   the React fallback shows for the placeholder PNG). Paste a screenshot
   into a thread; confirm it renders (proves `/api/local-image` traversal
   guard allows the temp dir).
7. **Auth REST** — Open the auth panel; check the network tab — no 404s
   on `/api/auth/*`. Bootstrap a pairing link; confirm it appears in the
   list; revoke it; confirm it disappears.
8. **Restart mid-turn** — Start a turn, close Tauri mid-response, reopen;
   confirm the thread list restores and the session resumes (cursor store
   rehydrates from disk).
9. **Console noise** — Webview devtools console should only show the
   documented CSP inline-style warnings. Any other warning or error is a
   regression.

/**
 * NativeApi + DesktopBridge — Electron/Tauri desktop shell bridge interfaces.
 *
 * In MCode these live in `@t3tools/contracts` `ipc.ts` as ~170-line
 * (`NativeApi`) + ~65-line (`DesktopBridge`) TypeScript interfaces. They are
 * the stable boundary the rest of the UI consumes (window controls, webview /
 * browser panels, notifications, updates, editor launch, …).
 *
 * STATUS: Tier 0 placeholder. Real interfaces are copied verbatim from MCode
 * during the shell-swap task (B4 / "T6") and re-pointed at Tauri `invoke`.
 *
 * TODO T6: copy verbatim from MCode ipc.ts (~170+65 lines) during shell swap.
 *   Source: /home/vibe-dev/mcode/packages/contracts/src/ipc.ts
 *   Replace `unknown` stubs with the real method signatures, then implement
 *   the Tauri side via `@tauri-apps/api` `invoke` in the transport re-wire.
 *   For capabilities Tauri lacks (e.g. embedded browser webview panels),
 *   stub to "unsupported."
 */

/**
 * Placeholder desktop-shell API. Until T6 this is `unknown` so any clone
 * import that touches it surfaces as a compile error (intentionally — the
 * shell isn't wired yet). After T6 this becomes the real interface.
 */
export type NativeApi = unknown;

/**
 * Placeholder desktop bridge (the Electron->renderer channel surface). Same
 * treatment as `NativeApi`: `unknown` until T6.
 */
export type DesktopBridge = unknown;

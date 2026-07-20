## [0.1.3] - 2026-07-21

### Bug Fixes

- **tauri**: Fix `orchestration.dispatchCommand` typo that hung the desktop shell on launch — `tauriNativeApi.ts` was sending `orchestration/dispatch` (rejected by backend with `Method not found`), which broke `prewarmHomeChatProject` in the Sidebar. Browser path was unaffected.
- **tauri**: Guard `window.desktopBridge?.browser?.onBrowserUseOpenPanelRequest` with optional chaining on `.browser` — Tauri's desktop bridge does not expose the Electron-style `browser` property, so accessing `.onBrowserUseOpenPanelRequest` on `undefined` crashed the chat thread surface.


## [0.1.2] - 2026-07-20

### Bug Fixes

- **tauri**: Wire welcome push + add file logger (v0.1.2) (#217) (86a1985)


### CI/CD

- **changelog**: Fix 'same file' cp error + soften PR-creation failure (#216) (dd62aa2)



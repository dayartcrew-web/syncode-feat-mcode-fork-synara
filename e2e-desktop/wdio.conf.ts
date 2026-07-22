// @ts-nocheck — WDIO config is runtime-validated by the runner; avoid fighting
// @wdio/types version churn in a config file.
import type { Options } from "@wdio/types";

// WebdriverIO config — drives the REAL Syncode Tauri app via the embedded
// WebDriver server (tauri-plugin-wdio-webdriver, compiled in with
// `cargo build --release --features webdriver`). This catches Tauri-specific
// behaviour the browser path can't: the in-process WS server, IPC bridge,
// cmd-window hiding, devtools, settings persistence, provider dispatch.
//
// Run: `npm run build:test-binary` then `npm test` (under a display: xhost/xvfb
// or a real session; WebKitGTK needs a DISPLAY).

const isWin = process.platform === "win32";
const appBinaryPath = isWin
  ? "../target/release/syncode-tauri.exe"
  : "../target/release/syncode-tauri";

export const config: Options.Testrunner = {
  runner: "local",
  specs: ["./test/**/*.e2e.ts"],
  suites: {
    smoke: ["./test/smoke.e2e.ts"],
  },
  maxInstances: 1,
  capabilities: [
    {
      // The @wdio/tauri-service launches appBinaryPath + drives the webview.
      maxInstances: 1,
      browserName: "wry", // Tauri's webview runtime
      "tauri:options": {
        appBinaryPath,
        driverProvider: "embedded",
      },
    } as WebdriverIO.Capabilities,
  ],
  logLevel: "warn",
  bail: 0,
  baseUrl: "http://localhost",
  waitforTimeout: 30000,
  framework: "mocha",
  mochaOpts: {
    ui: "bdd",
    timeout: 120000,
  },
  reporters: ["spec"],
  services: [
    [
      "tauri",
      {
        appBinaryPath,
        driverProvider: "embedded",
        captureBackendLogs: true,
      },
    ],
  ],
};

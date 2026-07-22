import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";

import { invoke, isTauri } from "@tauri-apps/api/core";

import "@fontsource-variable/jetbrains-mono";
import "./index.css";
import "./storageKeyMigration";

import { appHistory } from "./appNavigation";
import { getRouter } from "./router";
import { APP_DISPLAY_NAME } from "./branding";
import { isElectron } from "./env";

const router = getRouter(appHistory);

document.title = APP_DISPLAY_NAME;

if (isElectron) {
  document.documentElement.dataset.runtime = "electron";
}

// Desktop DevTools hotkey (F12 / Ctrl+Shift+I). A release build's `devtools`
// Cargo feature gates the Tauri webview devtools API; the `toggle_devtools`
// IPC command (syncode-tauri/src/desktop_commands.rs) opens/closes it. Without
// this wiring DevTools is entirely unavailable in a release build — the
// v0.1.6 regression. Browser/Vite-dev mode ignores this (the browser's own
// F12 applies); the command is a graceful no-op when the feature is off.
if (isTauri()) {
  document.documentElement.dataset.runtime = "tauri";
  const onDevtoolsHotkey = (event: KeyboardEvent) => {
    if (
      event.key === "F12" ||
      ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toUpperCase() === "I")
    ) {
      event.preventDefault();
      void invoke<boolean>("toggle_devtools", { label: "main" }).catch(() => {
        // Feature off / command unavailable — swallow.
      });
    }
  };
  window.addEventListener("keydown", onDevtoolsHotkey);
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>,
);

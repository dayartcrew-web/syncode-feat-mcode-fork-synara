// @ts-check
// Diagnostic probe: hooks WebSocket to log every socket open/close and capture
// the call stack for each open. Useful when investigating whether the app
// leaks sockets or reconnects unexpectedly. Artifacts are written to
// `.e2e-tmp/ws-probe/` (samples.json + final.png).
//
// IMPORTANT: when monkey-patching window.WebSocket we MUST re-attach the
// static constants (CONNECTING/OPEN/CLOSING/CLOSED). App code compares
// `socket.readyState === WebSocket.OPEN`; without these constants the
// comparison is `1 === undefined` and the app's transport opens a new socket
// for every RPC, producing a probe-induced flood that looks like a real bug.
const { chromium } = require("playwright");
const fs = require("node:fs");
const path = require("node:path");

const APP_URL = process.env.APP_URL ?? "http://localhost:5173";
const OUT_DIR = path.resolve(__dirname, "..", ".e2e-tmp", "ws-probe");
fs.mkdirSync(OUT_DIR, { recursive: true });

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  page.on("console", (msg) => {
    const text = msg.text();
    if (
      text.includes("WebSocket") ||
      text.includes("ws://") ||
      text.includes("error") ||
      text.includes("ws-debug") ||
      text.includes("ws-hook")
    ) {
      console.log(`[browser ${msg.type()}]`, text.substring(0, 400));
    }
  });

  // Install WS hook BEFORE any page script runs — addInitScript injects
  // before document creation, so we catch every WebSocket construction
  // including the first one during module evaluation.
  await context.addInitScript(() => {
    window.__wsLog = [];
    window.__wsOpenSites = [];
    const origWS = window.WebSocket;
    window.WebSocket = function (url, protocols) {
      try {
        const stack = new Error().stack || "";
        window.__wsOpenSites.push({
          t: Date.now(),
          url,
          stack: stack.split("\n").slice(0, 15).join(" || "),
        });
      } catch {}
      console.log("[ws-hook] opening:", url);
      const ws = protocols ? new origWS(url, protocols) : new origWS(url);
      ws.addEventListener("open", () => console.log("[ws-hook] OPEN:", url));
      ws.addEventListener("close", (e) =>
        console.log(`[ws-hook] CLOSE: code=${e.code} clean=${e.wasClean}`),
      );
      ws.addEventListener("error", () => console.log("[ws-hook] ERROR:", url));
      ws.addEventListener("message", (e) => {
        try {
          const data = typeof e.data === "string" ? e.data : "[binary]";
          window.__wsLog.push({ t: Date.now(), data: data.substring(0, 400) });
        } catch {}
      });
      return ws;
    };
    window.WebSocket.prototype = origWS.prototype;
    // Preserve static constants — app code reads WebSocket.OPEN/CLOSING/etc.
    // Without these, `readyState === WebSocket.OPEN` evaluates to `1 === undefined`
    // and ensureOpen falls through to openSession, creating a probe-induced flood.
    window.WebSocket.CONNECTING = origWS.CONNECTING;
    window.WebSocket.OPEN = origWS.OPEN;
    window.WebSocket.CLOSING = origWS.CLOSING;
    window.WebSocket.CLOSED = origWS.CLOSED;
  });

  await page.goto(APP_URL, { waitUntil: "domcontentloaded", timeout: 30_000 });

  // Wait 5s so we capture any reconnect storms during page bootstrap.
  await page.waitForTimeout(5000);

  const initialDump = await page.evaluate(() => ({
    wsFrames: window.__wsLog.length,
    openSites: window.__wsOpenSites.length,
  }));
  console.log(
    `[probe] after 5s: wsFrames=${initialDump.wsFrames} openSites=${initialDump.openSites}`,
  );

  // Now send a message.
  const editor = page.locator('[contenteditable="true"]').first();
  await editor.waitFor({ state: "visible", timeout: 30_000 });
  await editor.click();
  await page.keyboard.type("Think step by step: design a tiny REST API. Reason briefly.", {
    delay: 5,
  });
  await page.keyboard.press("Enter");

  // Sample WS frame count over a short window (10s — long enough to see
  // the flood pattern, short enough to survive Windows socket exhaustion).
  const start = Date.now();
  const samples = [];
  while (Date.now() - start < 10_000) {
    const elapsedMs = Date.now() - start;
    const dump = await page.evaluate(() => ({
      wsCount: window.__wsLog.length,
      openSites: window.__wsOpenSites.length,
      lastFrames: window.__wsLog.slice(-2).map((f) => f.data.substring(0, 200)),
      bodyLen: (document.body.innerText ?? "").length,
    }));
    samples.push({ t: elapsedMs, ...dump });
    if (samples.length <= 5 || samples.length % 10 === 0) {
      console.log(
        `[probe] +${elapsedMs}ms wsCount=${dump.wsCount} openSites=${dump.openSites} bodyLen=${dump.bodyLen}`,
      );
    }
    await page.waitForTimeout(1000);
  }

  // Dump ALL open sites with their stack traces.
  const sites = await page.evaluate(() => window.__wsOpenSites);
  console.log(`[probe] === OPEN SITE STACKS (${sites.length} total) ===`);
  for (const site of sites) {
    console.log(`t=${site.t} url=${site.url}`);
    console.log(site.stack);
    console.log("---");
  }

  fs.writeFileSync(path.join(OUT_DIR, "samples.json"), JSON.stringify(samples, null, 2));
  await page.screenshot({ path: path.join(OUT_DIR, "final.png") });
  await browser.close();
  console.log(`[probe] artifacts in ${OUT_DIR}`);
})().catch((err) => {
  console.error("[probe] fatal:", err);
  process.exit(1);
});

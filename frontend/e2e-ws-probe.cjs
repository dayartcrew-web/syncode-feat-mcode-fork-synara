// @ts-check
// Minimal WS probe: hooks WebSocket, sends one message, and reports
// exactly which frames arrive (if any) during a 30s window.
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

  const wsEvents = [];
  page.on("console", (msg) => {
    const text = msg.text();
    if (text.includes("WebSocket") || text.includes("ws://") || text.includes("error")) {
      console.log(`[browser ${msg.type()}]`, text.substring(0, 250));
    }
  });

  await page.goto(APP_URL, { waitUntil: "domcontentloaded", timeout: 30_000 });

  // Install WS hook BEFORE the app opens its WS — early init script.
  await page.evaluate(() => {
    window.__wsLog = [];
    const origWS = window.WebSocket;
    window.WebSocket = function (url, protocols) {
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
  });

  // Wait briefly for any initial WS to establish.
  await page.waitForTimeout(2000);

  const initialWsCount = await page.evaluate(() => window.__wsLog.length);
  console.log("[probe] initial ws frames after 2s:", initialWsCount);

  // Now send a message.
  const editor = page.locator('[contenteditable="true"]').first();
  await editor.waitFor({ state: "visible", timeout: 30_000 });
  await editor.click();
  await page.keyboard.type("Think step by step: design a tiny REST API. Reason briefly.", {
    delay: 5,
  });
  await page.keyboard.press("Enter");

  // Sample WS frame count over 30 seconds.
  const start = Date.now();
  const samples = [];
  while (Date.now() - start < 30_000) {
    const elapsedMs = Date.now() - start;
    const dump = await page.evaluate(() => ({
      wsCount: window.__wsLog.length,
      lastFrames: window.__wsLog.slice(-2).map((f) => f.data.substring(0, 200)),
      bodyLen: (document.body.innerText ?? "").length,
    }));
    samples.push({ t: elapsedMs, ...dump });
    if (samples.length <= 5 || samples.length % 10 === 0) {
      console.log(
        `[probe] +${elapsedMs}ms wsCount=${dump.wsCount} bodyLen=${dump.bodyLen}`,
      );
    }
    await page.waitForTimeout(1000);
  }

  fs.writeFileSync(path.join(OUT_DIR, "samples.json"), JSON.stringify(samples, null, 2));
  await page.screenshot({ path: path.join(OUT_DIR, "final.png") });
  await browser.close();
  console.log(`[probe] artifacts in ${OUT_DIR}`);
})().catch((err) => {
  console.error("[probe] fatal:", err);
  process.exit(1);
});

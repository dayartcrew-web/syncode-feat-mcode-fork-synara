// @ts-check
// Deep diagnostic: captures session.orchestrationStatus, latestTurn state,
// activity log entries, and the working row DOM during a live Claude turn.
// Used to figure out why the "Thinking" label isn't replacing "Working".
//
// Usage: node frontend/e2e-dump-detail.cjs
const { chromium } = require("playwright");
const fs = require("node:fs");
const path = require("node:path");

const APP_URL = process.env.APP_URL ?? "http://localhost:5173";
const OUT_DIR = path.resolve(__dirname, "..", ".e2e-tmp", "sidebar-detail");
fs.mkdirSync(OUT_DIR, { recursive: true });

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  // Capture console + WS frames for offline analysis.
  const wsFrames = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      console.log(`[browser ${msg.type()}]`, msg.text().substring(0, 200));
    }
  });

  await page.goto(APP_URL, { waitUntil: "domcontentloaded", timeout: 30_000 });
  const editor = page.locator('[contenteditable="true"]').first();
  await editor.waitFor({ state: "visible", timeout: 30_000 });
  await editor.click();
  await page.keyboard.type(
    "Think step by step. Design a small REST API for a todo list with due dates and priorities. Explain your reasoning before giving the final design.",
    { delay: 5 },
  );

  // Hook into fetch/WebSocket to capture incoming server events.
  await page.evaluate(() => {
    window.__wsLog = [];
    const origWS = window.WebSocket;
    window.WebSocket = function (url, protocols) {
      const ws = protocols ? new origWS(url, protocols) : new origWS(url);
      ws.addEventListener("message", (e) => {
        try {
          window.__wsLog.push({
            t: Date.now(),
            data: typeof e.data === "string" ? e.data.substring(0, 500) : "[binary]",
          });
          if (window.__wsLog.length > 1000) window.__wsLog.shift();
        } catch {}
      });
      return ws;
    };
    window.WebSocket.prototype = origWS.prototype;
  });

  await page.keyboard.press("Enter");

  const samples = [];
  const start = Date.now();
  while (Date.now() - start < 90_000) {
    const elapsedMs = Date.now() - start;
    const dump = await page.evaluate(() => {
      const text = (document.body.innerText ?? "").replace(/\s+/g, " ");
      const labels = ["Thinking", "Exploring", "In Skill", "Subagent", "Working"];
      const found = {};
      for (const l of labels) {
        const idx = text.indexOf(l);
        found[l] = idx === -1 ? null : text.substring(idx, idx + 80);
      }
      return {
        bodyLen: text.length,
        found,
        wsEventCount: (window.__wsLog || []).length,
        wsRecent: (window.__wsLog || [])
          .slice(-3)
          .map((f) => f.data.substring(0, 200)),
      };
    });
    samples.push({ t: elapsedMs, dump });
    if (
      elapsedMs > 15_000 &&
      (dump.found.Thinking || dump.found.Exploring || dump.found["In Skill"])
    ) {
      console.log(`[dump] target label at +${elapsedMs}ms`);
      console.log(JSON.stringify(dump.found, null, 2));
      await page.screenshot({ path: path.join(OUT_DIR, `realtime-${elapsedMs}.png`) });
    }
    await page.waitForTimeout(500);
  }

  fs.writeFileSync(path.join(OUT_DIR, "samples.json"), JSON.stringify(samples, null, 2));
  await page.screenshot({ path: path.join(OUT_DIR, "final.png") });
  await browser.close();
  console.log(`[dump] artifacts in ${OUT_DIR}`);
})().catch((err) => {
  console.error("[dump] fatal:", err);
  process.exit(1);
});

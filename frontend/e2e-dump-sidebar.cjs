// @ts-check
// Diagnostic harness: dumps the sidebar thread rows + their status pills +
// the chat timeline trailing row text during a live turn. Used to verify
// that classifyLiveActivityLabel / resolveThreadStatusPill / MessagesTimeline
// actually render the right text when activities arrive.
//
// Usage: node frontend/e2e-dump-sidebar.cjs
const { chromium } = require("playwright");
const fs = require("node:fs");
const path = require("node:path");

const APP_URL = process.env.APP_URL ?? "http://localhost:5173";
const OUT_DIR = path.resolve(__dirname, "..", ".e2e-tmp", "sidebar-dump");
fs.mkdirSync(OUT_DIR, { recursive: true });

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto(APP_URL, { waitUntil: "domcontentloaded", timeout: 30_000 });
  const editor = page.locator('[contenteditable="true"]').first();
  await editor.waitFor({ state: "visible", timeout: 30_000 });
  await editor.click();
  await page.keyboard.type(
    "Think step by step. Design a small REST API for a todo list with due dates and priorities. Explain your reasoning before giving the final design.",
    { delay: 5 },
  );
  await page.keyboard.press("Enter");

  const samples = [];
  const start = Date.now();
  while (Date.now() - start < 120_000) {
    const elapsedMs = Date.now() - start;
    const dump = await page.evaluate(() => {
      const text = (document.body.innerText ?? "").replace(/\s+/g, " ");
      // Snapshot pill labels — look for the four target labels plus Working.
      const labels = ["Thinking", "Exploring", "In Skill", "Subagent", "Working"];
      const found = {};
      for (const l of labels) {
        const idx = text.indexOf(l);
        found[l] = idx === -1 ? null : text.substring(idx, idx + 60);
      }
      return { found, len: text.length };
    });
    samples.push({ t: elapsedMs, dump });
    // Save the first transition where any of "Thinking/Exploring/In Skill" appears.
    if (
      dump.found.Thinking ||
      dump.found.Exploring ||
      dump.found["In Skill"]
    ) {
      console.log(`[dump] realtime label at +${elapsedMs}ms`);
      console.log(JSON.stringify(dump.found, null, 2));
      await page.screenshot({ path: path.join(OUT_DIR, `realtime-${elapsedMs}.png`) });
      break;
    }
    await page.waitForTimeout(400);
  }

  fs.writeFileSync(path.join(OUT_DIR, "samples.json"), JSON.stringify(samples, null, 2));
  await page.screenshot({ path: path.join(OUT_DIR, "final.png") });
  await browser.close();
  console.log(`[dump] artifacts in ${OUT_DIR}`);
})().catch((err) => {
  console.error("[dump] fatal:", err);
  process.exit(1);
});

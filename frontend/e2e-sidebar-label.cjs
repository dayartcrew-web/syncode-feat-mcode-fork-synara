// @ts-check
// Focused verification harness for the realtime sidebar + timeline labels.
// Boots a headless chromium against the running vite dev server (5173) and
// the running syncode-ws backend (3000), opens the default project, sends a
// turn, then polls for any of {Thinking, Exploring, In Skill, Subagent,
// Working} on (a) the sidebar thread row pill and (b) the chat timeline
// trailing row. Captures screenshots into .e2e-tmp/sidebar-label/.
//
// Usage: node frontend/e2e-sidebar-label.cjs
const { chromium } = require("playwright");
const fs = require("node:fs");
const path = require("node:path");

const APP_URL = process.env.APP_URL ?? "http://localhost:5173";
const OUT_DIR = path.resolve(__dirname, "..", ".e2e-tmp", "sidebar-label");
fs.mkdirSync(OUT_DIR, { recursive: true });

const LABELS = ["Thinking", "Exploring", "In Skill", "Subagent", "Working"];

/**
 * @param {import("playwright").Page} page
 * @returns {Promise<{sidebar: string[], timeline: string[]}>}
 */
async function snapshotLabels(page) {
  return page.evaluate((labels) => {
    /** @param {string} text */
    const norm = (text) => (text ?? "").replace(/\s+/g, " ").trim();
    const sidebar = [];
    const timeline = [];
    const all = document.body.innerText ?? "";
    for (const label of labels) {
      // Sidebar pill: "<label>" on its own (e.g. "Thinking").
      // Timeline trailing row: "<label> for <duration>".
      if (all.includes(`${label} for `) || all.includes(`${label}...`)) {
        timeline.push(label);
      }
      // Sidebar pill is just the bare label, harder to disambiguate from the
      // timeline. Accept the bare label only if the timeline form is absent.
      if (all.includes(label) && !all.includes(`${label} for `) && !all.includes(`${label}...`)) {
        sidebar.push(label);
      }
    }
    return { sidebar: [...new Set(sidebar)], timeline: [...new Set(timeline)] };
  }, LABELS);
}

(async () => {
  console.log("[sidebar-label] launching chromium (headless)");
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  page.on("console", (msg) => {
    if (msg.type() === "error") {
      console.log(`[browser console error] ${msg.text()}`);
    }
  });

  console.log(`[sidebar-label] navigating to ${APP_URL}`);
  await page.goto(APP_URL, { waitUntil: "domcontentloaded", timeout: 30_000 });

  // Wait for composer to mount. The new-thread composer is a contenteditable.
  const editor = page.locator('[contenteditable="true"]').first();
  await editor.waitFor({ state: "visible", timeout: 30_000 });
  await editor.click();
  console.log("[sidebar-label] composer ready, typing prompt");

  // A prompt that will reliably trigger reasoning + a tool call (Grep) +
  // likely a subagent dispatch on Claude.
  await page.keyboard.type(
    "Briefly: use Grep to find the string 'ProviderEvent' in crates/syncode-provider/src/. Then in one short sentence, summarize what you found.",
    { delay: 5 },
  );
  await page.keyboard.press("Enter");
  console.log("[sidebar-label] turn submitted, polling for live labels");

  /** @type {Array<{t: number, sidebar: string[], timeline: string[]}>} */
  const samples = [];
  const start = Date.now();
  let foundRealtime = false;
  while (Date.now() - start < 90_000) {
    const elapsedMs = Date.now() - start;
    const snap = await snapshotLabels(page);
    samples.push({ t: elapsedMs, ...snap });
    if (
      snap.timeline.some((l) => l !== "Working") ||
      snap.sidebar.some((l) => l !== "Working")
    ) {
      foundRealtime = true;
      console.log(
        `[sidebar-label] realtime label surfaced at +${elapsedMs}ms: sidebar=${JSON.stringify(snap.sidebar)} timeline=${JSON.stringify(snap.timeline)}`,
      );
      await page.screenshot({
        path: path.join(OUT_DIR, `realtime-${elapsedMs}.png`),
        fullPage: false,
      });
      break;
    }
    await page.waitForTimeout(500);
  }

  // Capture final state.
  await page.screenshot({ path: path.join(OUT_DIR, "final.png"), fullPage: false });
  console.log(`[sidebar-label] final labels:`, await snapshotLabels(page));

  fs.writeFileSync(
    path.join(OUT_DIR, "samples.json"),
    JSON.stringify(samples, null, 2),
  );
  console.log(`[sidebar-label] artifacts written to ${OUT_DIR}`);

  await browser.close();

  if (!foundRealtime) {
    console.error("[sidebar-label] FAILED: no realtime label (Thinking/Exploring/In Skill/Subagent) surfaced within 90s.");
    process.exit(2);
  }
  console.log("[sidebar-label] PASS: realtime label surfaced.");
})().catch((err) => {
  console.error("[sidebar-label] fatal:", err);
  process.exit(1);
});

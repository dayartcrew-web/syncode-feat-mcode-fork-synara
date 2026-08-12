/* eslint-disable */
/* PR #239 verification harness — capture raw push/orchestration frames during
 * a real Claude turn and classify each ActivityLogged by activity_type.
 *
 * Boots nothing — expects:
 *   - vite dev server on http://localhost:5173 (run: npm run dev in frontend/)
 *   - syncode-ws server on ws://127.0.0.1:3100/ws (run: SYNCODE_WS_PORT=3100 cargo run -p syncode-ws --bin server)
 *
 * Writes:
 *   - ../.e2e-tmp/pr239-raw-frames.json  (every push frame)
 *   - ../.e2e-tmp/pr239-analysis.json    (per-frame classification)
 *   - ../.e2e-tmp/pr239-screenshot.png   (final UI state)
 *   - ../.e2e-tmp/pr239-summary.txt      (human-readable summary)
 */
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const BASE = process.env.E2E_BASE || 'http://localhost:5173';
const OUT_DIR = process.env.E2E_OUT_DIR || path.resolve(__dirname, '..', '.e2e-tmp');
const PROJECT_NAME = process.env.E2E_PROJECT_NAME || 'syncode-feat-mcode-fork-synara';
const SEND_MESSAGE = process.env.E2E_PROMPT ||
  'Use Glob to find "**/*.rs" files in crates/syncode-provider, then use Grep to find "ProviderEvent::Reasoning". Report what you find. Be concise.';
const POLL_MS = 800;
const TURN_TIMEOUT_MS = Number(process.env.E2E_TURN_TIMEOUT_MS || 180_000);

fs.mkdirSync(OUT_DIR, { recursive: true });

// Browser-side WS shim: capture every push/orchestration frame verbatim.
const INJECT = `
(function(){
  window.__e2e = { wsSent: [], wsRecv: [], rawPush: [], consoleErrors: [] };
  var oe = console.error;
  console.error = function(){
    try {
      var m = Array.prototype.map.call(arguments, function(a){
        if (typeof a === 'string') return a;
        if (a && a.message) return a.message;
        try { return JSON.stringify(a); } catch(_) { return '?'; }
      }).join(' ');
      window.__e2e.consoleErrors.push(m);
    } catch(_) {}
    oe.apply(console, arguments);
  };
  var N = window.WebSocket;
  function W(url, protocols){
    var ws = protocols ? new N(url, protocols) : new N(url);
    ws.addEventListener('message', function(ev){
      try {
        if (typeof ev.data !== 'string') return;
        window.__e2e.wsRecv.push(ev.data);
        var j = JSON.parse(ev.data);
        if (j && j.method === 'push/orchestration') {
          window.__e2e.rawPush.push(j.params);
        }
      } catch(_) {}
    });
    var os = ws.send.bind(ws);
    ws.send = function(d){
      try { if (typeof d === 'string') window.__e2e.wsSent.push(d); } catch(_) {}
      return os(d);
    };
    return ws;
  }
  W.prototype = N.prototype;
  W.CONNECTING = N.CONNECTING;
  W.OPEN = N.OPEN;
  W.CLOSING = N.CLOSING;
  W.CLOSED = N.CLOSED;
  window.WebSocket = W;
})();
`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function clickForce(loc, timeout = 8000) {
  try {
    await loc.click({ timeout: Math.min(timeout, 4000) });
    return 'normal';
  } catch (_) {}
  try {
    await loc.click({ force: true, timeout });
    return 'force';
  } catch (_) {}
  return 'failed';
}

function classifyFrame(p) {
  // The wire envelope is:
  //   { eventType: "ActivityLogged", aggregateId, data: { event_type, data: {snake_case} } }
  // unwrapPayload in adaptPushEvent.ts extracts env.data.data as `raw`.
  const eventType = p && p.eventType;
  const data = p && p.data;
  const inner = data && typeof data === 'object' && data.data && typeof data.data === 'object'
    ? data.data
    : null;
  const activityType = inner ? inner.activity_type : undefined;
  const turnId = inner ? inner.turn_id : undefined;
  return {
    eventType,
    activityType,
    turnId,
    innerKeys: inner ? Object.keys(inner) : null,
    hasDescription: !!(inner && typeof inner.description === 'string'),
    description: inner && typeof inner.description === 'string' ? inner.description : undefined,
  };
}

// Format a classified frame as a one-line realtime snippet. Returns null for
// frames that aren't worth showing (snapshots, historical replay). The emoji
// prefix lets the user scan the stream visually: 🧠 thinking, 🔍 explore,
// 🔧 tool, 🤖 subagent, ⚡ skill, ✅ done, ❌ fail.
function formatFrameSnippet(c, t) {
  if (!c || !c.eventType) return null;
  // Skip snapshots — they're full-state replays, not realtime activity.
  if (c.eventType === 'snapshot') return null;
  const desc = c.description ? truncate(c.description, 80) : '';
  switch (c.eventType) {
    case 'TurnStarted':
      return `  [${t}] ▶ TurnStarted`;
    case 'TurnCompleted':
      return `  [${t}] ✅ TurnCompleted`;
    case 'TurnFailed':
      return `  [${t}] ❌ TurnFailed`;
    case 'MessageAdded':
      return `  [${t}] 💬 MessageAdded`;
    case 'ActivityLogged': {
      const kind = c.activityType || 'unknown';
      const tag = activityTag(kind);
      return `  [${t}] ${tag} ${kind}${desc ? ' :: ' + desc : ''}`;
    }
    default:
      return `  [${t}] • ${c.eventType}`;
  }
}

function activityTag(kind) {
  if (kind === 'provider_reasoning') return '🧠';
  if (kind === 'provider_tool_call') return '🔧';
  if (kind === 'provider_tool_result') return '📤';
  if (kind === 'provider_explore_started' || kind === 'provider_explore_updated') return '🔍';
  if (kind === 'provider_subagent_started' || kind === 'provider_subagent_completed') return '🤖';
  if (kind === 'provider_skill_dispatched') return '⚡';
  return '•';
}

function truncate(s, n) {
  if (s.length <= n) return s;
  return s.slice(0, n - 1) + '…';
}

(async () => {
  console.log('[pr239-harness] launching chromium (headless)');
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();

  console.log('[pr239-harness] navigating to', BASE);
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(3000);

  // open the project's first thread
  console.log('[pr239-harness] opening thread in project', PROJECT_NAME);
  const threadBtn = page.locator(`[aria-label="Create new thread in ${PROJECT_NAME}"]`).first();
  await clickForce(threadBtn, 8000);
  await page.waitForTimeout(1500);

  // type + send the prompt
  const editor = page.locator('[contenteditable="true"]').first();
  await editor.click({ force: true });
  await page.waitForTimeout(200);
  await page.keyboard.type(SEND_MESSAGE, { delay: 8 });
  await page.waitForTimeout(400);

  const prePushN = await page.evaluate(() => window.__e2e.rawPush.length);
  console.log('[pr239-harness] sending prompt; pre-send push frames:', prePushN);
  await page.keyboard.press('Enter');

  // Wait for TurnStarted first (so we know our turn actually fired), then poll
  // for the matching TurnCompleted AFTER prePushN (ignore historical frames).
  // Each poll also prints any newly-arrived frames so the user can watch the
  // activity stream in real time (thinking → explore → tool_call → tool_result).
  console.log('[pr239-harness] polling for turn completion (up to', TURN_TIMEOUT_MS, 'ms)');
  console.log('[pr239-harness] --- realtime activity stream ---');
  const t0 = Date.now();
  let turnStartedAt = -1;
  let completionSeen = false;
  let lastSeenN = prePushN;
  while (Date.now() - t0 < TURN_TIMEOUT_MS) {
    const allPushes = await page.evaluate(() => window.__e2e.rawPush);
    // Print newly-arrived frames since the last poll.
    for (let i = lastSeenN; i < allPushes.length; i++) {
      const c = classifyFrame(allPushes[i]);
      const t = ((Date.now() - t0) / 1000).toFixed(1) + 's';
      const snippet = formatFrameSnippet(c, t);
      if (snippet) console.log(snippet);
    }
    lastSeenN = allPushes.length;
    const newPushes = allPushes.slice(prePushN);
    if (turnStartedAt < 0) {
      const idx = newPushes.findIndex((p) => {
        const et = p && p.eventType;
        return et === 'TurnStarted' || et === 'turn_started';
      });
      if (idx >= 0) {
        turnStartedAt = Date.now() - t0;
        console.log('[pr239-harness] TurnStarted at t=' + turnStartedAt + 'ms (frame +' + idx + ')');
      }
    }
    // Only look for completion AFTER TurnStarted has been seen. Match the
    // top-level eventType (the wire envelope uses PascalCase event names).
    if (turnStartedAt >= 0) {
      completionSeen = newPushes.some((p) => {
        const et = p && p.eventType;
        return et === 'TurnCompleted' || et === 'turn_completed'
            || et === 'TurnFailed' || et === 'turn_failed';
      });
      if (completionSeen) {
        console.log('[pr239-harness] completion frame at t=' + (Date.now() - t0) + 'ms');
        break;
      }
    }
    await sleep(POLL_MS);
  }
  console.log('[pr239-harness] --- end realtime stream ---');
  if (!completionSeen) {
    console.log('[pr239-harness] WARNING: no completion frame seen before timeout');
  }

  // give the UI 2s to settle after completion
  await sleep(2000);

  const rawPush = await page.evaluate(() => window.__e2e.rawPush);
  const consoleErrors = await page.evaluate(() => window.__e2e.consoleErrors);
  const analysis = rawPush.map(classifyFrame);

  // write outputs
  fs.writeFileSync(path.join(OUT_DIR, 'pr239-raw-frames.json'), JSON.stringify(rawPush, null, 2));
  fs.writeFileSync(path.join(OUT_DIR, 'pr239-analysis.json'), JSON.stringify(analysis, null, 2));
  await page.screenshot({
    path: path.join(OUT_DIR, 'pr239-screenshot.png'),
    fullPage: true,
  });

  // summary
  const activityTypeCounts = {};
  for (const a of analysis) {
    if (!a.activityType) continue;
    activityTypeCounts[a.activityType] = (activityTypeCounts[a.activityType] || 0) + 1;
  }
  const newVariants = [
    'provider_reasoning',
    'provider_skill_dispatched',
    'provider_subagent_started',
    'provider_subagent_completed',
    'provider_explore_started',
    'provider_explore_updated',
    'provider_tool_call',
    'provider_tool_result',
  ];
  const observedNew = newVariants.filter((v) => activityTypeCounts[v] > 0);
  const summary = [
    `PR #239 e2e harness summary`,
    `============================`,
    `Total push frames captured: ${rawPush.length}`,
    `Activity types observed:`,
    ...Object.entries(activityTypeCounts).map(([k, v]) => `  ${k}: ${v}`),
    ``,
    `PR #239 new variants observed: ${observedNew.length}/${newVariants.length}`,
    ...newVariants.map((v) => `  ${activityTypeCounts[v] ? '[x]' : '[ ]'} ${v}`),
    ``,
    `Console errors: ${consoleErrors.length}`,
    ...consoleErrors.slice(0, 5).map((e) => `  - ${e.slice(0, 200)}`),
  ].join('\n');
  fs.writeFileSync(path.join(OUT_DIR, 'pr239-summary.txt'), summary);

  console.log('\n' + summary);
  console.log('\n[pr239-harness] wrote raw frames + analysis to', OUT_DIR);

  await browser.close();
})().catch((e) => {
  console.error('FATAL', e);
  process.exit(1);
});

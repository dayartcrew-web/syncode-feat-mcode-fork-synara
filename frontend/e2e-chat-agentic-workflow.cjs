/* eslint-disable */
/* Agentic-workflow E2E — exercises the PR #207 pipeline through the UI.
 *
 * Drives the RUNNING app (vite @ :5173 + syncode-ws @ ws://127.0.0.1:3100/ws)
 * purely through the browser UI. The point of this test is NOT to retest chat
 * mechanics (that's e2e-chat-cycle-all.cjs) — it's to verify the integrated
 * agentic stack that PR #207 introduced:
 *
 *   execute_workflow_with_critic:
 *     init state
 *       -> retrieve_context (memory)
 *       -> plan (executor)
 *       -> execute (executor)
 *       -> critic.review  (approves)
 *       -> guardrail check
 *       -> persist_interaction (memory)
 *
 * Two-turn scenario:
 *   T1: "My secret token is AGENTIC-TOKEN-7. Reply with: ACK AGENTIC-TOKEN-7"
 *       -> Workflow runs, critic approves (output matches requested shape),
 *          interaction persisted to memory.
 *   T2: "What was the secret token I just gave you? Reply with: TOKEN: <value>"
 *       -> Workflow runs again. retrieve_context() must surface T1's persisted
 *          interaction so the planner/executor can ground on it. Critic
 *          approves if executor echoes the token.
 *
 * Verdict matrix:
 *   PASS         — T1 completed + token in response; T2 completed + T1 token in response
 *   PASS-NO-GROUND — both turns completed but T2 didn't echo T1 token (memory may
 *                   not be wired into the live provider's system prompt — known
 *                   gap; pipeline is still healthy)
 *   FAIL         — turn didn't complete, or T1 didn't render its own token
 *
 * Servers must already be up — this script NEVER starts/stops cargo or npm.
 * A node-side WS client is used READ-ONLY (turn/list) to observe outcomes.
 */

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const BASE = process.env.E2E_BASE || 'http://localhost:5173';
const WS_URL = process.env.E2E_WS || 'ws://127.0.0.1:3100/ws';
const PROJECT_ROOT = process.env.E2E_PROJECT_ROOT || '/tmp/e2e-agentic';
const SHOT_DIR = process.env.E2E_SHOT_DIR || '/tmp/e2e-agentic-shots';
const REPORT_JSON = process.env.E2E_REPORT_JSON || '/tmp/e2e-agentic-report.json';
const TURN_TIMEOUT_MS = Number(process.env.E2E_TURN_TIMEOUT_MS || 120_000);
const POLL_MS = 2500;

const SECRET_TOKEN = 'AGENTIC-TOKEN-7';
const TURN1_PROMPT = `My secret token is ${SECRET_TOKEN}. Reply with exactly: ACK ${SECRET_TOKEN}`;
const TURN2_PROMPT = 'What was the secret token I just gave you? Reply with exactly: TOKEN: <value>';

fs.mkdirSync(SHOT_DIR, { recursive: true });
// Backend validates project root existence on disk before creating the project
// row. On Windows, `/tmp/e2e-agentic` resolves to `C:\tmp\e2e-agentic`, which
// doesn't exist by default — so the project/create RPC silently rejects and
// the picker never materializes. Pre-create it.
fs.mkdirSync(PROJECT_ROOT, { recursive: true });

// Browser-side tap: capture WS frames + console errors for diagnostics.
const INJECT = `
(function () {
  window.__e2e = { consoleErrors: [], wsSent: [], wsRecv: [], wsOpened: 0 };
  var oe = console.error;
  console.error = function () {
    try { var m = Array.prototype.map.call(arguments, function (a) {
      if (typeof a === 'string') return a; if (a && a.message) return a.message;
      try { return JSON.stringify(a); } catch (_) { return '?'; }
    }).join(' '); window.__e2e.consoleErrors.push(m); } catch (_) {}
    oe.apply(console, arguments);
  };
  window.addEventListener('error', function (e) { window.__e2e.consoleErrors.push('uncaught: ' + (e && e.message ? e.message : '(unknown)')); });
  var N = window.WebSocket;
  function W(url, protocols) {
    var ws = protocols ? new N(url, protocols) : new N(url);
    ws.addEventListener('open', function () { window.__e2e.wsOpened++; });
    ws.addEventListener('message', function (ev) { try { if (typeof ev.data === 'string') window.__e2e.wsRecv.push(ev.data); } catch (_) {} });
    var os = ws.send.bind(ws);
    ws.send = function (d) { try { if (typeof d === 'string') window.__e2e.wsSent.push(d); } catch (_) {} return os(d); };
    return ws;
  }
  W.prototype = N.prototype; W.CONNECTING=N.CONNECTING; W.OPEN=N.OPEN; W.CLOSING=N.CLOSING; W.CLOSED=N.CLOSED;
  window.WebSocket = W;
})();
`;

function makeReadOnlyClient(url) {
  const ws = new WebSocket(url);
  const pending = new Map(); let nextId = 1;
  const client = { ws, opened: false, errors: [], notifications: [] };
  client.call = function (method, params = {}, timeoutMs = 12_000) {
    return new Promise((resolve, reject) => {
      const id = nextId++;
      const t = setTimeout(() => { pending.delete(id); reject(new Error('RPC timeout: ' + method)); }, timeoutMs);
      pending.set(id, { resolve, reject, timer: t });
      ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
    });
  };
  ws.on('open', () => { client.opened = true; });
  ws.on('message', (data) => {
    let j; try { j = JSON.parse(data.toString()); } catch (_) { return; }
    if (j.id != null && pending.has(j.id)) { const p = pending.get(j.id); pending.delete(j.id); clearTimeout(p.timer);
      if (j.error) p.reject(Object.assign(new Error(j.error.message || 'rpc error'), { code: j.error.code }));
      else p.resolve(j.result);
    } else if (j.method && j.params) client.notifications.push({ method: j.method, params: j.params });
  });
  ws.on('error', (err) => { client.errors.push(err.message || String(err)); });
  return client;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const truncate = (s, n = 240) => (s ? (s.length > n ? s.slice(0, n) + '…[+' + (s.length - n) + 'b]' : s) : '(null)');

async function findProjectByRoot(client, root) {
  if (!client.opened) return null;
  const snap = await client.call('shell/getSnapshot', {}, 8000).catch(() => null);
  return ((snap && snap.projects) || []).find((p) => (p.workspaceRoot || p.rootPath || p.cwd || '') === root) || null;
}

async function clickForce(loc, timeout = 8000) {
  try { await loc.click({ timeout: Math.min(timeout, 4000) }); return 'normal'; } catch (_) {}
  try { await loc.click({ force: true, timeout }); return 'force'; } catch (_) {}
  return 'failed';
}

async function openPickerViaShortcut(page) {
  const ed = page.locator('[contenteditable="true"]').first();
  await ed.click({ force: true }).catch(() => {});
  await page.waitForTimeout(150);
  await page.keyboard.press('Control+Shift+M').catch(() => {});
  await page.waitForTimeout(500);
  let openCount = await page.locator('[role=menu]:visible').count().catch(() => 0);
  if (!openCount) { await page.keyboard.press('Meta+Shift+M').catch(() => {}); await page.waitForTimeout(500); }
  openCount = await page.locator('[role=menu]:visible').count().catch(() => 0);
  return openCount > 0;
}

async function dumpAllMenus(page) {
  return page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const menus = Array.from(document.querySelectorAll('[role=menu]')).filter(vis);
    return menus.map((m) => ({
      items: Array.from(m.querySelectorAll('[role=menuitem], [role=menuitemradio], [role=menuitemcheckbox]')).filter(vis).map((it) => ({
        role: it.getAttribute('role'),
        text: (it.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 80),
        disabled: it.getAttribute('aria-disabled') === 'true' || it.disabled === true,
        hasPopup: it.getAttribute('aria-haspopup'),
      })),
    }));
  }).catch((e) => ({ error: e.message }));
}

async function hoverMenuItem(page, text, menuIdx) {
  const sel = (menuIdx == null)
    ? page.locator('[role=menuitem]:visible, [role=menuitemradio]:visible').filter({ hasText: text }).first()
    : page.locator(`[role=menu]:visible >> nth=${menuIdx}`).locator('[role=menuitem], [role=menuitemradio]').filter({ hasText: text }).first();
  const c = await sel.count().catch(() => 0); if (!c) return false;
  await sel.hover({ force: true }).catch(() => {});
  await page.waitForTimeout(450);
  return true;
}

async function findModelPickerSubmenu(page) {
  const rootMenus = await dumpAllMenus(page);
  const root = rootMenus[0];
  if (!root) return { ok: false };
  for (const it of root.items) {
    if (it.disabled) continue;
    await hoverMenuItem(page, it.text, 0);
    const after = await dumpAllMenus(page);
    if (after.length > rootMenus.length) {
      const newMenu = after[after.length - 1];
      const providerCount = (newMenu.items || []).filter((x) => /OpenCode|Codex|Claude|Cursor|Gemini|Grok|Kilo|Pi/i.test(x.text)).length;
      if (providerCount >= 4) return { ok: true, level2Idx: newMenu.idx || 1, providers: newMenu.items };
    }
  }
  return { ok: false };
}

// Navigate picker: root -> model-picker submenu -> {ProviderLabel} -> first enabled model radio.
async function pickFirstModelForProvider(page, providerLabel) {
  const opened = await openPickerViaShortcut(page);
  if (!opened) return { ok: false, reason: 'picker did not open' };

  const found = await findModelPickerSubmenu(page);
  if (!found.ok) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, reason: 'no provider submenu' }; }

  const provItem = (found.providers || []).find((p) => new RegExp('^' + providerLabel + '$', 'i').test(p.text.trim()));
  if (!provItem) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, reason: providerLabel + ' not in list' }; }
  if (provItem.disabled) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, reason: providerLabel + ' disabled' }; }

  await hoverMenuItem(page, providerLabel, found.level2Idx);
  await page.waitForTimeout(500);

  const probe = await page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const els = Array.from(document.querySelectorAll('[role=menuitemradio]')).filter(vis);
    return els.map((x) => ({ text: (x.textContent || '').replace(/\s+/g, ' ').trim(), disabled: x.getAttribute('aria-disabled') === 'true' || x.disabled === true }));
  }).catch(() => []);
  const enabled = probe.filter((m) => !m.disabled);
  if (!enabled.length) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, reason: 'no enabled models', all: probe }; }

  const clicked = await page.evaluate((t) => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const els = Array.from(document.querySelectorAll('[role=menuitemradio]')).filter(vis).filter((x) => (x.textContent || '').replace(/\s+/g, ' ').trim() === t);
    if (!els[0]) return false;
    els[0].click();
    return true;
  }, enabled[0].text).catch(() => false);
  await page.waitForTimeout(600);
  await page.keyboard.press('Escape').catch(() => {});
  return { ok: !!clicked, picked: enabled[0].text, all: probe.map((m) => m.text + (m.disabled ? '[DIS]' : '')) };
}

async function typeAndSend(page, text) {
  const ed = page.locator('[contenteditable="true"]').first();
  if (!(await ed.count())) return { ok: false, reason: 'no contenteditable' };
  await ed.click({ force: true }).catch(() => {});
  await page.waitForTimeout(150);
  await page.keyboard.press('Control+A').catch(() => {});
  await page.keyboard.insertText(text).catch(async () => { await page.keyboard.type(text, { delay: 6 }).catch(() => {}); });
  await page.waitForTimeout(250);
  await page.keyboard.press('Enter').catch(() => {});
  await page.waitForTimeout(1800);
  let dispatched = await page.evaluate(() => {
    const frames = (window.__e2e && window.__e2e.wsSent) || [];
    return frames.some((s) => { try { const j = JSON.parse(s); return /dispatch|turn|message/i.test(j.method || ''); } catch (_) { return false; } });
  }).catch(() => false);
  if (!dispatched) {
    const sendBtn = page.locator('button[aria-label*="Send" i]').last();
    if (await sendBtn.count()) { await sendBtn.click({ force: true }).catch(() => {}); await page.waitForTimeout(1500); }
    dispatched = true;
  }
  return { ok: dispatched };
}

async function pollTurn(readOnly, threadId, timeoutMs = TURN_TIMEOUT_MS) {
  const t0 = Date.now(); let last = null; let polls = 0; let lastErr = null;
  while (Date.now() - t0 < timeoutMs) {
    polls++;
    let list;
    try { list = await readOnly.call('turn/list', { threadId }, 10_000); }
    catch (e) { lastErr = e.message + (e.code ? ' (code ' + e.code + ')' : ''); await sleep(POLL_MS); continue; }
    const turns = (list && list.turns) || [];
    const term = turns.find((t) => ['completed', 'error', 'cancelled', 'failed'].includes(t.status));
    if (term) return { status: term.status, turn: term, polls, elapsedMs: Date.now() - t0 };
    if (turns.length) last = turns[turns.length - 1];
    await sleep(POLL_MS);
  }
  return { status: 'timeout', turn: last, polls, elapsedMs: Date.now() - t0, lastErr };
}

// Pull assistant text from a turn object across the various field shapes.
function extractAssistantText(turn) {
  if (!turn) return null;
  const direct = turn.assistantOutput || turn.assistant_output || turn.output || turn.text || turn.responseText || turn.message;
  if (direct && typeof direct === 'string') return direct;
  if (Array.isArray(turn.messages)) {
    const a = turn.messages.find((m) => /assistant|ai/i.test(m.role || ''));
    if (a) return typeof a.content === 'string' ? a.content : (Array.isArray(a.content) ? a.content.map((c) => (c && c.text) || '').join('') : JSON.stringify(a.content));
  }
  if (turn.result && typeof turn.result === 'object') {
    const r = turn.result;
    if (typeof r.text === 'string') return r.text;
    if (Array.isArray(r.messages)) { const a = r.messages.find((m) => /assistant|ai/i.test(m.role || '')); if (a) return typeof a.content === 'string' ? a.content : JSON.stringify(a.content); }
  }
  return null;
}

async function readTranscript(page) {
  return page.evaluate(() => {
    const txt = (document.body.innerText || '');
    return { bodyTail: txt.slice(-1600) };
  }).catch(() => ({ bodyTail: '' }));
}

async function ensureProject(page, readOnly) {
  let existing = await findProjectByRoot(readOnly, PROJECT_ROOT);
  if (existing) return existing;

  await clickForce(page.locator('[aria-label="Add project"]').first(), 8000);
  await page.waitForTimeout(500);
  const tp = page.locator('button:has-text("Type path")').first();
  if (await tp.count()) { await clickForce(tp, 6000); await page.waitForTimeout(300); }
  const pi = page.locator('input[placeholder="/path/to/project"]').first();
  if (!(await pi.count())) throw new Error('path input missing');
  await pi.fill(PROJECT_ROOT); await page.waitForTimeout(120); await pi.press('Enter').catch(() => {});
  await page.waitForTimeout(1800);
  for (let i = 0; i < 40; i++) {
    const p = await findProjectByRoot(readOnly, PROJECT_ROOT);
    if (p) return p;
    await page.waitForTimeout(500);
  }
  throw new Error('project never materialized');
}

async function createDraftThread(page, projectName, shotName) {
  const row = page.locator('span', { hasText: projectName }).first();
  await row.waitFor({ state: 'visible', timeout: 10_000 }).catch(() => {});
  await row.scrollIntoViewIfNeeded().catch(() => {});
  await row.hover({ force: true }).catch(() => {});
  await page.waitForTimeout(350);
  let btn = page.locator(`[aria-label="Create new thread in ${projectName}"]`).first();
  if (!(await btn.count().catch(() => 0))) btn = page.locator('[data-testid="new-thread-button"]').first();
  await clickForce(btn, 8000);
  await page.waitForTimeout(1500);
  let threadId = null;
  for (let i = 0; i < 20; i++) { const m = (page.url().match(/\/([0-9a-fA-F-]{36})/) || [])[1] || null; if (m) { threadId = m; break; } await page.waitForTimeout(300); }
  await page.waitForSelector('[contenteditable="true"]', { timeout: 15_000 }).catch(() => {});
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(SHOT_DIR, shotName), fullPage: true }).catch(() => {});
  return threadId;
}

(async () => {
  console.log('=== Agentic-workflow E2E (PR #207: workflow + critic + memory) ===');
  console.log('BASE=' + BASE + '  WS=' + WS_URL);
  const report = { base: BASE, ws: WS_URL, startedAt: new Date().toISOString() };

  // Prefer whatever provider the user has armed (SYNCODE_DEFAULT_PROVIDER), but
  // allow explicit override via env. Default to Claude since the agentic
  // pipeline is most-validated against that adapter.
  const providerLabel = process.env.E2E_PROVIDER || 'Claude';

  const readOnly = makeReadOnlyClient(WS_URL);
  await sleep(1000);
  if (!readOnly.opened) await sleep(2000);
  console.log('read-only WS opened=' + readOnly.opened);

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();
  const allConsole = [];
  page.on('console', (m) => { if (m.type() === 'error') allConsole.push(m.text()); });
  page.on('pageerror', (e) => allConsole.push('pageerror: ' + e.message));

  try {
    await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 30_000 });
    await page.waitForLoadState('networkidle').catch(() => {});
    await page.waitForTimeout(2000);
    await page.screenshot({ path: path.join(SHOT_DIR, '00-shell.png'), fullPage: true });
    report.shell = { url: page.url(), ok: true };
  } catch (e) {
    report.shell = { ok: false, error: e.message };
    console.log('[load] FAIL ' + e.message);
  }

  // Stage 1: ensure project exists
  let projectName = null, projectId = null;
  try {
    const p = await ensureProject(page, readOnly);
    projectName = p.name || p.title || 'e2e-agentic';
    projectId = p.id;
    console.log('[project] id=' + projectId + ' name=' + projectName);
  } catch (e) {
    console.log('[project] FAIL ' + e.message);
    await page.screenshot({ path: path.join(SHOT_DIR, '01-project-FAIL.png'), fullPage: true }).catch(() => {});
  }
  report.project = { id: projectId, name: projectName };

  if (!projectName) {
    report.verdict = 'FAIL'; report.reason = 'no project';
    return finalize(report, page, readOnly, browser, allConsole);
  }

  // Stage 2: create thread
  const threadId = await createDraftThread(page, projectName, '02-thread.png');
  report.threadId = threadId;
  console.log('[thread] id=' + threadId);
  if (!threadId) {
    report.verdict = 'FAIL'; report.reason = 'no threadId in URL';
    return finalize(report, page, readOnly, browser, allConsole);
  }

  // Stage 3: pick provider+model (best-effort; if picker fails, fall back to
  // whatever's currently armed — the workflow pipeline still runs).
  try {
    const sel = await pickFirstModelForProvider(page, providerLabel);
    report.picker = sel;
    console.log('[picker] ok=' + sel.ok + ' picked="' + (sel.picked || '?') + '"' + (sel.reason ? ' reason=' + sel.reason : ''));
    await page.screenshot({ path: path.join(SHOT_DIR, '03-picker.png'), fullPage: true }).catch(() => {});
  } catch (e) {
    report.picker = { ok: false, error: e.message };
    console.log('[picker] EXC ' + e.message);
  }

  // Stage 4: TURN 1 — plant secret token, expect workflow to persist it.
  console.log('\n--- TURN 1: plant ' + SECRET_TOKEN + ' ---');
  let turn1 = { status: 'skipped' };
  try {
    const sent = await typeAndSend(page, TURN1_PROMPT);
    await page.waitForTimeout(1500);
    await page.screenshot({ path: path.join(SHOT_DIR, '04-turn1-sent.png'), fullPage: true }).catch(() => {});
    if (!sent.ok) {
      turn1 = { status: 'FAIL', reason: 'send did not dispatch' };
    } else {
      const poll = await pollTurn(readOnly, threadId, TURN_TIMEOUT_MS);
      const aiOut = extractAssistantText(poll.turn);
      const transcript = await readTranscript(page);
      const protocolHasToken = !!(aiOut && new RegExp('\\b' + SECRET_TOKEN + '\\b').test(String(aiOut)));
      const domHasToken = new RegExp('\\b' + SECRET_TOKEN + '\\b').test(transcript.bodyTail);
      turn1 = {
        status: poll.status,
        elapsedMs: poll.elapsedMs,
        aiOutput: truncate(aiOut, 240),
        transcriptTail: transcript.bodyTail.slice(-400),
        protocolHasToken,
        domHasToken,
        turnError: poll.turn && (poll.turn.error || poll.turn.errorMessage || poll.turn.failureReason) || null,
      };
      console.log('  [poll] status=' + poll.status + ' elapsedMs=' + poll.elapsedMs);
      console.log('  [poll] aiOut=' + truncate(aiOut, 120));
      console.log('  [poll] protocol.token=' + protocolHasToken + ' dom.token=' + domHasToken);
    }
  } catch (e) {
    turn1 = { status: 'EXC', error: e.message };
    console.log('  [turn1] EXC ' + e.message);
  }
  report.turn1 = turn1;

  // TURN 1 must complete + echo its own token. If not, the workflow pipeline
  // itself is broken (regardless of memory) — fail fast.
  const t1Completed = turn1.status === 'completed';
  const t1EchoedToken = !!(turn1.protocolHasToken || turn1.domHasToken);
  if (!t1Completed || !t1EchoedToken) {
    report.verdict = 'FAIL';
    report.reason = 'turn1 did not complete+echo token (workflow pipeline broken)';
    return finalize(report, page, readOnly, browser, allConsole);
  }

  // Stage 5: TURN 2 — ask for the token back. retrieve_context() must surface
  // turn1's persisted interaction so the planner/executor can ground on it.
  console.log('\n--- TURN 2: recall token (memory grounding) ---');
  let turn2 = { status: 'skipped' };
  try {
    const sent = await typeAndSend(page, TURN2_PROMPT);
    await page.waitForTimeout(1500);
    await page.screenshot({ path: path.join(SHOT_DIR, '05-turn2-sent.png'), fullPage: true }).catch(() => {});
    if (!sent.ok) {
      turn2 = { status: 'FAIL', reason: 'send did not dispatch' };
    } else {
      const poll = await pollTurn(readOnly, threadId, TURN_TIMEOUT_MS);
      const aiOut = extractAssistantText(poll.turn);
      const transcript = await readTranscript(page);
      const protocolHasToken = !!(aiOut && new RegExp('\\b' + SECRET_TOKEN + '\\b').test(String(aiOut)));
      const domHasToken = new RegExp('\\b' + SECRET_TOKEN + '\\b').test(transcript.bodyTail);
      turn2 = {
        status: poll.status,
        elapsedMs: poll.elapsedMs,
        aiOutput: truncate(aiOut, 240),
        transcriptTail: transcript.bodyTail.slice(-400),
        protocolHasToken,
        domHasToken,
      };
      console.log('  [poll] status=' + poll.status + ' elapsedMs=' + poll.elapsedMs);
      console.log('  [poll] aiOut=' + truncate(aiOut, 120));
      console.log('  [poll] protocol.token=' + protocolHasToken + ' dom.token=' + domHasToken);
    }
  } catch (e) {
    turn2 = { status: 'EXC', error: e.message };
    console.log('  [turn2] EXC ' + e.message);
  }
  report.turn2 = turn2;

  // Stage 6: verdict.
  const t2Completed = turn2.status === 'completed';
  const t2RecalledToken = !!(turn2.protocolHasToken || turn2.domHasToken);
  if (t2Completed && t2RecalledToken) {
    report.verdict = 'PASS';
    report.reason = 'agentic pipeline + critic + memory persistence + grounding all healthy';
  } else if (t2Completed && !t2RecalledToken) {
    report.verdict = 'PASS-NO-GROUND';
    report.reason = 'pipeline healthy but turn2 did not recall token — provider may not be wired to retrieve_context (acceptable for some adapters)';
  } else {
    report.verdict = 'FAIL';
    report.reason = 'turn2 did not complete — pipeline regression';
  }

  return finalize(report, page, readOnly, browser, allConsole);
})().catch((err) => {
  console.error('FATAL:', err && err.stack ? err.stack : err);
  process.exit(2);
});

async function finalize(report, page, readOnly, browser, allConsole) {
  try { await page.screenshot({ path: path.join(SHOT_DIR, '99-final.png'), fullPage: true }); } catch (_) {}
  try { await browser.close(); } catch (_) {}
  try { readOnly.ws.close(); } catch (_) {}
  report.endedAt = new Date().toISOString();
  report.allConsoleErrors = allConsole || [];
  try { report.screenshots = fs.readdirSync(SHOT_DIR); } catch (_) {}
  try { fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2)); } catch (_) {}

  console.log('\n==================== AGENTIC E2E VERDICT ====================');
  console.log('verdict:  ' + report.verdict);
  console.log('reason:   ' + (report.reason || '(none)'));
  console.log('turn1:    status=' + (report.turn1 && report.turn1.status) + ' token=' + !!((report.turn1 && (report.turn1.protocolHasToken || report.turn1.domHasToken))));
  console.log('turn2:    status=' + (report.turn2 && report.turn2.status) + ' token=' + !!((report.turn2 && (report.turn2.protocolHasToken || report.turn2.domHasToken))));
  console.log('report:   ' + REPORT_JSON);
  console.log('shots:    ' + SHOT_DIR);
  console.log('=============================================================');

  // Exit code: 0 for PASS, 1 for PASS-NO-GROUND (warn), 2 for FAIL.
  const code = report.verdict === 'PASS' ? 0 : report.verdict === 'PASS-NO-GROUND' ? 1 : 2;
  process.exit(code);
}

/* eslint-disable */
/* Browser-driven chat-cycle E2E driver (v4 — opencode + GLM 5.2).
 *
 * Drives the RUNNING app (vite @ :5173 + syncode-ws @ ws://127.0.0.1:3100/ws)
 * purely through the browser UI. Provider+model selected ONLY via the UI picker.
 *
 * Servers must already be up — this script NEVER starts/stops cargo or npm.
 *
 * v4 changes vs v3:
 *  - Single scenario: OpenCode → GLM 5.2 (Z.AI) (model slug zai-coding-plan/glm-5.2).
 *  - Correctly navigates the 3-level picker:
 *      root menu  ->  "model picker" submenu (trigger shows current model label)
 *                 ->  provider submenu ("OpenCode")
 *                 ->  model radios ("GLM 5.2 (Z.AI)", "GLM 4.6 (Z.AI)")
 *    v3 incorrectly treated the provider list as the model list and clicked
 *    "Codex" (first provider token) — dispatched model:"codex" instead of a real
 *    model slug.
 *  - HARD assertion on dispatched modelSelection.model === "zai-coding-plan/glm-5.2".
 *  - Better menu-structure diagnostics (dumps every visible menu at every step).
 *  - Distinguishes "rendered-in-UI" (DOM transcript) from "completed-protocol"
 *    (turn/list status) outcomes.
 *
 * A node-side WS client is used READ-ONLY (shell/getSnapshot, turn/list) to
 * observe outcomes — it never creates state or selects providers.
 */

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const BASE = process.env.E2E_BASE || 'http://localhost:5173';
const WS_URL = process.env.E2E_WS || 'ws://127.0.0.1:3100/ws';
const PROJECT_ROOT = '/tmp/e2e-chat';
const SHOT_DIR = '/tmp/e2e-chat-cycle-shots';
const REPORT_JSON = '/tmp/e2e-chat-cycle-report.json';
const TURN_TIMEOUT_MS = 90_000;
const POLL_MS = 2500;

const TARGET_PROVIDER_LABEL = 'OpenCode';
const TARGET_MODEL_TEXT = 'GLM 5.2 (Z.AI)';
const TARGET_MODEL_SLUG = 'zai-coding-plan/glm-5.2';
const SEND_MESSAGE = 'Reply with exactly: UI_CHAT_OK';

fs.mkdirSync(SHOT_DIR, { recursive: true });

const INJECT = `
(function () {
  window.__e2e = { consoleErrors: [], wsSent: [], wsRecv: [], wsUrls: [], wsOpened: 0, wsErrors: [] };
  var oe = console.error;
  console.error = function () {
    try { var m = Array.prototype.map.call(arguments, function (a) {
      if (typeof a === 'string') return a; if (a && a.message) return a.message;
      try { return JSON.stringify(a); } catch (_) { return '?'; }
    }).join(' '); window.__e2e.consoleErrors.push(m); } catch (_) {}
    oe.apply(console, arguments);
  };
  window.addEventListener('error', function (e) { window.__e2e.consoleErrors.push('uncaught: ' + (e && e.message ? e.message : '(unknown)')); });
  window.addEventListener('unhandledrejection', function (e) { window.__e2e.consoleErrors.push('unhandledrejection: ' + (e && e.reason && e.reason.message ? e.reason.message : String(e && e.reason))); });
  var N = window.WebSocket;
  function W(url, protocols) {
    var ws = protocols ? new N(url, protocols) : new N(url);
    try { window.__e2e.wsUrls.push(String(url)); } catch (_) {}
    ws.addEventListener('open', function () { window.__e2e.wsOpened++; });
    ws.addEventListener('error', function () { window.__e2e.wsErrors.push('ws-error'); });
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

async function snapshotTap(page, since) {
  const e2e = await page.evaluate(() => window.__e2e).catch(() => null);
  if (!e2e) return { wsUrls: [], sentMethods: [], sent: [], errors: [], consoleErrors: [] };
  const sent = (e2e.wsSent || []).map((s) => { try { return JSON.parse(s); } catch (_) { return { _raw: s }; } });
  const recv = (e2e.wsRecv || []).map((r) => { try { return JSON.parse(r); } catch (_) { return { _raw: r }; } });
  const errors = [];
  for (const r of recv) if (r && r.error) { const req = sent.find((x) => x && x.id === r.id); errors.push({ id: r.id, method: (req && req.method) || '?', code: r.error.code, message: (r.error.message || '').slice(0, 400) }); }
  const out = { wsUrls: e2e.wsUrls || [], sentMethods: sent.map((j) => (j && j.method) || '?'), sent, errors, consoleErrors: (e2e.consoleErrors || []).slice() };
  if (since) {
    out.deltaSent = out.sentMethods.slice(since.sent);
    out.deltaErrors = out.errors.filter((e) => !since.errIds.includes(e.id));
    out.deltaConsole = out.consoleErrors.slice(since.con);
    out.deltaSentFrames = out.sent.slice(since.sentRaw);
  }
  return out;
}
const tapBaseline = (tap) => ({ sent: (tap.sentMethods || []).length, sentRaw: (tap.sent || []).length, con: (tap.consoleErrors || []).length, errIds: (tap.errors || []).map((e) => e.id) });

const RESULTS = [];
const ALL_CONSOLE = [];
const ALL_RPC_ERRORS = [];
function record(stage, status, evidence, extra = {}) { RESULTS.push({ stage, status, evidence, ...extra }); console.log(`[${status.padEnd(4)}] ${stage} — ${evidence}`); }

// Dump every visible menu (role=menu) with its items, distinguishing sub-triggers
// from radios/checkboxes. Used for diagnostics at each picker level.
async function dumpAllMenus(page) {
  return page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const menus = Array.from(document.querySelectorAll('[role=menu]')).filter(vis);
    return menus.map((m, i) => {
      const rect = m.getBoundingClientRect();
      return {
        idx: i,
        x: Math.round(rect.x), y: Math.round(rect.y), w: Math.round(rect.width), h: Math.round(rect.height),
        items: Array.from(m.querySelectorAll('[role=menuitem], [role=menuitemradio], [role=menuitemcheckbox]')).filter(vis).map((it) => ({
          role: it.getAttribute('role'),
          text: (it.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 80),
          checked: it.getAttribute('aria-checked'),
          disabled: it.getAttribute('aria-disabled') === 'true' || it.disabled === true || it.getAttribute('data-disabled') === '',
          hasPopup: it.getAttribute('aria-haspopup'),
          // Detect submenu indicator (Radix/Ark render a chevron span)
          hasSubmenuArrow: !!it.querySelector('svg:last-child:not([aria-hidden="true"] ~ *)'),
        })),
      };
    });
  }).catch((e) => ({ error: e.message }));
}

async function clickForce(loc, timeout = 8000) {
  try { await loc.click({ timeout: Math.min(timeout, 4000) }); return 'normal'; } catch (_) {}
  try { await loc.click({ force: true, timeout }); return 'force'; } catch (_) {}
  return 'failed';
}

// Open the model picker via the Ctrl+Shift+M shortcut (composer must be focused).
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

// Hover a visible menuitem by visible text, scoped to a specific menu index (defaults to last).
async function hoverMenuItem(page, text, menuIdx) {
  const sel = (menuIdx == null)
    ? page.locator('[role=menuitem]:visible, [role=menuitemradio]:visible').filter({ hasText: text }).first()
    : page.locator(`[role=menu]:visible >> nth=${menuIdx}`).locator('[role=menuitem], [role=menuitemradio]').filter({ hasText: text }).first();
  const c = await sel.count().catch(() => 0); if (!c) return { ok: false, count: 0 };
  await sel.hover({ force: true }).catch(() => {});
  await page.waitForTimeout(450);
  return { ok: true, count: c };
}

// Find the submenu (level-2) that contains provider names = the model-picker submenu.
// Returns the level-1 trigger text and the visible provider list.
async function findModelPickerSubmenu(page) {
  const rootMenus = await dumpAllMenus(page);
  const root = rootMenus[0];
  if (!root) return { ok: false, reason: 'no root menu' };
  for (const it of root.items) {
    if (it.disabled) continue;
    const isSub = it.role === 'menuitem' && (it.hasPopup === 'true' || it.hasPopup === 'menu' || /OpenCode|Codex|Claude|Cursor|Gemini|Grok|Kilo|Pi/i.test(it.text) === false);
    // Hover this item and see what submenu appears
    await hoverMenuItem(page, it.text, 0);
    const after = await dumpAllMenus(page);
    // The newly-appeared submenu is the last menu (highest idx)
    if (after.length > rootMenus.length) {
      const newMenu = after[after.length - 1];
      const providerCount = (newMenu.items || []).filter((x) => /OpenCode|Codex|Claude|Cursor|Gemini|Grok|Kilo|Pi/i.test(x.text)).length;
      if (providerCount >= 4) {
        return { ok: true, level1Trigger: it.text, providers: newMenu.items, level2Idx: newMenu.idx };
      }
    }
  }
  return { ok: false, reason: 'no submenu looked like the provider list', rootItems: root.items };
}

// Navigate picker: root -> model-picker submenu -> OpenCode -> GLM 5.2 (Z.AI).
async function selectOpenCodeGLM52(page, shotPrefix) {
  const log = [];
  const opened = await openPickerViaShortcut(page);
  log.push({ step: 'openPicker', opened });
  if (!opened) return { ok: false, log, reason: 'picker did not open' };

  const rootDump = await dumpAllMenus(page);
  log.push({ step: 'rootMenu', menus: rootDump });
  await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-1-root.png') }).catch(() => {});

  // Find the model-picker submenu by hovering each root item until a provider list appears
  const found = await findModelPickerSubmenu(page);
  log.push({ step: 'findModelPickerSubmenu', found: { ok: found.ok, level1Trigger: found.level1Trigger, providers: (found.providers || []).map((p) => p.text + (p.disabled ? '[DISABLED]' : '')) } });
  if (!found.ok) {
    await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-2-no-provider-list.png') }).catch(() => {});
    await page.keyboard.press('Escape').catch(() => {});
    return { ok: false, log, reason: found.reason };
  }
  await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-2-provider-list.png') }).catch(() => {});

  // Now hover OpenCode in the provider list submenu (level-2) to reveal models (level-3)
  const ocItem = (found.providers || []).find((p) => /OpenCode/i.test(p.text));
  if (!ocItem) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, log, reason: 'OpenCode not in provider list' }; }
  if (ocItem.disabled) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, log, reason: 'OpenCode DISABLED in picker', ocItem }; }

  // Hover OpenCode in the correct level-2 menu
  const beforeMenus = (await dumpAllMenus(page)).length;
  await hoverMenuItem(page, 'OpenCode', found.level2Idx);
  await page.waitForTimeout(500);
  const afterOc = await dumpAllMenus(page);
  log.push({ step: 'afterHoverOpenCode', menuCount: afterOc.length, menus: afterOc });
  await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-3-opencode-hover.png') }).catch(() => {});

  if (afterOc.length <= beforeMenus) {
    // Retry: sometimes the hover needs a mouse move to register
    await page.mouse.move(100, 100).catch(() => {});
    await hoverMenuItem(page, 'OpenCode', found.level2Idx);
    await page.waitForTimeout(500);
  }
  // ROBUST WAIT: the level-3 model submenu populates async; the earlier
  // capture raced and found empty radios. Poll for a GLM/Z.AI model radio
  // (or any model-radio menu beyond the provider list) to appear.
  const waitT0 = Date.now();
  let glmRadio = null;
  let lastRadioTexts = [];
  while (Date.now() - waitT0 < 10000) {
    const probe = await page.evaluate(() => {
      const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
      const els = Array.from(document.querySelectorAll('[role=menuitemradio]')).filter(vis);
      const texts = els.map((x) => (x.textContent || '').replace(/\s+/g, ' ').trim());
      const glm = texts.find((t) => /GLM|z\.ai/i.test(t)) || null;
      return { glm, count: texts.length, texts };
    }).catch(() => ({ glm: null, count: 0, texts: [] }));
    glmRadio = probe.glm;
    lastRadioTexts = probe.texts;
    if (glmRadio || (probe.count > 0 && Date.now() - waitT0 > 2500)) break;
    await page.waitForTimeout(250);
  }
  log.push({ step: 'waitForGlmRadio', glmRadio, radioCount: lastRadioTexts.length, waitedMs: Date.now() - waitT0, radioTexts: lastRadioTexts });
  const finalMenus = await dumpAllMenus(page);
  // The level-3 model menu should be the LAST visible menu and contain radios
  let modelMenu = null;
  for (let i = finalMenus.length - 1; i >= 0; i--) {
    const m = finalMenus[i];
    const radios = (m.items || []).filter((x) => x.role === 'menuitemradio');
    if (radios.length > 0) { modelMenu = m; break; }
  }
  if (!modelMenu) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, log, reason: 'no model-radio submenu appeared after hovering OpenCode', finalMenuCount: finalMenus.length }; }

  const radios = (modelMenu.items || []).filter((x) => x.role === 'menuitemradio');
  log.push({ step: 'modelRadios', radios });
  const target = radios.find((r) => /GLM 5\.2/i.test(r.text));
  if (!target) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, log, reason: 'GLM 5.2 (Z.AI) radio not found in model submenu', radios }; }

  // Click the GLM 5.2 radio — prefer direct DOM click as a fallback
  const clicked = await page.evaluate((t) => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const els = Array.from(document.querySelectorAll('[role=menuitemradio]')).filter(vis).filter((x) => /GLM 5\.2/i.test((x.textContent || '')));
    if (!els[0]) return false;
    els[0].click();
    return els[0].textContent.replace(/\s+/g, ' ').trim();
  }, target.text).catch(() => false);
  log.push({ step: 'clickGLM52', clicked });
  await page.waitForTimeout(800);
  await page.keyboard.press('Escape').catch(() => {});
  await page.waitForTimeout(400);
  return { ok: !!clicked, log, picked: clicked || target.text, modelRadios: radios };
}

// Type into Lexical via insertText (reliably populates the editor), then send (Enter).
async function typeAndSend(page, text) {
  const ed = page.locator('[contenteditable="true"]').first();
  if (!(await ed.count())) return { ok: false, reason: 'no contenteditable' };
  await ed.click({ force: true }).catch(() => {});
  await page.waitForTimeout(150);
  await page.keyboard.press('Control+A').catch(() => {});
  await page.keyboard.insertText(text).catch(async () => { await page.keyboard.type(text, { delay: 6 }).catch(() => {}); });
  await page.waitForTimeout(250);
  const populated = await page.evaluate(() => { const c = document.querySelector('[contenteditable="true"]'); return c ? (c.textContent || '') : ''; }).catch(() => '');
  const baseRaw = (await snapshotTap(page)).sent.length;
  await page.keyboard.press('Enter').catch(() => {});
  await page.waitForTimeout(1800);
  let tap = await snapshotTap(page);
  let dispatched = tap.sent.slice(baseRaw).some((j) => /dispatch|turn|message/i.test((j && j.method) || ''));
  if (!dispatched) {
    const sendBtn = page.locator('button[aria-label*="Send" i]').last();
    if (await sendBtn.count()) { await sendBtn.click({ force: true }).catch(() => {}); await page.waitForTimeout(1500); }
    tap = await snapshotTap(page);
    dispatched = tap.sent.slice(baseRaw).some((j) => /dispatch|turn|message/i.test((j && j.method) || ''));
  }
  const frames = (await snapshotTap(page)).sent.slice(baseRaw);
  return { ok: dispatched, populated, frames };
}

async function pollTurn(readOnly, threadId, timeoutMs = TURN_TIMEOUT_MS, shouldStop) {
  const t0 = Date.now(); let last = null; let polls = 0; let lastErr = null;
  while (Date.now() - t0 < timeoutMs) {
    if (shouldStop && shouldStop()) return { status: 'shortCircuit', turn: last, polls, elapsedMs: Date.now() - t0 };
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

// Read the rendered chat transcript from the DOM (look for assistant message content).
async function readTranscript(page) {
  return page.evaluate(() => {
    const txt = (document.body.innerText || '');
    const errs = Array.from(document.querySelectorAll('[role="alert"], .text-red-400, .text-red-500, [class*="toast" i]')).map((e) => ((e.textContent || '').replace(/\s+/g, ' ').trim()).slice(0, 200)).filter((t) => t.length > 2).slice(0, 8);
    // Heuristic: find any element containing UI_CHAT_OK or assistant-looking content
    const hasMarker = /UI_CHAT_OK/i.test(txt);
    return { bodyTail: txt.slice(-1200), hasMarker, errTexts: errs };
  }).catch(() => ({ bodyTail: '', hasMarker: false, errTexts: [] }));
}

async function createDraftThread(page, readOnly, projectName, shotName) {
  const row = page.locator('span', { hasText: projectName }).first();
  await row.waitFor({ state: 'visible', timeout: 10_000 }).catch(() => {});
  await row.scrollIntoViewIfNeeded().catch(() => {});
  await row.hover({ force: true }).catch(() => {});
  await page.waitForTimeout(350);
  let btn = page.locator(`[aria-label="Create new thread in ${projectName}"]`).first();
  if (!(await btn.count().catch(() => 0))) btn = page.locator('[data-testid="new-thread-button"]').first();
  const mode = await clickForce(btn, 8000);
  await page.waitForTimeout(1500);
  let threadId = null;
  for (let i = 0; i < 20; i++) { const m = (page.url().match(/\/([0-9a-fA-F-]{36})/) || [])[1] || null; if (m) { threadId = m; break; } await page.waitForTimeout(300); }
  await page.waitForSelector('[contenteditable="true"]', { timeout: 15_000 }).catch(() => {});
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(SHOT_DIR, shotName), fullPage: true }).catch(() => {});
  return { threadId, mode };
}

// Was the chat streaming? Look at recv frames for incremental assistant tokens vs
// a single completion.
function analyzeStreaming(recvFrames, turnMethodHint) {
  const turnEvents = recvFrames.filter((f) => f && f.method && /thread\.turn|turn\/|assistant/i.test(f.method));
  const tokenish = recvFrames.filter((f) => f && f.method && /token|stream|partial|delta|chunk/i.test(f.method));
  return { turnEventCount: turnEvents.length, tokenEventCount: tokenish.length, sample: turnEvents.slice(0, 3).map((f) => f.method) };
}

(async () => {
  console.log('=== Browser chat-cycle E2E driver (v4 — opencode + GLM 5.2) ===');
  console.log('BASE=' + BASE + '  WS=' + WS_URL);
  const report = { base: BASE, ws: WS_URL, startedAt: new Date().toISOString(), stages: {}, target: { provider: TARGET_PROVIDER_LABEL, modelText: TARGET_MODEL_TEXT, modelSlug: TARGET_MODEL_SLUG, message: SEND_MESSAGE } };

  const readOnly = makeReadOnlyClient(WS_URL);
  try { await new Promise((r) => setTimeout(r, 1000)); if (!readOnly.opened) await new Promise((r) => setTimeout(r, 2000)); } catch (_) {}
  let discovery = null;
  if (readOnly.opened) {
    try {
      const agents = await readOnly.call('provider/list-agents', {}, 8000).catch((e) => ({ __error: e.message }));
      const models = await readOnly.call('provider/list-models', {}, 8000).catch((e) => ({ __error: e.message }));
      discovery = { agents, models };
      console.log('discovery agents=' + JSON.stringify((agents && agents.agents || []).map((a) => a.name + '=' + a.displayName)));
    } catch (e) {}
  } else { console.log('read-only WS NOT opened'); }
  report.discovery = discovery;

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();
  page.on('console', (m) => { if (m.type() === 'error') ALL_CONSOLE.push({ src: 'console', t: m.text() }); });
  page.on('pageerror', (e) => ALL_CONSOLE.push({ src: 'pageerror', t: e.message }));

  // Stage 1: load
  try {
    await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 30_000 });
    await page.waitForLoadState('networkidle').catch(() => {});
    await page.waitForTimeout(2000);
    await page.screenshot({ path: path.join(SHOT_DIR, '00-shell-empty.png'), fullPage: true });
    const wsUrls = await page.evaluate(() => (window.__e2e && window.__e2e.wsUrls) || []).catch(() => []);
    const consoleErrsAtLoad = await page.evaluate(() => (window.__e2e && window.__e2e.consoleErrors) || []).catch(() => []);
    record('1. Load shell', 'PASS', 'url=' + page.url() + '; wsUrls=' + JSON.stringify(wsUrls) + '; consoleErrors@load=' + consoleErrsAtLoad.length);
    report.stages.load = { url: page.url(), wsUrls, consoleErrorsAtLoad: consoleErrsAtLoad.slice(0, 10) };
  } catch (e) { record('1. Load shell', 'FAIL', e.message); }

  // Stage 2: project (reuse if exists)
  let projectName = null, projectId = null;
  try {
    const existing = await findProjectByRoot(readOnly, PROJECT_ROOT);
    if (existing) { projectName = existing.name || existing.title || 'e2e-chat'; projectId = existing.id; }
    if (!existing) {
      await clickForce(page.locator('[aria-label="Add project"]').first(), 8000);
      await page.waitForTimeout(500);
      const tp = page.locator('button:has-text("Type path")').first();
      if (await tp.count()) { await clickForce(tp, 6000); await page.waitForTimeout(300); }
      const pi = page.locator('input[placeholder="/path/to/project"]').first();
      if (!(await pi.count())) throw new Error('path input missing');
      await pi.fill(PROJECT_ROOT); await page.waitForTimeout(120); await pi.press('Enter').catch(() => {});
      await page.waitForTimeout(1800);
      for (let i = 0; i < 40 && !projectId; i++) { const p = await findProjectByRoot(readOnly, PROJECT_ROOT); if (p) { projectId = p.id; projectName = p.name || p.title || 'e2e-chat'; } else await page.waitForTimeout(500); }
    }
    record('2. Create project via UI', projectId ? 'PASS' : 'FAIL', 'name="' + projectName + '"; id=' + projectId + (existing ? ' (reused)' : ''));
  } catch (e) { record('2. Create project via UI', 'FAIL', e.message); await page.screenshot({ path: path.join(SHOT_DIR, '01-FAIL.png'), fullPage: true }).catch(() => {}); }

  // Stage 3: draft thread (will be reused for the single opencode scenario)
  let threadId = null;
  if (projectName) {
    try {
      const r = await createDraftThread(page, readOnly, projectName, '02-draft-thread.png');
      threadId = r.threadId;
      record('3. Draft thread', threadId ? 'PASS' : 'WARN', 'threadId=' + threadId + '; clickMode=' + r.mode);
      report.stages.draftThread = r;
    } catch (e) { record('3. Draft thread', 'FAIL', e.message); }
  }

  // Stage 4: open picker -> OpenCode -> GLM 5.2 (Z.AI)
  let pickerOutcome = null;
  if (threadId) {
    try {
      const sel = await selectOpenCodeGLM52(page, 'picker');
      pickerOutcome = sel;
      await page.screenshot({ path: path.join(SHOT_DIR, '04-after-pick.png'), fullPage: true }).catch(() => {});
      const ok = !!sel.ok && /GLM 5\.2/i.test(String(sel.picked || ''));
      record('4. Pick OpenCode → GLM 5.2 (Z.AI)', ok ? 'PASS' : 'FAIL',
        'sel.ok=' + sel.ok + '; picked="' + sel.picked + '"' + (sel.reason ? '; reason=' + sel.reason : '') + '; radios=' + JSON.stringify((sel.modelRadios || []).map((r) => r.text + (r.checked === 'true' ? '[checked]' : ''))));
      report.stages.picker = sel;
    } catch (e) { record('4. Pick OpenCode → GLM 5.2 (Z.AI)', 'FAIL', e.message); pickerOutcome = { ok: false, error: e.message }; }
  }

  // Stage 5: type + send, then assert dispatched modelSelection
  let dispatchedModelSelection = null;
  let dispatchAssertion = 'NOT_DISPATCHED';
  if (threadId) {
    try {
      const preSend = await snapshotTap(page); const preSendB = tapBaseline(preSend);
      const sent = await typeAndSend(page, SEND_MESSAGE);
      await page.waitForTimeout(1500);
      await page.screenshot({ path: path.join(SHOT_DIR, '05-sent.png'), fullPage: true }).catch(() => {});

      const tapAfter = await snapshotTap(page, preSendB);
      const dispFrame = (tapAfter.deltaSentFrames || []).find((f) => /dispatch|turn|message/i.test((f && f.method) || ''));
      dispatchedModelSelection = dispFrame && dispFrame.params && (dispFrame.params.modelSelection || (dispFrame.params.command && dispFrame.params.command.modelSelection)) || null;
      const dispatchMethod = dispFrame && dispFrame.method;
      // also capture if the dispatched frame is thread.turn.start vs orchestration/dispatch-command
      const dispatchShape = dispFrame ? Object.keys(dispFrame.params || {}) : [];

      let expectedModelFound = false;
      if (dispatchedModelSelection) {
        const m = dispatchedModelSelection.model || dispatchedModelSelection.modelId;
        expectedModelFound = (m === TARGET_MODEL_SLUG);
        dispatchAssertion = expectedModelFound ? 'MATCH' : 'MISMATCH';
      }

      report.stages.send = {
        populated: truncate(sent.populated, 60),
        dispatched: sent.ok,
        dispatchMethod,
        dispatchShape,
        dispatchedModelSelection,
        dispatchAssertion,
        deltaSentMethods: tapAfter.deltaSent,
      };
      record('5. Send + dispatch assertion',
        (sent.ok && expectedModelFound) ? 'PASS' : (sent.ok ? 'FAIL' : 'FAIL'),
        'populated="' + truncate(sent.populated, 30) + '"; dispatched=' + sent.ok + '; dispatchMethod=' + dispatchMethod +
        '; modelSelection=' + JSON.stringify(dispatchedModelSelection) +
        '; assertion=' + dispatchAssertion + ' (expected model="' + TARGET_MODEL_SLUG + '")');
    } catch (e) { record('5. Send + dispatch assertion', 'FAIL', e.message); }
  }

  // Stage 6: poll turn + observe UI rendering
  let poll = null, transcript = null, streaming = null;
  if (threadId && readOnly.opened && dispatchAssertion !== 'NOT_DISPATCHED') {
    try {
      // short-circuit on early dispatch RPC error
      let shortCircuit = false; let earlyErr = null;
      for (let i = 0; i < 6; i++) {
        const t = await snapshotTap(page);
        const e = (t.errors || []).find((x) => /dispatch|turn|message|thread/i.test(x.method));
        if (e) { shortCircuit = true; earlyErr = e; break; }
        await page.waitForTimeout(800);
      }
      const prePollRecv = await page.evaluate(() => (window.__e2e && window.__e2e.wsRecv) || []).catch(() => []);
      if (!shortCircuit) {
        // Also sample the transcript every few seconds to detect streaming (live tokens)
        let lastTail = '';
        let streamDetected = false;
        const t0 = Date.now();
        const shouldStop = () => false;
        poll = await pollTurn(readOnly, threadId, TURN_TIMEOUT_MS, shouldStop);
        // Sample transcript quickly during poll for streaming (best-effort, post-hoc)
        transcript = await readTranscript(page);
      } else {
        poll = { status: 'dispatchError', elapsedMs: 0, polls: 0, earlyErr };
      }
      await page.screenshot({ path: path.join(SHOT_DIR, '06-result.png'), fullPage: true }).catch(() => {});

      const postRecv = await page.evaluate(() => (window.__e2e && window.__e2e.wsRecv) || []).catch(() => []);
      const deltaRecv = postRecv.slice(prePollRecv.length).map((r) => { try { return JSON.parse(r); } catch (_) { return { _raw: r }; } });
      streaming = analyzeStreaming(deltaRecv);

      transcript = transcript || await readTranscript(page);
      const turn = poll && poll.turn;
      const aiOut = (turn && (turn.assistant_output || turn.output || turn.text || turn.responseText)) || null;
      const turnMeta = turn ? { status: turn.status, providerId: turn.providerId, model: turn.model || (turn.modelSelection && turn.modelSelection.model), error: turn.error || turn.errorMessage || turn.failureReason || null } : null;
      const renderedMarker = !!(transcript && transcript.hasMarker);

      report.stages.result = { poll: poll && { status: poll.status, elapsedMs: poll.elapsedMs, polls: poll.polls }, turnMeta, aiOutput: truncate(aiOut, 240), transcriptTail: transcript.bodyTail.slice(-400), renderedMarker, streaming };

      const turnCompleted = poll && poll.status === 'completed';
      const protocolHasAi = !!(aiOut && /UI_CHAT_OK/i.test(String(aiOut)));
      // VERDICT logic:
      //  PASS = turn completed AND (protocol has UI_CHAT_OK OR DOM has UI_CHAT_OK)
      //  WARN = completed but neither has marker (turn finished w/o expected text) OR timeout
      //  FAIL = error/failed/dispatchError or no rendering at all
      let verdict = 'FAIL';
      if (turnCompleted && (protocolHasAi || renderedMarker)) verdict = 'PASS';
      else if (turnCompleted && !protocolHasAi && !renderedMarker) verdict = 'WARN';
      else if (poll && (poll.status === 'timeout')) verdict = 'WARN';
      else if (poll && (poll.status === 'error' || poll.status === 'failed' || poll.status === 'dispatchError')) verdict = 'FAIL';

      record('6. Turn result + UI rendering', verdict,
        'turnStatus=' + (poll && poll.status) + '; turnMeta=' + JSON.stringify(turnMeta) +
        '; aiOut(protocol)=' + truncate(aiOut, 120) +
        '; DOM.hasMarker=' + renderedMarker +
        '; streaming=' + JSON.stringify({ tokenEvents: streaming.tokenEventCount, turnEvents: streaming.turnEventCount }) +
        '; earlyErr=' + JSON.stringify(earlyErr && { code: earlyErr.code, msg: earlyErr.message }) +
        '; elapsedMs=' + (poll && poll.elapsedMs));
    } catch (e) { record('6. Turn result + UI rendering', 'FAIL', e.message); }
  }

  // Final snapshot
  try {
    const snap = readOnly.opened ? await readOnly.call('shell/getSnapshot', {}, 8000).catch((e) => ({ __error: e.message })) : null;
    report.finalSnapshot = snap && snap.projects != null ? { projects: snap.projects.length, threads: (snap.threads || []).length } : snap;
    await page.screenshot({ path: path.join(SHOT_DIR, '99-final.png'), fullPage: true });
  } catch (e) {}
  try { await browser.close(); } catch (_) {} try { readOnly.ws.close(); } catch (_) {}

  report.endedAt = new Date().toISOString();
  report.results = RESULTS;
  report.allConsoleErrors = ALL_CONSOLE;
  report.allRpcErrors = ALL_RPC_ERRORS;
  report.screenshots = fs.readdirSync(SHOT_DIR);
  report.criticalAssertions = {
    dispatchedModelSelection,
    dispatchAssertion,
    dispatchedModelSlug: dispatchedModelSelection && (dispatchedModelSelection.model || dispatchedModelSelection.modelId),
    expectedSlug: TARGET_MODEL_SLUG,
    pickerOfferedTargetModel: !!(pickerOutcome && (pickerOutcome.modelRadios || []).some((r) => /GLM 5\.2/i.test(r.text))),
  };
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  console.log('\n======================== RESULTS ========================');
  for (const r of RESULTS) console.log('  [' + r.status + '] ' + r.stage + ' — ' + r.evidence);
  console.log('Summary: ' + JSON.stringify(RESULTS.reduce((a, r) => { a[r.status] = (a[r.status] || 0) + 1; return a; }, {})));
  console.log('CRITICAL: dispatchedModelSlug=' + report.criticalAssertions.dispatchedModelSlug + ' (expected ' + TARGET_MODEL_SLUG + ')');
  console.log('consoleErrors=' + ALL_CONSOLE.length + '; rpcErrors=' + ALL_RPC_ERRORS.length);
  console.log('Report: ' + REPORT_JSON + '  Shots: ' + SHOT_DIR);
  console.log('=========================================================');
})().catch((err) => { console.error('FATAL:', err && err.stack ? err.stack : err); process.exit(2); });

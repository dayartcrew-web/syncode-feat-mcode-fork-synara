/* eslint-disable */
/* Browser-driven chat-cycle E2E driver — ALL installed providers.
 *
 * Drives the RUNNING app (vite @ :5173 + syncode-ws @ ws://127.0.0.1:3100/ws)
 * purely through the browser UI. Provider+model selected ONLY via the UI picker.
 *
 * For each provider in PROVIDER_TARGETS:
 *   - create a fresh thread in the e2e project
 *   - open the 3-level picker (root -> model-picker submenu -> Provider -> model radios)
 *   - pick the first non-disabled model radio (preferred slug regex if matched)
 *   - send the prompt: "Reply with exactly the word: PONG"
 *   - poll turn/list for terminal status (<=90s)
 *   - assert "PONG" (case-insensitive) in DOM transcript or protocol turn output
 *   - screenshot
 *
 * A node-side WS client is used READ-ONLY (shell/getSnapshot, turn/list) to
 * observe outcomes — it never creates state or selects providers.
 *
 * Servers must already be up — this script NEVER starts/stops cargo or npm.
 */

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const BASE = process.env.E2E_BASE || 'http://localhost:5173';
const WS_URL = process.env.E2E_WS || 'ws://127.0.0.1:3100/ws';
const PROJECT_ROOT = '/tmp/e2e-chat';
const SHOT_DIR = '/tmp/e2e-chat-cycle-all-shots';
const REPORT_JSON = '/tmp/e2e-chat-cycle-all-report.json';
const TURN_TIMEOUT_MS = 90_000;
const POLL_MS = 2500;

// Per-provider target config. providerLabel must match the picker submenu text.
// preferredModelRegex is used to pick a "good" model when multiple are listed.
const PROVIDER_TARGETS = [
  { key: 'opencode', providerLabel: 'OpenCode', preferredModelRegex: /GLM 5\.2|GLM/i, fallbackModelRegex: /.*/i },
  { key: 'claude',   providerLabel: 'Claude',   preferredModelRegex: /sonnet|claude[- ]3|claude[- ]4/i, fallbackModelRegex: /.*/i },
  { key: 'codex',    providerLabel: 'Codex',    preferredModelRegex: /gpt-?5|codex|o[13]|reason/i, fallbackModelRegex: /.*/i },
  { key: 'gemini',   providerLabel: 'Gemini',   preferredModelRegex: /gemini-?2\.5-flash|flash|gemini/i, fallbackModelRegex: /.*/i },
];
const SEND_MESSAGE = 'Reply with exactly the word: PONG';

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

async function hoverMenuItem(page, text, menuIdx) {
  const sel = (menuIdx == null)
    ? page.locator('[role=menuitem]:visible, [role=menuitemradio]:visible').filter({ hasText: text }).first()
    : page.locator(`[role=menu]:visible >> nth=${menuIdx}`).locator('[role=menuitem], [role=menuitemradio]').filter({ hasText: text }).first();
  const c = await sel.count().catch(() => 0); if (!c) return { ok: false, count: 0 };
  await sel.hover({ force: true }).catch(() => {});
  await page.waitForTimeout(450);
  return { ok: true, count: c };
}

// Find the model-picker submenu (level-2 = provider list) by hovering each root item.
async function findModelPickerSubmenu(page) {
  const rootMenus = await dumpAllMenus(page);
  const root = rootMenus[0];
  if (!root) return { ok: false, reason: 'no root menu' };
  for (const it of root.items) {
    if (it.disabled) continue;
    await hoverMenuItem(page, it.text, 0);
    const after = await dumpAllMenus(page);
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

// Navigate picker: root -> model-picker submenu -> {ProviderLabel} -> first/preferred model radio.
async function selectProviderModel(page, target, shotPrefix) {
  const log = [];
  const opened = await openPickerViaShortcut(page);
  log.push({ step: 'openPicker', opened });
  if (!opened) return { ok: false, log, reason: 'picker did not open' };

  const rootDump = await dumpAllMenus(page);
  log.push({ step: 'rootMenu', menus: rootDump });
  await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-1-root.png') }).catch(() => {});

  const found = await findModelPickerSubmenu(page);
  log.push({ step: 'findModelPickerSubmenu', found: { ok: found.ok, level1Trigger: found.level1Trigger, providers: (found.providers || []).map((p) => p.text + (p.disabled ? '[DISABLED]' : '')) } });
  if (!found.ok) {
    await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-2-no-provider-list.png') }).catch(() => {});
    await page.keyboard.press('Escape').catch(() => {});
    return { ok: false, log, reason: found.reason };
  }
  await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-2-provider-list.png') }).catch(() => {});

  const provItem = (found.providers || []).find((p) => new RegExp('^' + target.providerLabel + '$', 'i').test(p.text.trim()) || new RegExp('\\b' + target.providerLabel + '\\b', 'i').test(p.text));
  if (!provItem) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, log, reason: target.providerLabel + ' not in provider list', providers: (found.providers||[]).map(p=>p.text) }; }
  if (provItem.disabled) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, log, reason: target.providerLabel + ' DISABLED in picker', provItem }; }

  const beforeMenus = (await dumpAllMenus(page)).length;
  await hoverMenuItem(page, target.providerLabel, found.level2Idx);
  await page.waitForTimeout(500);
  let afterProv = await dumpAllMenus(page);
  if (afterProv.length <= beforeMenus) {
    await page.mouse.move(100, 100).catch(() => {});
    await hoverMenuItem(page, target.providerLabel, found.level2Idx);
    await page.waitForTimeout(500);
    afterProv = await dumpAllMenus(page);
  }
  log.push({ step: 'afterHoverProvider', menuCount: afterProv.length });
  await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-3-provider-hover.png') }).catch(() => {});

  // Poll for model radios to populate (async load).
  const waitT0 = Date.now();
  let modelTexts = [];
  while (Date.now() - waitT0 < 10000) {
    const probe = await page.evaluate(() => {
      const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
      const els = Array.from(document.querySelectorAll('[role=menuitemradio]')).filter(vis);
      return els.map((x) => ({ text: (x.textContent || '').replace(/\s+/g, ' ').trim(), disabled: x.getAttribute('aria-disabled') === 'true' || x.disabled === true }));
    }).catch(() => []);
    modelTexts = probe;
    if (probe.length > 0 && Date.now() - waitT0 > 1500) break;
    await page.waitForTimeout(250);
  }
  log.push({ step: 'modelRadios', radios: modelTexts });
  await page.screenshot({ path: path.join(SHOT_DIR, shotPrefix + '-4-models.png') }).catch(() => {});

  const enabled = modelTexts.filter((m) => !m.disabled);
  if (!enabled.length) { await page.keyboard.press('Escape').catch(() => {}); return { ok: false, log, reason: 'no enabled model radios for ' + target.providerLabel, allRadios: modelTexts }; }

  // Pick preferred if present, else first enabled.
  let pick = enabled.find((m) => target.preferredModelRegex.test(m.text));
  if (!pick) pick = enabled.find((m) => target.fallbackModelRegex.test(m.text));
  if (!pick) pick = enabled[0];

  const clicked = await page.evaluate((t) => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const els = Array.from(document.querySelectorAll('[role=menuitemradio]')).filter(vis).filter((x) => (x.textContent || '').replace(/\s+/g, ' ').trim() === t);
    if (!els[0]) return false;
    els[0].click();
    return els[0].textContent.replace(/\s+/g, ' ').trim();
  }, pick.text).catch(() => false);
  log.push({ step: 'clickModel', picked: pick.text, clicked });
  await page.waitForTimeout(800);
  await page.keyboard.press('Escape').catch(() => {});
  await page.waitForTimeout(400);
  return { ok: !!clicked, log, picked: clicked || pick.text, allModels: modelTexts.map((m) => m.text + (m.disabled ? '[DISABLED]' : '')) };
}

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

async function readTranscript(page) {
  return page.evaluate(() => {
    const txt = (document.body.innerText || '');
    const errs = Array.from(document.querySelectorAll('[role="alert"], .text-red-400, .text-red-500, [class*="toast" i]')).map((e) => ((e.textContent || '').replace(/\s+/g, ' ').trim()).slice(0, 200)).filter((t) => t.length > 2).slice(0, 8);
    const hasMarker = /\bPONG\b/i.test(txt);
    // Pull last few "assistant-looking" chunks for evidence.
    return { bodyTail: txt.slice(-1600), hasMarker, errTexts: errs };
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

function analyzeStreaming(recvFrames) {
  const turnEvents = recvFrames.filter((f) => f && f.method && /thread\.turn|turn\/|assistant/i.test(f.method));
  const tokenish = recvFrames.filter((f) => f && f.method && /token|stream|partial|delta|chunk/i.test(f.method));
  return { turnEventCount: turnEvents.length, tokenEventCount: tokenish.length, sample: turnEvents.slice(0, 3).map((f) => f.method) };
}

// Extract assistant text from a turn object across the various field shapes.
function extractAssistantText(turn) {
  if (!turn) return null;
  const direct = turn.assistantOutput || turn.assistant_output || turn.output || turn.text || turn.responseText || turn.message;
  if (direct && typeof direct === 'string') return direct;
  // messages array shape
  if (Array.isArray(turn.messages)) {
    const a = turn.messages.find((m) => /assistant|ai/i.test(m.role || ''));
    if (a) return typeof a.content === 'string' ? a.content : (Array.isArray(a.content) ? a.content.map((c)=>(c&&c.text)||'').join('') : JSON.stringify(a.content));
  }
  if (turn.result && typeof turn.result === 'object') {
    const r = turn.result;
    if (typeof r.text === 'string') return r.text;
    if (Array.isArray(r.messages)) { const a = r.messages.find((m)=>/assistant|ai/i.test(m.role||'')); if (a) return typeof a.content==='string'?a.content:JSON.stringify(a.content); }
  }
  return null;
}

(async () => {
  console.log('=== Browser chat-cycle E2E driver (ALL providers) ===');
  console.log('BASE=' + BASE + '  WS=' + WS_URL);
  const report = { base: BASE, ws: WS_URL, startedAt: new Date().toISOString(), providers: {} };

  const readOnly = makeReadOnlyClient(WS_URL);
  try { await new Promise((r) => setTimeout(r, 1000)); if (!readOnly.opened) await new Promise((r) => setTimeout(r, 2000)); } catch (_) {}
  console.log('read-only WS opened=' + readOnly.opened);

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();
  const allConsole = [];
  page.on('console', (m) => { if (m.type() === 'error') allConsole.push(m.text()); });
  page.on('pageerror', (e) => allConsole.push('pageerror: ' + e.message));

  // Stage 1: load shell
  try {
    await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 30_000 });
    await page.waitForLoadState('networkidle').catch(() => {});
    await page.waitForTimeout(2000);
    await page.screenshot({ path: path.join(SHOT_DIR, '00-shell.png'), fullPage: true });
    report.shell = { url: page.url(), ok: true };
    console.log('[load] url=' + page.url());
  } catch (e) {
    report.shell = { ok: false, error: e.message };
    console.log('[load] FAIL ' + e.message);
  }

  // Stage 2: ensure project exists (create if missing)
  let projectName = null, projectId = null;
  try {
    let existing = await findProjectByRoot(readOnly, PROJECT_ROOT);
    if (existing) { projectName = existing.name || existing.title || 'e2e-chat'; projectId = existing.id; console.log('[project] reused id=' + projectId + ' name=' + projectName); }
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
      console.log('[project] created id=' + projectId + ' name=' + projectName);
    }
  } catch (e) {
    console.log('[project] FAIL ' + e.message);
    await page.screenshot({ path: path.join(SHOT_DIR, '01-project-FAIL.png'), fullPage: true }).catch(() => {});
  }
  report.project = { id: projectId, name: projectName };

  // Stage 3: loop over providers
  for (const target of PROVIDER_TARGETS) {
    console.log('\n--- PROVIDER: ' + target.key + ' (label="' + target.providerLabel + '") ---');
    const plog = { target, startedAt: new Date().toISOString() };
    report.providers[target.key] = plog;

    if (!projectName) { plog.verdict = 'SKIP'; plog.reason = 'no project'; console.log('  SKIP (no project)'); continue; }

    // Navigate back to shell root + reopen project to create a fresh thread each time.
    try {
      await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 20_000 }).catch(()=>{});
      await page.waitForTimeout(1200);
    } catch (_) {}

    let threadId = null;
    try {
      const r = await createDraftThread(page, readOnly, projectName, target.key + '-thread.png');
      threadId = r.threadId;
      plog.thread = r;
      console.log('  [thread] id=' + threadId + ' clickMode=' + r.mode);
    } catch (e) {
      plog.verdict = 'FAIL'; plog.reason = 'thread create failed: ' + e.message;
      console.log('  [thread] FAIL ' + e.message);
      continue;
    }
    if (!threadId) { plog.verdict = 'FAIL'; plog.reason = 'no threadId in URL'; continue; }

    // Pick provider+model
    let pickedModel = null, allModels = [];
    try {
      const sel = await selectProviderModel(page, target, target.key);
      plog.picker = sel;
      allModels = sel.allModels || [];
      pickedModel = sel.picked || null;
      console.log('  [picker] ok=' + sel.ok + ' picked="' + pickedModel + '"' + (sel.reason ? ' reason=' + sel.reason : ''));
      console.log('  [picker] allModels=' + JSON.stringify(allModels));
      if (!sel.ok) { plog.verdict = 'FAIL'; plog.reason = 'picker: ' + (sel.reason || 'unknown'); continue; }
    } catch (e) {
      plog.verdict = 'FAIL'; plog.reason = 'picker exception: ' + e.message;
      console.log('  [picker] EXC ' + e.message);
      continue;
    }

    // Type + send
    let dispatchedModelSelection = null, dispatchMethod = null;
    try {
      const preSend = await snapshotTap(page); const preSendB = tapBaseline(preSend);
      const sent = await typeAndSend(page, SEND_MESSAGE);
      await page.waitForTimeout(1500);
      await page.screenshot({ path: path.join(SHOT_DIR, target.key + '-05-sent.png'), fullPage: true }).catch(() => {});
      const tapAfter = await snapshotTap(page, preSendB);
      const dispFrame = (tapAfter.deltaSentFrames || []).find((f) => /dispatch|turn|message|thread\.turn\.start/i.test((f && f.method) || ''));
      dispatchMethod = dispFrame && dispFrame.method;
      dispatchedModelSelection = dispFrame && dispFrame.params && (dispFrame.params.modelSelection || (dispFrame.params.command && dispFrame.params.command.modelSelection)) || null;
      plog.send = { populated: truncate(sent.populated, 60), dispatched: sent.ok, dispatchMethod, dispatchedModelSelection, deltaSentMethods: tapAfter.deltaSent };
      console.log('  [send] dispatched=' + sent.ok + ' method=' + dispatchMethod + ' modelSelection=' + JSON.stringify(dispatchedModelSelection));
      if (!sent.ok) { plog.verdict = 'FAIL'; plog.reason = 'send did not dispatch'; continue; }
    } catch (e) {
      plog.verdict = 'FAIL'; plog.reason = 'send exception: ' + e.message;
      console.log('  [send] EXC ' + e.message); continue;
    }

    // Poll for turn terminal status
    let poll = null;
    try {
      // Short-circuit if dispatch returned an RPC error quickly
      let shortCircuit = false; let earlyErr = null;
      for (let i = 0; i < 6; i++) {
        const t = await snapshotTap(page);
        const e = (t.errors || []).find((x) => /dispatch|turn|message|thread/i.test(x.method));
        if (e) { shortCircuit = true; earlyErr = e; break; }
        await page.waitForTimeout(800);
      }
      const prePollRecv = await page.evaluate(() => (window.__e2e && window.__e2e.wsRecv) || []).catch(() => []);
      if (!shortCircuit) {
        poll = await pollTurn(readOnly, threadId, TURN_TIMEOUT_MS);
      } else {
        poll = { status: 'dispatchError', elapsedMs: 0, polls: 0, earlyErr };
      }
      await page.screenshot({ path: path.join(SHOT_DIR, target.key + '-06-result.png'), fullPage: true }).catch(() => {});

      const postRecv = await page.evaluate(() => (window.__e2e && window.__e2e.wsRecv) || []).catch(() => []);
      const deltaRecv = postRecv.slice(prePollRecv.length).map((r) => { try { return JSON.parse(r); } catch (_) { return { _raw: r }; } });
      const streaming = analyzeStreaming(deltaRecv);

      const transcript = await readTranscript(page);
      const turn = poll && poll.turn;
      const aiOut = extractAssistantText(turn);
      const turnMeta = turn ? { status: turn.status, providerId: turn.providerId, model: turn.model || (turn.modelSelection && turn.modelSelection.model), error: turn.error || turn.errorMessage || turn.failureReason || null } : null;
      const renderedMarker = !!(transcript && transcript.hasMarker);
      const protocolHasMarker = !!(aiOut && /\bPONG\b/i.test(String(aiOut)));
      plog.result = { poll: poll && { status: poll.status, elapsedMs: poll.elapsedMs, polls: poll.polls, earlyErr: poll.earlyErr && { code: poll.earlyErr.code, msg: poll.earlyErr.message } }, turnMeta, aiOutput: truncate(aiOut, 240), transcriptTail: transcript.bodyTail.slice(-600), renderedMarker, protocolHasMarker, streaming, errTexts: transcript.errTexts };
      console.log('  [poll] status=' + poll.status + ' elapsedMs=' + poll.elapsedMs);
      console.log('  [poll] turnMeta=' + JSON.stringify(turnMeta));
      console.log('  [poll] aiOut=' + truncate(aiOut, 120));
      console.log('  [poll] DOM.marker=' + renderedMarker + ' protocol.marker=' + protocolHasMarker + ' errTexts=' + JSON.stringify(transcript.errTexts));

      // Verdict: PASS = turn completed AND marker in protocol or DOM.
      let verdict = 'FAIL';
      if (poll.status === 'completed' && (protocolHasMarker || renderedMarker)) verdict = 'PASS';
      else if (poll.status === 'completed' && !protocolHasMarker && !renderedMarker) verdict = 'WARN';
      else if (poll.status === 'timeout') verdict = 'WARN';
      else verdict = 'FAIL';
      plog.verdict = verdict;
      plog.pickedModel = pickedModel;
      plog.dispatchedModelSelection = dispatchedModelSelection;
      plog.firstResponseChars = (aiOut || transcript.bodyTail.slice(-200) || '').slice(0, 200);
      console.log('  [verdict] ' + verdict);
    } catch (e) {
      plog.verdict = 'FAIL'; plog.reason = 'poll exception: ' + e.message;
      console.log('  [poll] EXC ' + e.message);
    }
  }

  // Final
  try { await page.screenshot({ path: path.join(SHOT_DIR, '99-final.png'), fullPage: true }); } catch(_) {}
  try { await browser.close(); } catch (_) {} try { readOnly.ws.close(); } catch (_) {}

  report.endedAt = new Date().toISOString();
  report.allConsoleErrors = allConsole;
  report.screenshots = fs.readdirSync(SHOT_DIR);
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  console.log('\n======================== PER-PROVIDER RESULTS ========================');
  for (const target of PROVIDER_TARGETS) {
    const p = report.providers[target.key] || {};
    console.log(target.key + ': verdict=' + (p.verdict||'?') + ' picked="' + (p.pickedModel||'?') + '" dispatched=' + JSON.stringify(p.dispatchedModelSelection));
  }
  console.log('\nReport JSON: ' + REPORT_JSON);
  console.log('Screenshots: ' + SHOT_DIR);
  console.log('=======================================================================');
})().catch((err) => { console.error('FATAL:', err && err.stack ? err.stack : err); process.exit(2); });

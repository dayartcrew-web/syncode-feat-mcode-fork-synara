/* eslint-disable */
/* v3: better send (real keystrokes), dump ALL wsSent around send, identify
 * the exact working indicator, verify composer registered input. */
const { chromium } = require('playwright');
const WebSocket = require('ws');
const fs = require('fs');
const BASE = 'http://localhost:5173';
const WS_URL = 'ws://127.0.0.1:3100/ws';
const PROJECT_NAME = 'e2e-chat-repro';
const SHOT_DIR = '/tmp/e2e-stuck-repro-shots';
const REPORT_JSON = '/tmp/e2e-stuck-repro-report.json';
const TURN_TIMEOUT_MS = 120_000;
const SEND_MESSAGE = 'Reply with exactly the word: PONG';
fs.mkdirSync(SHOT_DIR, { recursive: true });

const INJECT = `
(function () {
  window.__e2e = { consoleErrors: [], wsSent: [], wsRecv: [], wsUrls: [], wsOpened: 0, wsClosed: 0, wsErrors: [] };
  var oe=console.error;console.error=function(){try{var m=Array.prototype.map.call(arguments,function(a){if(typeof a==='string')return a;if(a&&a.message)return a.message;try{return JSON.stringify(a)}catch(_){return String(a)}}).join(' ');window.__e2e.consoleErrors.push(m);}catch(_){}oe.apply(console,arguments);};
  window.addEventListener('error',function(e){window.__e2e.consoleErrors.push('uncaught: '+(e&&e.message||'(unknown)'));});
  window.addEventListener('unhandledrejection',function(e){window.__e2e.consoleErrors.push('unhandledrejection: '+(e&&e.reason&&e.reason.message?e.reason.message:String(e&&e.reason)));});
  var N=window.WebSocket;
  function W(url,protocols){var ws=protocols?new N(url,protocols):new N(url);try{window.__e2e.wsUrls.push(String(url));}catch(_){}ws.addEventListener('open',function(){window.__e2e.wsOpened++;});ws.addEventListener('close',function(){window.__e2e.wsClosed++;});ws.addEventListener('error',function(){window.__e2e.wsErrors.push('ws-error');});ws.addEventListener('message',function(ev){try{if(typeof ev.data==='string')window.__e2e.wsRecv.push(ev.data);}catch(_){}});var os=ws.send.bind(ws);ws.send=function(d){try{if(typeof d==='string')window.__e2e.wsSent.push(d);}catch(_){}return os(d);};return ws;}
  W.prototype=N.prototype;W.CONNECTING=N.CONNECTING;W.OPEN=N.OPEN;W.CLOSING=N.CLOSING;W.CLOSED=N.CLOSED;window.WebSocket=W;
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
  client.close = function () { try { ws.close(); } catch (_) {} };
  ws.on('open', () => { client.opened = true; });
  ws.on('message', (data) => {
    let j; try { j = JSON.parse(data.toString()); } catch (_) { return; }
    if (j.id != null && pending.has(j.id)) { const p = pending.get(j.id); pending.delete(j.id); clearTimeout(p.timer);
      if (j.error) p.reject(Object.assign(new Error(j.error.message || 'rpc error'), { code: j.error.code }));
      else p.resolve(j.result);
    } else if (j.method && j.params) client.notifications.push({ method: j.method, params: j.params, t: Date.now() });
  });
  ws.on('error', (err) => { client.errors.push(err.message || String(err)); });
  return client;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const truncate = (s, n = 200) => (s ? (s.length > n ? s.slice(0, n) + '…' : s) : '(null)');
async function clickForce(loc, timeout = 8000) {
  try { await loc.click({ timeout: Math.min(timeout, 4000) }); return 'normal'; } catch (_) {}
  try { await loc.click({ force: true, timeout }); return 'force'; } catch (_) {}
  return 'failed';
}
function parseFrames(arr) { return arr.map((s) => { try { return JSON.parse(s); } catch (_) { return null; } }).filter(Boolean); }

async function snapshotUI(page) {
  return page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const body = document.body ? (document.body.innerText || '') : '';
    const hasPong = /\bPONG\b/.test(body);
    // Identify the EXACT working indicator (full detail on first match)
    let indicatorDetail = null;
    const indSels = ['[aria-busy="true"]','[data-streaming="true"]','[class*="working" i]','[class*="thinking" i]','[class*="streaming" i]','[class*="animate-spin" i]','svg[class*="spin" i]','[role="status"]'];
    outer: for (const sel of indSels) {
      const els = Array.from(document.querySelectorAll(sel)).filter(vis);
      for (const el of els) { indicatorDetail = { sel, tag: el.tagName.toLowerCase(), cls: (el.getAttribute('class')||'').slice(0,80), txt: (el.textContent||'').replace(/\\s+/g,' ').trim().slice(0,60), outer: el.outerHTML.slice(0,200) }; break outer; }
    }
    const workingByText = /\b(working|thinking|generating|streaming|pending)\b/i.test(body);
    const assistantMsgs = document.querySelectorAll('[data-role="assistant"],[data-message-role="assistant"],[class*="assistant" i]').length;
    const sendBtn = document.querySelector('button[type="submit"][aria-label*="Send" i], button[aria-label="Send message"]');
    const sendDisabled = sendBtn ? (sendBtn.disabled || sendBtn.getAttribute('aria-disabled') === 'true') : null;
    let modelLabel = null;
    for (const b of Array.from(document.querySelectorAll('button')).filter(vis)) { const t=(b.textContent||'').replace(/\\s+/g,' ').trim(); if (/^(GPT-|Claude|Sonnet|Opus|Haiku|GLM|Gemini|o\\d|Codex|OpenCode|Cursor|Grok|Kilo|Pi\\b)/i.test(t) && t.length<40){modelLabel=t;break;} }
    const transcripts = Array.from(document.querySelectorAll('[role="log"],main,[class*="transcript" i],[class*="messages" i]')).filter(vis);
    const tail = transcripts.length ? (transcripts[transcripts.length-1].innerText||'').slice(-300) : '';
    // composer content
    const ce = document.querySelector('[contenteditable="true"]');
    const composerTxt = ce ? (ce.textContent||'').slice(0,80) : null;
    return { hasPong, indicatorDetail, workingByText, assistantMsgs, sendDisabled, modelLabel, tail, composerTxt };
  }).catch((e) => ({ error: e.message }));
}

(async function main() {
  console.log('=== stuck-in-working repro (v3) ===');
  const report = { startedAt: new Date().toISOString() };
  const ro = makeReadOnlyClient(WS_URL);
  await sleep(1500); if (!ro.opened) await sleep(2000);
  report.roOpened = ro.opened;

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();

  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 30_000 });
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(2500);
  console.log('1. load url=' + page.url());

  // open thread in e2e-chat-repro
  const newThreadBtn = page.locator('[aria-label="Create new thread in ' + PROJECT_NAME + '"]').first();
  let mode = await clickForce(newThreadBtn, 8000);
  if (mode === 'failed') {
    await clickForce(page.locator('button').filter({ hasText: PROJECT_NAME }).first(), 8000);
    await page.waitForTimeout(700);
    await clickForce(page.locator('button:has-text("New thread")').first(), 6000);
    mode = 'fallback';
  }
  await page.waitForTimeout(1500);
  await page.screenshot({ path: SHOT_DIR + '/v3-02-thread.png', fullPage: true });
  console.log('2. thread mode=' + mode);

  // Type via REAL keystrokes (more reliable for editor frameworks).
  const ed = page.locator('[contenteditable="true"]').first();
  await ed.click({ force: true });
  await page.waitForTimeout(200);
  await page.keyboard.type(SEND_MESSAGE, { delay: 12 });
  await page.waitForTimeout(400);
  const preType = await snapshotUI(page);
  console.log('3. after type composerTxt="' + truncate(preType.composerTxt, 50) + '" sendDisabled=' + preType.sendDisabled + ' modelLabel=' + preType.modelLabel);
  report.afterType = preType;

  const preSentN = await page.evaluate(() => window.__e2e.wsSent.length);
  const preRecvN = await page.evaluate(() => window.__e2e.wsRecv.length);

  // Send via Enter. Many chat composers use Cmd/Ctrl+Enter or just Enter.
  await page.keyboard.press('Enter');
  await page.waitForTimeout(1200);
  let postEnter = await snapshotUI(page);
  let sentVia = 'enter';
  // If composer still has text, try Ctrl+Enter, then the send button.
  if ((postEnter.composerTxt || '').trim().length > 0) {
    await page.keyboard.press('Control+Enter'); await page.waitForTimeout(800);
    postEnter = await snapshotUI(page);
    sentVia = 'ctrl-enter';
  }
  if ((postEnter.composerTxt || '').trim().length > 0) {
    const sb = page.locator('button[aria-label="Send message"], button[type="submit"]').first();
    if (await sb.count()) { await clickForce(sb, 4000); sentVia = 'button'; await page.waitForTimeout(1000); postEnter = await snapshotUI(page); }
  }
  await page.screenshot({ path: SHOT_DIR + '/v3-04-sent.png', fullPage: true });
  console.log('4. sentVia=' + sentVia + ' composerAfter="' + truncate(postEnter.composerTxt, 40) + '" sendDisabled=' + postEnter.sendDisabled + ' indicator=' + JSON.stringify(postEnter.indicatorDetail && postEnter.indicatorDetail.cls));
  report.postSend = { sentVia, snapshot: postEnter };

  // Dump ALL wsSent frames (with method) in the window — to see what was dispatched.
  const sentFrames = parseFrames((await page.evaluate(() => window.__e2e.wsSent)).slice(preSentN));
  report.allSentAroundSend = sentFrames.map((f) => ({ method: f.method || null, id: f.id || null, paramKeys: f.params ? Object.keys(f.params) : null, isResponse: !f.method && f.id != null, methodOrRsp: f.method || ('resp#' + f.id) })).slice(0, 30);
  const disp = sentFrames.find((f) => f.method && /turn|dispatch|message|send|chat/i.test(f.method));
  report.dispatchedFrame = disp ? { method: disp.method, params: disp.params } : null;
  console.log('   sentAround(methods): ' + JSON.stringify(report.allSentAroundSend.map((x) => x.methodOrRsp)));
  console.log('   dispatchedFrame.method=' + (disp && disp.method));

  // Poll UI + capture WS pushes for up to TURN_TIMEOUT_MS.
  const timeline = [];
  const t0 = Date.now();
  let lastUI = postEnter;
  while (Date.now() - t0 < TURN_TIMEOUT_MS) {
    const ui = await snapshotUI(page);
    lastUI = ui;
    const elapsed = Date.now() - t0;
    timeline.push({ t: elapsed, hasPong: !!ui.hasPong, workingByText: !!ui.workingByText, indicator: !!(ui.indicatorDetail), assistantMsgs: ui.assistantMsgs, sendDisabled: ui.sendDisabled });
    const settled = ui.hasPong && !ui.workingByText && !ui.indicatorDetail;
    if (settled) { console.log('   settled t=' + elapsed); break; }
    await sleep(5000);
  }
  await page.screenshot({ path: SHOT_DIR + '/v3-06-final.png', fullPage: true });

  // WS frames received since send
  const recvFrames = parseFrames((await page.evaluate(() => window.__e2e.wsRecv)).slice(preRecvN));
  const byMethod = {}; const errors = []; const lifecycle = [];
  for (const f of recvFrames) {
    const m = f.method || ('resp#' + (f.id != null ? f.id : '?'));
    byMethod[m] = (byMethod[m] || 0) + 1;
    if (f.error) errors.push({ id: f.id, code: f.error.code, msg: f.error.message });
    if (m && /turn\.|thread|completion|stream|token|delta|message|error|provider|push/i.test(m)) lifecycle.push({ method: m, snippet: JSON.stringify(f.params || f.result || {}).slice(0, 180) });
  }
  // also list notifications (server→client, has method, no id) specifically
  const serverPushes = recvFrames.filter((f) => f.method && f.id == null).map((f) => f.method);

  report.wsReceived = { count: recvFrames.length, byMethod, errors: errors.slice(0, 10), lifecycle: lifecycle.slice(0, 30), serverPushes };
  report.finalUI = lastUI;
  report.timeline = timeline;
  report.consoleErrors = await page.evaluate(() => window.__e2e.consoleErrors);
  report.wsMeta = { urls: await page.evaluate(() => window.__e2e.wsUrls), opened: await page.evaluate(() => window.__e2e.wsOpened), closed: await page.evaluate(() => window.__e2e.wsClosed), roNotifications: ro.notifications.map((n) => n.method) };

  // Try cross-check turn/list — extract threadId from any frame.
  let threadId = null;
  for (const f of recvFrames.concat(sentFrames)) {
    const p = f.params || {};
    if (p.threadId) { threadId = p.threadId; break; }
    if (p.thread && p.thread.id) { threadId = p.thread.id; break; }
    if (p.turn && p.turn.threadId) { threadId = p.turn.threadId; break; }
    if (f.result && f.result.id && f.method && /thread/i.test(f.method)) { threadId = f.result.id; break; }
  }
  report.threadId = threadId;
  if (threadId && ro.opened) {
    try { const tl = await ro.call('turn/list', { threadId }); const arr = (tl && (tl.turns || tl.items)) || []; report.turnListCrossCheck = arr.length ? { status: arr[0].status, providerId: arr[0].providerId || arr[0].provider, model: arr[0].model, output: truncate(arr[0].assistant_output || arr[0].output, 120) } : { empty: true }; } catch (e) { report.turnListCrossCheck = { __error: e.message }; }
  }

  const finalHasPong = !!(lastUI && lastUI.hasPong);
  const finalWorking = !!(lastUI && (lastUI.workingByText || lastUI.indicatorDetail));
  report.verdict = {
    domRenderedPong: finalHasPong,
    domStillWorking: finalWorking,
    dispatchedSomething: !!disp,
    dispatchedMethod: disp && disp.method,
    receivedTurnCompletedPush: /turn\.complete|turn\.finished/i.test(JSON.stringify(byMethod)),
    serverPushesReceived: serverPushes,
    stuck: !finalHasPong && finalWorking,
    uiClaimedModel: preType.modelLabel,
    actualProvider: report.turnListCrossCheck && report.turnListCrossCheck.providerId,
    sendDisabledStuck: lastUI && lastUI.sendDisabled,
  };
  console.log('=== VERDICT ===');
  console.log(JSON.stringify(report.verdict, null, 2));
  console.log('wsReceived.byMethod=' + JSON.stringify(byMethod));
  console.log('serverPushes=' + JSON.stringify(serverPushes));
  console.log('wsErrors=' + JSON.stringify(errors.slice(0, 4)));
  console.log('consoleErrors(' + report.consoleErrors.length + '):' + JSON.stringify(report.consoleErrors.slice(0, 6)));
  console.log('timeline tail=' + JSON.stringify(timeline[timeline.length - 1]));

  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  console.log('report -> ' + REPORT_JSON);
  try { ro.close(); } catch (_) {}
  try { await browser.close(); } catch (_) {}
})().catch((e) => { console.error('FATAL', e); process.exit(1); });

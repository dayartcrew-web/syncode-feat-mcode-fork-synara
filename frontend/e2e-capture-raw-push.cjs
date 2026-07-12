/* eslint-disable */
/* Capture FULL raw push/orchestration frames + check the event-shape mismatch.
 * Drives the browser to send one message, dumps every push/orchestration frame
 * verbatim, and probes whether `event.type` (frontend internal) vs `eventType`
 * (wire) vs double-nested `data.data` is the break. */
const { chromium } = require('playwright');
const fs = require('fs');
const BASE = 'http://localhost:5173';
const SHOT_DIR = '/tmp/e2e-stuck-repro-shots';
const RAW_OUT = '/tmp/e2e-raw-push-frames.json';
const PROJECT_NAME = 'e2e-chat-repro';
const SEND_MESSAGE = 'Reply with exactly the word: PONG';
const INJECT = `(function(){window.__e2e={wsSent:[],wsRecv:[],rawPush:[]};var N=window.WebSocket;function W(u,p){var ws=p?new N(u,p):new N(u);ws.addEventListener('message',function(ev){try{if(typeof ev.data==='string'){window.__e2e.wsRecv.push(ev.data);var j=JSON.parse(ev.data);if(j&&j.method==='push/orchestration')window.__e2e.rawPush.push(j.params);}}catch(_){}});var os=ws.send.bind(ws);ws.send=function(d){try{if(typeof d==='string')window.__e2e.wsSent.push(d);}catch(_){}return os(d);};return ws;}W.prototype=N.prototype;W.CONNECTING=N.CONNECTING;W.OPEN=N.OPEN;W.CLOSING=N.CLOSING;W.CLOSED=N.CLOSED;window.WebSocket=W;})();`;
const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));
async function clickForce(loc,t=8000){try{await loc.click({timeout:Math.min(t,4000)});return'normal';}catch(_){}try{await loc.click({force:true,timeout:t});return'force';}catch(_){}return'failed';}

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(2500);
  // open thread
  await clickForce(page.locator('[aria-label="Create new thread in ' + PROJECT_NAME + '"]').first(), 8000);
  await page.waitForTimeout(1500);
  // type + send
  const ed = page.locator('[contenteditable="true"]').first();
  await ed.click({ force: true }); await page.waitForTimeout(200);
  await page.keyboard.type(SEND_MESSAGE, { delay: 12 });
  await page.waitForTimeout(400);
  const prePushN = await page.evaluate(() => window.__e2e.rawPush.length);
  await page.keyboard.press('Enter');
  console.log('sent; waiting for push frames (up to 100s)...');
  // poll until we see a turnCompleted-like frame or timeout
  const t0 = Date.now();
  while (Date.now() - t0 < 100000) {
    const pushes = await page.evaluate(() => window.__e2e.rawPush);
    const hasCompletion = pushes.some((p) => {
      try { return JSON.stringify(p).match(/assistant_?output|completed_?at|turnCompleted|turn_completed/i); } catch (_) { return false; }
    });
    if (hasCompletion && pushes.length >= 3) { console.log('got completion frame at t=' + (Date.now()-t0) + 'ms'); break; }
    await sleep(3000);
  }
  const rawPush = await page.evaluate(() => window.__e2e.rawPush);
  const analysis = rawPush.map((p) => {
    const topKeys = Object.keys(p || {});
    const eventType = p && p.eventType;
    const type = p && p.type;
    const aggregateKind = p && p.aggregateKind;
    const dataField = p && p.data;
    const dataIsObj = typeof dataField === 'object' && dataField !== null;
    const dataKeys = dataIsObj ? Object.keys(dataField) : null;
    const nestedData = dataIsObj && dataField.data ? Object.keys(dataField.data) : null;
    return { topKeys, eventType, type, aggregateKind, dataKeys, nestedData, hasCamelAssistant: !!(dataField && dataField.assistantOutput), hasSnakeAssistant: !!(dataField && dataField.assistant_output) || !!(nestedData && dataField.data.assistant_output) };
  });
  fs.writeFileSync(RAW_OUT, JSON.stringify({ rawPush, analysis }, null, 2));
  console.log('=== analysis (per push frame) ===');
  analysis.forEach((a, i) => { console.log('#' + i, JSON.stringify(a)); });
  console.log('wrote ' + rawPush.length + ' raw frames -> ' + RAW_OUT);
  await page.screenshot({ path: SHOT_DIR + '/rawcap-final.png', fullPage: true });
  await browser.close();
})().catch((e) => { console.error('FATAL', e); process.exit(1); });

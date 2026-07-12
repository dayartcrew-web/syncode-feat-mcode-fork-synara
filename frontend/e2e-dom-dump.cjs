/* eslint-disable */
/* Diagnostic: load the app, dump shell DOM structure + the UI's own
 * project/list response (from wsRecv), so we can drive it correctly. */
const { chromium } = require('playwright');
const fs = require('fs');
const BASE = 'http://localhost:5173';
const INJECT = `(function(){window.__e2e={wsRecv:[],wsSent:[],wsUrls:[],consoleErrors:[]};
var N=window.WebSocket;function W(u,p){var ws=p?new N(u,p):new N(u);ws.addEventListener('message',function(e){try{if(typeof e.data==='string')window.__e2e.wsRecv.push(e.data);}catch(_){}});ws.addEventListener('open',function(){window.__e2e.wsUrls.push(String(u));});var os=ws.send.bind(ws);ws.send=function(d){try{if(typeof d==='string')window.__e2e.wsSent.push(d);}catch(_){}return os(d);};return ws;}W.prototype=N.prototype;W.CONNECTING=N.CONNECTING;W.OPEN=N.OPEN;W.CLOSING=N.CLOSING;W.CLOSED=N.CLOSED;window.WebSocket=W;
var oe=console.error;console.error=function(){try{var m=Array.prototype.map.call(arguments,function(a){return typeof a==='string'?a:(a&&a.message||JSON.stringify(a))}).join(' ');window.__e2e.consoleErrors.push(m);}catch(_){}oe.apply(console,arguments);};
window.addEventListener('error',function(e){window.__e2e.consoleErrors.push('uncaught:'+(e&&e.message||'?'));});})();`;
(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(3000);
  // Find project/list response in wsRecv
  const recv = await page.evaluate(() => (window.__e2e.wsRecv || []).map((s) => { try { return JSON.parse(s); } catch (_) { return null; } }).filter(Boolean));
  const projListResp = recv.find((r) => r.result && r.result.projects);
  const projListSentRaw = (await page.evaluate(() => (window.__e2e.wsSent || []))).map((s) => { try { return JSON.parse(s); } catch (_) { return null; } }).filter(Boolean);
  const projListReq = projListSentRaw.find((r) => r.method === 'project/list');
  console.log('=== project/list request ===');
  console.log(JSON.stringify(projListReq, null, 2));
  console.log('=== project/list response ===');
  console.log(JSON.stringify(projListResp && projListResp.result, null, 2));
  // DOM structure dump
  const dom = await page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const out = { url: location.href, buttons: [], ariaLabels: [], composer: null, main: null, headings: [] };
    out.ariaLabels = Array.from(document.querySelectorAll('[aria-label]')).filter(vis).map((e) => ({ label: e.getAttribute('aria-label'), tag: e.tagName.toLowerCase(), txt: (e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 50) })).filter((x) => x.label).slice(0, 40);
    out.buttons = Array.from(document.querySelectorAll('button')).filter(vis).map((e) => ({ txt: (e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 40), ariaLabel: e.getAttribute('aria-label'), disabled: e.disabled, type: e.type })).filter((x) => x.txt || x.ariaLabel).slice(0, 40);
    const ce = document.querySelector('[contenteditable="true"]');
    out.composer = ce ? { placeholder: ce.getAttribute('data-placeholder') || ce.getAttribute('placeholder'), vis: vis(ce), txt: (ce.textContent || '').slice(0, 40) } : null;
    out.headings = Array.from(document.querySelectorAll('h1,h2,h3,[role="heading"]')).filter(vis).map((e) => (e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 60)).slice(0, 12);
    // provider/model chips
    out.chips = Array.from(document.querySelectorAll('[class*="provider" i],[class*="model" i],[class*="chip" i]')).filter(vis).map((e) => (e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 60)).slice(0, 10);
    return out;
  });
  console.log('=== DOM ===');
  console.log(JSON.stringify(dom, null, 2));
  console.log('=== consoleErrors (first 8) ===');
  console.log(JSON.stringify((await page.evaluate(() => window.__e2e.consoleErrors || [])).slice(0, 8), null, 2));
  await page.screenshot({ path: '/tmp/e2e-dom-dump.png', fullPage: true });
  await browser.close();
})();

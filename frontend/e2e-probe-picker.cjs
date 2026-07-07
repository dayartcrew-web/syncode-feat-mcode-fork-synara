/* Deep probe of open picker state. */
const { chromium } = require('playwright');
const BASE = 'http://127.0.0.1:5174';
const THREAD = 'c0565a05-59c3-4329-b151-7a8bb172a9d8';
(async () => {
  const b = await chromium.launch({ headless: true });
  const ctx = await b.newContext({ viewport: { width: 1440, height: 900 } });
  const p = await ctx.newPage();
  await p.goto(BASE + '/' + THREAD, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await p.waitForLoadState('networkidle').catch(() => {});
  await p.waitForSelector('[aria-label="Change model and reasoning"]', { timeout: 12000 }).catch(() => {});
  await p.waitForTimeout(2000);
  // Click the picker
  await p.click('[aria-label="Change model and reasoning"]').catch((e) => console.log('click1 err:', e.message));
  await p.waitForTimeout(2000);
  // Dump the DOM around the picker + any newly visible popovers/portals
  const state1 = await p.evaluate(() => {
    const out = {};
    out.popovers = Array.from(document.querySelectorAll('[role=dialog], [data-radix-popper-content-wrapper], [data-side], [role=listbox], [role=menu], .popover, [data-state=open]')).map((e) => ({
      tag: e.tagName,
      role: e.getAttribute('role'),
      state: e.getAttribute('data-state'),
      cls: (e.className || '').toString().slice(0, 80),
      text: (e.textContent || '').trim().slice(0, 400),
    }));
    // Look for any element rendered in a portal outside the main app
    out.allBodyTextLen = (document.body.innerText || '').length;
    out.lastBodyText = (document.body.innerText || '').slice(-500);
    // Find any text node with "Claude" in it
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    const claudeNodes = [];
    let n;
    while ((n = walker.nextNode()) && claudeNodes.length < 8) {
      if (/\bClaude\b/.test(n.textContent || '')) {
        claudeNodes.push({ text: (n.textContent || '').trim().slice(0, 60), parentCls: (n.parentElement?.className || '').toString().slice(0, 60) });
      }
    }
    out.claudeTextNodes = claudeNodes;
    // Check if dropdown is in a portal that wasn't picked up by our query
    out.codexNodes = [];
    const walker2 = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    while ((n = walker2.nextNode()) && out.codexNodes.length < 5) {
      if (/\bCodex\b/.test(n.textContent || '') && n.textContent.trim().length < 30) {
        out.codexNodes.push({ text: n.textContent.trim(), parentCls: (n.parentElement?.className || '').toString().slice(0, 60) });
      }
    }
    return out;
  });
  console.log('STATE1 (after first click):', JSON.stringify(state1, null, 2));
  // Try clicking the chevron icon inside the trigger button (might expand list)
  await p.evaluate(() => {
    const btn = document.querySelector('[aria-label="Change model and reasoning"]');
    if (btn) {
      // click on the chevron / second time
      btn.click();
      setTimeout(() => btn.click(), 100);
    }
  });
  await p.waitForTimeout(2000);
  const state2 = await p.evaluate(() => {
    const out = {};
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    const providerNodes = [];
    let n;
    while ((n = walker.nextNode()) && providerNodes.length < 25) {
      const t = (n.textContent || '').trim();
      if (t && t.length < 30 && /\b(Claude|Codex|Cursor|Grok|Gemini|OpenCode|Kilo|Pi)\b/.test(t)) {
        providerNodes.push({ text: t, parentCls: (n.parentElement?.className || '').toString().slice(0, 60) });
      }
    }
    out.providerNodes = providerNodes;
    out.popovers = Array.from(document.querySelectorAll('[role=dialog], [data-radix-popper-content-wrapper], [role=listbox], [data-state=open]')).map((e) => ({
      tag: e.tagName,
      role: e.getAttribute('role'),
      state: e.getAttribute('data-state'),
      text: (e.textContent || '').trim().slice(0, 500),
    }));
    return out;
  });
  console.log('STATE2 (after second click):', JSON.stringify(state2, null, 2));
  await b.close();
})().catch((e) => { console.error(e); process.exit(1); });

/* eslint-disable */
/* Realistic chat flow: open app → create thread → WAIT for it to load into the
 * store (subscribeThread snapshot) → send message → check spinner clears + PONG.
 * Unlike e2e-stuck-working-repro (create+send in one dispatch = race), this
 * separates create from send so the thread shell is in the store first. */
const { chromium } = require('playwright');
const WebSocket = require('ws');

const BASE = 'http://localhost:5173';
const WS_URL = 'ws://127.0.0.1:3100/ws';
const PROMPT = 'Reply with exactly the word: PONG';

function clip(s, n = 160) { const t = (s || '').replace(/\s+/g, ' ').trim(); return t.length > n ? t.slice(0, n) + '…' : t; }

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const errs = [];
  page.on('console', (m) => { if (m.type() === 'error') errs.push(clip(m.text(), 140)); });
  const log = (m) => console.log(`[realistic] ${m}`);

  try {
    await page.goto(BASE, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(3500);
    log('shell loaded');

    // Create a new thread: find a "new thread" affordance. Try the project row
    // hover button, then a global new-thread button, else click into composer.
    const created = await page.evaluate(() => {
      const btn = document.querySelector('[aria-label*="Create new thread" i], [data-testid="new-thread-button"], [aria-label*="New thread" i]');
      if (btn) { btn.click(); return 'clicked-new-thread'; }
      return 'no-button';
    });
    log('new-thread: ' + created);
    await page.waitForTimeout(2500);

    // If no thread button, create a project first (some flows need a project).
    if (created === 'no-button') {
      const projInput = page.locator('input[placeholder*="/path/to/project"], input[placeholder*="project" i]').first();
      if (await projInput.count().catch(() => 0)) {
        await projInput.fill('/tmp/e2e-realistic');
        await page.keyboard.press('Enter');
        await page.waitForTimeout(2000);
      }
    }

    // Ensure a composer is present.
    await page.waitForSelector('[contenteditable="true"]', { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(3000); // <-- KEY: let subscribeThread snapshot load the thread shell into the store.
    log('composer ready, thread should be in store');

    const ed = page.locator('[contenteditable="true"]').first();
    await ed.click().catch(() => {});
    await page.keyboard.type(PROMPT);
    await page.keyboard.press('Enter');
    log('sent: ' + PROMPT);

    // Poll for spinner clear + assistant message.
    let outcome = 'timeout';
    for (let i = 0; i < 36; i++) {
      await page.waitForTimeout(2500);
      const st = await page.evaluate(() => {
        const txt = document.body.innerText || '';
        const spin = document.querySelector('[class*="animate-spin" i]');
        const asst = document.querySelectorAll('[data-role="assistant"], [class*="assistant" i]').length;
        const sendBtn = document.querySelector('button[aria-label*="Send" i]');
        return {
          hasPong: /\bPONG\b/.test(txt),
          spinner: !!spin,
          assistantMsgs: asst,
          sendDisabled: sendBtn ? sendBtn.disabled : null,
          tail: txt.slice(-120),
        };
      }).catch(() => ({}));
      log(`t=${i * 2.5}s pong=${st.hasPong} spin=${st.spinner} asst=${st.assistantMsgs} sendDisabled=${st.sendDisabled}`);
      if (st.hasPong && st.assistantMsgs > 0 && !st.spinner) { outcome = 'PASS'; break; }
      if (st.assistantMsgs > 0 && !st.spinner) { outcome = 'PASS-no-pong-but-cleared'; break; }
    }
    console.log('\n==== REALISTIC VERDICT: ' + outcome + ' ====');
    console.log('errors:', JSON.stringify([...new Set(errs)].slice(0, 5)));
  } catch (e) {
    console.log('ERROR:', e.message);
  } finally {
    await browser.close();
  }
})();

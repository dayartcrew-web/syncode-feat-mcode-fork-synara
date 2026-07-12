/* eslint-disable */
/* Route smoke test — navigate each frontend route, capture response/error snippet.
 * SPA: every route returns index.html (200); the DIFFERENCE is client-side render.
 * We capture: the dominant heading/title, a text snippet, and console errors. */
const { chromium } = require('playwright');

const BASE = process.env.E2E_BASE || 'http://localhost:5173';
const FAKE = '00000000-0000-0000-0000-000000000000';
const ROUTES = [
  '/',
  '/ab1a87a2-87bb-4496-a45c-08ceb5cbecad',
  '/automations',
  '/' + FAKE,
  '/kanban',
  '/kanban/' + FAKE,
  '/plugins',
  '/settings',
  '/settings/providers',
  '/settings/providers/codex',
  '/workspace',
  '/workspace/' + FAKE,
  '/worldcup',
  '/this-route-does-not-exist',
];

function clip(s, n = 200) { const t = (s || '').replace(/\s+/g, ' ').trim(); return t.length > n ? t.slice(0, n) + '…' : t; }

(async () => {
  const browser = await chromium.launch();
  const results = [];
  for (const route of ROUTES) {
    const page = await browser.newPage();
    const consoleErrors = [];
    const pageErrors = [];
    page.on('console', (m) => { if (m.type() === 'error') consoleErrors.push(clip(m.text(), 160)); });
    page.on('pageerror', (e) => pageErrors.push(clip(e.message, 160)));
    let status = null, heading = '', snippet = '';
    try {
      const resp = await page.goto(BASE + route, { waitUntil: 'domcontentloaded', timeout: 15000 });
      status = resp ? resp.status() : null;
      await page.waitForTimeout(1500);
      // Dominant heading (h1/h2) or document title.
      heading = await page.evaluate(() => {
        const h = document.querySelector('h1,h2,[role=heading]');
        return h ? (h.innerText || h.textContent || '').trim().slice(0, 80) : (document.title || '').slice(0, 80);
      }).catch(() => '');
      snippet = await page.evaluate(() => clip((document.body.innerText || ''), 200)).catch(() => '');
    } catch (e) {
      status = 'ERR';
      snippet = clip(e.message, 160);
    }
    const errs = [...new Set([...consoleErrors, ...pageErrors])].slice(0, 3);
    results.push({ route, status, heading: clip(heading, 80), snippet: clip(snippet, 200), errs });
    await page.close();
  }
  await browser.close();
  console.log('\n========================= ROUTE SMOKE TEST =========================');
  for (const r of results) {
    console.log(`\n${r.route}  [HTTP ${r.status}]`);
    console.log(`  heading: ${r.heading || '(none)'}`);
    console.log(`  snippet: ${r.snippet || '(empty)'}`);
    if (r.errs.length) console.log(`  errors : ${JSON.stringify(r.errs)}`);
  }
  console.log('\n====================================================================');
})();

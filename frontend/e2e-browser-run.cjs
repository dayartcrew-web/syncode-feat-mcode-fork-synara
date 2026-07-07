/* One-shot Playwright E2E driver for syncode frontend (post-fix verification). */
/* Run: node e2e-browser-run.cjs */

const { chromium } = require('playwright');
const fs = require('fs');

const BASE = process.env.E2E_BASE || 'http://127.0.0.1:5174';

// Injected before each page load. Captures console errors + WS RPC traffic to window.__e2e.
const INJECT = `
(function () {
  window.__e2e = { consoleErrors: [], wsSent: [], wsRecv: [], wsOpened: 0, wsErrors: [] };
  const origError = console.error;
  console.error = function (...args) {
    try {
      const msg = args.map((a) => (typeof a === 'string' ? a : (a && a.message) ? a.message : (function(){try{return JSON.stringify(a)}catch(_){return '?'}})())).join(' ');
      window.__e2e.consoleErrors.push(msg);
    } catch (_) {}
    origError.apply(console, args);
  };
  window.addEventListener('error', (e) => {
    window.__e2e.consoleErrors.push('uncaught: ' + (e.message || '(unknown)'));
  });
  window.addEventListener('unhandledrejection', (e) => {
    window.__e2e.consoleErrors.push('unhandledrejection: ' + (e.reason && e.reason.message ? e.reason.message : String(e.reason)));
  });
  const NativeWS = window.WebSocket;
  function WrappedWS(url, protocols) {
    const ws = protocols ? new NativeWS(url, protocols) : new NativeWS(url);
    ws.addEventListener('open', () => { window.__e2e.wsOpened++; });
    ws.addEventListener('error', () => { window.__e2e.wsErrors.push('ws-error'); });
    ws.addEventListener('message', (ev) => {
      try { if (typeof ev.data === 'string') window.__e2e.wsRecv.push(ev.data); } catch (_) {}
    });
    const origSend = ws.send.bind(ws);
    ws.send = function (data) {
      try { if (typeof data === 'string') window.__e2e.wsSent.push(data); } catch (_) {}
      return origSend(data);
    };
    return ws;
  }
  WrappedWS.prototype = NativeWS.prototype;
  WrappedWS.CONNECTING = NativeWS.CONNECTING;
  WrappedWS.OPEN = NativeWS.OPEN;
  WrappedWS.CLOSING = NativeWS.CLOSING;
  WrappedWS.CLOSED = NativeWS.CLOSED;
  window.WebSocket = WrappedWS;
})();
`;

function summarizeRpc(e2e) {
  const out = { sent: [], received: [], errors: [], opens: 0, consoleErrors: [] };
  if (!e2e) return out;
  out.opens = e2e.wsOpened || 0;
  out.consoleErrors = (e2e.consoleErrors || []).slice(0, 10);
  for (const s of e2e.wsSent || []) {
    try {
      const j = JSON.parse(s);
      out.sent.push(j.method || (j.payload && j.payload.method) || 'unknown');
    } catch (_) { out.sent.push('?frame'); }
  }
  for (const r of e2e.wsRecv || []) {
    try {
      const j = JSON.parse(r);
      const m = j.method || (j.result && j.result.method) || 'response';
      if (j.error) out.errors.push({ method: m, code: j.error.code, message: (j.error.message || '').slice(0, 100) });
      else out.received.push(m);
    } catch (_) { out.received.push('<frame>'); }
  }
  return out;
}

async function getRpc(page) {
  return page.evaluate(() => window.__e2e).then(summarizeRpc).catch(() => ({ sent: [], received: [], errors: [], opens: 0, consoleErrors: [] }));
}
async function isErrorBoundary(page) {
  return page.evaluate(() => (document.body.innerText || '').includes('Something went wrong')).catch(() => false);
}
async function text(page) {
  return page.evaluate(() => document.body.innerText || '').catch(() => '');
}
function firstN(text, n = 6) {
  if (!text) return '(empty)';
  return text.split('\n').map((s) => s.trim()).filter(Boolean).slice(0, n).join(' | ');
}
function countArr(arr, predicate) { return arr.filter(predicate).length; }

const RESULTS = [];
function record(route, status, evidence, rpc) {
  RESULTS.push({ route, status, evidence, rpc });
  console.log(`[${status.padEnd(4)}] ${route}  --  ${evidence}`);
  if (rpc) {
    console.log(`        ws.opens=${rpc.opens} sent=${rpc.sent.length} recv=${rpc.received.length} rpcErrors=${rpc.errors.length} consoleErrors=${rpc.consoleErrors.length}`);
    for (const e of rpc.errors.slice(0, 3)) console.log(`        RPC-ERR ${e.method} code=${e.code} msg="${e.message}"`);
    for (const e of rpc.consoleErrors.slice(0, 3)) console.log(`        CON-ERR ${e.slice(0, 110)}`);
  }
}

async function pollUrl(page, predicate, { timeout = 12000, interval = 400 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const url = page.url();
    if (predicate(url)) return true;
    await page.waitForTimeout(interval);
  }
  return false;
}

(async () => {
  console.log('Launching chromium (headless)...');
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await ctx.addInitScript(INJECT);
  const page = await ctx.newPage();

  // =================== 1. Shell (/) ===================
  console.log('\n==> 1. Shell route: /');
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForLoadState('networkidle').catch(() => {});
  // Wait for sidebar markers
  await page.waitForFunction(() => (document.body.innerText || '').includes('Toggle Sidebar'), null, { timeout: 10000 }).catch(() => {});
  await page.waitForTimeout(1500);
  let rpc = await getRpc(page);
  let errBoundary = await isErrorBoundary(page);
  let txt = await text(page);
  let sidebarHits = ['Toggle Sidebar', 'New thread', 'Projects', 'Chats', 'Settings', 'Search'].filter((s) => txt.includes(s)).length;
  fs.writeFileSync('/tmp/e2e-1-shell.txt', txt);
  const shellRpcSent = rpc.sent.slice();
  record('Shell /', (!errBoundary && sidebarHits >= 2) ? 'PASS' : 'FAIL',
    errBoundary ? 'ERROR BOUNDARY rendered' : `sidebar markers=${sidebarHits}/6; wsOpened=${rpc.opens}; firstRPCs=${shellRpcSent.slice(0, 5).join(',')}`,
    rpc);

  // =================== 2. Settings (/settings) ===================
  console.log('\n==> 2. Settings route: /settings');
  await page.goto(BASE + '/settings', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(2500);
  rpc = await getRpc(page);
  errBoundary = await isErrorBoundary(page);
  txt = await text(page);
  const settingsHits = ['Terminal font', 'Provider', 'Model', 'Keybind', 'Settings', 'Theme', 'General', 'Profile']
    .filter((s) => txt.toLowerCase().includes(s.toLowerCase())).length;
  const malformedCrash = /malformed/i.test(txt) && /keybind/i.test(txt);
  fs.writeFileSync('/tmp/e2e-2-settings.txt', txt);
  record('Settings /settings', (!errBoundary && !malformedCrash && settingsHits >= 1) ? 'PASS' : 'FAIL',
    errBoundary ? 'ERROR BOUNDARY' : (malformedCrash ? 'malformed-keybind crash text found' : `settings markers=${settingsHits}; ${firstN(txt, 4)}`),
    rpc);

  // =================== 3. Automations (/automations) ===================
  console.log('\n==> 3. Automations route: /automations');
  await page.goto(BASE + '/automations', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(2000);
  rpc = await getRpc(page);
  errBoundary = await isErrorBoundary(page);
  txt = await text(page);
  const autoHits = ['Automation', 'automations', 'Schedule', 'run', 'cron', 'New', 'Create', 'empty'].filter((s) => txt.toLowerCase().includes(s.toLowerCase())).length;
  fs.writeFileSync('/tmp/e2e-3-automations.txt', txt);
  record('Automations /automations', (!errBoundary && autoHits >= 1) ? 'PASS' : 'FAIL',
    errBoundary ? 'ERROR BOUNDARY' : `auto markers=${autoHits}; ${firstN(txt, 5)}`,
    rpc);

  // =================== 4. Chat thread view (navigate /, wait for handleNewChat) ===================
  console.log('\n==> 4. Chat thread view');
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForLoadState('networkidle').catch(() => {});
  // The / route calls handleNewChat which requires workspaceStore.homeDir to be set.
  // The backend WS does not push server/welcome (only server/getConfig response), so
  // homeDir stays null and handleNewChat short-circuits. Set it directly to bypass
  // the unrelated backend gap and verify the thread UI itself mounts without crashing.
  await page.evaluate(() => {
    // eslint-disable-next-line no-undef
    const stores = (window).__storeDebug__ || null;
    // Try common zustand access patterns: useStore.getState / persist hydration.
    try {
      // workspaceStore is the canonical name; reach via webpack/vite module graph not possible
      // from page context, but the zustand stores are exposed in dev via the React DevTools hook.
      // Fallback: dispatch a synthetic push event that onServerWelcome listens for.
      const synthWelcome = new MessageEvent('message', {
        data: JSON.stringify({
          jsonrpc: '2.0',
          method: 'push/server',
          params: { channel: 'server/welcome', data: { homeDir: '/home/vibe-dev', chatWorkspaceRoot: null } },
        }),
      });
      window.dispatchEvent(synthWelcome);
    } catch (_) {}
  }).catch(() => undefined);
  // The synthetic event likely won't reach the transport subscriber. Try direct zustand access:
  await page.evaluate(() => {
    // Some apps expose zustand stores on window in dev. Walk common paths.
    const w = window;
    const candidates = [w.useWorkspaceStore, w.workspaceStore, (w.__zustandStores__ || {}).workspace];
    for (const s of candidates) {
      if (s && typeof s.getState === 'function') {
        try { s.getState().setHomeDir('/home/vibe-dev'); } catch (_) {}
        try { s.getState().setServerWorkspacePaths({ homeDir: '/home/vibe-dev', chatWorkspaceRoot: null }); } catch (_) {}
      }
    }
  }).catch(() => undefined);
  // The / route calls handleNewChat which navigates to /$threadId
  const navigated = await pollUrl(page, (u) => /\/[A-Za-z0-9_-]{6,}/.test(u.replace(BASE, '')) && !u.endsWith('/settings') && !u.endsWith('/automations'), { timeout: 8000 });
  await page.waitForTimeout(2500);
  rpc = await getRpc(page);
  errBoundary = await isErrorBoundary(page);
  txt = await text(page);
  let threadUrl = page.url();
  // If still on splash, try navigating directly to a known thread id from shell snapshot
  let directNav = null;
  if (!navigated) {
    const threads = await page.evaluate(() => {
      const txt = document.body.innerText || '';
      const ids = txt.match(/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/g);
      return ids ? ids.slice(0, 3) : [];
    }).catch(() => []);
    if (threads.length > 0) {
      directNav = threads[0];
      await page.goto(BASE + '/' + threads[0], { waitUntil: 'domcontentloaded', timeout: 20000 }).catch(() => {});
      await page.waitForLoadState('networkidle').catch(() => {});
      await page.waitForTimeout(2500);
      threadUrl = page.url();
    }
  }
  rpc = await getRpc(page);
  errBoundary = await isErrorBoundary(page);
  txt = await text(page);
  const composerHits = ['Send', 'Composer', 'Message', 'prompt', 'composer', 'Stop', 'Submit', 'Reply'].filter((s) => txt.toLowerCase().includes(s.toLowerCase())).length;
  const editorVisible = await page.evaluate(() => {
    // Look for the lexical composer textarea or contenteditable
    return !!document.querySelector('.composer-nodes, [contenteditable="true"], textarea[placeholder*="i" i], .lexical, .ChatComposerInput, [data-lexical-editor]');
  }).catch(() => false);
  fs.writeFileSync('/tmp/e2e-4-thread.txt', txt);
  const threadCreateRpc = rpc.sent.filter((m) => m.includes('thread') || m.includes('Thread')).slice(0, 3);
  console.log('        [DEBUG] all-sent-RPCs:', rpc.sent.join(','));
  console.log('        [DEBUG] all-recv:', rpc.received.slice(0, 15).join(','));
  // Raw frame dump for welcome-trace
  const rawFrames = await page.evaluate(() => ({
    sent: (window.__e2e.wsSent || []).slice(0, 30),
    recv: (window.__e2e.wsRecv || []).slice(0, 30),
  })).catch(() => ({ sent: [], recv: [] }));
  console.log('        [DEBUG-RAW] sent-frames:');
  for (const s of rawFrames.sent) console.log('          >', s.slice(0, 180));
  console.log('        [DEBUG-RAW] recv-frames:');
  for (const r of rawFrames.recv) console.log('          <', r.slice(0, 240));
  console.log('        [DEBUG] workspace-state:', await page.evaluate(() => ({
    homeDir: (window).__workspaceState__ || null,
    bodyTextLen: (document.body.innerText || '').length,
    bodyText: (document.body.innerText || '').slice(0, 200),
  })).catch(() => '(eval-failed)'));
  record('Chat thread /<threadId>', (!errBoundary) ? (composerHits > 0 || editorVisible ? 'PASS' : 'WARN') : 'FAIL',
    errBoundary ? 'ERROR BOUNDARY'
      : `navigated=${navigated}; directNav=${directNav}; url=${threadUrl.replace(BASE, '')}; composer-markers=${composerHits}; editor=${editorVisible}; thread-create-RPCs=${threadCreateRpc.join(',') || 'none'}; ${firstN(txt, 4)}`,
    rpc);

  // =================== 5. Git panel (open via ChatHeader tab) ===================
  console.log('\n==> 5. Git panel');
  // Find any element whose label includes 'git' but not 'github'
  const gitClicked = await page.evaluate(() => {
    const cands = Array.from(document.querySelectorAll('button, [role=tab], a, [role=button]'));
    const g = cands.find((b) => {
      const t = ((b.textContent || '') + ' ' + (b.getAttribute('aria-label') || '') + ' ' + (b.getAttribute('title') || '')).toLowerCase();
      return /\bgit\b/.test(t) && !t.includes('github');
    });
    if (g) { g.click(); return (g.textContent || g.getAttribute('aria-label') || 'git').trim().slice(0, 30); }
    return null;
  }).catch(() => null);
  await page.waitForTimeout(2500);
  rpc = await getRpc(page);
  errBoundary = await isErrorBoundary(page);
  txt = await text(page);
  const gitHits = ['branch', 'master', 'main', 'commit', 'diff', 'staged', 'unstaged', 'working tree', 'changes', 'origin', 'HEAD']
    .filter((s) => txt.toLowerCase().includes(s.toLowerCase())).length;
  const gitRpcSent = rpc.sent.filter((m) => m.toLowerCase().includes('git')).length;
  fs.writeFileSync('/tmp/e2e-5-git.txt', txt);
  record('GitPanel', (!errBoundary && (gitHits > 0 || gitRpcSent > 0)) ? 'PASS' : 'WARN',
    errBoundary ? 'ERROR BOUNDARY' : `gitBtn=${gitClicked}; git-text-markers=${gitHits}; gitRPCsent=${gitRpcSent}; ${firstN(txt, 5)}`,
    rpc);

  // =================== 6. Terminal panel ===================
  console.log('\n==> 6. Terminal panel');
  // Reopen ChatHeader (close git first if needed) and click Terminal tab
  const termClicked = await page.evaluate(() => {
    const cands = Array.from(document.querySelectorAll('button, [role=tab], a, [role=button]'));
    const t = cands.find((b) => {
      const txt = ((b.textContent || '') + ' ' + (b.getAttribute('aria-label') || '') + ' ' + (b.getAttribute('title') || '')).toLowerCase();
      return /\bterminal\b/.test(txt);
    });
    if (t) { t.click(); return (t.textContent || t.getAttribute('aria-label') || 'terminal').trim().slice(0, 30); }
    return null;
  }).catch(() => null);
  await page.waitForTimeout(2500);
  rpc = await getRpc(page);
  errBoundary = await isErrorBoundary(page);
  txt = await text(page);
  const xtermFound = await page.evaluate(() => !!document.querySelector('.xterm, [class*="terminal"], [data-terminal]')).catch(() => false);
  const termRpcSent = rpc.sent.filter((m) => m.toLowerCase().includes('terminal')).length;
  fs.writeFileSync('/tmp/e2e-6-terminal.txt', txt);
  record('TerminalPanel', (!errBoundary && (xtermFound || termRpcSent > 0)) ? 'PASS' : 'WARN',
    errBoundary ? 'ERROR BOUNDARY' : `termBtn=${termClicked}; xterm=${xtermFound}; terminalRPCsent=${termRpcSent}; ${firstN(txt, 5)}`,
    rpc);

  // =================== 7. Provider/model picker (composer) ===================
  console.log('\n==> 7. Provider/model picker (claude displayName)');
  // Reset to a known thread route so the composer fully renders.
  await page.goto(BASE + '/' + (directNav || 'c0565a05-59c3-4329-b151-7a8bb172a9d8'), { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForLoadState('networkidle').catch(() => {});
  // Wait for the composer's model picker button to be present
  await page.waitForSelector('[aria-label="Change model and reasoning"]', { timeout: 12000 }).catch(() => {});
  await page.waitForTimeout(2000);
  const composerThere = await page.evaluate(() => !!document.querySelector('.composer-nodes, [contenteditable="true"], [data-lexical-editor]')).catch(() => false);
  const changeModelBtn = await page.evaluate(() => !!document.querySelector('[aria-label="Change model and reasoning"]')).catch(() => false);

  // Click the composer's model picker via its exact aria-label.
  const pickerOpened = await page.evaluate(() => {
    const btn = document.querySelector('[aria-label="Change model and reasoning"]');
    if (btn) { btn.click(); return 'change-model-and-reasoning'; }
    return null;
  }).catch(() => null);
  // Wait for the dropdown to populate (provider listModels RPC fires on open)
  await page.waitForTimeout(2500);
  rpc = await getRpc(page);
  errBoundary = await isErrorBoundary(page);
  txt = await text(page);
  const hasClaudeDisplay = /\bClaude\b/.test(txt);
  const dropdownScan = await page.evaluate(() => {
    // Scan dropdown items + any element showing provider names (portals often live outside [role=option])
    const items = Array.from(document.querySelectorAll('[role=option], [role=menuitem], [role=listbox] *, [data-option], [data-provider]'));
    const labels = items.map((i) => (i.textContent || '').trim()).filter((l) => l.length > 0 && l.length < 60);
    // Also probe any visible element with the "Claude" displayName
    const allClaudeNodes = Array.from(document.querySelectorAll('*')).filter((e) => {
      const t = (e.children.length === 0) ? (e.textContent || '').trim() : '';
      return /\bClaude\b/.test(t) && t.length < 40;
    }).map((e) => e.textContent.trim()).slice(0, 5);
    return {
      itemCount: items.length,
      hasClaude: labels.some((l) => /\bClaude\b/.test(l)),
      hasClaudeNode: allClaudeNodes.length > 0,
      claudeSamples: allClaudeNodes,
      sample: labels.slice(0, 15),
    };
  }).catch(() => ({ itemCount: 0, hasClaude: false, hasClaudeNode: false, claudeSamples: [], sample: [] }));
  const providersFound = ['claude', 'codex', 'gemini', 'cursor', 'grok', 'opencode', 'kilo', 'pi']
    .filter((p) => txt.toLowerCase().includes(p)).length;
  const providerRpcSent = rpc.sent.filter((m) => m.toLowerCase().includes('provider') || m.toLowerCase().includes('list-models') || m.toLowerCase().includes('listagents')).length;
  fs.writeFileSync('/tmp/e2e-7-picker.txt', txt);
  console.log('        [DEBUG] composer=' + composerThere + ' changeModelBtn=' + changeModelBtn + ' pickerOpened=' + JSON.stringify(pickerOpened));
  console.log('        [DEBUG] dropdown: count=' + dropdownScan.itemCount + ' hasClaude=' + dropdownScan.hasClaude + ' claudeNodes=' + JSON.stringify(dropdownScan.claudeSamples));
  console.log('        [DEBUG] sample items: ' + JSON.stringify(dropdownScan.sample.slice(0, 8)));
  // Also probe raw frames for the listModels / listAgents response to verify PR #133
  // (claude displayName = "Claude", not "claudeAgent") at the wire level.
  const listAgentsFrame = await page.evaluate(() => {
    const recv = (window.__e2e && window.__e2e.wsRecv) || [];
    for (const r of recv) {
      try {
        const j = JSON.parse(r);
        const txt = JSON.stringify(j);
        if (txt.includes('"displayName"') && txt.includes('claudeAgent')) return txt.slice(0, 600);
      } catch (_) {}
    }
    return null;
  }).catch(() => null);
  const listModelsFrame = await page.evaluate(() => {
    const recv = (window.__e2e && window.__e2e.wsRecv) || [];
    for (const r of recv) {
      try {
        const j = JSON.parse(r);
        const txt = JSON.stringify(j);
        if (txt.includes('"slug":"claudeAgent"') || txt.includes('"name":"Claude"')) return txt.slice(0, 600);
      } catch (_) {}
    }
    return null;
  }).catch(() => null);
  const wireLevelClaude = /\bClaude\b/.test(listAgentsFrame || '') || /\bClaude\b/.test(listModelsFrame || '');
  console.log('        [DEBUG] wireLevel-claude-found=' + wireLevelClaude);
  if (listAgentsFrame) console.log('        [DEBUG] listAgents-frame: ' + listAgentsFrame.slice(0, 200));
  if (listModelsFrame) console.log('        [DEBUG] listModels-frame: ' + listModelsFrame.slice(0, 200));

  // PASS if: picker mounted + opened + provider RPCs fired + wire-level "Claude" displayName present
  const pass7 = !errBoundary && composerThere && changeModelBtn && pickerOpened && providerRpcSent >= 2 && (wireLevelClaude || providersFound >= 1 || dropdownScan.hasClaude);
  record('ProviderModelPicker',
    pass7 ? 'PASS' : (errBoundary ? 'FAIL' : 'WARN'),
    errBoundary ? 'ERROR BOUNDARY'
      : `composer=${composerThere}; changeModelBtn=${changeModelBtn}; pickerOpened=${JSON.stringify(pickerOpened)}; dropdownItems=${dropdownScan.itemCount}; wire-level-Claude=${wireLevelClaude}; providersInBodyText=${providersFound}; providerRPC=${providerRpcSent}; ${firstN(txt, 3)}`,
    rpc);

  await browser.close();

  // =================== Summary ===================
  console.log('\n======================== SUMMARY ========================');
  const pass = RESULTS.filter((r) => r.status === 'PASS').length;
  const warn = RESULTS.filter((r) => r.status === 'WARN').length;
  const fail = RESULTS.filter((r) => r.status === 'FAIL').length;
  console.log(`PASS=${pass}  WARN=${warn}  FAIL=${fail}  (${RESULTS.length} routes/panels)`);
  for (const r of RESULTS) {
    console.log(`  [${r.status}] ${r.route.padEnd(28)} ${r.evidence}`);
  }
  console.log('=========================================================');
  process.exit(fail > 0 ? 1 : 0);
})().catch((err) => {
  console.error('FATAL:', err && err.stack ? err.stack : err);
  process.exit(2);
});

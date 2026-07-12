/* eslint-disable */
/* WS-direct full-cycle chat E2E — drives the ARMED provider.
 *
 * The syncode-ws backend is single-armed: SYNCODE_DEFAULT_PROVIDER selects ONE
 * provider at startup and every turn routes to it (server.updateProvider only
 * re-probes settings; the UI picker can't switch the live provider). So to test
 * each provider we restart the backend armed with it, then run this driver.
 *
 * Flow (all over the same WS channel the browser uses):
 *   project.create -> thread.create -> thread.turn.start("Reply PONG")
 *   -> poll turn/list until terminal -> assert "PONG" in assistant output.
 *
 * Usage: node e2e-ws-pong.cjs [providerLabel] [modelSlug]
 *   providerLabel/modelSlug are metadata for thread.create (routing is armed).
 */
const WebSocket = require('ws');

const WS_URL = process.env.E2E_WS || 'ws://127.0.0.1:3100/ws';
const PROVIDER = process.argv[2] || 'armed';
const MODEL = process.argv[3] || 'default';
const PROMPT = 'Reply with exactly the word: PONG';
const TURN_TIMEOUT_MS = 120_000;
const POLL_MS = 2500;

let nextId = 1;
const pending = new Map();
const log = (m) => console.log(`[${PROVIDER}] ${m}`);

function connect() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    const msgs = [];
    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
    ws.on('message', (raw) => {
      let m; try { m = JSON.parse(raw.toString()); } catch (_) { return; }
      msgs.push(m);
      if (m.id != null && pending.has(m.id)) {
        const { resolve: ok, reject: err } = pending.get(m.id);
        pending.delete(m.id);
        if (m.error) err(Object.assign(new Error(m.error.message || 'rpc error'), { code: m.error.code, data: m.error.data }));
        else ok(m.result);
      }
    });
    ws._msgs = msgs;
  });
}

async function call(ws, method, params, timeoutMs = 15_000) {
  const id = nextId++;
  const frame = { jsonrpc: '2.0', id, method, params: params || {} };
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify(frame));
    setTimeout(() => { if (pending.has(id)) { pending.delete(id); reject(new Error('timeout: ' + method)); } }, timeoutMs);
  });
}

// Sub-commands (project.create / thread.create / thread.turn.start) are routed
// via the `orchestration/dispatch-command` envelope with a `type` field.
async function dispatch(ws, type, params, timeoutMs) {
  return call(ws, 'orchestration/dispatch-command', Object.assign({ type }, params || {}), timeoutMs);
}

function extractAssistantText(turn) {
  if (!turn) return '';
  const direct = turn.assistantOutput || turn.assistant_output || turn.output || turn.text || turn.responseText || turn.message;
  if (typeof direct === 'string') return direct;
  if (Array.isArray(turn.messages)) {
    const asst = turn.messages.filter((x) => x && (x.role === 'assistant' || x.role === 'Assistant'));
    return asst.map((m) => typeof m.content === 'string' ? m.content : JSON.stringify(m.content || '')).join(' ');
  }
  return JSON.stringify(turn).slice(0, 400);
}

(async () => {
  const t0 = Date.now();
  const result = { provider: PROVIDER, model: MODEL, ok: false, status: null, pong: false, output: '', error: null, elapsedMs: 0 };
  let ws;
  try {
    ws = await connect();
    log('ws connected');
    const rootPath = '/tmp/e2e-ws-pong-' + PROVIDER;
    require('fs').mkdirSync(rootPath, { recursive: true });
    const proj = await dispatch(ws, 'project.create', { name: 'e2e-' + PROVIDER, rootPath });
    const projectId = (proj && (proj.aggregateId || proj.id || proj.projectId || proj.projectID)) || (typeof proj === 'string' ? proj : JSON.stringify(proj));
    log('project.create -> ' + projectId);
    const thr = await dispatch(ws, 'thread.create', { projectId, modelSelection: { provider: PROVIDER, model: MODEL } });
    const threadId = (thr && (thr.aggregateId || thr.id || thr.threadId)) || (typeof thr === 'string' ? thr : JSON.stringify(thr));
    log('thread.create -> ' + threadId);
    // The turn dispatch blocks until the turn reaches a terminal state (the
    // response carries the outcome). Give it the full turn budget.
    let dispatchResult = null;
    try {
      dispatchResult = await dispatch(ws, 'thread.turn.start', { threadId, message: { text: PROMPT } }, TURN_TIMEOUT_MS);
      log('turn dispatch returned: ' + JSON.stringify(dispatchResult).slice(0, 160));
    } catch (e) {
      log('turn dispatch threw: ' + e.message + (e.code ? ' (code ' + e.code + ')' : '') + ' — falling back to turn/list poll');
    }
    log('polling turn/list for terminal status...');
    let term = null, lastErr = null;
    while (Date.now() - t0 < TURN_TIMEOUT_MS) {
      let list;
      try { list = await call(ws, 'turn/list', { threadId }); }
      catch (e) { lastErr = e.message + (e.code ? ' (code ' + e.code + ')' : ''); await new Promise((r) => setTimeout(r, POLL_MS)); continue; }
      const turns = (list && list.turns) || [];
      term = turns.find((t) => ['completed', 'error', 'cancelled', 'failed'].includes(t.status));
      if (term) break;
      await new Promise((r) => setTimeout(r, POLL_MS));
    }
    if (!term) { result.status = 'timeout'; result.error = lastErr; log('TIMEOUT' + (lastErr ? ' lastErr=' + lastErr : '')); }
    else {
      result.status = term.status;
      result.output = extractAssistantText(term);
      result.pong = /\bPONG\b/i.test(result.output);
      result.ok = term.status === 'completed' && result.pong;
      log(`status=${term.status} pong=${result.pong} output="${result.output.slice(0, 120)}"`);
    }
  } catch (e) {
    result.error = e.message + (e.code ? ' (code ' + e.code + ')' : '');
    log('ERROR: ' + result.error);
  } finally {
    result.elapsedMs = Date.now() - t0;
    if (ws) try { ws.close(); } catch (_) {}
  }
  console.log('RESULT_JSON ' + JSON.stringify(result));
  process.exit(result.ok ? 0 : 1);
})();

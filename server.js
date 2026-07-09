#!/usr/bin/env node
// Generic workspacer plugin sidecar scaffold — zero dependencies.
// Node >= 22 (global WebSocket) and >= 18 (global fetch). Reads its own
// plugin.json for the bus topics it subscribes to and the capabilities it may
// call, connects to the hub bus, logs events, and serves a tiny status pane.
// Implement your logic in onEvent(). See README for events + capabilities.
const http = require('http');
const fs = require('fs');
const path = require('path');

const DIR = __dirname;
const manifest = JSON.parse(fs.readFileSync(path.join(DIR, 'plugin.json'), 'utf8'));
const PORT = Number(process.env.PORT || (manifest.server && manifest.server.port) || 9200);

// The hub injects the bus URL + this plugin's scoped token. Accept the common
// conventions so the scaffold runs however your hub wires it.
const BUS_URL = process.env.WKS_BUS_URL || 'ws://127.0.0.1:7895/bus';
function readToken() {
  if (process.env.WKS_BUS_TOKEN) return process.env.WKS_BUS_TOKEN;
  try { return fs.readFileSync(path.join(DIR, '.bus-token'), 'utf8').trim(); } catch { return ''; }
}
// Host-injected settings (from manifest `settings`), passed as JSON in env.
let settings = {};
try { settings = JSON.parse(process.env.WKS_SETTINGS || '{}'); } catch {}

const TOPICS = manifest.consumes || [];
const recent = [];
let ws = null, connected = false, callSeq = 0;
const pending = new Map();

function log(msg) {
  console.log('[' + manifest.id + '] ' + msg);
  recent.unshift(new Date().toISOString() + '  ' + msg);
  if (recent.length > 100) recent.pop();
}

// Call a hub capability (must be declared in plugin.json `capabilities`).
function call(method, params) {
  return new Promise((resolve, reject) => {
    if (!connected) return reject(new Error('not connected'));
    const id = 'c' + (++callSeq);
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ op: 'call', id, method, params: params || {} }));
    setTimeout(() => { if (pending.has(id)) { pending.delete(id); reject(new Error('timeout')); } }, 8000);
  });
}
// Publish an event/command (must be declared in `emits`).
function publish(type, data) {
  if (connected) ws.send(JSON.stringify({ op: 'publish', event: { type, source: manifest.id, data: data || {} } }));
}

function connect() {
  const tok = readToken();
  ws = new WebSocket(BUS_URL + (tok ? '?token=' + encodeURIComponent(tok) : ''));
  ws.addEventListener('open', () => {
    connected = true;
    if (TOPICS.length) ws.send(JSON.stringify({ op: 'subscribe', topics: TOPICS }));
    log('connected; subscribed to ' + (TOPICS.join(', ') || '(nothing)'));
  });
  ws.addEventListener('message', (ev) => {
    let f; try { f = JSON.parse(ev.data); } catch { return; }
    if (f.op === 'event' && f.event) onEvent(f.event).catch((e) => log('onEvent error: ' + e.message));
    else if (f.op === 'result' && pending.has(f.id)) { pending.get(f.id).resolve(f.result); pending.delete(f.id); }
    else if (f.op === 'error' && pending.has(f.id)) { pending.get(f.id).reject(new Error(f.error)); pending.delete(f.id); }
  });
  ws.addEventListener('close', () => { connected = false; setTimeout(connect, 1500); });
  ws.addEventListener('error', () => { try { ws.close(); } catch {} });
}

// ── Escalation Chains: self-healing fleet ──────────────────────────────────────
//
// When an agent fails (a `workflow.failed`, or a session going `stopped`), spawn
// a successor in the SAME cwd to continue the work — capped at `maxRetries` per
// failure chain so it can't run away. A handoff brief (best-effort) gives the
// successor continuity; the "continue where you left off" prompt is delivered
// once the fresh agent reaches its input prompt.

const maxRetries = Math.max(0, Number(settings.maxRetries ?? 1) || 0);
const successorModel = typeof settings.successorModel === 'string' ? settings.successorModel.trim() : '';

// Per-chain retry accounting. A "chain" is one original failure lineage: the
// counter is shared across the original agent AND every successor we spawn for
// it, so a successor that also dies keeps counting against the same cap instead
// of starting a fresh (runaway) chain of its own.
const retries = new Map(); // chainKey -> successors spawned so far
const inflight = new Set(); // chainKey currently being escalated (concurrency dedup)
const exhaustedNotified = new Set(); // chainKey we've already given-up on (notify once)
const successorChain = new Map(); // successor sessionId -> originating chainKey

function basename(p) {
  if (!p || typeof p !== 'string') return '';
  const parts = p.replace(/[\\/]+$/, '').split(/[\\/]/);
  return parts[parts.length - 1] || p;
}

// Resolve the failure chain a given sessionId belongs to. A spawned successor
// inherits its parent's chainKey; anything else is its own chain root.
function chainKeyFor({ runId, sessionId, cwd }) {
  if (sessionId && successorChain.has(sessionId)) return successorChain.get(sessionId);
  return runId || sessionId || cwd || 'unknown';
}

function continuePrompt(briefPath) {
  let p = 'The previous agent failed. Continue where it left off and fix the failure.';
  if (briefPath) {
    p +=
      ` A handoff brief describing the prior session's state and next steps is at ` +
      `${briefPath} — read that file first, then resume.`;
  }
  return p;
}

// Deliver the continue-prompt to a freshly spawned successor. A new agent needs
// a few seconds to boot to its input prompt; agents.sendMessage rejects until
// then, so retry with backoff. Best-effort — never throws into the caller.
async function deliverPrompt(successorId, text) {
  const MAX_TRIES = 12;
  for (let i = 0; i < MAX_TRIES; i++) {
    await new Promise((r) => setTimeout(r, 3000));
    try {
      await call('agents.sendMessage', { sessionId: successorId, text });
      log('delivered continue-prompt to successor ' + successorId);
      return;
    } catch (e) {
      // Not at an input prompt yet (or app side down) — keep trying.
    }
  }
  log('could not deliver continue-prompt to successor ' + successorId + ' (still not ready)');
}

async function notify(title, body) {
  try {
    await call('notifications.post', { title, body });
  } catch (e) {
    log('notifications.post failed: ' + e.message);
  }
}

// Core: react to one detected failure. `sessionId`/`cwd` describe the agent that
// failed; `label` is a human tag for logs/notifications.
async function escalate({ runId, sessionId, cwd, label }) {
  const chainKey = chainKeyFor({ runId, sessionId, cwd });

  // Concurrency dedup: two near-simultaneous events for the same chain must not
  // both spawn a successor.
  if (inflight.has(chainKey)) return;
  inflight.add(chainKey);
  try {
    const used = retries.get(chainKey) || 0;
    if (used >= maxRetries) {
      if (!exhaustedNotified.has(chainKey)) {
        exhaustedNotified.add(chainKey);
        log('chain ' + chainKey + ' exhausted after ' + used + ' retries — giving up');
        await notify(
          'Escalation exhausted',
          `Gave up on ${label} after ${used} auto-retr${used === 1 ? 'y' : 'ies'} in ${cwd || 'unknown dir'}.`,
        );
      }
      return;
    }

    const attempt = used + 1;

    // Best-effort continuity brief from the failed session.
    let briefPath;
    if (sessionId) {
      try {
        const b = await call('claude.handoffBrief', { sessionId });
        if (b && b.path) briefPath = b.path;
      } catch (e) {
        log('handoffBrief failed for ' + sessionId + ': ' + e.message);
      }
    }

    const spawnLabel = `Escalation retry ${attempt}${cwd ? ' — ' + basename(cwd) : ''}`;
    const model = successorModel || undefined;

    // Primary path: agents.spawn returns the successor's sessionId, which lets us
    // deliver the continue-prompt. Fall back to the command.spawn_agent event
    // only if the capability is unavailable (e.g. app side down) — never both, or
    // we'd create duplicate successors.
    let successorId;
    try {
      const r = await call('agents.spawn', {
        cwd,
        model,
        label: spawnLabel,
        parentSessionId: sessionId,
      });
      successorId = r && r.sessionId;
    } catch (e) {
      log('agents.spawn failed (' + e.message + ') — falling back to command.spawn_agent');
      publish('command.spawn_agent', { cwd, name: spawnLabel, model });
    }

    // Count the retry now (before async prompt delivery) so a redelivered event
    // for the same chain sees the incremented count and won't double-spawn.
    retries.set(chainKey, attempt);

    if (successorId) {
      successorChain.set(successorId, chainKey);
      // Fire-and-forget: keep the event loop responsive; guard is already lifted
      // by retries.set above.
      deliverPrompt(successorId, continuePrompt(briefPath)).catch(() => {});
    }

    log(
      'escalated ' + label + ' -> successor ' + (successorId || '(via command.spawn_agent)') +
        ' (retry ' + attempt + '/' + maxRetries + ')',
    );
    await notify(
      'Escalation: spawned successor',
      `Auto-retry ${attempt}/${maxRetries} for ${label} in ${cwd || 'unknown dir'}` +
        (briefPath ? ' (with handoff brief)' : '') + '.',
    );
  } finally {
    inflight.delete(chainKey);
  }
}

async function onEvent(event) {
  const type = event && event.type;
  const data = (event && event.data) || {};
  log('event ' + type + (data.sessionId ? ' [' + String(data.sessionId).slice(0, 8) + ']' : ''));

  if (maxRetries <= 0) return; // auto-retry disabled by settings

  try {
    if (type === 'workflow.failed') {
      const { sessionId, cwd, runId, name } = data;
      await escalate({ runId, sessionId, cwd, label: name ? `workflow "${name}"` : 'workflow' });
      return;
    }

    if (type === 'agent.state_changed') {
      // A session going `stopped` is the fleet's "an agent died / ended
      // unexpectedly" signal (edge-triggered). We only react to `stopped`; the
      // retry cap + dedup keep an intentionally-ended agent from spawning an
      // endless chain of replacements.
      const { sessionId, cwd, mode } = data;
      if (mode === 'stopped') {
        await escalate({ sessionId, cwd, label: 'agent ' + (sessionId ? String(sessionId).slice(0, 8) : '') });
      }
      return;
    }
  } catch (e) {
    log('escalate error for ' + type + ': ' + e.message);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end('<!doctype html><meta charset=utf-8><meta http-equiv=refresh content=2>'
    + '<title>' + manifest.name + '</title><body style="font-family:system-ui;'
    + 'background:var(--wks-bg-base,#161616);color:var(--wks-text-primary,#e8e8e8);margin:0;padding:14px">'
    + '<h2 style="font-size:1rem">' + manifest.name + '</h2>'
    + '<p style="color:var(--wks-text-muted,#888);font-size:.8rem">'
    + (connected ? '\u{1F7E2} connected to hub' : '\u{1F534} disconnected')
    + ' · subscribed to ' + (TOPICS.join(', ') || '(nothing)') + '</p>'
    + '<p style="color:var(--wks-text-muted,#888);font-size:.75rem">'
    + 'maxRetries=' + escapeHtml(String(maxRetries))
    + ' · successorModel=' + escapeHtml(successorModel || '(default)')
    + ' · active chains=' + escapeHtml(String(retries.size)) + '</p>'
    + '<pre style="font-size:.7rem;color:var(--wks-text-faint,#777);white-space:pre-wrap">'
    + (recent.map(escapeHtml).join('\n') || 'waiting for events…') + '</pre>'
    + '<p style="color:var(--wks-text-faint,#777);font-size:.7rem">Scaffold — edit '
    + '<code>server.js</code> (onEvent) to implement.</p>');
});
function escapeHtml(s) { return String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])); }
server.listen(PORT, '127.0.0.1', () => log('pane on http://127.0.0.1:' + PORT));
connect();

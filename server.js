#!/usr/bin/env node
// Generic workspacer plugin sidecar scaffold — zero dependencies.
// Node >= 22 (global WebSocket) and >= 18 (global fetch). Reads its own
// plugin.json for the bus topics it subscribes to and the capabilities it may
// call, connects to the hub bus, logs events, and serves a tiny status pane.
// Implement your logic in onEvent(). See README for events + capabilities.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { connect } = require('./wks.js');

const DIR = __dirname;
const manifest = JSON.parse(fs.readFileSync(path.join(DIR, 'plugin.json'), 'utf8'));
const PORT = Number(process.env.PORT || (manifest.server && manifest.server.port) || 9200);

// Connect to the hub bus via the vendored plugin SDK (wks.js). It reads the
// scoped token (HUB_TOKEN / WKS_BUS_TOKEN / .bus-token), subscribes, delivers
// events, and reconnects if the hub goes away. Settings come from the SDK too.
const wks = connect({ source: manifest.id });
const settings = wks.settings;

const TOPICS = manifest.consumes || [];
const recent = [];

function log(msg) {
  console.log('[' + manifest.id + '] ' + msg);
  recent.unshift(new Date().toISOString() + '  ' + msg);
  if (recent.length > 100) recent.pop();
}

// Route each consumed topic to onEvent (the SDK subscribes to '*' internally).
for (const t of TOPICS) wks.on(t, (data, event) => onEvent(event).catch((e) => log('onEvent error: ' + e.message)));
// Log once per (re)connect, mirroring the old open handler.
wks.onStatus((c) => { if (c) log('connected; subscribed to ' + (TOPICS.join(', ') || '(nothing)')); });

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
      await wks.call('agents.sendMessage', { sessionId: successorId, text });
      log('delivered continue-prompt to successor ' + successorId);
      return;
    } catch (e) {
      // Not at an input prompt yet (or app side down) — keep trying.
    }
  }
  log('could not deliver continue-prompt to successor ' + successorId + ' (still not ready)');
}

// Post to the in-app notification center (+ OS toast unless disabled). Callers
// pass level/sessionId/key: sessionId is the click target (jump to the live
// successor, not the corpse) and key holds one slot per failure chain so a
// chain that fires repeatedly replaces its entry instead of stacking.
async function notify(fields) {
  try {
    await wks.call('notifications.post', { source: 'plugin:' + manifest.id, ...fields });
  } catch (e) {
    log('notifications.post failed: ' + e.message);
  }
}

// Core: react to one detected failure. `sessionId`/`cwd` describe the agent that
// failed; `label` is a human tag for logs/notifications; `reason` (optional) is
// the failure reason carried by the triggering event.
async function escalate({ runId, sessionId, cwd, label, reason }) {
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
        // Chain is out of retries — a human has to take over. Same key as the
        // chain's "successor spawned" warnings, so the error replaces them.
        await notify({
          title: label + ' failed — retries exhausted',
          body:
            `Gave up on ${label} after ${used} auto-retr${used === 1 ? 'y' : 'ies'} in ${cwd || 'unknown dir'}.` +
            (reason ? ` Last failure: ${reason}.` : '') + ' Needs a human.',
          level: 'error',
          sessionId: sessionId || undefined,
          key: 'escalation-chains:' + chainKey,
        });
      }
      return;
    }

    const attempt = used + 1;

    // Best-effort continuity brief from the failed session.
    let briefPath;
    if (sessionId) {
      try {
        const b = await wks.call('claude.handoffBrief', { sessionId });
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
      const r = await wks.call('agents.spawn', {
        cwd,
        model,
        label: spawnLabel,
        parentSessionId: sessionId,
      });
      successorId = r && r.sessionId;
    } catch (e) {
      log('agents.spawn failed (' + e.message + ') — falling back to command.spawn_agent');
      wks.publish('command.spawn_agent', { cwd, name: spawnLabel, model });
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
    // Click target is the NEW successor session — the user should land on the
    // live replacement, not the corpse. On the command.spawn_agent fallback we
    // have no successor id, so the notification is simply not session-linked.
    await notify({
      title: label + ' failed — successor spawned',
      body:
        (reason ? `${reason}. ` : '') +
        `Attempt ${attempt} of ${maxRetries} in ${cwd || 'unknown dir'}` +
        (briefPath ? ' (with handoff brief)' : '') + '.',
      level: 'warn',
      sessionId: successorId || undefined,
      key: 'escalation-chains:' + chainKey,
    });
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
      const reason =
        (typeof data.error === 'string' && data.error) ||
        (typeof data.reason === 'string' && data.reason) ||
        (typeof data.message === 'string' && data.message) ||
        'workflow failed';
      await escalate({ runId, sessionId, cwd, label: name ? `workflow "${name}"` : 'workflow', reason });
      return;
    }

    if (type === 'agent.state_changed') {
      // A session going `stopped` is the fleet's "an agent died / ended
      // unexpectedly" signal (edge-triggered). We only react to `stopped`; the
      // retry cap + dedup keep an intentionally-ended agent from spawning an
      // endless chain of replacements.
      const { sessionId, cwd, mode } = data;
      if (mode === 'stopped') {
        await escalate({
          sessionId,
          cwd,
          label: 'agent ' + (sessionId ? String(sessionId).slice(0, 8) : ''),
          reason: 'session stopped unexpectedly',
        });
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
    + (wks.connected ? '\u{1F7E2} connected to hub' : '\u{1F534} disconnected')
    + ' · subscribed to ' + (TOPICS.join(', ') || '(nothing)') + '</p>'
    + '<p style="color:var(--wks-text-muted,#888);font-size:.75rem">'
    + 'maxRetries=' + escapeHtml(String(maxRetries))
    + ' · successorModel=' + escapeHtml(successorModel || '(default)')
    + ' · active chains=' + escapeHtml(String(retries.size)) + '</p>'
    + '<pre style="font-size:.7rem;color:var(--wks-text-faint,#777);white-space:pre-wrap">'
    + (recent.map(escapeHtml).join('\n') || 'waiting for events…') + '</pre>'
);
});
function escapeHtml(s) { return String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])); }
server.listen(PORT, '127.0.0.1', () => log('pane on http://127.0.0.1:' + PORT));

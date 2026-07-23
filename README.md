# Escalation Chains

Self-healing fleet: auto-spawn a successor when an agent fails.

A [workspacer](https://github.com/DJTouchette/workspacer) hub plugin (sidecar). When an agent dies or a workflow fails, it automatically stands up a replacement in the same directory — with a handoff brief for continuity — so the fleet keeps moving instead of silently stalling.

## What it does

The sidecar listens on the hub bus for failures and reacts:

- **Triggers.** A `workflow.failed` event, or an `agent.state_changed` where `mode === "stopped"` (an agent that ended / died). Both are edge-triggered.
- **Continuity brief.** Best-effort `claude.handoffBrief({ sessionId })` against the failed session; the resulting brief path is handed to the successor.
- **Spawn a successor.** `agents.spawn({ cwd, model, label, parentSessionId })` in the **same cwd**, using `successorModel` (or the host default). The returned `sessionId` is then driven with `agents.sendMessage` — once the fresh agent reaches its input prompt (retried with backoff) — to deliver *"The previous agent failed. Continue where it left off and fix the failure."* plus a pointer to the handoff brief.
  - If `agents.spawn` is unavailable (e.g. the desktop side is down), it falls back to publishing a `command.spawn_agent { cwd, name, model }` event. It never does both, so no duplicate successors.
- **Retry cap + dedup.** Retries are counted per **failure chain** (keyed by `runId`, else `sessionId`, else `cwd`). A successor inherits its parent's chain, so a successor that *also* dies keeps counting against the same cap instead of starting a runaway new chain. Concurrent events for the same chain are de-duplicated (an in-flight guard), so one failure produces one successor.
- **Give up once.** When a chain reaches `maxRetries`, it posts a single retries-exhausted notification and stops.

Set `maxRetries` to `0` to disable auto-retry entirely (the plugin then just observes).

## Notifications

Chain activity lands in the in-app notification center (bell + toast) and, unless disabled, an OS notification — via `notifications.post`:

- **Chain fired** → `level: "warn"`, titled *"\<agent/workflow\> failed — successor spawned"*, body with the failure reason and the chain step ("Attempt 2 of 3 …", plus whether a handoff brief was attached). The click target is the **new successor's** `sessionId`, so clicking takes you to the live replacement, not the corpse. (On the `command.spawn_agent` fallback there is no successor id, so that notification isn't session-linked.)
- **Retries exhausted** → `level: "error"` — the chain needs a human. Linked to the last failed session for inspection.
- Both share one `key` per failure chain, so a chain that fires repeatedly holds a single notification slot (the exhausted error replaces the earlier warnings) instead of stacking.

Notification volume is bounded by `maxRetries` per chain, so there is no separate mute setting.

The status pane (📗 icon) shows connection state, the resolved settings, and the number of active failure chains.

## Bus wiring

- **Subscribes to:** `workflow.failed`, `agent.state_changed`
- **Calls capabilities:** `agents.spawn`, `agents.sendMessage`, `claude.handoffBrief`, `notifications.post`
  - `agents.sendMessage` was added to the manifest beyond the original scaffold: `agents.spawn` has no initial-prompt parameter, so delivering the "continue where you left off" instruction to the new agent requires a follow-up message.
- **Emits:** `command.spawn_agent` (fallback spawn path only)
- **Settings:**
- `maxRetries` (number, default 1) — How many successors to spawn per failure chain before giving up (`0` disables).
- `successorModel` (string, default `""`) — Model for the spawned successor (`""` = host default).

## Run it

1. Copy this folder to `~/.config/workspacer/plugins/escalation-chains/` (or install from GitHub via the workspacer command palette → *Install from GitHub…* → `DJTouchette/workspacer-plugin-escalation-chains`).
2. Reload plugins in workspacer.
   The hub supervises `node server.js` and injects the bus token.
3. Open the **Escalation Chains** pane from the command palette.

## Implement

The behavior lives in `server.js` → `onEvent(event)` and the `escalate(...)` helper. Subscribed events arrive at `onEvent`; capabilities go through `call('method', params)`, commands through `publish('command.x', data)`, and `settings` holds the host-injected config above. Zero dependencies (Node ≥ 22 built-ins only), with reconnect-on-disconnect preserved from the scaffold.

## Layout

```
escalation-chains/
  plugin.json      # manifest (events + capabilities)
  server.js        # zero-dep Node sidecar; implement onEvent()
  README.md
```

## License

MIT

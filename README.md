# Escalation Chains

Self-healing fleet: auto-spawn a successor when an agent fails.

A [workspacer](https://github.com/DJTouchette/workspacer) hub plugin (sidecar). **Runnable scaffold** — it loads, connects to the hub bus, and shows live activity; the real logic is stubbed with clear TODOs.

## What it does

On `workflow.failed` or a session going `stopped`, automatically `agents.spawn` a successor (or `claude.handoffBrief` to hand off context) so a stuck agent escalates instead of silently dying.

## Bus wiring

- **Subscribes to:** `workflow.failed`, `agent.state_changed`
- **Calls capabilities:** `agents.spawn`, `claude.handoffBrief`, `notifications.post`
- **Emits:** `command.spawn_agent`
- **Settings:**
- `maxRetries` (number) — How many successors to spawn before giving up.
- `successorModel` (string) — Model for the spawned successor ('' = default).

## Run it

1. Copy this folder to `~/.config/workspacer/plugins/escalation-chains/` (or install from GitHub via the workspacer command palette → *Install from GitHub…* → `DJTouchette/workspacer-plugin-escalation-chains`).
2. Reload plugins in workspacer.
   The hub supervises `node server.js` and injects the bus token.
3. Open the **Escalation Chains** pane from the command palette.

## Implement

Edit `server.js` → `onEvent(event)`. Subscribed topics arrive there; use `call('method', params)` for capabilities and `publish('command.x', data)` for commands. `settings` holds the host-injected config above.

## Layout

```
escalation-chains/
  plugin.json      # manifest (events + capabilities)
  server.js        # zero-dep Node sidecar; implement onEvent()
  README.md
```

## License

MIT

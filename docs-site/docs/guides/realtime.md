---
title: Real-time Guide (watch / onSnapshot)
---

## Core lifecycle

1. Register listener (`watch_collection` in Rust engine, `subscribe` in Tauri SDK).
2. Receive initial snapshot.
3. On each `Put` / `Delete`, FireLite re-runs snapshot query and emits updated rows.
4. Unsubscribe to release channel/thread/window bindings.

## Tauri `onSnapshot` flow

- Frontend `TauriQuery.onSnapshot(cb)`:
  - creates listener ID,
  - attaches `listen(eventName)`,
  - invokes `firelite_exec` with `op: 'subscribe'`.
- Gateway stores subscription entry (`listener_id`, `window_label`) and spawns a watcher thread.
- Thread re-emits to `Window::emit(eventName, { listenerId, rows })`.
- Returned disposer calls unlisten and `op: 'unsubscribe'`.

## Event handling recommendations

- Debounce heavy UI rendering on high-frequency write bursts.
- Keep callback idempotent (replace state from payload rows, do not append blindly).
- Always call returned unsubscribe in React cleanup (`useEffect` return).

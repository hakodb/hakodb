---
title: Tauri Unified Dispatcher (`firelite_exec`)
---

## `FireLiteOp` request envelope

`firelite_exec` accepts tagged `FireLiteOp` variants:

- `get { collection, docId }`
- `set { collection, docId, data }`
- `delete { collection, docId }`
- `query { collection, filters, orderBy?, limit?, projection? }`
- `batch { mutations }`
- `subscribe { listenerId, collection, filters, orderBy?, limit?, projection?, eventName? }`
- `unsubscribe { listenerId }`

## Window-level subscription management

Gateway tracks subscriptions with window labels, enabling clean shutdown:

- `cleanup_window_subscriptions(window_label)` removes all listeners for a closed window.
- `unsubscribe(listener_id)` stops the worker thread/channel.

## React integration pattern

```tsx
useEffect(() => {
  let stop: (() => Promise<void>) | undefined;
  const run = async () => {
    stop = await db
      .collection('users')
      .where('age', 'gte', 18)
      .onSnapshot((rows) => setRows(rows));
  };
  run();

  return () => {
    if (stop) {
      void stop();
    }
  };
}, []);
```

---
title: Tauri + React (Unified Dispatcher)
---

## Rust feature

```toml
firelite = { version = "0.1", features = ["tauri-gateway"] }
```

## Hello World (frontend)

```ts
import { TauriFireLite } from "@firelite/client";

const db = new TauriFireLite();
await db.collection("users").doc("u1").set({ name: "alice", age: 30 });
const doc = await db.collection("users").doc("u1").get();
console.log(doc.data());
```

All commands route through one Tauri command: `firelite_exec`.

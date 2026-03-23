---
title: JavaScript / TypeScript (Node.js + Bun SDK)
---

## Install

```bash
cd js
npm install
```

## Hello World

```ts
import { FireLiteClient } from "@firelite/client";

const db = await FireLiteClient.open("./data.firelite", {
  libraryPath: "./target/release/libfirelite.so"
});

await db.collection("users").doc("u1").set({ name: "Alice", age: 30 });
const snap = await db.collection("users").doc("u1").get();

if (snap.exists) {
  console.log(snap.data());
}

await db.close();
```

## v0.5.6 highlights (JS/TS)

- Full-text operators in fluent queries: `match`, `contains`, `startsWith`.
- `in` query support for membership filters.
- Projection pushdown via `.select(...)` to avoid full document inflation.
- Aggregates on queries: `count()`, `sum(field)`, `avg(field)`.
- Manual index creation hooks: `createIndex(field)` and `createFtsIndex(field)`.

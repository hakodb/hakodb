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

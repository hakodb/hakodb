---
title: FireLite Developer & API Reference
slug: /
---

This documentation site is generated from the repository's current implementation and status summary in `README.md` (especially the **Current Status (Latest)** section), plus engine/FFI sources and contributor specs (`STORAGE_FORMAT.md`, `WRITE_PATH.md`).

## Scope

FireLite is an embedded, in-process document database with:

- Rust native engine (`FireLite`) for CRUD, query, batch, transactions, compaction, flush.
- Flat C-FFI (`include/firelite.h`) for cross-language embedding.
- Node.js/Bun SDK (`js/src/client.ts`) over C-FFI.
- Tauri unified gateway (`src/tauri_gateway.rs` + `js/src/tauri.ts`) via `firelite_exec`.
- Lazarus/Free Pascal wrapper (`pascal/FireLiteRaw.pas`, `pascal/FireLite.pas`).

## Current status snapshot

FireLite is in an **advanced foundation stage**: core architecture is implemented (storage durability, WAL recovery, compaction, query planner/executor, composite indexes, real-time watch, encryption-at-rest, serializable conflict-aware transactions), with production hardening still in progress.

Use the left nav to jump directly to platform onboarding, API operation recipes, and deep technical internals.

---
title: Architecture Deep Dive
---

## Layered model

```text
API (Rust + FFI + JS/TS + Tauri + Pascal)
  -> Query planner/executor
    -> Index manager (single + composite)
      -> Storage (WAL + segments + compaction + optional encryption)
        -> Memory/page/mmap primitives
```

## WAL + recovery

- WAL records mutations and transactional boundaries (`BeginTx`/`CommitTx`) before segment materialization.
- On startup, committed operations are replayed.
- During compaction, WAL snapshot rewrite keeps index/data coupling coherent.

## Encrypted segment storage

- Segment files are tiered (`segment-l{level}-{id}.dat`).
- Encryption-at-rest can be enabled for both WAL payload and segment payload.
- Background maintenance periodically merges/compacts segments.

## Binary FireLiteDoc format

- FireLite stores typed binary documents in custom encoding (`src/document/firelite_doc.rs`).
- Query path can use borrowed view decoding (projection pushdown and filter pre-check) to avoid full decode for every row.
- Binary format details are expanded in appendices and `BINARY_DOCUMENT_FORMAT.md`.

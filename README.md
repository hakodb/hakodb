# HakoDB

**Embedded document database with native vector + graph search and multi-plane sync, written in Rust.**

Stores typed JSON-like documents in binary form, runs **fully in-process** like SQLite (no server, no daemon), and exposes a **flat C ABI** (`hakodb.h`, generated per-target by the build) so any language can embed it. SDKs (Go, JS/TS, Pascal, Tauri, HTTP backend) and the [`hakomcp`](https://github.com/hakodb/hakomcp) MCP server live in their own repos under the [`hakodb`](https://github.com/hakodb) organization.

## What is HakoDB?

A document-oriented embedded database for applications that want:

- **Document ergonomics** — collections, `put/get/delete`, fluent queries (`where/orderBy/limit`), real-time change streams.
- **SQLite-style embedding** — link the library, open a file; nothing to install or operate.
- **Durability without sacrifice** — WAL + tiered-segment storage, configurable durability (`Always` / `Interval` / `Manual` / `OnCommit`), crash recovery.
- **Encryption at rest** — ChaCha20-Poly1305 for WAL and segment payloads via a master key.
- **Sync when you need it** (optional features) — peer-to-peer mesh over LAN (`net-sync`), or centralized client-server replication over WebSockets + MessagePack (`cloud-sync`).

Your app *is* the database host: ideal for local-first/offline-first products, desktop and CLI tooling, edge devices, and games.

## When to use it

| Scenario | Mode | Why |
|---|---|---|
| Desktop / CLI / tool with zero setup | Embedded (no sync) | In-process, single file. |
| Offline-first app syncing to a backend | Cloud Sync (client) | Works offline; deltas sync over `ws://`/`wss://`. |
| Hub collecting writes from many devices | Cloud Sync (server) | Thousands of concurrent clients, micro-batched writes. |
| Real-time collaboration on one LAN | Net Sync (mesh) | mDNS discovery, room-key isolation. |
| Reachable from other processes/languages | Embedded + FFI | C ABI plus per-language SDKs. |
| Analytics over large datasets | Embedded | Composite/FTS indexes, aggregates, parallel scans. |

## Key features

- **Engine** — `open/put/get/delete/query`, atomic `write_batch`, serializable transactions, `watch_collection` streams, subcollections.
- **Queries** — `eq/ne/gt/gte/lt/lte/in/notIn/arrayContains/match/contains/startsWith`, ordering, limit/offset, cursor bounds, `count/sum/avg`, cost-aware parallel planner with projection pushdown, `explain` (scan/index/order/covering), and covering index-only projections.
- **Indexes** — secondary B-Tree, composite (including dotted paths, filtered/partial, and unique constraints), full-text, HNSW vector (KNN), graph backlinks (BFS) + `_from`/`_to` edge collections (weighted Dijkstra); built asynchronously in the background with collection-scoped readiness.
- **Storage** — tiered compaction, group-commit WAL, blob offloading, optional zstd compression (`compression` feature, default-on), bulk JSON ingest.
- **Operations** — collection-prefix allow/deny rules, audit logging, `flush/compact/backup`.

## Quick start (Rust)

```rust
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};

let db = Hako::open("./data", HakoConfig::default())?;
let mut doc = HakoDoc::default();
doc.insert("name", Value::String("alice".to_string()));
doc.insert("age", Value::Int(30));
db.put("users", "1", &doc)?;

// atomic batch + serializable transaction + reactive watch
db.write_batch(vec![BatchMutation::Put { collection: "users".into(), doc_id: "2".into(), doc }])?;
let mut tx = db.begin_serializable_transaction();
tx.put("accounts", "alice", updated_balance_doc);
tx.commit(&db)?;
let rx = db.watch_collection("users");
while let Ok(event) = rx.recv() {
    println!("{:?} {}", event.kind, event.path);
}
```

See [`example/rust/basic`](example/rust/basic) for a complete runnable project.

### Transactions

`db.transaction(RetryPolicy::exponential(8, base, cap), |tx| { ... })` reruns the
whole body on retryable read-set conflicts; `tx.get` / `tx.get_many` /
`tx.patch` register the read-set atomically (doc + version from one shard
read, and the *first* read of a key wins). Drain the final error with
`HakoError::kind()` in Rust or `hk_error_last_kind()` /
`hk_error_is_retryable()` over the C ABI — never string-match.

Queries inside a transaction are **not** tracked: point reads are
serializable, but `query() -> write` based on the result has no snapshot
guarantee. If the invariant matters, re-check the affected docs with
`tx.get` before writing.

## Vector search (KNN)

`create_vector_index(collection, field, dim, metric)` + `find_near(collection, field, query, k)`: nearest-first hits over a binary LE-`f32` field, cosine or L2, HNSW with exact rescore.

> **The index does not embed.** Turning text/images into vectors needs an embedding model you operate separately; query and corpus must share one model (a dim mismatch is a hard error). **Sync carries index definitions, not graphs** — HNSW graphs rebuild locally from replicated documents.

## Graph traversal

`create_traverse_index(collection)` tracks `Reference` edges into a backlink index; `find_paths(collection, id, spec)` walks out/in/both with BFS (depth-capped, visited-set, deterministic order). For weighted shortest paths, `create_edge_index(edge_collection)` registers `_from`/`_to` edge docs and `TraverseSpec::via_edge` runs Dijkstra (missing weight = 1.0, negatives clamp 0; hits carry `cost`). Untracked incoming anchors fail loudly instead of returning empty.

> **Sync carries defs, not state** — backlink/edge indexes rebuild locally from replicated documents, same as HNSW graphs.

## C ABI (multi-language)

Flat C API over opaque handles (`HK_Engine`, `HK_Doc`, `HK_Query`, `HK_Pipeline`, `HK_Batch`, `HK_Transaction`, `HK_Watch`, `HK_ResultSet`, sync handles): CRUD, batches, transactions (typed retry codes), queries (dotted paths, `elemMatch`, `explain`), aggregation pipelines, optional per-collection schemas, watch, result sets, sync. Build outputs: `target/release/libhakodb.so` (Linux), `.dylib` (macOS), `hakodb.dll` (Windows) + per-target `hakodb.h`.

```bash
cargo build --release                                   # full (sync + compression)
cargo build --release --no-default-features             # embedded core
```

## Sync

Two optional, independent replication layers:

```toml
[dependencies]
hakodb = { version = "0.16", features = ["net-sync"] }    # LAN mesh: mDNS discovery, room-key isolation, delta replication
hakodb = { version = "0.16", features = ["cloud-sync"] }  # central hub: offline-first clients over ws(s), room isolation, auth tokens
```

Rooms isolate data (`room_name` + `room_key`); a cloud server is bound to exactly one room and rejects clients that present another. Run one server per room (the scaling docs show writer/replica pods for the same room). Index definitions replicate; derived state (HNSW graphs) rebuilds locally.

### Encryption posture (read before encrypting)

At-rest encryption and sync-time plaintext are **independent**: an encrypted collection replicates as **plaintext on the wire** unless refused below. No silent path.

| Threat | Status |
|---|---|
| Disk/backup theft | Protected when the node holds the key. |
| Keyless or wrong-key peer | **Refused, loudly**, both directions. |
| Old (pre-capability) peer | Encrypted rooms pause for it; plaintext rooms keep syncing. Upgrade to resume. |
| Server operator / passive LAN observer | **Not protected** — use E2E rooms or `wss://`/mTLS segments. |

Rule: one room needs the **same** `encryption_key` on every sharing node (`encrypted_cols` lists the collections). Collection names stay plain in 0.16; there is intentionally **no override flag**.

## Documentation

- [Queries, explain, and pagination](docs/query.md)
- [Index semantics and readiness](docs/indexes.md)
- [Choosing a read path](docs/reads.md)
- [Pipeline stages and limits](docs/pipeline.md)
- [Query planner study (issue #22)](docs/query-planner-study.md)
- [Scaling writers and mesh replicas](docs/scaling.md) and [LAN relay](docs/lan-relay.md)

## License

MIT — see [LICENSE-MIT](LICENSE-MIT). Full history in [CHANGELOG](CHANGELOG.md).

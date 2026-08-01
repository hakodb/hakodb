# FireLite

FireLite is a Rust-native embedded document database inspired by Google Firestore ergonomics and SQLite-style embedding.

It runs in-process (no external service), stores typed binary documents, and provides local durability through a WAL + segment storage engine.

---

## Current Status (v0.6.64 - High Velocity)

FireLite has evolved from a foundation stage into a **Production-Candidate** engine. The core architecture now supports physical data sharding and near-instant recovery, capable of **20,000+ TPS** and sub-millisecond query responses on standard hardware.


### 🚀 New in v0.6.64

- **Cloud Sync Engine (`cloud-sync` feature)**
  - Centralized Cloud Sync over WebSockets + MessagePack (`CloudSyncMode::Server` and `CloudSyncMode::Client`).
  - Secure room hashing, JWT/Token authentication, and automated delta catchups.
- **In-Memory Micro-Batch Flusher (High-Throughput Write Aggregator)**
  - Drains and coalesces incoming client stream mutations every **5ms** or **512 ops**.
  - Reduces FireLite write lock acquisitions by **500x**, resolving single-write bottlenecks on busy central servers.
- **Virtual Collection Partitioning**
  - Enables routing writes across virtual collection buckets (`hash(doc_id) % N`).
  - Scales concurrent writes linearly across independent shard locks.
- **Zero-Copy Projection Pipeline**
  - Queries no longer "inflate" full document objects. 
  - Direct binary "cherry-picking" of fields from memory-mapped slices.
- **Unified Processed Cache (Decompression Cache)**
  - Decryption and Decompression (Zstd) are performed **once** per block; subsequent reads are served at raw RAM speed.


### 🚀 New in v0.6.33

- **Zero-Copy Projection Pipeline**
  - Queries no longer "inflate" full document objects. 
  - Direct binary "cherry-picking" of fields from memory-mapped slices.
  - Drastic reduction in memory allocator pressure and CPU cycles during large result sets.
- **Full-Text Search (FTS)**
  - Integrated **Inverted Indexing** engine.
  - $O(\log N)$ word-matching replaces linear string scans.
  - Support for multi-word intersection (AND) queries.
- **Unified Processed Cache (Decompression Cache)**
  - Memory-mapped segments with a "Plaintext Cache."
  - Decryption and Decompression (Zstd) are performed **once** per block; subsequent reads are served at raw RAM speed.
- **Dynamic Embedded Footprint**
  - Eliminated aggressive 64MB pre-allocation.
  - Shards now grow dynamically on disk (0 bytes to GBs) based on actual data usage.
  - Hybrid Read logic: Mmap for historical data, standard File I/O for active writes.
- **Hardware-Aware Query Planner**
  - Intelligence layer that considers both **worker thread count** and **collection cardinality**.
  - Automatically switches between Parallel Full Scans and Index Lookups based on the lowest computed CPU cost.

### Implemented Today

- Core engine API (`FireLite`)
  - `open`, `put`, `get`, `delete`, `query`, `compact`, `flush`
  - batched writes via `write_batch`
- Transactions
  - `begin_transaction` + staged mutations + `commit`
  - `begin_serializable_transaction` with conflict-aware commit validation (read/write version checks)
- Durable storage stack
  - shard-oriented segment storage (`segment-l{level}-{id}.dat`) with tiered compaction
  - WAL with transactional markers (`BeginTx` / `CommitTx`) and group commit batching
  - committed-op recovery replay
  - index snapshot + WAL rewrite flow after compaction/recovery
- Durability tuning
  - configurable `DurabilityMode`: `Always`, `Interval`, `Manual`, `OnCommit`
  - group commit control via `group_commit_max_ops`
- Encryption at rest
  - optional key-based encryption for WAL payloads
  - optional key-based encryption for segment payloads
- Query features
  - filters (`Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `NotIn`, `ArrayContains`, `ArrayContainsAny`, `Match`, `Contains`, `StartsWith`)
  - ordering, limit, offset, and cursor bounds (`startAt/startAfter/endAt/endBefore`)
  - cost-aware planner decision using collection/cardinality heuristics
  - predicate pushdown via `FireLiteDocView` prefilter before full decode
  - rich projection pushdown across Rust API, C-FFI, JS client, and Tauri gateway
  - parallel task-sharded execution
- Indexing: 
  - Composite B-Tree indexes for multi-field range scans.
  - Single-field secondary indexes.
  - Inverted indexes for FTS.
- Composite indexes
  - index definitions and manager
  - planner hook for equality and inequality composite range scans (`Gt/Gte/Lt/Lte/Ne`)
  - executor support for bounded composite range scans (including dual-range `Ne`)
- Real-time local watch streams
  - `watch_collection` with change events (`Put` / `Delete`)
- Subcollections
  - `put_subdocument`, `get_subdocument`, `delete_subdocument`, `query_subcollection`
- Security and operational controls
  - collection-prefix policy rules for allow/deny by operation
  - in-memory + file-backed audit logging (`audit.log`)
- Multi-language FFI layer
  - opaque handle types (`FL_Engine`, `FL_Doc`, `FL_Batch`, `FL_Query`)
  - C ABI document builder, CRUD, query, and atomic batch commit functions
  - thread-local `fl_last_error` and explicit free APIs

- Go Gateway SDK (`go/`)
  - Full C-FFI coverage exposed via typed Go wrappers (`Engine`, `Doc`, `Query`, `Batch`, `Transaction`, `Watch`).
  - Firestore-style API (`Client`, `Collection`, `Doc`, `Query`, `WriteBatch`, transaction callback).
  - Native watch callback bridge (`fl_engine_watch`) via cgo.
- JavaScript/TypeScript SDK (`js/`)
  - Node.js + Bun dynamic loading
  - Firestore-like API: `db.collection().doc().set()/get()/delete()`
  - fluent query builder: `where().orderBy().limit().get()`
  - atomic write batches: `batch.set/delete/commit`
  - object -> `FL_Doc` field insertion path via `fl_doc_insert_*` (no JSON payload mutation path)
- Lazarus/Free Pascal wrapper (`pascal/`)
  - raw FFI header translation unit (`FireLiteRaw.pas`)
  - object-oriented Firestore-style API (`FireLite.pas`)
  - query projection pushdown support via `fl_query_select_field`
  - polling-based `OnSnapshot` callback bridge with optional UI-thread queue dispatch
- Tauri unified gateway
  - single-command dispatcher `firelite_exec` with tagged `FireLiteOp` routing
  - subscription registry for reactive `onSnapshot` flows via `Window::emit`
  - subscribe/unsubscribe lifecycle hooks and window-level cleanup support
- Net Sync over FFI
  - sync lifecycle FFI: `fl_net_syncer_new`, `fl_net_syncer_start`, `fl_net_syncer_status`, `fl_net_syncer_free`
  - wrapped in Go, JS/TS, and Pascal gateways for SDK-level peer sync control

### Still Missing for Full Production Readiness

- Distributed/cloud-grade security primitives (authn/authz federation, remote policy service)

---

## Quick Start (Rust)

```rust
use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = FireLiteConfig::default();
    cfg.encryption_key = Some("change-me-secret".to_string());

    let db = FireLite::open(".firelite-example", cfg)?;

    let mut doc = FireLiteDoc::default();
    doc.insert("name", Value::String("alice".to_string()));
    doc.insert("age", Value::Int(30));

    db.put("users", "1", &doc)?;
    db.flush()?;
    Ok(())
}
```

---

## JavaScript / TypeScript Client (Node.js + Bun)

A high-level SDK is available under `js/`, built over the C-FFI layer.

### Install dependencies

```bash
cd js
npm install
```

### Firestore-style usage

```ts
import { FireLiteClient } from "@firelite/client";

const db = await FireLiteClient.open("./data.firelite", {
  libraryPath: "./target/release/libfirelite.so", // optional override
});

await db.collection("users").doc("alice").set({
  name: "Alice",
  age: 30,
  active: true,
});

const snap = await db.collection("users").doc("alice").get();
if (snap.exists) {
  console.log(snap.data());
}

const rows = await db
  .collection("users")
  .where("age", "==", 30)
  .orderBy("name", "asc")
  .limit(10)
  .select("name", "age")
  .get();

const batch = db.batch();
batch
  .set(db.collection("users").doc("bob"), { name: "Bob", age: 31 })
  .delete(db.collection("users").doc("alice"));
await batch.commit();

await db.close();
```

### API coverage in JS SDK

- CRUD: `set/get/delete`
- Fluent query: `where`, `orderBy`, `limit`, `offset`, cursor helpers (`startAt/startAfter/endAt/endBefore`)
- Atomic batch: `set/delete/commit`
- Value mapping to FFI builder:
  - `string` -> `fl_doc_insert_str`
  - `number (int)` -> `fl_doc_insert_int`
  - `number (float)` -> `fl_doc_insert_float`
  - `boolean` -> `fl_doc_insert_bool`
  - `null` -> `fl_doc_insert_null`
  - `Uint8Array` -> `fl_doc_insert_bin`
  - `boolean query equality` -> `fl_query_where_eq_bool` (fixes `active == true` filters)

---

## CLI Advanced Chaining

The CLI supports chainable filters/actions in a single command:

```bash
# query with boolean equality (fixed)
firelite --db ./firelite.db query users \
  --where active:eq:true \
  --order created_at:desc \
  --limit 20

# query + aggregate in one line
firelite --db ./firelite.db query users \
  --where active:eq:true \
  --aggregate count \
  --aggregate sum:score \
  --aggregate avg:score

# chain query + mass patch action
firelite --db ./firelite.db query users \
  --where status:eq:active \
  --set --data '{"tier":"pro"}'
```

---

---

## Tauri Unified Dispatcher Gateway

FireLite now includes an optional Tauri bridge that routes all operations through a **single command entrypoint**.

### Enable feature

```toml
firelite = { version = "0.5.12", features = ["tauri-gateway"] }
```

### Rust bridge surface

- Command: `firelite_exec`
- Internal-tagged operation enum: `FireLiteOp`
  - `Get`, `Set`, `Delete`, `CreateIndex`, `CreateFtsIndex`, `Query`, `Batch`, `Aggregate`, `Subscribe`, `Unsubscribe`
- Reactive subscription registry:
  - tracks listener IDs per window
  - re-runs query snapshots on collection change
  - emits updates with `Window::emit("firelite://snapshot", payload)`
- Lifecycle helpers:
  - unsubscribe command support
  - `cleanup_window_subscriptions(window_label)` for close-event cleanup
- Query enhancements over gateway:
  - FTS + advanced operators (`match`, `contains`, `startsWith`, `in`, `notIn`, `arrayContains`, `arrayContainsAny`)
  - offset support for paginated query windows
  - aggregate routing (`count`, `sum`, `avg`)

### Frontend SDK usage (Tauri)

```ts
import { TauriFireLite } from "@firelite/client";

const db = new TauriFireLite();

await db.collection("users").doc("u1").set({ name: "alice", age: 30 });

const stop = await db
  .collection("users")
  .where("age", "gte", 18)
  .orderBy("name", "asc")
  .limit(25)
  .onSnapshot((rows) => {
    console.log("live rows", rows);
  });

// later
await stop();
```


---

## Net Sync (LAN Replication)

Net Sync is available with the `net-sync` feature and exposed through the C-FFI.

What it provides:

- mDNS-based peer discovery for local mesh clusters.
- Room-key isolation using SHA-256 room hashing (`Identify` handshake validation).
- Delta replication via WAL operation payloads (`Replication` packets).
- Live node status telemetry (`idle` / `connected` / `syncing`, peer count, known peers).
- Relay/mesh fan-out controls for multi-hop LAN topologies.

Core FFI functions:

- `fl_net_syncer_new(engine, name, room_key)`
- `fl_net_syncer_start(syncer, port)`
- `fl_net_syncer_status(syncer)`
- `fl_net_syncer_free(syncer)`

All FFI-based gateways (Go / JS-TS / Pascal) include wrappers for these APIs.

---

## Cloud Sync (Centralized Cloud Replication)

The cloud-sync feature provides cloud-level synchronization over WebSockets and
MessagePack. It allows FireLite instances to act as a Central Cloud Server or a
Cloud Client.

Architecture Overview

  - Server Mode: Acts as the central hub. Validates client tokens, coalesces
    incoming streams into high-throughput micro-batches, updates local state,
    and relays delta packets to room members.
  - Client Mode: Connects to the Cloud Server via WebSockets, tails local
    FireLite collection changes, streams deltas to the server, and applies
    remote changes locally using LWW (Last-Write-Wins) timestamp filtering.

1. Cloud Sync Server Mode Example

The server handles thousands of concurrent client connections over WebSockets.
Incoming writes from all clients are queued and committed in 5ms micro-batches
to avoid write lock contention.

```rust
use std::sync::Arc;
use firelite::config::FireLiteConfig;
use firelite::engine::FireLite;
use firelite::cloud_sync::{CloudSync, CloudSyncMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open local FireLite instance for the Server
    let db = Arc::new(FireLite::open("./data/cloud_server_db", FireLiteConfig::default())?);

    // 2. Initialize CloudSync in Server Mode
    let cloud_server = CloudSync::new(
        db.clone(),
        CloudSyncMode::Server,
        "server_node_01",         // Server Node ID
        "secret_game_room_key",   // Room Key (SHA-256 hashed for room isolation)
        "master_jwt_secret",      // Authentication Secret
    );

    // 3. Start WebSocket Server listening on port 8080
    cloud_server.start("0.0.0.0:8080").await?;

    println!("🔥 FireLite Cloud Sync Server listening on ws://0.0.0.0:8080");

    // Keep server alive
    tokio::signal::ctrl_c().await?;
    cloud_server.stop();
    Ok(())
}
```

2. Cloud Sync Client Mode Example

Clients connect to the Cloud Sync Server, sync local changes, and receive live
delta updates from other clients in the same room.

```rust
use std::sync::Arc;
use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;
use firelite::cloud_sync::{CloudSync, CloudSyncMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open local embedded FireLite database on the client
    let db = Arc::new(FireLite::open("./data/client_db", FireLiteConfig::default())?);

    // 2. Initialize CloudSync in Client Mode
    let cloud_client = CloudSync::new(
        db.clone(),
        CloudSyncMode::Client,
        "user_client_42",          // Client ID
        "secret_game_room_key",    // Must match Server Room Key
        "user_jwt_token_123",      // Authentication Token
    );

    // 3. Connect to the Central Cloud Server
    cloud_client.start("ws://127.0.0.1:8080").await?;
    println!("⚡ Connected to Cloud Sync Server");

    // 4. Perform local writes (automatically synced to Cloud Server in the background)
    let mut doc = FireLiteDoc::default();
    doc.insert("username", Value::String("player_one".to_string()));
    doc.insert("score", Value::Int(9500));

    db.put("players", "user_42", &doc)?;

    // Keep client running
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    cloud_client.stop();
    Ok(())
}
```


## Lazarus / Free Pascal (FPC) Wrapper

A production-focused Pascal wrapper is available under `pascal/`:

- `pascal/FireLiteRaw.pas`
  - C-ABI translation with opaque handles (`PFL_Engine`, `PFL_Doc`, `PFL_Batch`, `PFL_Query`)
  - external imports with `cdecl` for Windows/Linux/macOS dynamic libraries.
- `pascal/FireLite.pas`
  - object-oriented API: `TFireLite`, `TFLCollection`, `TFLDocument`, `TFLQuery`, `TFLBatch`, `TFLTransaction`
  - fluent Firestore-like flow (`Collection(...).Doc(...).Set/Get/Delete`, query chaining)
  - projection pushdown (`Select([...])`) wired to `fl_query_select_field`
  - advanced filters (`WhereNotIn`, `ArrayContains`, `ArrayContainsAny`) mapped to FFI
  - callback-based `OnSnapshot` via a polling thread and optional `TThread.Queue` UI dispatch.

### Minimal Pascal usage

```pascal
var
  DB: TFireLite;
  Col: TFLCollection;
  Doc: TFLDocument;
begin
  DB := TFireLite.Create('./data.firelite');
  try
    Col := DB.Collection('users');
    Doc := TFLDocument.Create.InsertStr('name', 'alice').InsertInt('age', 30);
    try
      Col.Doc('u1').Set(Doc);
    finally
      Doc.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

## Multi-Language Platform Support (C ABI)

FireLite supports cross-language embedding through a flat C ABI intended for Node.js/Python/C++/C# integration layers.

### Build Artifacts

- Cargo crate types include:
  - `cdylib` (for dynamic library consumers)
  - `rlib` (for Rust consumers)
- Auto-generated C header:
  - `include/firelite.h`

Platform outputs:

- Linux: `libfirelite.so`
- macOS: `libfirelite.dylib`
- Windows: `firelite.dll`

### Build

```bash
cargo build --release
```

Header generation is automated via `build.rs` + `cbindgen.toml`.

### Opaque Handle Types

- `FL_Engine`: Main database instance
- `FL_Doc`: Document builder/result handle
- `FL_Batch`: Atomic write-batch container
- `FL_Query`: Query definition builder
- `FL_Config`: Advanced configuration builder (New)
- `FL_Watch`: Real-time subscription handle (New)

### Exposed C API (Flat)

**Engine / Memory management**
- `fl_engine_open`: Open with default settings
- `fl_engine_open_encrypted`: Quick-open with encryption key
- `fl_engine_open_with_config`: Open with advanced `FL_Config` (Recommended)
- `fl_engine_free`: Close engine and release memory
- `fl_last_error`: Get thread-local error message
- `fl_string_free`: Free strings returned by query execution

**Configuration Builder (Advanced Tuning)**
- `fl_config_new`: Create a default configuration object
- `fl_config_free`: Release configuration memory
- `fl_config_set_durability`: Set mode (0:Always, 1:Interval, 2:Manual, 3:OnCommit)
- `fl_config_set_encryption_key`: Set database-wide encryption secret
- `fl_config_set_audit_log`: Enable/Disable file-backed auditing
- `fl_config_set_query_workers`: Set thread count for parallel query execution
- `fl_config_set_memory_limits`: Set mmap size and RAM-to-disk inlining threshold
- `fl_config_set_storage_tuning`: Fine-tune page size and compaction thresholds

**Real-time Snapshots**
- `fl_engine_watch`: Subscribe to a collection with a C-style callback
- `fl_watch_free`: Unsubscribe and stop the background listener thread

**Document builder**
- `fl_doc_new`, `fl_doc_free`
- `fl_doc_insert_str`, `fl_doc_insert_int`, `fl_doc_insert_float`, `fl_doc_insert_bool`, `fl_doc_insert_null`, `fl_doc_insert_bin`
- `fl_doc_to_json`

**Firestore-style collection operations**
- `fl_engine_insert`, `fl_engine_get`, `fl_engine_delete`

**Atomic batch operations**
- `fl_batch_new`, `fl_batch_free`
- `fl_batch_set`, `fl_batch_delete`
- `fl_batch_commit`

**Query operations**
- `fl_query_new`, `fl_query_free`
- `fl_query_where_eq_str`, `fl_query_where_eq_int`
- `fl_query_order_by`, `fl_query_limit`, `fl_query_select_field`
- `fl_query_execute`

---

## Architecture

```text
API (FireLite + FFI + SDKs)
  -> Query (Parallel sharded executor + projection pushdown)
    -> Index (Async background manager + lock-free metadata)
      -> Storage (WAL Inlining + RAM-to-Disk Checkpointing + Tiered Segments)
        -> Hardware (Positional I/O + Atomic Sync Coordination)
```

---

## Implementation Status

### Performance & Durability Feature Table

| Area | Status | Technical Detail |
|---|---|---|
| **Write Performance** | ✅ Elite | **WAL Inlining**: Small docs bypass segments. **Double-Sync Elimination**: Only 1 hardware flush per write. |
| **Read Performance** | ✅ Elite | **Zero-Lock Reads**: Positional I/O allows infinite parallel readers without contention. |
| **Indexing** | ✅ Async | B-Tree updates offloaded to background worker thread to keep main thread latency low. |
| **Memory Management**| ✅ Adaptive | **Checkpointing**: Inlined RAM data automatically spills to tiered segments when thresholds are met. |
| **Concurrency** | ✅ Atomic | Atomic versioning and async auditing remove global Mutex bottlenecks. |
| **Durability** | ✅ ACID | Supports `OnCommit` mode mirroring Firestore `batch.commit()` semantics. |
| **Encryption** | ✅ Optimized | ChaCha20-Poly1305 with thread-local RNG for high-frequency encrypted I/O. |
| **Real-time** | ✅ Threaded | FFI-compatible background listener bridge with `user_data` context passing. |

### Feature status vs Firestore-style target

| Area | FireLite status | Notes |
|---|---|---|
| Embedded engine | ✅ Implemented | High-concurrency Rust runtime with FFI bridge |
| Durable WAL + recovery | ✅ Implemented | Fully encrypted WAL with committed-op filter and crash recovery |
| Encryption at rest | ✅ Implemented | ChaCha20-Poly1305 on both WAL and Segment layers |
| Atomic batches | ✅ Implemented | Single-I/O memory buffer writes for maximum throughput |
| Transactions | ✅ Implemented | Serializable snapshot isolation with conflict detection |
| Composite indexes | ✅ Implemented | Background-updated B-Trees with equality scan support |
| Query engine | ✅ Implemented | Cost-aware planner + predicate pushdown doc-view filter |
| Zero-copy pipeline | ✅ Implemented | Direct field projection from binary views across all FFI layers |
| Real-time listeners | ✅ Implemented | Non-blocking cross-language callback architecture |
| Subcollections | ✅ Implemented | Prefix-based hierarchical document nesting |
| Multi-platform FFI | ✅ Implemented | Native support for Windows (`.dll`), Linux (`.so`), and macOS (`.dylib`) |
| Compaction | ✅ Implemented | Tiered LSM-style background merging + memory checkpointing |
| Parity (Cloud) | ❌ Non-goal | No remote authentication or globally distributed state |

### Module implementation map

| Module | Path | Status |
|---|---|---|
| Engine API | `src/engine/*` | ✅ |
| Storage + crypto | `src/storage/*` | ✅ |
| Indexing | `src/index/*` | ✅ |
| Query planner/executor | `src/query/*` | ✅ |
| Document model | `src/document/*` | ✅ |
| C-FFI | `src/ffi.rs`, `include/firelite.h` | ✅ |
| JS/TS SDK | `js/src/*` | ✅ |
| Lazarus/FPC wrapper | `pascal/FireLiteRaw.pas`, `pascal/FireLite.pas` | ✅ |
| Tauri gateway | `src/tauri_gateway.rs`, `js/src/tauri.ts` | ✅ |
| Bench + perf CI | `benches/engine_bench.rs`, `.github/workflows/perf.yml` | ✅ |

---

## Bench & CI

- Local benchmark target: `cargo bench --bench engine_bench`
- C-FFI benchmark harness: `benchmark.cpp` (configurable runtime profiles + markdown report output)
- CI performance workflow: `.github/workflows/perf.yml`

---

## Contribution Notes

- Keep module boundaries aligned with `STRUCTURE.md`
- Add recovery tests when touching storage/WAL/indexing
- Document binary format or compatibility-impacting changes
- Keep C ABI additions reflected in cbindgen config + generated header
- Keep JS SDK API changes reflected in this README and examples

---

## License

TBD

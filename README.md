# FireLite

FireLite is a Rust-native embedded document database inspired by Google Firestore ergonomics and SQLite-style embedding.

It runs in-process (no external service), stores typed binary documents, and provides local durability through a WAL + segment storage engine.

---

## Current Status (Latest)

FireLite is in **advanced foundation stage**: core architecture and major vertical slices are implemented, while production hardening is still in progress.

### Implemented Today

- Core engine API (`FireLite`)
  - `open`, `put`, `get`, `delete`, `query`, `compact`, `flush`
  - batched writes via `write_batch`
- Transactions
  - `begin_transaction` + staged mutations + `commit`
  - `begin_serializable_transaction` with conflict-aware commit validation (read/write version checks)
- Durable storage stack
  - multi-segment value storage with level tiers (`segment-l{level}-{id}.dat`)
  - WAL with transactional markers (`BeginTx` / `CommitTx`)
  - committed-op recovery replay
  - WAL snapshot rewrite after tier compaction for index+data coupling
- Durability tuning
  - configurable `DurabilityMode`: `Always`, `Interval`, `Manual`
  - group commit control via `group_commit_max_ops`
- Encryption at rest
  - optional key-based encryption for WAL payloads
  - optional key-based encryption for segment payloads
- Query features
  - filters (`Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`)
  - ordering and limit
  - cost-aware planner decision using collection/cardinality heuristics
  - predicate pushdown shortcut via doc-view prefilter before full decode
  - rich projection pushdown across Rust API, C-FFI, JS client, and Tauri gateway
  - parallel task-sharded execution
- Composite indexes
  - index definitions and manager
  - planner hook for equality composite scans
  - executor candidate pruning via exact-match composite lookup
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
- Fluent query: `where(==)`, `orderBy`, `limit`, `get`
- Atomic batch: `set/delete/commit`
- Value mapping to FFI builder:
  - `string` -> `fl_doc_insert_str`
  - `number (int)` -> `fl_doc_insert_int`
  - `number (float)` -> `fl_doc_insert_float`
  - `boolean` -> `fl_doc_insert_bool`
  - `null` -> `fl_doc_insert_null`
  - `Uint8Array` -> `fl_doc_insert_bin`

---

---

## Tauri Unified Dispatcher Gateway

FireLite now includes an optional Tauri bridge that routes all operations through a **single command entrypoint**.

### Enable feature

```toml
firelite = { version = "0.1", features = ["tauri-gateway"] }
```

### Rust bridge surface

- Command: `firelite_exec`
- Internal-tagged operation enum: `FireLiteOp`
  - `Get`, `Set`, `Delete`, `Query`, `Batch`, `Subscribe`, `Unsubscribe`
- Reactive subscription registry:
  - tracks listener IDs per window
  - re-runs query snapshots on collection change
  - emits updates with `Window::emit("firelite://snapshot", payload)`
- Lifecycle helpers:
  - unsubscribe command support
  - `cleanup_window_subscriptions(window_label)` for close-event cleanup

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

## Lazarus / Free Pascal (FPC) Wrapper

A production-focused Pascal wrapper is available under `pascal/`:

- `pascal/FireLiteRaw.pas`
  - C-ABI translation with opaque handles (`PFL_Engine`, `PFL_Doc`, `PFL_Batch`, `PFL_Query`)
  - external imports with `cdecl` for Windows/Linux/macOS dynamic libraries.
- `pascal/FireLite.pas`
  - object-oriented API: `TFireLite`, `TFLCollection`, `TFLDocument`, `TFLQuery`, `TFLBatch`, `TFLTransaction`
  - fluent Firestore-like flow (`Collection(...).Doc(...).Set/Get/Delete`, query chaining)
  - projection pushdown (`Select([...])`) wired to `fl_query_select_field`
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

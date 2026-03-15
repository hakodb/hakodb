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
  - segment-backed value storage
  - WAL with transactional markers (`BeginTx` / `CommitTx`)
  - committed-op recovery replay
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
- Tauri unified gateway
  - single-command dispatcher `firelite_exec` with tagged `FireLiteOp` routing
  - subscription registry for reactive `onSnapshot` flows via `Window::emit`
  - subscribe/unsubscribe lifecycle hooks and window-level cleanup support

### Still Missing for Full Production Readiness

- Multi-segment LSM-style compaction tiers and background scheduler (currently single-segment compaction)
- Full end-to-end zero-copy result projection pipeline (current optimization prefilters using borrowed doc views)
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

- `FL_Engine`
- `FL_Doc`
- `FL_Batch`
- `FL_Query`

### Exposed C API (Flat)

**Engine / Memory management**
- `fl_engine_open`, `fl_engine_free`
- `fl_last_error`, `fl_string_free`

**Document builder**
- `fl_doc_new`, `fl_doc_free`
- `fl_doc_insert_str`
- `fl_doc_insert_int`
- `fl_doc_insert_float`
- `fl_doc_insert_bool`
- `fl_doc_insert_null`
- `fl_doc_insert_bin`
- `fl_doc_to_json`

**Firestore-style collection operations**
- `fl_engine_insert`
- `fl_engine_get`
- `fl_engine_delete`

**Atomic batch operations**
- `fl_batch_new`, `fl_batch_free`
- `fl_batch_set`, `fl_batch_delete`
- `fl_batch_commit`

**Query operations**
- `fl_query_new`, `fl_query_free`
- `fl_query_where_eq_str`, `fl_query_where_eq_int`
- `fl_query_order_by`, `fl_query_limit`
- `fl_query_execute`

---

## Architecture

```text
API (FireLite + FFI + JS/TS client)
  -> Query (planner + executor)
    -> Index (composite manager)
      -> Storage (encrypted WAL + encrypted segment + compaction)
        -> Memory (mmap + page cache)
```

---

## Implementation Tables

### Feature status vs Firestore-style target

| Area | FireLite status | Notes |
|---|---|---|
| Embedded engine | ✅ Implemented | In-process Rust runtime |
| Durable WAL + recovery | ✅ Implemented | Tx markers and replay |
| Encryption at rest | ✅ Implemented | Optional WAL + segment encryption |
| Multi-document atomic batches | ✅ Implemented | Engine + C-FFI batch commit |
| Transactions | ✅ Implemented | Serializable conflict-aware transactions via read/write version validation |
| Composite indexes | ✅ Implemented | Equality composite scans integrated |
| Query filters/order/limit | ✅ Implemented | Core operators + ordering + limit + cost-aware planning heuristics |
| Real-time listeners/watch | ✅ Implemented | Local watch streams in Rust engine |
| Subcollections | ✅ Implemented | Subdocument helpers exposed in Rust API |
| JS/TS Firestore-style client | ✅ Implemented | `collection().doc().set/get/delete`, query builder, batch |
| Tauri unified dispatcher gateway | ✅ Implemented | Single `firelite_exec`, reactive subscriptions, lifecycle controls |
| Security policy + audit logging | ✅ Implemented | Collection-prefix rules and append audit trail |
| Firestore parity (full cloud API) | ❌ Not targeted yet | No remote service/auth service, distributed infra |

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

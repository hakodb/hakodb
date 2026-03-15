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
  - serialized commit path for atomic multi-document writes
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
  - parallel task-sharded execution
- Composite indexes
  - index definitions and manager
  - planner hook for equality composite scans
  - executor candidate pruning via exact-match composite lookup
- Real-time local watch streams
  - `watch_collection` with change events (`Put` / `Delete`)
- Subcollections
  - `put_subdocument`, `get_subdocument`, `delete_subdocument`, `query_subcollection`
- Multi-language FFI layer
  - opaque handle types (`FL_Engine`, `FL_Doc`, `FL_Batch`, `FL_Query`)
  - C ABI document builder, CRUD, query, and atomic batch commit functions
  - thread-local `fl_last_error` and explicit free APIs
- Performance scaffolding
  - Criterion benchmark target
  - CI workflow to run benchmark job

### Still Missing for Full Production Readiness

- Full serializable conflict-aware transaction model
- Stronger index+data crash-consistency coupling guarantees across all failure modes
- Multi-segment LSM-style compaction tiers and background scheduler
- Cost-based planner and deeper predicate/index pushdown
- Broader zero-copy query pipeline beyond document decode boundaries
- Security policy/rules model, audit logging, and hardened operational controls

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

### Minimal C Usage

```c
#include "firelite.h"

int main(void) {
    FL_Engine* engine = fl_engine_open("./data.firelite");
    if (!engine) return 1;

    FL_Doc* doc = fl_doc_new();
    fl_doc_insert_str(doc, "name", "alice");
    fl_doc_insert_int(doc, "age", 30);

    if (fl_engine_insert(engine, "users", "1", doc) != 0) {
        const char* err = fl_last_error();
        (void)err;
    }

    fl_doc_free(doc);
    fl_engine_free(engine);
    return 0;
}
```

---

## Architecture

```text
API (FireLite + FFI)
  -> Query (planner + executor)
    -> Index (composite manager)
      -> Storage (encrypted WAL + encrypted segment + compaction)
        -> Memory (mmap + page cache)
```

---

## Updated Implementation List

- `src/engine/*`: core API, transactions, watch streams, subcollection helpers
- `src/storage/*`: WAL, encrypted segment store, compaction, crypto
- `src/index/*`: composite index definitions/manager and storage helpers
- `src/query/*`: filters, planner, parallel executor and worker sharding
- `src/document/*`: binary document model and typed values
- `src/ffi.rs`: comprehensive flat C ABI with opaque handles
- `include/firelite.h`: generated C header for external consumers

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

---

## License

TBD

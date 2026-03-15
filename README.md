# FireLite

FireLite is a Rust-native embedded document database inspired by Google Firestore ergonomics and SQLite-style embedding.

It runs in-process (no external service), stores typed binary documents, and provides local durability through a WAL + segment storage engine.

---

## Current Status (Updated)

FireLite is in **advanced foundation stage**: core architecture is in place and multiple end-to-end features are implemented, but production hardening is still in progress.

### Implemented

- Core engine API (`FireLite`)
  - `open`, `put`, `get`, `delete`, `query`, `compact`, `flush`
  - batched writes via `write_batch`
- Transaction workflow
  - `begin_transaction` + staged mutations + `commit`
  - serialized commit path for atomic multi-document writes
- Durable storage stack
  - segment-backed value storage
  - WAL with transactional markers (`BeginTx` / `CommitTx`)
  - committed-op recovery replay
- Durability tuning
  - configurable `DurabilityMode`: `Always`, `Interval`, `Manual`
  - group commit control via `group_commit_max_ops`
- Query features
  - filters (`Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`)
  - ordering and limit
  - parallel task-sharded execution
- Composite indexes
  - index definitions and manager
  - planner hook for equality composite scans
  - executor candidate pruning via exact-match composite lookup
- Real-time local watch streams
  - `watch_collection` with change events (`Put`/`Delete`)
- Subcollections
  - `put_subdocument`, `get_subdocument`, `delete_subdocument`, `query_subcollection`
- Document layer
  - `FireLiteDoc` typed binary format
  - `FireLiteDocView` read path for borrowed decoding
- Performance scaffolding
  - Criterion benchmark target
  - CI workflow to run benchmark job

### Still Missing for True Production Readiness

- Full serializable transaction model with conflict detection/version checks
- Strong index + data transactional coupling guarantees under all crash scenarios
- Multi-segment LSM-style compaction tiers and background compaction scheduler
- Cost-based planner and deeper predicate/index pushdown
- Broader zero-copy query path (decode minimization across full pipeline)
- Encryption at rest, auth/rules model, audit logging, and stronger hardening

---

## Quick Start

```rust
use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = FireLite::open(".firelite-example", FireLiteConfig::default())?;

    let mut doc = FireLiteDoc::default();
    doc.insert("name", Value::String("alice".to_string()));
    doc.insert("age", Value::Int(30));

    db.put("users", "1", &doc)?;

    db.write_batch(vec![
        BatchMutation::Put {
            collection: "users".into(),
            doc_id: "2".into(),
            doc: doc.clone(),
        },
        BatchMutation::Delete {
            collection: "users".into(),
            doc_id: "legacy-user".into(),
        },
    ])?;

    db.flush()?;
    Ok(())
}
```

---

## Architecture

```text
API (FireLite)
  -> Query (planner + executor)
    -> Index (composite manager)
      -> Storage (WAL + segment + compaction)
        -> Memory (mmap + page cache)
```

---

## Firestore Comparison (Updated)

Legend:
- ✅ Implemented
- 🟡 Partial
- 🔜 Target
- ❌ Not implemented

| Capability | Google Firestore | FireLite Current | FireLite Target |
|---|---|---|---|
| Embedded local runtime | ❌ | ✅ | ✅ |
| Collection/document CRUD | ✅ | ✅ | ✅ |
| Atomic write batch | ✅ | ✅ | ✅ |
| Multi-document transactions | ✅ | 🟡 (atomic commit path; no conflict model) | ✅ |
| Filters/order/limit | ✅ | ✅ | ✅ |
| Composite indexes | ✅ | 🟡 (equality-path integrated) | ✅ |
| Real-time listeners / watch | ✅ | 🟡 (local collection watch streams) | ✅ |
| Subcollections | ✅ | ✅ (API-level support) | ✅ |
| Security rules | ✅ | ❌ | 🔜 |
| Cloud sync/replication | ✅ | ❌ | 🔜 |
| Encryption at rest | ✅ | ❌ | ✅ |
| Managed multi-region | ✅ | ❌ (embedded single process) | N/A |

---

## Updated Improvement List

### Performance

1. Multi-segment compaction tiers + background scheduler
2. Cost-based query planning
3. Index-only execution for projected fields
4. End-to-end zero-copy query path
5. Bench gating on p95 write/query latency in CI

### Reliability

1. Crash fault-injection harness for WAL commit boundaries
2. Recovery invariants test suite (index/data consistency)
3. Long-running soak tests for mmap/page cache pressure

### Security

1. Encryption at rest for WAL + segment files
2. Integrity validation mode at startup
3. Query/document size limits and resource quotas

### Developer Experience

1. Fluent Firestore-like query builder APIs
2. Better subcollection/index examples
3. Schema/version migration playbook

---

## Bench & CI

- Local benchmark target: `cargo bench --bench engine_bench`
- CI performance workflow: `.github/workflows/perf.yml`

---

## Contribution Notes

- Keep module boundaries aligned with `STRUCTURE.md`
- Add recovery tests when touching storage/WAL/indexing
- Document binary format or compatibility-impacting changes

---

## License

TBD

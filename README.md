# FireLite

FireLite is a Rust-native embedded document database inspired by Google Firestore’s developer ergonomics and SQLite’s deployability.

It is designed to run in-process (no external server), store typed binary documents, and provide durable local persistence with transactional write batching.

---

## Current Project Status

FireLite is currently in **active foundational development**.

### Implemented Today

- Embedded Rust library API (`FireLite`) with:
  - `open`
  - `put`
  - `get`
  - `delete`
  - `query`
  - `compact`
  - `write_batch` (atomic multi-mutation commit)
- Binary document format (`FireLiteDoc`) with typed values and decode support
- WAL + segment-backed storage engine
- Transaction markers in WAL (`BeginTx`/`CommitTx`) and replay of committed transactions
- Storage-level batch mutation application (`apply_batch`)
- Basic composite index definitions and manager
- Query planner/executor with filtering, ordering, limits, and parallel task sharding
- mmap-backed memory layer and page cache modules
- Basic examples and unit tests

### Not Yet Production-Ready (Important)

FireLite has important missing pieces before real-world production use, including but not limited to:

- Formal on-disk format compatibility/versioning guarantees
- Robust crash-consistency semantics for index + storage dual-write reconciliation
- Complete Firestore feature parity (subcollections, realtime listeners, auth rules, etc.)
- Comprehensive benchmarking, fuzzing, and fault-injection validation
- Security hardening and encryption-at-rest
- Operational observability (metrics/tracing/logging maturity)

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

    // Single write
    db.put("users", "1", &doc)?;

    // Atomic write batch (multiple mutations committed as one unit)
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

    let loaded = db.get("users", "1")?;
    println!("loaded = {:?}", loaded);

    Ok(())
}
```

---

## Architecture (Current)

```text
API (FireLite)
  -> Query (planner + executor)
    -> Index (composite manager)
      -> Storage (WAL + segment + compaction)
        -> Memory (mmap + page cache)
```

High-level design goals:

- Keep the API ergonomic and embedded
- Keep writes durable and recoverable
- Keep reads efficient with typed binary docs and indexing hooks
- Keep module boundaries explicit for future evolution

---

## Firestore Feature Comparison

The table below compares Firestore capabilities with FireLite’s current status.

Legend:

- ✅ Implemented
- 🟡 Partial / basic
- 🔜 Planned / target
- ❌ Not implemented

| Capability | Google Firestore | FireLite (Current) | FireLite Target |
|---|---|---|---|
| Embedded local runtime | ❌ (managed cloud service) | ✅ | ✅ |
| Collection/document model | ✅ | ✅ | ✅ |
| Document CRUD | ✅ | ✅ | ✅ |
| Atomic write batch | ✅ | ✅ (local transactional unit) | ✅ |
| Multi-document ACID transactions | ✅ | 🟡 (batch semantics, no full conflict/serializable model) | ✅ |
| Query filters/order/limit | ✅ | 🟡 (basic support) | ✅ |
| Composite indexes | ✅ | 🟡 (definition + manager + planner hooks) | ✅ |
| Index auto-build lifecycle | ✅ | ❌ | 🔜 |
| Real-time listeners / watch streams | ✅ | ❌ | 🔜 |
| Offline sync w/ cloud | ✅ (SDK dependent) | ❌ | 🔜 (optional replication layer) |
| Subcollections | ✅ | ❌ | 🔜 |
| Security rules engine | ✅ | ❌ | 🔜 |
| Managed auth integration | ✅ | ❌ | 🔜 |
| Serverless triggers | ✅ | ❌ | 🔜 |
| Built-in geo queries | ✅ (with patterns/extensions) | ❌ | 🔜 |
| TTL policies | ✅ | ❌ | 🔜 |
| PITR / backups | ✅ | ❌ | 🔜 |
| Encryption at rest | ✅ | ❌ (not yet integrated) | ✅ |
| Multi-region availability | ✅ | ❌ (single embedded process) | N/A / out of scope |

---

## Performance Suggestions (Next Updates)

1. **WAL group commit + fsync policy tuning**
   - Add configurable durability modes (`always`, `interval`, `manual`) and group commit to reduce sync overhead.
2. **Segment compaction improvements**
   - Move to multi-segment LSM-like compaction tiers and background compaction scheduling.
3. **Query execution optimization**
   - Add cost-based planning, predicate pushdown, and index-only scan pathways.
4. **Zero-copy reads end-to-end**
   - Extend borrowed document views through query pipeline to minimize allocations.
5. **Bench + profiling pipeline**
   - Add criterion benchmarks + flamegraph profiling + CI performance gates.

---

## Security Suggestions (Next Updates)

1. **Encryption at rest**
   - Encrypt WAL/segment pages (AES-GCM or ChaCha20-Poly1305) with key rotation support.
2. **Integrity and tamper checks**
   - Add authenticated record/page checksums and startup verification modes.
3. **Input and resource hardening**
   - Enforce document/key size limits, query complexity limits, and configurable memory ceilings.
4. **Crash safety + recovery auditability**
   - Add deterministic recovery journal validation and corruption quarantine.
5. **Supply chain and release hardening**
   - SBOM generation, dependency audit CI, signed releases, and reproducible builds.

---

## Additional Suggestions

### Reliability

- Add randomized fault-injection tests (power-loss simulation during WAL append/commit).
- Add model-based tests for storage/index consistency after recovery.
- Add long-running soak tests for memory/page cache behavior.

### Developer Experience

- Provide a stable schema/migration story for persisted data.
- Add higher-level fluent API helpers (Firestore-like builders).
- Add detailed examples for write batches, indexing, and query patterns.

### Observability

- Add metrics (`ops/sec`, WAL flush latency, compaction duration, query scan counts).
- Add tracing spans for write path/query path.
- Add structured logs with event IDs and transaction IDs.

---

## Contribution Notes

When proposing significant changes:

- Keep module boundaries aligned with `STRUCTURE.md`.
- Prefer adding tests for crash/recovery semantics when touching storage or WAL.
- Document format changes must include versioning and backward-compatibility notes.

---

## License

TBD.

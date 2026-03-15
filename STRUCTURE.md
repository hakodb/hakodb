Below is the **complete FireLite project structure** based on everything we designed so far.  
This layout keeps the system **modular, scalable, and database-engine friendly**.

The architecture follows a layered model:

```
API → Query → Engine → Storage → Memory → Index
```

This is similar in layering philosophy to embedded engines like SQLite and document systems like Google Firestore, but FireLite remains **fully embedded, binary-first, and Rust-native**.

---

# FireLite Final Project Structure

```
firelite/
│
├─ Cargo.toml
├─ README.md
├─ ARCHITECTURE.md
├─ STORAGE_FORMAT.md
├─ WRITE_PATH.md
│
├─ src/
│
│  ├─ lib.rs
│  ├─ config.rs
│  ├─ error.rs
│
│  ├─ engine/
│  │   ├─ engine.rs
│  │   └─ mod.rs
│
│  ├─ document/
│  │   ├─ firelite_doc.rs
│  │   ├─ value.rs
│  │   └─ mod.rs
│
│  ├─ storage/
│  │   ├─ engine.rs
│  │   ├─ wal.rs
│  │   ├─ segment.rs
│  │   ├─ compaction.rs
│  │   └─ mod.rs
│
│  ├─ memory/
│  │   ├─ memory_engine.rs
│  │   ├─ mmap_store.rs
│  │   ├─ page.rs
│  │   ├─ page_cache.rs
│  │   ├─ arena.rs
│  │   ├─ allocator.rs
│  │   ├─ doc_view.rs
│  │   └─ mod.rs
│
│  ├─ index/
│  │   ├─ mod.rs
│  │   │
│  │   ├─ composite/
│  │   │   ├─ definition.rs
│  │   │   ├─ key_encoder.rs
│  │   │   ├─ range_builder.rs
│  │   │   ├─ prefix_scan.rs
│  │   │   ├─ composite_index.rs
│  │   │   ├─ manager.rs
│  │   │   └─ mod.rs
│  │   │
│  │   └─ storage/
│  │       ├─ index_log.rs
│  │       ├─ index_snapshot.rs
│  │       ├─ index_recovery.rs
│  │       ├─ index_storage.rs
│  │       └─ mod.rs
│
│  ├─ query/
│  │   ├─ query.rs
│  │   ├─ filter.rs
│  │   ├─ order.rs
│  │   ├─ planner.rs
│  │   │
│  │   ├─ composite_matcher.rs
│  │   ├─ composite_planner.rs
│  │   │
│  │   └─ executor/
│  │       ├─ executor.rs
│  │       ├─ scheduler.rs
│  │       ├─ worker.rs
│  │       ├─ task.rs
│  │       ├─ result_stream.rs
│  │       └─ mod.rs
│
│  └─ util/
│      ├─ log.rs
│      ├─ bytes.rs
│      ├─ clock.rs
│      └─ mod.rs
│
└─ examples/
   ├─ basic_usage.rs
   └─ benchmark.rs
```

---

# Root Files

### `Cargo.toml`

Defines dependencies.

Typical dependencies:

```toml
memmap2
serde
parking_lot
crossbeam
bytes
```

---

### `README.md`

Project overview.

Contains:

- FireLite goals
- feature list
- quick start
- architecture diagram

---

### `ARCHITECTURE.md`

Deep system design:

```
storage engine
query engine
memory engine
index engine
```

---

### `STORAGE_FORMAT.md`

Binary layout of:

```
FireLiteDoc
WAL records
index records
pages
```

---

### `WRITE_PATH.md`

Describes write pipeline:

```
Client
 → WAL
 → Memory Engine
 → Index update
 → Snapshot / compaction
```

---

# Core Source Files

---

# src/lib.rs

Main crate entry.

Exports the public API.

Example:

```rust
pub mod engine;
pub mod query;
pub mod document;
pub mod storage;
pub mod index;
pub mod memory;
```

---

# src/config.rs

Configuration struct.

Controls:

```
cache size
mmap size
worker threads
index options
```

---

# src/error.rs

Custom FireLite error types.

Example:

```rust
FireLiteError
StorageError
QueryError
IndexError
```

---

# Engine Layer

Handles high-level database operations.

---

### engine/engine.rs

Top-level FireLite engine.

Responsibilities:

```
database open
collection management
query dispatch
write pipeline
```

This is the main user-facing interface.

Example API:

```rust
engine.insert(collection, doc)
engine.query(query)
engine.delete(id)
```

---

# Document Layer

Binary document format.

---

### document/firelite_doc.rs

Defines:

```
FireLiteDoc
```

Binary encoded document similar to BSON but lighter.

Contains:

```
field table
value offsets
field types
```

---

### document/value.rs

Defines supported value types:

```
Int
Float
String
Bool
Null
Binary
Timestamp
```

Used by query engine and index encoder.

---

# Storage Layer

Handles durable data storage.

---

### storage/engine.rs

Low-level storage engine.

Responsible for:

```
document persistence
segment access
read/write pipeline
```

---

### storage/wal.rs

Write-ahead log.

Provides:

```
crash recovery
atomic writes
append-only durability
```

---

### storage/segment.rs

Segment file manager.

Segments contain:

```
document blocks
metadata
compression
```

---

### storage/compaction.rs

Compacts storage segments.

Removes:

```
deleted docs
old versions
fragmentation
```

---

# Memory Engine

Provides high-performance access to data.

---

### memory/memory_engine.rs

Central memory subsystem.

Coordinates:

```
mmap store
page cache
allocator
```

---

### memory/mmap_store.rs

Memory-mapped file storage.

Allows:

```
zero-copy document reads
OS-level page caching
```

---

### memory/page.rs

Database page abstraction.

Page size:

```
4KB
```

Used for caching and IO boundaries.

---

### memory/page_cache.rs

LRU page cache.

Speeds up:

```
repeated document access
index scanning
```

---

### memory/arena.rs

Temporary allocation pool.

Used for:

```
query execution buffers
temporary decoding
```

---

### memory/allocator.rs

Atomic allocator for document writes.

Ensures:

```
thread-safe offsets
lock-free allocation
```

---

### memory/doc_view.rs

Zero-copy document view.

Allows reading fields **directly from mmap memory**.

---

# Index System

Handles document indexing.

---

# Composite Index Engine

---

### index/composite/definition.rs

Defines index schema.

Example:

```rust
(age ASC, created_at DESC)
```

---

### index/composite/key_encoder.rs

Encodes fields into **sortable binary keys**.

Critical for range queries.

---

### index/composite/range_builder.rs

Converts query filters into index scan ranges.

---

### index/composite/prefix_scan.rs

Supports prefix index scans.

Example:

```rust
where(age = 30)
```

---

### index/composite/composite_index.rs

Actual index structure.

Uses:

```rust
BTreeMap<Vec<u8>, DocID>
```

---

### index/composite/manager.rs

Manages all indexes per collection.

Handles:

```
insert
delete
update
lookup
```

---

# Index Persistence

---

### index/storage/index_log.rs

Index WAL.

Stores:

```
insert
delete
```

operations.

---

### index/storage/index_snapshot.rs

Full index snapshot file.

Used to accelerate startup.

---

### index/storage/index_recovery.rs

Rebuilds indexes by replaying WAL.

---

### index/storage/index_storage.rs

Coordinates:

```
snapshot
WAL
recovery
```

---

# Query System

Handles query parsing and execution.

---

### query/query.rs

Defines FireLite query structure.

Example:

```rust
Query {
 filters
 order_by
 limit
}
```

---

### query/filter.rs

Defines filter operators.

```
==
>
<
>=
<=
in
```

---

### query/order.rs

Defines ordering rules.

```
ASC
DESC
```

---

### query/planner.rs

Chooses execution strategy:

```
index scan
collection scan
```

---

### query/composite_matcher.rs

Determines whether a composite index can satisfy a query.

---

### query/composite_planner.rs

Selects the best composite index.

---

# Query Executor

Parallel execution engine.

---

### executor/executor.rs

Entry point for executing queries.

---

### executor/scheduler.rs

Dispatches query tasks.

---

### executor/worker.rs

Worker threads executing queries.

---

### executor/task.rs

Defines query execution tasks.

---

### executor/result_stream.rs

Streams results from worker threads.

---

# Utility Layer

---

### util/log.rs

High-performance logging system.

---

### util/bytes.rs

Binary encoding utilities.

---

### util/clock.rs

Timestamp utilities.

---

# Examples

---

### examples/basic_usage.rs

Minimal FireLite database usage.

---

### examples/benchmark.rs

Performance benchmarking.

Measures:

```
insert speed
query speed
index scan
```

---

# Approximate Codebase Size

| Component | LOC |
| :--- | :--- |
| Core Engine | ~800 |
| Memory Engine | ~1500 |
| Index Engine | ~900 |
| Query Engine | ~900 |
| Storage | ~700 |

Total:

```
~4800 lines
```

Which is a **realistic size for an early embedded database engine**.

---

# Next Critical System (Recommended)

The next subsystem that would significantly improve FireLite is:

**MVCC Transaction Engine**

This enables:

```
snapshot reads
concurrent writes
atomic commits
```

It is the final major component before FireLite becomes a **production-grade embedded Firestore-like database**.

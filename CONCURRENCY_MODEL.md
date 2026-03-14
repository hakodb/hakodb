# FireLite Concurrency Model

This document describes how FireLite handles concurrent access from multiple threads.

FireLite is designed to be safely used in multi-threaded applications while maintaining predictable performance and data consistency.

The concurrency model follows these principles:

* safe multi-threaded access
* multiple concurrent readers
* serialized writes
* deterministic storage ordering
* crash-safe operations

---

# Concurrency Overview

FireLite uses a **Multiple Readers / Single Writer** concurrency model.

This model allows:

* multiple threads to read data simultaneously
* only one thread to perform writes at a time

This approach provides a good balance between:

* simplicity
* performance
* safety

The overall structure:

```text id="yq47kg"
             ┌───────────────┐
             │ Application   │
             └───────┬───────┘
                     │
        ┌────────────▼────────────┐
        │ FireLite Database Core  │
        └───────┬─────────┬───────┘
                │         │
           Readers     Writer
        (concurrent)   (single)
```

---

# Thread Safety

FireLite components are designed to be thread-safe using Rust synchronization primitives.

Primary tools used:

* `Arc`
* `RwLock`
* atomic operations

Example structure:

```rust id="p4ad7c"
Arc<RwLock<DatabaseCore>>
```

This allows the database instance to be safely shared across threads.

---

# Read Operations

Read operations can occur concurrently.

Example operations:

* document retrieval
* index lookup
* query execution

Multiple threads may perform reads without blocking each other.

Flow:

```text id="4m6a7d"
Thread A → read
Thread B → read
Thread C → read
```

All readers access shared data structures using read locks.

---

# Write Operations

Write operations must be serialized.

Examples:

* document insert
* document update
* document deletion
* index update

Writes acquire an exclusive lock to ensure storage consistency.

Flow:

```text id="hpr75m"
Thread A → write lock
append record
update indexes
release lock
```

This ensures write ordering in the append log.

---

# Write Queue

To improve write throughput, writes may be queued internally.

Write flow:

```text id="n6j49m"
client write request
     ↓
write queue
     ↓
storage append
     ↓
index update
```

Batching multiple writes improves IO efficiency.

---

# Storage Ordering

FireLite relies on deterministic ordering of writes.

Because storage uses an append-only log:

* writes must occur sequentially
* log position determines record order

Example:

```text id="bawet8"
offset 100 → users:123
offset 140 → users:124
offset 180 → users:125
```

The latest offset represents the current document version.

---

# Atomicity

Each write operation is atomic at the record level.

Write process:

1. encode document
2. append record
3. flush record to disk
4. update in-memory index

If a crash occurs before completion:

* partial records are ignored during recovery.

---

# Crash Safety

FireLite ensures crash safety using append-only writes.

Crash scenarios:

### crash during write

If a record is incomplete:

* the record is discarded during recovery.

### crash after write

If the record was fully written:

* it becomes the latest version of the document.

Recovery process:

```text id="7si9yl"
scan storage file
validate record sizes
rebuild key index
```

---

# Index Synchronization

Indexes must remain consistent with the storage engine.

Write flow:

```text id="f4skpr"
append document
update index entries
commit write
```

If a crash occurs before index update:

* index will be rebuilt during startup.

This guarantees index consistency.

---

# Compaction Concurrency

Compaction runs in a background thread.

Compaction process:

```text id="0trh7p"
scan current storage
rewrite latest records
create new file
swap files
```

While compaction runs:

* reads continue normally
* writes append to active log

After compaction completes:

* the new file replaces the old one atomically.

---

# Locking Strategy

FireLite minimizes lock contention by separating locks across components.

Example:

```text id="0tq9du"
Storage Engine Lock
Index Engine Lock
Document Cache Lock
```

This prevents unrelated operations from blocking each other.

---

# Document Cache Concurrency

The document cache is shared across threads.

Cache operations:

* lookup
* insert
* eviction

Cache structures must be thread-safe.

Possible structures:

* concurrent LRU cache
* lock-protected cache

---

# Query Execution Concurrency

Query execution is mostly read-only.

Queries may run concurrently across threads.

Execution flow:

```text id="1rj4cc"
query start
index lookup
document retrieval
filtering
result streaming
```

Because queries use read locks, they do not block each other.

---

# Deadlock Prevention

FireLite enforces a strict lock acquisition order.

Lock order example:

```text id="fo8f6t"
storage lock
index lock
cache lock
```

This prevents circular dependencies between locks.

---

# Future Improvements

Possible concurrency enhancements:

### lock-free structures

Reduce locking overhead.

---

### multi-writer support

Parallel writes with transaction scheduling.

---

### optimistic concurrency

Allow temporary write conflicts with resolution.

---

### async storage pipeline

Use asynchronous IO for write batching.

---

# Concurrency Goals

FireLite aims to provide:

* safe concurrent access
* predictable write ordering
* efficient multi-threaded reads
* minimal lock contention
* crash-safe persistence

This concurrency model ensures FireLite can be safely used in modern multi-threaded applications.

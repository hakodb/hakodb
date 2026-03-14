# FireLite

**FireLite** is a lightweight embedded NoSQL document database written in Rust.

It aims to provide a **Firestore-like developer experience** while maintaining the **simplicity and portability of an embedded database like SQLite**.

FireLite is designed for:

* offline-first applications
* desktop software
* edge computing
* IoT devices
* local-first systems
* embedded applications

The goal is a **small, fast, dependency-light document database** that runs entirely inside your application.

---

# Vision

Modern applications often need a database that is:

* simple to embed
* easy to use
* resource efficient
* capable of storing flexible documents

FireLite combines ideas from:

* document databases
* embedded databases
* log-structured storage engines

The result is a **small document store that requires zero external services**.

---

# Key Features (Planned)

* Embedded database (single file)
* Document-based data model
* Firestore-style collections and documents
* JSON developer API
* Automatic JSON → binary conversion
* High-performance binary storage
* Simple query API
* Secondary indexes
* Append-only storage engine
* Crash-safe writes
* Automatic compaction
* Minimal memory usage

---

# Developer Experience

FireLite is designed to feel **very similar to Firestore** while running locally inside your application.

Example usage:

```rust
let db = FireLite::open("data.firelite")?;

db.collection("users")
  .doc("123")
  .set(json!({
      "name": "Alice",
      "age": 25
  }))?;

let user = db.collection("users")
             .doc("123")
             .get()?;
```

Query example:

```rust
let users = db.collection("users")
    .where_gt("age", 20)
    .limit(10)
    .get()?;
```

Developers interact with **JSON documents**, while FireLite handles all internal optimizations automatically.

---

# Data Model

FireLite follows a **collection → document** model.

Example structure:

```
users/
   123
   456

orders/
   999
```

Documents contain arbitrary JSON data:

```json
{
  "name": "Alice",
  "age": 25,
  "active": true
}
```

Internally, documents are stored as key-value pairs:

```
users:123 → binary document
```

---

# Architecture

FireLite uses a **layered architecture**.

```
┌─────────────────────────┐
│       FireLite API      │
│ Firestore-like queries  │
│ JSON document interface │
└─────────────┬───────────┘
              │
┌─────────────▼───────────┐
│     Document Engine     │
│ JSON → binary encoding  │
│ query processing        │
│ secondary indexes       │
└─────────────┬───────────┘
              │
┌─────────────▼───────────┐
│     Storage Engine      │
│ append-only log         │
│ in-memory key index     │
│ compaction system       │
└─────────────────────────┘
```

---

# JSON API with Binary Storage

FireLite provides a **JSON-based API** for simplicity.

Internally, JSON documents are automatically converted into an optimized **binary document format**.

```
User JSON
   ↓
serde_json
   ↓
binary document format
   ↓
append-only storage engine
```

Benefits:

* fast document access
* smaller disk usage
* minimal parsing overhead
* predictable memory layout

This design allows FireLite to maintain a **simple developer API while achieving high performance internally**.

---

# Binary Document Format

Internally, documents are stored as **typed binary records**.

Example layout:

```
document_length
field_count

[field]
key_length
key
type
value
```

Supported value types:

* string
* integer
* float
* boolean
* null
* object
* array

Binary documents allow:

* faster reads
* smaller storage size
* efficient indexing

---

# Storage Engine Design

FireLite uses a **log-structured append-only storage engine**.

Records are appended sequentially:

```
[record_size][key][binary_document]
[record_size][key][binary_document]
[record_size][key][binary_document]
```

An in-memory index maps document keys to file offsets:

```
HashMap<Key, Offset>
```

Advantages:

* very fast writes
* crash-safe operations
* simple storage design

Background compaction periodically removes obsolete records.

---

# Indexing

Secondary indexes allow efficient queries.

Example index:

```
users.age
```

Index entries:

```
25 → doc_id
30 → doc_id
```

Internally indexes use binary typed values for fast comparisons.

---

# Caching

FireLite includes multiple caching layers.

### Document Cache

Frequently accessed documents are stored in memory.

```
LRU Cache
doc_id → binary document
```

### Storage Page Cache

Disk pages may be cached to reduce IO.

---

# Concurrency

FireLite is designed to be **thread-safe**.

Concurrency model:

* multiple concurrent readers
* serialized writes

Implementation approach:

* `Arc`
* `RwLock`
* lock-efficient data structures

This allows safe use across multiple threads.

---

# Compaction

Because FireLite uses append-only storage, old records accumulate.

Background compaction:

```
scan log
keep latest document
rewrite storage file
```

This process reclaims disk space and maintains performance.

---

# Technology Stack

Rust ecosystem libraries used by FireLite:

Core:

* `serde`
* `serde_json`

Performance:

* `bytes`
* `memmap2`

Concurrency:

* `parking_lot`

Caching:

* `lru`

Utilities:

* `hashbrown`

---

# Project Structure

```
firelite/
│
├─ src/
│
├─ api/
│   db.rs
│   collection.rs
│   query.rs
│
├─ document/
│   document.rs
│   encoding.rs
│
├─ index/
│   index.rs
│
├─ storage/
│   engine.rs
│   log.rs
│   compaction.rs
│
└─ lib.rs
```

---

# Design Goals

FireLite prioritizes:

* simplicity
* small binary size
* predictable performance
* minimal dependencies
* fast startup time

Non-goals (for now):

* distributed clustering
* complex SQL support
* heavy query planners

---

# Roadmap

## Phase 1 — Core Storage

* [ ] append-only log file
* [ ] record format
* [ ] in-memory key index
* [ ] crash recovery
* [ ] basic get / put operations

---

## Phase 2 — Document Layer

* [ ] JSON → binary document encoding
* [ ] document decoding
* [ ] collection abstraction
* [ ] document CRUD operations

---

## Phase 3 — Query Engine

* [ ] simple filtering
* [ ] secondary indexes
* [ ] query builder API
* [ ] sorting and limits

---

## Phase 4 — Performance

* [ ] memory-mapped storage
* [ ] batch writes
* [ ] background compaction
* [ ] document cache

---

## Phase 5 — Advanced Features

* [ ] transactions
* [ ] real-time change streams
* [ ] replication support
* [ ] synchronization layer

---

# Performance Goals

Target performance:

* single-file database
* <10MB memory usage
* high sequential write throughput
* microsecond read latency

---

# Status

FireLite is **currently in early development**.

The storage engine and document format are under active design.

APIs may change during early development.

---

# License

MIT License

---

# Contributing

Contributions are welcome.

Areas where help is appreciated:

* storage engine improvements
* indexing algorithms
* performance optimization
* benchmarking
* documentation

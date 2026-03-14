# FireLite Architecture

This document describes the internal architecture of **FireLite**, an embedded document database designed to provide a Firestore-like API with SQLite-like deployment simplicity.

FireLite focuses on:

* embedded usage
* predictable performance
* low resource usage
* simple storage design
* JSON developer experience with binary performance

---

# System Overview

FireLite uses a layered architecture to separate concerns between API design, document management, indexing, and storage.

```
Application
     │
     ▼
FireLite API Layer
     │
     ▼
Document Engine
     │
     ▼
Query Engine
     │
     ▼
Index Engine
     │
     ▼
Storage Engine
```

Each layer has a specific responsibility.

---

# Core Design Principles

FireLite follows several key principles.

### Embedded First

The database runs inside the application process.

There is no server, daemon, or external service.

---

### JSON API, Binary Internals

Developers interact with JSON documents.

Internally FireLite converts JSON into an optimized binary format.

This provides both usability and performance.

---

### Append-Only Storage

The storage engine uses a log-structured design.

All writes append new records instead of modifying existing ones.

Advantages:

* fast sequential writes
* crash safety
* simple recovery

---

### Minimal Dependencies

FireLite aims to remain lightweight with minimal dependencies.

---

# Layer Breakdown

## API Layer

The API layer provides a developer-friendly interface similar to document databases.

Example usage:

```
db.collection("users")
  .doc("123")
  .set({...})
```

Responsibilities:

* user-facing database API
* query builder
* transaction interface (future)

This layer operates entirely with JSON documents.

---

## Document Engine

The document engine manages JSON documents.

Responsibilities:

* JSON parsing
* binary document encoding
* binary document decoding
* document validation

Flow:

```
JSON input
   ↓
serde_json
   ↓
binary document
   ↓
storage
```

Binary documents are optimized for fast access.

---

## Query Engine

The query engine processes queries.

Example query:

```
where age > 20
order by name
limit 10
```

Responsibilities:

* query planning
* index selection
* result filtering
* sorting and limiting

Queries operate primarily on indexed values.

---

## Index Engine

Indexes enable efficient document retrieval.

Example index:

```
users.age
```

Index structure:

```
value → document_id
```

Indexes are stored using ordered data structures such as B-Trees.

Responsibilities:

* maintain secondary indexes
* update indexes on document write
* support range queries

---

## Storage Engine

The storage engine manages persistent data.

Responsibilities:

* append-only log storage
* key-to-offset index
* crash recovery
* background compaction

The storage engine is intentionally simple to maximize reliability.

---

# Key Components

## In-Memory Key Index

The storage engine maintains an in-memory map:

```
document_key → file_offset
```

Example:

```
users:123 → 48291
```

This enables fast direct reads.

---

## Append Log

All writes append new records to the storage file.

Example:

```
PUT users:123
PUT users:124
DELETE users:122
```

This approach avoids random writes.

---

## Compaction

Over time the append log accumulates outdated records.

Compaction periodically rewrites the storage file.

Process:

1. scan log
2. keep latest version of each key
3. write new compacted file
4. swap files atomically

---

# Caching Strategy

FireLite uses multiple caches.

## Document Cache

Frequently accessed documents are cached in memory.

Structure:

```
doc_id → binary document
```

LRU eviction ensures bounded memory usage.

---

## Page Cache

Storage pages may also be cached to reduce disk IO.

---

# Concurrency Model

FireLite uses a **multiple reader / single writer model**.

Reads can execute concurrently.

Writes are serialized to preserve log order.

Synchronization primitives include:

* atomic references
* reader-writer locks

This model balances safety and performance.

---

# Crash Recovery

Crash recovery occurs during database startup.

Recovery process:

1. read storage file
2. rebuild in-memory index
3. discard incomplete records

Because writes are append-only, recovery is straightforward.

---

# Compaction Strategy

Compaction is triggered when:

* storage file grows beyond threshold
* large number of obsolete records

Compaction runs in a background thread.

During compaction:

* reads continue
* writes append to new log

After compaction finishes, the new file replaces the old one.

---

# Future Extensions

Possible future features:

### Transactions

Atomic multi-document operations.

---

### Replication

Synchronization between nodes.

---

### Real-time Subscriptions

Streaming document updates to clients.

---

### Distributed Storage

Clustered FireLite instances.

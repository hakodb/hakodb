# FireLite Write Path

This document describes the **write pipeline** used by FireLite when inserting, updating, or deleting documents.

The write path defines how data flows through the system from the API layer down to persistent storage.

Goals of the write path:

* deterministic storage ordering
* crash-safe writes
* consistent index updates
* minimal write latency
* predictable performance

---

# Overview

The FireLite write path follows a deterministic sequence of operations.

```text
Client Request
      │
      ▼
API Layer
      │
      ▼
Document Validation
      │
      ▼
JSON → Binary Encoding
      │
      ▼
Append to Storage Log
      │
      ▼
Update Primary Index
      │
      ▼
Update Secondary Indexes
      │
      ▼
Update Document Cache
      │
      ▼
Return Result
```

Each step ensures consistency between storage, indexes, and cache.

---

# Step 1 — API Request

Write operations originate from the FireLite API.

Example:

```rust
db.collection("users")
  .doc("123")
  .set(json!({
      "name": "Alice",
      "age": 25
  }));
```

The API layer extracts:

* collection name
* document ID
* JSON document

Resulting key:

```
users:123
```

---

# Step 2 — Document Validation

FireLite validates the document before processing.

Validation may include:

* JSON structure validation
* field type validation
* document size limits
* reserved key checks

Invalid documents are rejected before reaching the storage engine.

---

# Step 3 — JSON to Binary Encoding

The JSON document is converted into FireLite's internal binary format.

Example:

JSON:

```json
{
  "name": "Alice",
  "age": 25
}
```

Binary representation:

```
document_length
field_count
field_table
data_section
```

Encoding provides:

* smaller storage size
* faster document parsing
* predictable field layout

---

# Step 4 — Append to Storage Log

The encoded document is appended to the storage file.

Record format:

```
record_length
key_length
key
document_length
binary_document
```

Example record:

```
[record_length]
[9]
users:123
[document_length]
[binary_doc]
```

Writes are **always appended**, never modified in place.

Advantages:

* sequential disk writes
* simple crash recovery
* no page fragmentation

---

# Step 5 — Update Primary Index

After the record is written, the in-memory key index is updated.

Primary index mapping:

```
document_key → storage_offset
```

Example:

```
users:123 → offset 48291
```

This allows direct lookup of the latest document version.

---

# Step 6 — Update Secondary Indexes

If indexes exist on document fields, they must be updated.

Example index:

```
users.age
```

Index update:

```
age = 25 → doc_123
```

Process:

1. decode indexed fields
2. insert value into index
3. link to document identifier

If a document update modifies indexed fields:

* previous index entries must be removed
* new entries must be inserted

---

# Step 7 — Update Document Cache

The document cache stores recently accessed documents.

Cache insertion:

```
doc_id → binary document
```

Advantages:

* avoids repeated disk reads
* speeds up repeated queries

Cache uses LRU eviction.

---

# Step 8 — Return Result

After storage and indexes are updated, the operation returns success.

Example response:

```rust
Ok(())
```

At this point the document is:

* safely stored
* indexed
* available for queries

---

# Delete Operation

Deletion uses a tombstone record.

Example:

```rust
db.collection("users")
  .doc("123")
  .delete()
```

Stored record:

```
record_length
key_length
key
document_length = 0
```

Effects:

* primary index entry removed
* secondary index entries removed
* tombstone stored in log

Tombstones are cleaned during compaction.

---

# Write Ordering

Because FireLite uses an append-only log, **write order determines document version**.

Example:

```
offset 100 → users:123 age=20
offset 200 → users:123 age=25
```

The later record represents the current document state.

---

# Write Batching

FireLite may batch multiple writes before flushing to disk.

Batching reduces IO overhead.

Example batch:

```
PUT users:123
PUT users:124
PUT users:125
```

Batch writes improve throughput.

---

# Crash Recovery

If a crash occurs during write operations:

Incomplete records may exist at the end of the log.

During startup FireLite:

1. scans the log
2. validates record sizes
3. discards incomplete records
4. rebuilds indexes

This guarantees storage consistency.

---

# Write Path Guarantees

FireLite guarantees:

* deterministic write ordering
* crash-safe append operations
* consistent index updates
* atomic document writes

---

# Future Improvements

Possible improvements to the write pipeline:

### Write-Ahead Log (WAL)

Separate WAL file for stronger guarantees.

---

### Group Commit

Batch multiple writes before disk flush.

---

### Async Write Pipeline

Background IO for high throughput.

---

### Transaction Support

Atomic multi-document writes.

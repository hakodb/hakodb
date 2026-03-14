# FireLite Index Engine

This document describes the indexing subsystem used by FireLite.

Indexes allow FireLite to efficiently locate documents without scanning the entire collection.

The index engine is designed to be:

* simple
* fast for range queries
* efficient for updates
* lightweight in memory
* compatible with append-only storage

---

# Purpose of Indexes

Indexes improve query performance by mapping **field values to document identifiers**.

Without indexes:

* queries must scan every document in a collection.

With indexes:

* FireLite can jump directly to relevant documents.

Example:

```text
Query:
where age > 20
```

Index:

```text
users.age

18 → doc_1
21 → doc_4
25 → doc_2
30 → doc_3
```

FireLite can immediately find documents where age > 20.

---

# Index Architecture

Indexes are maintained separately from the main document storage.

Architecture:

```text
Query Engine
     │
     ▼
Index Engine
     │
     ▼
Index Structures
```

Each index maps:

```text
field_value → document_id
```

---

# Index Types

FireLite supports several index types.

## Primary Index

The primary index maps document keys to storage offsets.

Example:

```text
users:123 → offset
users:124 → offset
```

This index enables fast document retrieval.

---

## Secondary Index

Secondary indexes map field values to document IDs.

Example:

```text
users.age
```

Entries:

```text
25 → doc_1
30 → doc_2
```

Used for queries such as:

```text
where age == 25
```

---

## Range Index

Range indexes allow efficient comparison queries.

Example:

```text
where age > 20
```

Range indexes use ordered data structures.

---

# Index Data Structures

FireLite uses ordered structures for indexes.

Recommended structure:

```text
BTreeMap<Value, Vec<DocumentID>>
```

This provides:

* fast lookup
* efficient range scans
* predictable memory usage

Example:

```text
25 → [doc_1, doc_5]
30 → [doc_2]
```

---

# Index Key Format

Each index key contains:

```text
collection
field
value
```

Example:

```text
users:age:25
```

This structure allows efficient prefix scans.

---

# Index Creation

Indexes may be created manually or automatically.

Example API:

```rust
db.collection("users")
  .create_index("age")
```

Index creation process:

1. scan all documents
2. extract field value
3. insert into index structure

---

# Index Updates

Indexes are updated during document writes.

When inserting a document:

1. write document to storage
2. extract indexed fields
3. insert index entries

Example:

Document:

```json
{
  "name": "Alice",
  "age": 25
}
```

Index update:

```text
users.age → 25 → doc_123
```

---

# Index Removal

When documents are deleted:

1. tombstone record written
2. index entries removed

Example:

```text
remove doc_123 from users.age
```

---

# Index Persistence

Indexes may exist in memory or on disk.

Initial FireLite implementation:

```text
in-memory index
```

On startup:

1. read storage log
2. rebuild indexes

Advantages:

* simpler design
* faster development
* fewer disk structures

Future versions may support persistent indexes.

---

# Index Lookup

Example query:

```text
where age >= 25
```

Execution:

1. locate first key ≥ 25
2. iterate forward
3. retrieve document IDs
4. fetch documents

---

# Compound Indexes

Future versions may support compound indexes.

Example:

```text
users(age, name)
```

Entry format:

```text
(age, name) → document_id
```

Compound indexes support queries such as:

```text
where age > 20 and name == "Alice"
```

---

# Index Storage Strategy

Indexes are stored separately from the document log.

Possible storage formats:

```text
index_file
```

or

```text
memory-only
```

Initial FireLite versions may rebuild indexes during startup.

---

# Index Consistency

Index updates occur during document writes.

Write flow:

```text
write document
update indexes
commit
```

If a crash occurs:

* index can be rebuilt from storage log.

---

# Index Memory Management

Indexes must avoid uncontrolled memory growth.

Strategies:

* compact document identifiers
* reuse index nodes
* optional index persistence

---

# Index Recovery

During startup:

1. scan storage file
2. rebuild key index
3. rebuild secondary indexes

Recovery ensures index consistency with storage.

---

# Index Performance Goals

Target performance:

* O(log n) lookup
* fast range scans
* minimal memory overhead

---

# Future Improvements

Possible future improvements:

### persistent indexes

Store indexes on disk to avoid rebuild.

---

### compressed indexes

Reduce memory usage.

---

### bloom filters

Accelerate negative lookups.

---

### index intersection

Use multiple indexes for complex queries.

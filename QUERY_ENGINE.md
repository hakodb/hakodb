# FireLite Query Engine

This document describes how FireLite processes queries.

The query engine is responsible for retrieving documents based on user-defined filters and sorting conditions.

FireLite queries are inspired by document databases and aim to remain simple and predictable.

---

# Query Flow

Typical query execution flow:

```text
User Query
   ↓
Query Parser
   ↓
Query Planner
   ↓
Index Lookup
   ↓
Document Fetch
   ↓
Filtering
   ↓
Result Set
```

---

# Query API

Example query:

```rust
db.collection("users")
  .where_gt("age", 20)
  .limit(10)
  .get()
```

Operations supported:

* equality filters
* comparison filters
* range queries
* ordering
* limits

---

# Query Components

## Filter

Filters restrict which documents are returned.

Example:

```text
age > 20
```

Supported operators:

| Operator | Description      |
| -------- | ---------------- |
| ==       | equality         |
| >        | greater than     |
| >=       | greater or equal |
| <        | less than        |
| <=       | less or equal    |

---

## Sorting

Queries may include sorting.

Example:

```text
order_by(name)
```

Sorting uses index ordering when possible.

---

## Limit

Limits restrict the number of results returned.

Example:

```text
limit(10)
```

Limits reduce memory usage and improve performance.

---

# Query Planning

The query planner determines the most efficient way to execute a query.

Steps:

1. identify available indexes
2. choose index scan or full scan
3. execute scan
4. apply filters
5. return results

---

# Index-Based Queries

Indexes dramatically improve query performance.

Example index:

```text
users.age
```

Index entries:

```text
25 → doc_id
30 → doc_id
35 → doc_id
```

Range query example:

```text
age > 20
```

Execution:

1. locate first index value > 20
2. iterate forward
3. fetch documents

---

# Full Collection Scan

If no index exists, FireLite performs a collection scan.

Steps:

1. iterate all documents
2. decode document fields
3. apply filters

Collection scans are slower but still supported.

---

# Result Construction

Documents returned from queries are converted to JSON before returning to the user.

Process:

```text
binary document
   ↓
decode
   ↓
JSON
```

This maintains the developer-friendly API.

---

# Index Maintenance

Indexes are updated during document writes.

When inserting a document:

1. encode document
2. write to storage
3. update indexes

When deleting a document:

1. write tombstone record
2. remove index entries

---

# Query Optimization

Possible optimizations:

* index intersection
* index prefix matching
* query caching

These may be implemented in future versions.

---

# Memory Management

Query execution uses streaming results where possible.

Benefits:

* avoids loading entire result set into memory
* supports large datasets

---

# Future Improvements

Possible future features:

### compound indexes

Example:

```text
users(age, name)
```

---

### aggregation queries

Example:

```text
count()
sum()
avg()
```

---

### query planner improvements

More advanced cost-based optimization.

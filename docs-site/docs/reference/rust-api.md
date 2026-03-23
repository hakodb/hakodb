---
title: Rust API (Current Library Surface)
---

This reference summarizes the currently implemented Rust-native API surface in `firelite`.

## Core engine (`FireLite`)

```rust
pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self>;
pub fn put(&self, collection: &str, doc_id: &str, doc: &FireLiteDoc) -> Result<()>;
pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>>;
pub fn delete(&self, collection: &str, doc_id: &str) -> Result<()>;
pub fn query(&self, query: Query) -> Result<Vec<(String, FireLiteDoc)>>;
pub fn query_projected_zero_copy(
  &self,
  query: Query,
  fields: &[String]
) -> Result<Vec<(String, Vec<(String, Value)>)>>;
pub fn execute_aggregation(&self, query: Query) -> Result<HashMap<String, f64>>;
pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()>;
pub fn compact(&self) -> Result<()>;
pub fn flush(&self) -> Result<()>;
pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()>;
pub fn list_collections(&self) -> Result<Vec<String>>;
pub fn watch_collection(&self, col: &str) -> Receiver<ChangeEvent>;
```

## Transaction model

### Staged transaction

```rust
let mut tx = db.begin_transaction();
tx.put("users", "u1", doc);
tx.delete("users", "u2");
tx.commit(&db)?;
```

### Serializable transaction

```rust
let mut tx = db.begin_serializable_transaction();
let doc = tx.get(&db, "users", "u1")?;
tx.put("users", "u1", updated_doc);
tx.commit(&db)?;
```

## Query builder surface

```rust
let q = Query::new("users")
  .where_filter("age", Operator::Gte, Value::Int(18))
  .order_by("name", true)
  .limit(25)
  .offset(0)
  .select("name")
  .aggregate(AggregateOp::Count);
```

### Supported operators

- Scalar: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`
- Text: `Match`, `Contains`, `StartsWith`
- Membership: `In`

## Indexing and search

```rust
db.create_index("users", "age")?;            // simple field index
db.create_fts_index("users", "bio")?;        // inverted index
db.create_composite_index("users", vec![
  ("country".into(), SortDirection::Asc),
  ("age".into(), SortDirection::Desc),
]);
db.save_index_snapshots()?;
```

## Document model (`FireLiteDoc`, `Value`)

### Scalar values

- `Null`
- `Bool`
- `Int`
- `Float`
- `String`
- `Binary(Vec<u8>)`
- `Timestamp(i64)`
- `ServerTimestamp`

### Structured values

- `Reference { collection, doc_id }`
- `Map(Vec<(String, Value)>)`
- `Array(Vec<Value>)`

## Configuration (`FireLiteConfig`)

```rust
pub struct FireLiteConfig {
  pub mmap_size: usize,
  pub page_size: usize,
  pub page_cache_capacity: usize,
  pub query_workers: usize,
  pub auto_compaction_threshold_bytes: usize,
  pub durability_mode: DurabilityMode,
  pub group_commit_max_ops: usize,
  pub encryption_key: Option<String>,
  pub enable_audit_log: bool,
  pub audit_log_path: Option<String>,
  pub max_inlined_memory_bytes: usize,
  pub use_compression: bool,
  pub compression_level: i32,
}
```

### Durability modes

- `Always`
- `Interval`
- `Manual`
- `OnCommit`

## Realtime

`watch_collection` provides an in-process receiver emitting document change events (`Put`/`Delete`) for a collection.

## Operational endpoints

- `backup(...)`
- `compact()`
- `flush()`
- `list_collections()`
- audit snapshot utilities through FFI layer (`fl_engine_get_audit_log`, `fl_engine_get_stats`)


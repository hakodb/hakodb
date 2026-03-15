---
title: Rust API Signatures (Core)
---

## `FireLite`

```rust
pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self>;
pub fn begin_transaction(&self) -> Transaction;
pub fn begin_serializable_transaction(&self) -> SerializableTransaction;
pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()>;
pub fn put(&self, collection: &str, doc_id: &str, doc: &FireLiteDoc) -> Result<()>;
pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>>;
pub fn delete(&self, collection: &str, doc_id: &str) -> Result<()>;
pub fn query(&self, query: Query) -> Result<Vec<(String, FireLiteDoc)>>;
pub fn query_projected_zero_copy(&self, query: Query, fields: &[String])
  -> Result<Vec<(String, Vec<(String, Value)>)>>;
pub fn compact(&self) -> Result<()>;
pub fn flush(&self) -> Result<()>;
```

## Transaction helpers

```rust
pub fn put(&mut self, collection: &str, doc_id: &str, doc: FireLiteDoc);
pub fn delete(&mut self, collection: &str, doc_id: &str);
pub fn commit(self, db: &FireLite) -> Result<()>;
```

Serializable transaction additionally exposes:

```rust
pub fn get(&mut self, db: &FireLite, collection: &str, doc_id: &str)
  -> Result<Option<FireLiteDoc>>;
```

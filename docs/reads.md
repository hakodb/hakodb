# Reading data: choosing the right path

FireLite offers four read granularities for the same stored bytes. They
differ in per-row cost by orders of magnitude, so picking by habit leaves
5–50x on the table. Rule of thumb: **return the least materialization
that answers the question.**

| Path | Per-row work | Use when |
|---|---|---|
| `get` / `query` (decoded) | Full owned `FireLiteDoc` (~1–7µs, size-dependent) | You need whole documents as values |
| `query_raw` | Pinned bytes, no decode (~1µs) | Export, hash, relay, count bytes |
| `walk` | Borrowed bytes, zero allocs (~0.4µs) | Full scans: count, filter, copy out |
| `get_view` / `walk_view` | Borrow + pull only touched fields (~0.2µs + ~0.2µs/pull) | Sparse reads: 2 fields out of 20 |

Numbers: in-process release, 20k docs (see `docs/benchmarking.md` for
methodology). All four agree field-for-field — see `tests/codec_integrity.rs`.

## Point reads

```rust
// Whole document, owned. Hot repeats skip decode via an internal cache.
let doc: Option<FireLiteDoc> = db.get("orders", "id-123")?;

// Borrowed view: no decode, no interning, no cache churn. Pull fields lazily.
if let Some(view) = db.get_view("orders", "id-123")? {
    if let Some(Value::Int(total)) = view.get("total") { /* ... */ }
    let full: FireLiteDoc = view.to_owned_doc().expect("valid");
}
```

Views never inflate blobs: a blob-backed field reads back as its link
placeholder. Resolve through a full decode when you need the bytes
(`to_owned_doc` + `resolve_document_blobs`, or plain `get`).

FFI mirrors both: `fl_engine_get` / `fl_doc_to_json` for owned docs,
`fl_view_get` + typed `fl_view_get_int/float/bool/str/bytes` for lazy
pulls, `fl_view_to_doc` as the escape hatch. SDKs: Go (`GetView`),
Pascal (`TFireLite.GetView`), JS (`viewDoc` — numerics/bool only, no
backend memory reads by design), Tauri (`view_get_field` op).

## Scans

```rust
// Decoded, paged. Order/pagination handled by the planner.
let mut q = Query::new("orders").order_by("id", true);
q.limit = Some(1000);
// ... anchor with start_after(last_id) per page ...

// Raw bytes, paged. Same paging, no decode.
let rows: Vec<(String, Arc<Vec<u8>>)> = db.query_raw(q)?;

// Zero-alloc walk. One call, borrowed rows, false stops early.
let n = db.walk(q, &mut |id: &str, bytes: &[u8]| { /* ... */ true })?;

// Lazy walk. Same shape, each row lent as a DocView.
let n = db.walk_view(q, &mut |id: &str, view: &DocView| { /* ... */ true })?;
```

Raw and walk require index-satisfied filters and ordering (they cannot
match or sort without decoding) and return errors otherwise — loud, not
silent. Bytes are **opaque storage encoding**: hash/count/export them,
decode with `FireLiteDoc::decode`, never persist or compare across
versions. FFI: `fl_query_execute_raw` + result-set fns,
`fl_cursor_walk` / `fl_cursor_walk_view` with C callbacks.

## Pagination

Keyset only — no `OFFSET` scans. Order by `id`, take a page, bind the
last row as the next anchor (`start_after`); descending is symmetric
(same fast path both directions, ~1.1x apart):

```rust
let mut q = Query::new("bench").order_by("id", false);
q.limit = Some(1000);
// per page: q = q.start_after(vec![Value::String(last_id)]);
```

`limit`/`offset` truncate downstream; deep pages cost the same as shallow
ones (O(log N) anchor seek). Contrast: offset-based paging degrades
linearly with depth — see the deep-pagination duel in
`docs/benchmarking.md`.

## Before you measure or paginate: readiness

`FireLite::open` returns while index recovery still runs in the
background. Queries issued first silently plan full scans (cursor bounds
ignored, pages repeat). Poll before scanning:

```rust
while !db.is_indexes_ready() { /* sleep briefly, bounded */ }
// Measuring? Settle everything instead:
db.await_quiescent(Duration::from_secs(30)); // indexes + index worker + blobs + maintenance
```

`await_quiescent` (and `quiescence_status()` for diagnostics) is the
difference between benchmarking the engine and benchmarking contention.
FFI: `fl_engine_await_quiescent` / `fl_engine_quiescence_status`. The CLI
waits for readiness on every open; `benchmark.cpp` settles before scans.

## Blob semantics (the fine print)

Values over `value_blob_threshold_bytes` (16KB default) live in the blob
file; documents carry `BlobLink` placeholders. Reads inflate transparently
except on raw/view paths (by design — resolve explicitly). One asymmetry
to know: blob storage is type-erased bytes, so inflation restores
`String` for valid UTF-8 and `Binary` otherwise. Text round-trips exactly;
binary that happens to be valid UTF-8 comes back as text. Changing that
needs per-blob type tags — a separate, deliberate decision.

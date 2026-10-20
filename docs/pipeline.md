# Pipelines: stages and limits

`Pipeline` is a small, declarative aggregation pass over decoded rows.
There are no joins and no SQL; a pipeline reads one collection at a time.
Stages run in the order they are added.

```rust
let rows = db.run_pipeline(
    Pipeline::new("orders")
        .match_query(Query::new("orders").where_eq("status", Value::String("paid".into())))
        .unwind("items")
        .group(Some("items.sku"), vec![("qty", Accum::Sum("items.qty".into()))])
        .sort("sku", true),
)?;
```

## Stage order

`Match` is the only stage that runs the query planner and executor. It
**replaces** the current row set with the full result of its `Query`, so
place it first unless you deliberately want a later match to discard
earlier stages. A pipeline whose first stage is not `Match` seeds from a
full scan of `pipeline.collection`.

All other stages are streaming passes over the materialised rows:

| Stage | Behavior |
|---|---|
| `Project(fields)` | Keeps only the listed paths. Dotted paths prune subtrees with the same leaf-preserving rule as query projection. Invalid/empty paths are refused. |
| `Sort { field, ascending }` | Sorts rows by one field. Supports `id`, `_time`, and dotted paths. |
| `Limit(n)` | Truncates to the first `n` rows at that point in the pipeline. |
| `Offset(n)` | Skips `n` rows at that point in the pipeline. |
| `Unwind { field }` | Emits one row per array element. See limits below. |
| `Group { key, accums }` | Groups rows and computes accumulators. |

Because stages are ordered, `Project` before `Group` can remove fields the
group needs, and `Limit` before `Sort` limits rows before sorting. Put
`Match` first and apply sort/offset/limit in the order you intend.

A `Match` query's own `limit`/`offset` apply as written and truncate
before later stages. Prefer the pipeline `Limit`/`Offset` stages over a
limited `Match` unless you specifically want early truncation.

## Group output naming

For a group key path, the output field is the **last path segment**:

- `items.sku` -> output field `sku`
- `profile.address.city` -> output field `city`

Later stages must use that output name, not the original dotted path.
With no key (`group(None, ...)`), there is one global group; its row id is
`$all` and no key field is added. A missing key value groups under
`Value::Null`. Groups are emitted in key order (the implementation uses a
`BTreeMap`).

## Accumulators

`Accum::{Count, Sum, Min, Max, Avg}`:

- `Count` counts rows in the group, including rows where the field is
  missing.
- `Sum` and `Avg` consider numeric values only; non-numeric and missing
  values are ignored. With no numeric values, `Sum` is `Int(0)` and
  `Avg` is `Null`.
- `Sum` returns an `Int` when every contributing value was an integer,
  otherwise a `Float`.
- `Min` / `Max` use `Value` ordering and return `Null` when no value
  exists.

## Unwind limits

`unwind` is deliberately top-level only:

- Nested paths (`meta.items`) are refused with `InvalidInput`; there is
  no nested unwind yet.
- A missing field or a non-array value skips the whole document.
- Array elements are copied into the unwound field as-is. A scalar
  element cannot be traversed further by a dotted path.

## Near and traverse placement

Vector `Near` and graph `Traverse` are query-level scans, not pipeline
stages. Put them in the `Match` query:

```rust
// k = Query::limit; default 100 if unset.
Pipeline::new("docs").match_query(
    Query::new("docs")
        .where_near("embedding", &query_vec)
        .limit(10),
)

// max_depth is 1..=32. limit caps results; default 100.
Pipeline::new("docs").match_query(
    Query::new("docs")
        .where_traverse("docs", "a", TraverseDir::Outgoing, None, 5)
        .limit(50),
)
```

Once the rows are materialised, later stages cannot start a new vector or
graph scan. Traverse hits are filtered to the queried collection; use the
`find_paths` engine API for cross-collection paths. See
`tests/issue71c_pipeline.rs` for the executable examples.

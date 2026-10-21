# Queries: execution, explain, and paging

`Query` is a declarative read over one collection: filters, optional
`elemMatch` and vector/traverse markers, ordering, cursor bounds,
projection, and aggregates. The planner picks a scan from the available
indexes; `docs/indexes.md` covers index-specific behavior.

## Transactions

`SerializableTransaction` is a **point-key** API, not a general query
snapshot:

- `tx.get` / `tx.get_many` register the document id and its version in
  the read-set.
- `tx.put` / `tx.delete` queue mutations. `tx.patch` first calls
  `tx.get`, then queues the patch, so the patched document joins the
  read-set.
- There is no `tx.query`, scan, or aggregate. Calling `db.query` while a
  transaction object exists does **not** add its rows to the read-set.
- Commit validates every recorded version and then applies the mutations
  under the transaction lock. A version change returns a retryable
  `Conflict`; unchanged input is not guaranteed to succeed.
- The first read of a document wins; a later re-read does not refresh the
  recorded version.
- The read-set is keyed by document id, not by collection. Use distinct
  ids in one transaction if you mix collections.

For a read-modify-write transaction, use `db.transaction(policy, |tx|
...)`: it runs the closure against a fresh transaction on each retry,
with bounded exponential backoff. If the decision depends on a query,
re-check the affected documents with `tx.get` before writing; there is no
snapshot guarantee for the query result.

## Ordering, limit, offset, and cursors

- `order_by(field, ascending)` can be called more than once. Keys are
  applied in call order; each key has its own direction. `id` and
  `_time` are virtual fields.
- Without `order_by`, result order is scan order and is not a stable API
  contract. Use `order_by("id", true)` for deterministic output.
- After filters and ordering, `offset(n)` skips `n` rows and then
  `limit(n)` truncates. Both are `Option<usize>` and are applied on the
  decoded/result rows.
- Ordering can be satisfied by an index or by a post-scan sort. `limit`
  is pushed into a scan only when that cannot change the result; the
  observable slice is the same either way.
- Cursor bounds (`start_at`, `start_after`, `end_at`, `end_before`) are
  keyset bounds over index order. Order by `id`, take a page, then bind
  the last id with `start_after(vec![Value::String(last_id)])`.
- Offset paging gets slower as the offset grows because skipped rows are
  still produced. Keyset paging seeks directly to the anchor.

## Explain

`explain(query)` returns the `QueryPlan` that `query()` would execute.
`explain_report(query)` returns a serializable, human-readable report.
Neither executes the query. Both use the same admission check and
plan-cache path as `query()`.

`ExplainReport` fields:

| Field | Meaning |
|---|---|
| `collection` | Collection the plan targets. |
| `scan` | Scan descriptor, e.g. `secondary_index(status)`, `composite_index_range(3)`, `full_collection`. |
| `index` | Index label for the scan, or `None` for full/key walks. |
| `collection_rows` | Upper bound on rows the scan can visit, not the result count. |
| `filter_count` | Predicates still applied after the scan. |
| `or_group_count` | OR groups still applied after the scan. |
| `elem_match_count` | `elemMatch` clauses in the plan. |
| `filters_satisfied_by_index` | True when the scan returns exactly the filtered rows; no decode-time re-check is needed. |
| `order_by_satisfied` | True when the scan already yields the requested order. |
| `limit` / `offset` | The requested slice, unchanged by the report. |
| `projection` | Projected field paths. |
| `covering` | True when the projection can be served from the index alone. |
| `available_indexes` | Candidate definitions on the collection, sorted. Use this for "why did my index not run?" |

When indexes are not ready, the report shows the plan the next query
will execute: usually the `FullCollection` fallback, with the id-order
`SortedKeys` and vector lanes as the documented exceptions.

## Covering (index-only) projections

A covering read builds each projected value from the plan and reads no
document. `covering` is true only when all of these hold:

- `filters_satisfied_by_index` and `order_by_satisfied` are both true.
- There are no cursor bounds, no `elemMatch` clauses, and `raw` is false.
- The projection is non-empty.
- The projection is one of these proven shapes:
  - a composite **point lookup** (`ScanType::CompositeIndex`) whose
    top-level fields cover the projection;
  - a secondary **equality** scan whose projection contains only the
    indexed field (and optionally `id`);
  - `id` alone on an index-backed scan (the row id is already the output
    key, so the field list is empty).

Ranges, unions, FTS, and vector scans are not covering for field
projections; `id` alone can cover when the scan is index-backed and its
filters/order are satisfied. A composite range can narrow candidates, but
its plan reports the filter/order flags false, so it stays on the decode
path. Traverse is excluded. Every projected path
must be top-level (no dots); nested rebuild from an index prefix is still
unsupported and falls back to decode. The same fallback applies when the
projection includes any field the plan does not know.

Covering applies to projected reads such as
`query_projected_zero_copy` (and the projected FFI path); plain `query()`
returns whole documents and therefore is not a covering read. The
optimizer records this in `QueryPlan::covering_projection` in
`src/query/plan.rs`.

## See also

- `docs/reads.md` for raw and borrowed read paths.
- `docs/indexes.md` for index readiness, dotted paths, and filtered
  indexes.
- `tests/issue71d_explain_schema.rs` for explain assertions.
- `tests/issue71f_projection.rs` for covering and nested projection.

# FFI query lanes — which execution function to use

`hakodb.h` exposes many `hk_query_*` builders and several execution entry points. They are **not** interchangeable: they differ in what they materialize and therefore in cost. This page is the decision table for native consumers and SDK authors.

## Query builder surface

55 `hk_query_*` functions: 3 lifecycle (`new`/`reset`/`free`), 13 equality/ordering filters, 8 set/array/OR, 6 text/vector/graph, 5 order/page/projection, 5 cursor bounds, 7 execution, 5 aggregation helpers, 3 query-scoped mutation. The builder count is normal for a fluent C API; the execution count is the part that needs guidance.

## Execution entry points

| Function | Returns | Materialization | Use when |
|---|---|---|---|
| `hk_query_execute_ex` | `char*` (JSON array) | canonical executor with optional cancel handle | ordinary API calls |
| `hk_query_execute` | `char*` (JSON array) | full `HakoDoc` decode → serialize | legacy compatibility only |
| `hk_query_execute_with_cancel` | `char*` (JSON array) | same as `execute`, with `HK_Cancel*` | cancellable variant of the same path |
| `hk_query_execute_json` | `char*` (JSON array) | JSON emitted from pinned bytes via `query_json_into`; no owned `HakoDoc` (falls back to `execute` for projections) | JSON egress / SDK wire output |
| `hk_query_execute_to_handles` | `HK_ResultSet*` | borrowed `HK_Doc` handles from zero-copy row refs; no JSON | engine throughput; caller reads fields via handle accessors |
| `hk_query_execute_raw` | `HK_RawResultSet*` | pinned storage bytes, no decode and no field interning; index-satisfied filters/order only | cursor/keyset paging and raw scans |
| `hk_query_execute_aggregation` | `char*` (JSON) | aggregate values only | `count`/`sum`/`avg`/`min`/`max` |
| `hk_query_explain` | `char*` (JSON) | plan report, no rows | tuning; never for benchmarking throughput |

`hk_query_reset` reuses a handle and avoids re-allocation. `hk_view_*` gives typed field access without building a document. Together with `execute_to_handles`/`execute_raw` these are the low-allocation paths.

## Deprecation status

`hk_query_execute` and `hk_query_execute_with_cancel` are legacy compatibility wrappers and are marked deprecated in source/header comments. They also emit one warning per process when called. Prefer `hk_query_execute_ex` for API calls. They are **not** removed: no breaking major release is planned yet, and the generated C header carries advisory comments rather than compiler attributes.

## Materialization cost (illustrative)

Measured on the same hardware with the same 20-row query; treat as an order-of-magnitude guide, not a contract:

| Lane | Median | What dominates |
|---|---:|---|
| `handles` / `raw` | ~0.2 ms | borrowed storage bytes, no JSON |
| `docs` (`execute_ex`) | ~1.4 ms | owned document build + JSON |
| SDK `get()` with JSON parse | ~2 ms | JSON copy + JS object tree |

A large gap between these lanes is expected and does not indicate a slow engine. Always label the lane in results.

## Rules for benchmarking

- `benchmark.cpp` measures `hk_query_execute_to_handles` (plus `hk_doc_to_json` in its JSON-render lane). It does **not** measure `hk_query_execute` / `hk_query_execute_json`.
- `hk_query_execute` and `hk_query_execute_json` return the **same output shape** but take different paths; they must not be mixed in an A/B comparison.
- An SDK call that does `query.get()` → JSON string → `JSON.parse` → JS objects measures SDK+egress, not the engine. Compare it against an SDK handles/view lane, or against native `execute_to_handles`, before drawing engine conclusions.

## Related surface

- `hk_pipeline_*` (11 functions) is for composing multi-stage transformations (`match`/`project`/`sort`/`group`/…), not a replacement for simple `Query` calls.
- `hk_socket_sync_*` (5) is Unix-only and deliberately not bound in the JS SDK.
- `hk_engine_find_paths_v2/v3`, `hk_engine_find_simple_paths/v2` are superseded by `find_paths_v4` / `find_simple_paths_v3`, which are the bound variants.

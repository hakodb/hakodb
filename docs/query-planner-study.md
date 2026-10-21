# Query planner and index execution study (issue #22)

Status: P0 landed (#104); P1 audit partial; P2 items 1-2 landed (secondary
equality field + `id`-only covering). Scope: how plans are chosen and executed
on the query side. This does not change query semantics or add SQL/joins.

Current tree: `hakodb` main `7c152f0` (0.16.6 plus #103 P0/P1 and #22 P2
items 1-2).

## 1. What already exists

The issue text predates some of the 0.16.x work. Useful pieces are already in
place:

- `Hako::explain` / `Hako::explain_report` (F7) expose the chosen scan,
  collection rows, filter/or/elemMatch counts, index-satisfied flags, order
  satisfaction, projection, `covering`, and available indexes.
- The executor has a top-N heap for unsatisfied `ORDER BY` + `LIMIT` and an
  early-termination path for unordered/order-satisfied pages. Deep offset with
  a small limit already uses the cheap skip path.
- `id` order uses the storage `SortedKeys` slice, not an index walk.
- Covering (`index-only`) reads now cover three proven shapes: a composite
  point lookup whose prefix covers every filter, the order, and the whole
  top-level projection; a secondary equality scan whose projection is the
  indexed field; and `id` alone on an index-backed scan. Everything else
  returns `None` from `QueryPlan::covering_projection` (nested paths, cursor
  bounds, raw, elemMatch, unknown fields).
- Filtered composite indexes exist (`CompositeIndexDefinition::filter`). The
  planner may use one only when the query pins the predicate exactly with
  `Eq` or a single-value `In`; `Or` and ranges fall back to a sweep.
- There are no per-field cardinality/NDV statistics. Planning uses
  `collection_rows`, a `rows / workers > 50` heuristic, posting sizes for
  secondary equality, and fixed scan priorities.
- `index_query_ready(collection)` is per collection (P0, #104). The global
  counter remains only for `QuiescenceStatus.pending_index_ops`; a pending
  write in one collection no longer forces other collections to
  `FullCollection`.
- The plan cache has a 1 s TTL and is keyed by query shape, including the
  collection. A stats-driven plan would need an epoch or invalidation rule.

The standard query lanes in `hakobench` already include `Qry`, `Cmp`,
`QPS (Off/Cur)`, `Agg`, and the EXTENDED `cover`/`proj` lane for the
`(tenant, active)` composite. `--gate` pins the Manual profile.

## 2. Evidence and constraints

Two measurements matter before adding machinery:

1. The 0.16.5 review from box `fkip-234` measured a cold collection doing a
   full scan on 193/200 immediate explores after one write to a different
   collection. This is the global gate, not a planner-cost problem. It is
   correct (read-your-writes) but punishes mixed write/read workloads.
2. An existing planner note records that a secondary equality index still
   wins at 90% selectivity on an unbounded query (454 ms vs 802 ms). A naive
   "low selectivity -> full sweep" rule is therefore not justified for the
   simple equality case. Stats must be aimed at shapes where the current
   choice is demonstrably wrong, not at a textbook cost model.

Correctness invariant for all work below: a plan may be slower, but it must
return the same rows as the decode path. Any uncertainty falls back to the
existing scan.

## 3. Ranked work packages

### P0 - collection-scoped index readiness (landed in #104)

Problem: `index_query_ready()` was global. A write to collection A made a
query on collection B use `FullCollection` until A's index op was applied.

Approach: keep the existing global counter for `quiescence_status`, but add a
per-collection pending count (or a per-collection epoch) and pass the queried
collection into `index_query_ready(collection)`. All plan entry points already
have `query.collection`; the index worker already receives the collection in
`IndexOp::Update`.

Acceptance:

- With a pending write on `hot`, `explain_report(cold)` stays on the cold
  collection's index; `explain_report(hot)` may stay full-scan until drained.
- `QuiescenceStatus.pending_index_ops` still reports the total.
- A deterministic probe (barrier or a large batch, not a sleep) covers both
  collections and is added to the suite.
- No regression in `Qry`/`Cmp`/`QPS` on the standard profiles; watch the cost
  of the extra map read on the plan path. If a lock is measurable, use a
  sharded counter or store the counter on the shard.

Risk: moderate. This touches the write enqueue, worker completion, all query
entry points, and watch planning. It is independent of stats and should ship
alone before query tuning.

Status: landed in #104 (merge `efa3ade`). The deterministic two-collection
test is `index_readiness_is_collection_scoped`; all enqueue paths, including
replicated/cloud ingest, now share `enqueue_index_update`.

### P1 - EXPLAIN and benchmark audit

No new planner behavior. Turn the slow shapes into repeatable evidence:

- cold query while another collection has pending index work;
- unordered `ORDER BY` + small `LIMIT` (top-N) vs indexed order;
- deep offset with small limit;
- composite range vs equality;
- projection fully covered, partially covered, and nested;
- filtered index with exact predicate, implied range, and `Or`;
- `count`/`sum` with and without an index.

Use `explain_report` plus the existing `hakobench` lanes. Add only the missing
shapes; do not build a second benchmark harness.

Acceptance: each shape has a before/after number and an `explain_report` line
showing the expected scan. This is the gate for deciding whether P3 is worth
doing at all.

Status: partial. The projection shape now has a dedicated criterion group
(`benches/read_path.rs`, `query_projection`) on the 1k-doc fixture:

| Arm | Time | Relative |
|---|---:|---:|
| secondary equality, covered field | 48-58 µs | 1.0x |
| same scan, decode extra field | 184-191 µs | 3.2-4.0x |
| secondary equality, `id` only | 32-35 µs | 0.6x |

Ranges are two runs on the same box; the relative gap is stable even though
the absolute numbers move with machine load. The bench asserts
`explain_report.covering` for the covered arm, so it fails if the planner
silently drops the fast path.

Remote `hakobench` on `fkip-234` at `7c152f0` (1k docs, Manual,
`--index-groups=std,f2f6`) measured `cover` 26,188 QPS vs `proj` 9,207 QPS
(2.84x); `Qry` 14,174, `Cmp` 11,893, `Json` 8,354. Median of three gate runs:
`Qry` 13,864, `Cmp` 11,820, `Off` 15,379, `Cur` 13,500, `Json` 8,187;
`GATE RESULT: PASS`.

The FFI JSON path was checked on the same box: covered
`[{"active":true,"tenant":"tenant-1"}, ...]` vs decode
`[{"active":true,"extra":"decoded-only","tenant":"tenant-1"}, ...]`. A Python
check over five rows confirmed the shared fields match and only the projected
extra field differs; keys are sorted, so the raw bytes are stable.

A version/harness A/B on the same box (`fkip-234`, Manual, 1k docs,
`--no-maintenance`) separates the library from the benchmark:

| Benchmark | Library | Qry | Cmp | Json | cover | proj |
|---|---|---:|---:|---:|---:|---:|
| old `8d50319` | v0.16.4 | 14,167 | 11,939 | 4,912 | 20,794 | 10,642 |
| old `8d50319` | v0.16.6 | 13,944 | 11,756 | 8,263 | 26,127 | 9,081 |
| new `a228960` | v0.16.4 | 14,278 | 11,892 | 4,913 | 19,633 | 10,626 |
| new `a228960` | v0.16.6 | 14,417 | 11,939 | 8,272 | 27,026 | 9,044 |

`Qry`/`Cmp` are flat (within ~1%). `Json` improves ~1.68x in 0.16.6, which
matches `3fe814e` (bulk-copy writer). The `proj` decode lane is ~15% lower in
0.16.6 on both harnesses; that is a real residual, not a harness artifact.
The `8d50319` harness adds a 5 KB `body` on every 8th doc; `a228960` removes
it but adds a nested `profile` object and `slug` to every doc. Both change the
decode cost, so cross-harness absolute numbers are not interchangeable.

A repeat on `hako-backend` (i3-5005U, 2C/4T, two Manual runs per cell) shows
the same shape: `Qry`/`Cmp` flat (~16.2k/14.1k), `Json` 6.6k on v0.16.4 ->
10.4k on v0.16.6, `cover` 24.4k -> 36.6k, `proj` 13.0k -> 11.8k. The original
`hako-backend` baseline used `/home/riza/hakodb` at `v0.16.4-13-ga7964ae`;
that tree already measured `Json` 10.5k, so the original 10.4k was not a
v0.16.4 number.

The pre-#71 harness (`2b0ce1a`) is the true no-extra-field control: with
v0.16.1 it measures `Qry` 23,005 / `Cmp` 20,521, and with v0.16.6 it measures
22,662 / 20,122. New `a228960` with v0.16.1 measures 16,571 / 13,920. Removing
the nested `profile`/`slug` fields from `a228960` restores 22,532 / 19,946 on
v0.16.6. The 16k figures are therefore a dataset effect, not an engine
regression.

Cold/pending, top-N vs indexed order, filtered-index implication, and
aggregate shapes are still open.

### P2 - expand covering / index-only reads

Safe incremental targets, in order:

1. **Done.** Secondary equality where the projection is exactly the indexed
   field: return `(id, {field: value})` without a document read.
2. **Done.** `id`-only projection on an index-backed scan whose filters are
   satisfied: the id is already in the scan row, so the field list is empty.
3. Composite range/prefix scans where every projected field is inside the
   known prefix. Values beyond the prefix are not in the plan, so fail closed.

Items 1-2 live behind `QueryPlan::covering_projection` and are pinned by
`tests/issue22_covering.rs`. Item 3 is still open.

Do not start with dotted projection rebuilding or nested index-only
reconstruction. `covering_projection` intentionally returns `None` for dotted
paths; keep that until the nested semantics have their own tests.

Acceptance: `explain_report.covering` is true only for proven shapes; results
are byte-for-byte equal to the decode path; blob links still resolve; the
`cover` lane improves without a `proj` regression.

### P3 - planner statistics

Only after P0 and P1 identify a shape where the current heuristic loses.

Minimum useful design:

- per-index or per-field approximate distinct counts, updated on index
  maintenance (sampled/capped, not an exact set on every write);
- an epoch/version so the plan cache does not serve a plan computed from an
  older stats snapshot;
- use stats to choose among candidate indexes and order paths, not to abandon
  a proven equality index;
- stats are advisory: stale or missing stats mean "keep the current rule".

Start with a cheap sampled NDV, not histograms. A full histogram/statistics
catalog, persistence, and background ANALYZE are a separate project. Measure
the planning overhead and the write-path cost; both must stay in the noise.

Acceptance: at least one measured slow shape improves, with no regression on
the standard profiles, and `explain_report` can say which stats were used.

### P4 - partial-index implication

Current filtered indexes are useful for the common `status = active` slice.
The planner's exact `Eq` / single-value `In` gate is correct and should stay
the default. Extending it to range predicates requires proving that the query
implies the index predicate (for example, index `age >= 18` and query
`age >= 21`); `Or` groups and mixed bounds make this easy to get wrong.

Do this only if a real workload needs it. Add a conservative implication
checker with unit tests for each accepted/rejected pair; fail closed on
anything not proven.

### P5 - collection-group / join / order implications

There is no SQL JOIN surface and no collection-group query API in this tree.
Do not invent a join planner. If a concrete grouped-query API is proposed,
define its ordering and merge semantics first; otherwise mark this item not
planned.

### P6 - cursor and deep-offset cost

Already improved (SortedKeys + early termination + top-N). Measure before
adding a new cursor structure. A keyset cursor on a composite index is
implementable, but it is only worth it if the `Off/Cur` lanes show a real
gap on the standard profiles.

## 4. Recommended order

1. P0 collection-scoped readiness (landed in #104; highest measured impact).
2. P1 audit/benchmark shapes (partial: projection lane captured; evidence
   before design for the remaining shapes).
3. P2 covering expansion, starting with secondary equality and `id`-only
   (items 1-2 landed; item 3 remains).
4. P4 partial-range implication only on demand.
5. P3 statistics only if P1 finds a shape that the current rules lose.
6. P5/P6: no code until a concrete API or measured regression exists.

This order deliberately avoids a statistics subsystem first. The measured
evidence says the simple equality index already wins; the known pathology is
the global readiness gate, and the known feature gap is covering breadth.

## 5. Success criteria

For every implemented item:

- before/after numbers on the standard profiles, same dataset and durability;
- `explain_report` output matches the intended scan/covering decision;
- result-set equality against the decode path, including empty/missing fields,
  pending index ops, filtered indexes, and blobs;
- no more than noise-level regression on `Qry`, `Cmp`, `QPS`, `Agg`, `Scan`;
- `cargo test --locked` and the release gate stay green.

Non-goals: SQL, relational joins, changing `find_near`/traversal semantics,
persisted query hints, and a general cost-based optimizer.

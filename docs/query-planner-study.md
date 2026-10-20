# Query planner and index execution study (issue #22)

Status: study, no implementation. Scope: how plans are chosen and executed on
the query side. This does not change query semantics or add SQL/joins.

Current tree: `hakodb` main `ea08078` (0.16.5 plus #98/#99/#100).

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
- Covering (`index-only`) reads exist for one shape only: a composite point
  lookup whose prefix covers every filter, the order, and the whole top-level
  projection. `QueryPlan::covering_projection` returns `None` for anything
  else (nested paths, cursor bounds, raw, elemMatch).
- Filtered composite indexes exist (`CompositeIndexDefinition::filter`). The
  planner may use one only when the query pins the predicate exactly with
  `Eq` or a single-value `In`; `Or` and ranges fall back to a sweep.
- There are no per-field cardinality/NDV statistics. Planning uses
  `collection_rows`, a `rows / workers > 50` heuristic, posting sizes for
  secondary equality, and fixed scan priorities.
- `index_query_ready()` is gated by a **global** `index_inflight` counter, not
  per collection. One pending write anywhere sends all collections to
  `FullCollection` until the index worker drains (see section 3).
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

### P0 - collection-scoped index readiness

Problem: `index_query_ready()` is global. A write to collection A makes a
query on collection B use `FullCollection` until A's index op is applied.

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

### P2 - expand covering / index-only reads

Safe incremental targets, in order:

1. Secondary equality where the projection is exactly the indexed field: the
   plan already knows the value, so return `(id, {field: value})` without a
   document read. This is a natural analogue of the composite point path.
2. `id`-only projection on a scan whose filters are satisfied by the index:
   the id is already in the scan row. No decode.
3. Composite range/prefix scans where every projected field is inside the
   known prefix. Values beyond the prefix are not in the plan, so fail closed.

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

1. P0 collection-scoped readiness (separate PR; highest measured impact).
2. P1 audit/benchmark shapes (evidence before design).
3. P2 covering expansion, starting with secondary equality and `id`-only.
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

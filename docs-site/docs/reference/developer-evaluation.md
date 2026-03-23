---
title: Developer Evaluation Guide (Should You Use FireLite?)
---

This guide is intentionally decision-oriented: it summarizes strengths, limits, implementation coverage, and integration fit so teams can quickly judge whether FireLite is right for their project.

## Best-fit project profiles

FireLite is usually a strong fit when you need:

- Embedded document storage (single-process, local-first runtime).
- Firestore-like developer ergonomics without operating an external DB service.
- Durable writes (WAL + segment model) with tunable sync modes.
- Local query features with indexing, projections, and full-text capabilities.
- Cross-language use from one core engine (Rust/C-FFI/JS/Pascal/Tauri).

## Less ideal project profiles

FireLite may be a weaker fit when you need:

- Multi-region distributed consensus or hosted replication.
- Built-in cloud authn/authz and remote policy service.
- Server-managed multi-tenant fleet controls (as a native managed feature).

## Capability map by implementation area

| Area | Current capability | Notes |
|---|---|---|
| Storage durability | Implemented | WAL + segment persistence with configurable durability. |
| Query planner/executor | Implemented | Filters, ordering, limits, offset, cursor, aggregation, projection pushdown. |
| Full-text search | Implemented | `match`, `contains`, `startsWith`, inverted index support. |
| Indexing | Implemented | Simple, composite, and FTS indexing surfaces. |
| Transactions | Implemented | Basic + serializable conflict-aware style APIs. |
| Realtime listeners | Implemented | Collection watch callbacks and Tauri subscription gateway flow. |
| Encryption + audit | Implemented | Configurable encryption key and audit log surfaces. |
| Language bindings | Implemented | Rust API + C ABI + JS/TS + Pascal + Tauri gateway wrapper. |
| Distributed/cloud plane | Not implemented | Explicitly outside current scope. |

## Practical pros and cons

### Pros

- Fast local data path with no external service dependency.
- Broad API surface available through FFI and first-party wrappers.
- Good fit for desktop, edge, CLI, local backend workers, and offline-capable apps.
- Tunable storage and durability behavior for latency/safety balance.

### Cons / trade-offs

- No built-in distributed topology or remote failover semantics.
- Index strategy still matters; poor index coverage can degrade query performance.
- Operational tooling (observability, deployment policy) is primarily library-level, not managed service-level.

## Risk checklist before adoption

- [ ] Validate durability mode against your data-loss tolerance.
- [ ] Define index plan for your high-volume query patterns.
- [ ] Measure compression/encryption overhead under real workload.
- [ ] Confirm platform toolchain dependencies (especially Tauri desktop builds).
- [ ] Verify audit log footprint and retention behavior for your environment.

## Recommended pilot path

1. Start with one bounded domain (single collection family).
2. Run the benchmark harness (`benchmark.cpp`) with your expected data shape.
3. Capture p95/p99 and compare against your SLOs.
4. Add indexes incrementally and re-measure query/aggregation paths.
5. Decide promotion based on measured perf + feature coverage.


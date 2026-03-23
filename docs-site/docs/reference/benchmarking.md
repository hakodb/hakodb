---
title: Benchmark Harness (v0.5.6)
---

The repository ships a configurable benchmark harness in `benchmark.cpp`.

## What it measures

- Single-document insert latency/TPS.
- Batch commit latency/TPS.
- Point-read latency.
- Ordered query latency.
- Aggregation latency (`count`, `avg`).
- Offset vs cursor (`start_after`) query performance.
- `contains` scan vs `match` FTS path performance.
- Storage footprint after run.
- Raw stats/audit JSON snapshots from engine APIs.

## Configuration coverage

The harness exposes most C-FFI config knobs:

- Durability mode.
- Query worker count.
- Compression on/off + level.
- Encryption on/off + key.
- Audit log on/off + path.
- mmap and inline-memory sizing.
- Page size, compaction threshold, and group-commit tuning.
- Optional simple-index, FTS-index, and compaction toggles.

## Build and run

```bash
c++ -std=c++17 -O2 benchmark.cpp -o firelite_bench
./firelite_bench --docs=20000 --durability=1 --query-workers=8 --compression=true --enable-fts=true
```

### Output

- Human-readable stdout status.
- Markdown report file (default: `./benchmark_report.md`) including:
  - runtime config table
  - workload metrics
  - feature timings
  - strengths/trade-offs section for adoption decisions
  - raw JSON payloads from `fl_engine_get_stats` and `fl_engine_get_audit_log`

## Useful option examples

```bash
# Strict durability profile
./firelite_bench --durability=0 --compression=false --encryption=false

# Throughput profile
./firelite_bench --durability=2 --query-workers=16 --group-commit-max-ops=512

# Security-first profile
./firelite_bench --encryption=true --audit-log=true --compression=true
```


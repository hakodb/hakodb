# Codec perf baseline (pre A+C+D, v0.8.22)

Debug profile, 4GB Windows box. Absolute numbers move with machine load;
what matters is before→after deltas measured back-to-back.

## Micro (ns/op, debug)

| op | before |
|---|---|
| encode/small | 2179 |
| decode/small (5f) | 8709 |
| decode/wide200 | 256208 |
| decode/nested10x20 | 1028540 |
| decode/mid30 (32f) | 46176 |
| projected/small 1-of-5 | 4662 |
| projected/wide 1-of-200 | 63892 |
| view+pull/small | 584 |
| view+pull/wide-mid | 24728 |
| view+pull/mid30-last | 6734 |

## Query 10k (ms, debug)

| query | before |
|---|---|
| full scan, 10000 rows | 41 |
| selective age==42, 143 rows | 95 |

Note: the selective query costs MORE than the full scan — non-matching
rows pay full decode before being dropped. This is what fix A addresses.

## Gate medians (release, Manual)

| metric | before |
|---|---|
| Qry | 17884 |
| Cmp | 15372 |
| Off | 18746 |
| Cur | 16288 |
| Batch | 46938 |
| Single | 39603 |

## After (fill post-fix)

| op | after | Δ |
|---|---|---|
| _tbd_ | | |

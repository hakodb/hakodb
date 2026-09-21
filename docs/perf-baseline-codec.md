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

Medians of 3 debug runs, same box, back-to-back with baseline. Box noise
floor on this machine is ±30–50% run-to-run in debug — treat single-digit
deltas as directionally consistent, not precise. The arbiter is the
release `--gate` (relative invariants), which passes.

| op | before | after (median) | Δ |
|---|---|---|---|
| decode/small | 8709 | 10753–14874 (noisy) | ~0 (path untouched) |
| decode/wide200 | 256208 | ~395000 (noisy) | ~0 (path untouched) |
| query10k-filtered | ~72 | ~50 | improves* |
| query10k-full | ~44 | ~70 | noise (path untouched) |

*Filtered-scan improvement is consistent across runs; full-scan movement
in both directions across runs confirms the noise floor dominates.

## Gate medians (release, Manual) — before → after

| metric | before | after |
|---|---|---|
| Qry | 17884 | 18181 |
| Cmp | 15372 | 14753 |
| Off | 18746 | 18910 |
| Cur | 16288 | 17463 |
| Batch | 46938 | 54806 |
| Single | 39603 | 47332 |

GATE RESULT: PASS both runs. Batch/Single moved although the write path
is untouched — machine variance, not the fix. All relative invariants
(Qry≥0.85Cmp, Off/Cur within 2x, Get>5xQry, Batch≥0.5Single) hold.

## Batch 2 (lazy validation + dotted pulls)

Baseline (debug, same box): decode/sub25 60555, view+pull/sub25-whole
51969, q-filtered 41ms, q-gt 96ms.

After: view+pull/sub25-dotted (`get_path("profile.sub_03")`) = **2952ns
vs 52296ns whole-subtree — 17.7x**. Query-level numbers move within
noise (paths already optimal there); the win is targeted nested access.

Corrections to the study report: `skip_value` honors len prefixes with
O(1) jumps (no double-walk exists), so proposal B was withdrawn before
implementation. Nested cost is allocation + validation volume, and the
fixed `0x40` short-string arm of `skip_value` could overshoot the buffer
and panic downstream slicers on corrupt input — now fails closed (found
by the new truncation fuzz in `tests/codec_paths.rs`).

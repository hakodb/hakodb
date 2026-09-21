# Changelog

Full per-release history for HakoDB. The README keeps only the latest three.

### v0.8.21 — rebrand to HakoDB
- Crate `hakodb`, main type `Hako` (`HakoConfig`, `HakoDoc`,
  `HakoError`), FFI prefix `HK_*`/`hk_*`, header `include/hako.h`,
  binaries `hakodb.dll` / `libhakodb.so`.
- Data plane migrates on open: `__firelite_*` directories become their
  `__hako_*` canonical names with data intact (both-present keeps
  canonical; old spellings stay sync-excluded as aliases).
- Fixed along the way: `cbindgen.toml` never loaded (relative path +
  unknown fields → silent C++ defaults for years); the header is real
  C now, with `extern "C"` guards for C++ consumers.

### v0.8.20 — repo split phase 1: core ships alone
- **Removed `fsync/`** (5-line re-export wrapper, zero references —
  redundant since FFI + sync live in one crate) and narrowed the
  workspace to the library crate alone.
- **Watch API for out-of-tree gateways**: new narrow public methods
  (`plan_for_watch`, `matches_watch`, `get_raw_bytes`) carrying the
  exact zero-decode cost of the former in-tree path; the Tauri gateway
  module + `tauri`/`rmpv` deps leave the core.
- **MSVC-ready build**: `build.rs` keeps `hakodb.lib` on MSVC targets
  (GNU/MinGW keeps the stale-shadow delete); the tag-triggered release
  workflow ships `.dll`+`.lib` (Windows), `.so`+`.rlib` (Linux),
  Android `aarch64` `.so`, all with headers + checksums.

### v0.8.19 — excluded-plane enforcement on all cloud paths
- **Correctness fix (sync)**: a hostile `Replication{collection:"__groups"}`
  was applied — server planted an `alpha___groups` shard and the relay
  rebuilt the packet under the plain name, poisoning every room member's
  real credential store. `flush_ingest_buffer` now drops excluded batches
  in either namespace (single choke point for client+server ingest, also
  suppresses relay); server tailer, client catch-up push, and catch-up
  serve skip excluded names (source-side, protects unpatched peers).
  Regression test `ingest_drops_sync_excluded_plane` fails without the
  fix, passes with it.

### v0.8.18 — sync-saving WAL fixes, 4MB reserve default, maintenance hold
- **Correctness fix (sync)**: `Wal::tail` opened a fresh read handle per
  call. The old `try_clone` + seek + buffered read shared the file
  position and dragged the writer cursor, stranding appends inside
  preallocation padding where readers stop at the first zero header —
  cloud sync broke deterministically with reserve on (rooms test failed
  at 4MB, passed at 0MB). Positional `pread` was tried first and also
  moves the cursor on some platforms; the separate handle is immune
  everywhere.
- **Correctness fix (double write)**: `flush()` wrote the buffer twice;
  every WAL record persisted 2×. Now single-write (3 committed replay
  tests green again).
- **Default `wal_reserve_bytes` to 4MB** (was 0): presence-not-size —
  steady-state appends never extend the file. No delta on fast local
  disks; decisive on cloud disks with slow file-growth metadata.
  Internal (`__`) collections still skip it.
- **Maintenance hold**: `background_maintenance=false` (config +
  `hk_config_set_background_maintenance` + bench `--no-maintenance`,
  wired in Go/Pascal/JS) pauses the 5s checkpoint/compaction tick for
  flat bench rounds; engine stays correct.
- Re-measured Always singles post-fix (local, `--no-maintenance`):
  no reserve delta on fast disks; prior cloud figures predate the
  flush fix (2× WAL bytes).

### v0.8.17 — allocation census: allocator exonerated, one copy killed
- Temporary counting allocator over every read path (deleted after —
  numbers below). Per-op allocs: walk **0.0** (zero-alloc design
  verified empirically), raw 1.0 (id String), decoded 3.0 (id + fields
  + values — optimal for owned output), point-get ~9, isolated decode
  2.0, views ~0.
- Verdict: allocation was never the bottleneck (walk costs 367ns/row
  with zero allocs; decoded costs ~2.2µs with three). Time is CPU —
  hashing, decode loop, memcpy, locks. No allocator swap, no arena,
  no unsafe: all rejected with evidence.
- Shipped from the census: `get()` decodes borrowed from the shared
  Arc instead of cloning the full bytes into a transient Vec (−1 alloc
  and −1 memcpy per point-get, mechanical certainty).

### v0.8.16 — count-cache + a real correctness fix found profiling
- **O(1) live counts**: the `collection_counts` map existed but was
  dead (never initialized). Now maintained in `update_index_entry`,
  seeded from the recovery sort, read by `count_prefix` (122µs → 164ns
  per query). Small indexed queries ~1.6x (169 → 107µs); gate medians
  roughly doubled on the re-run.
- **Correctness fix (v0.8.5 regression)**: FullCollection limit
  pushdown pre-truncated *before* filter matching — unindexed
  filtered queries with limit returned wrong (usually empty) results.
  Gated on filter-free; filtered scans collect-then-truncate. Locked
  by a keeper test (unindexed `tag` filter + limit exactness).
- **Quiescence covers backfills**: `create_*_index` rebuilds run
  detached and served partial results with no signal (caught the
  profiler red-handed: 0-row pages mid-backfill). Panic-safe in-flight
  counter on all three backfill sites, surfaced as `index_backfills`
  (Rust + FFI JSON). `apply_replicated_ops` routed through
  `update_index_entry` (also fixes its sorted_keys bypass).

### v0.8.15 — startup latency: measure first, then cut
- Measured release open→ready: 14ms fresh, 93ms per 10k warm docs
  (Always 420ms on an 11MB WAL — honest I/O+parse, ~40MB/s). The
  felt slowness was elsewhere, and both causes are fixed:
- `benchmark.cpp`: the unconditional 1.5s "waiting for indexes" sleep
  is now a readiness poll (instant on fresh DBs, ~9s saved per full
  run, more correct on real ones).
- CLI one-shot: the v0.8.14 quiescence wait was over-scoped for reads
  (blob drain/maintenance don't affect correctness and can take
  minutes) — downgraded to readiness-only with the same 30s bound.
- Non-finding, recorded so nobody rediscovers it: an 11MB-vs-0.9MB WAL
  "asymmetry" between bench dirs was just different doc counts (gate
  runs reseed Manual at 1k docs; Always still held 10k). Census
  verified: exact counts, payloads intact, overwrites/deletes correct.
  Recovery stays sequential per collection (parallelism helps only
  multi-collection DBs — skipped with the evidence).

### v0.8.14 — quiescence API (settle the engine, then measure)
- New `QuiescenceStatus` (`indexes_ready`, `pending_index_ops`,
  `pending_blob_bytes`, `queued_blob_items`, `maintenance_running`)
  with `quiescence_status()`, `is_quiescent()` and
  `await_quiescent(timeout)` (two consecutive clear samples — a
  millisecond gap between write batches must not read as settled).
  Covers all four background stages, including async index updates
  (new in-flight counter; std mpsc has no `len`) and a new
  maintenance flag on the 5s system thread.
- FFI: `hk_engine_await_quiescent` + `hk_engine_quiescence_status`
  (JSON). CLI waits for quiescence after open (was readiness-only —
  cursor queries pre-settle repeat rows). `benchmark.cpp` settles
  before scan stages.
- Gate: `Batch>=0.5xSingle` (Manual-mode thin margins: batch and
  single do near-identical work per doc without fsync; observed median
  0.82x on load — the tripwire now catches breakage, not noise).
- Test-link fix: integration tests link `hakodb.dll` (fresh import
  lib) instead of `hakodb.lib` (deleted by build.rs to stop shadow
  staleness) — the 1181 break this caused, resolved properly.

### v0.8.13 — views across SDKs; node backend resurrected
- **Go**: `ViewDoc` (`GetView`, `GetInt/Float/Bool/String/Bytes`,
  `HasField`, `ToDoc`) + `CursorWalkView` via a second cgo trampoline.
  Builds clean under GCC 16.
- **Pascal** (FPC-clean): view imports, `THKViewDoc`, `THKQuery.WalkView`,
  `THako.GetView`.
- **JS**: point-view numerics + `ViewDocSnapshot`/`viewDoc` on both
  backends and the client (`tsc` clean; koffi verified end-to-end
  against the real DLL). Strings and walk callbacks stay on
  resolve/raw paths — no backend memory reads, by design.
- **Tauri**: `indexes_ready` probe and stateless `view_get_field`
  (lazy scalar pulls, no decode) + `tauri.ts` wrappers.
- **CLI**: waits for index recovery after open (pre-readiness cursor
  queries repeated rows — correctness, not just speed).
- **Found en route**: the node/koffi backend was dead three ways —
  missing opaque declarations (load-time throw on every struct type),
  `OnSnapshotCB` vs the registered proto name, and auto-decoded
  `char*` returns that leak + crash on free. Fixed with upfront
  opaques, the proto name, and a disposable string type wired to
  `hk_string_free` (never C free — Rust allocator). Also caught a
  glued `#[no_mangle]` that hid `hk_rawdoc_to_doc` from the DLL while
  rlib tests passed.

### v0.8.12 — gate margin hardening + stale import lib, fixed
- **Gate**: median-of-3 Manual runs (justified in one session: reps
  read Qry 4832/8966/8551, Cmp 6585/7855/4617, Off 1965/6612/5704 —
  rep 0 alone fails three checks). Medians reject transients;
  `Qry>=0.85Cmp` tolerance absorbs systematic wobble (both stages are
  fixed-cost-dominated at 20 rows, so the relation measures jitter;
  the 10x+ bug classes it guards still trip). ~3x gate time.
- **Import lib**: `build.rs` no longer copies `dll.lib` → `lib` (it ran
  pre-link, so lib lagged one build forever, shadowing the fresh DLL
  in MinGW search order with ghost undefined-references). It deletes
  the shadow instead — MinGW links the always-fresh DLL directly,
  proven by relink. No consumer needed the MSVC lib.

### v0.8.11 — FFI views + lazy-vs-lazy benchmark stage
- `HK_ViewDoc` + 9 functions (`hk_view_get/free`, `field_count`,
  `has_field`, typed `get_int/float/bool/str/bytes`, `to_doc`) and
  `hk_cursor_walk_view` with `HkViewWalkCallback` (borrowed id + view
  handle, valid for the call only — stack-slot views, no alloc, no
  free protocol). Strict scalar matches; views never inflate (resolve
  via `to_doc` + `hk_doc_resolve_blobs`).
- Both harnesses gain the lazy stage: HakoDB 2-pull view walk vs
  SQLite narrow id/tenant/age select. Measured at 10k complex docs:
  **893k vs 1.11M (0.8x)** — same work, honestly close; our remainder
  is per-row framing walks (tenant sorts last) + callback hops.
- Full map at 10k: decoded ~3x (owned construction), lazy 0.8x,
  raw/key parity-or-better. Each shape now has its fair fight.
- Gate note: `Qry>=Cmp` flaked twice (Cmp spiking 2x run-to-run) then
  passed at +53% — untouched paths, transient box noise, but the
  composite path's variance deserves its own look (margin hardening).

### v0.8.10 — borrowed views: our own sharp side
- New `DocView` (pinned `Arc` + lazy per-field pulls), `db.get_view`
  and `db.walk_view`: the `sqlite3_step` + typed-accessor analog.
  Header-only validation at construction (a full up-front framing pass
  cost ~2x on scans for corrupt data storage never holds); per-field
  access is bounds-checked, `to_owned_doc` decodes strictly.
- Measured in-process release: point-view **649k ops/s** (vs 235k owned
  get), walk count-only **2.33M docs/s**, walk + one field pull **887k**.
  The lazy-vs-lazy comparison SQLite's shape always deserved is now
  winnable on our side too.

### v0.8.9 — benchmark scan parity (HakoDB vs SQLite, 1:1)
- Both harnesses grow the same full-scan trio (×5 iters, printed after
  the matrix): decoded forward/reverse over all live docs plus a
  byte/key-only scan (`hk_cursor_walk` vs id-column select). Same stage
  order (scans run before bulk delete), same math, row counts printed
  for verification.
- Measured head-to-head, same box: at 10k complex docs, decoded
  ~130–220k vs ~430k owned-vs-owned (per-row construction efficiency —
  8 mallocs + cursor step vs id Strings + field vecs + interning), raw
  walk ~6.0M vs key scan ~6–7M (parity). At 1k hot docs the walk hits
  ~10M (L2-resident Arcs).
- Honest reading: the decoded gap narrowed to construction efficiency
  once SQLite truly materialized (accessor pokes alone measured borrowed
  buffers). Raw is where the designs meet, and the walk leads there.

### v0.8.8 — SDK wiring: raw + walk everywhere it fits
- **Go** (`go/hakodb`): vendored header + `RawDoc`/`RawResultSet`
  (`ExecuteQueryRaw`, `Bytes`, `ID`, `StartAfterRaw`, `ToDoc`) and
  `Engine.CursorWalk` via a `cgo.Handle` trampoline (same pattern as the
  watch bridge; builds clean under GCC 16). `IsIndexesReady` already
  existed.
- **Pascal** (`HakoDBRaw` + `HakoDB.pas`, both compile under FPC
  3.2.2): raw imports, `THKRawDoc`/`THKRawResultSet`,
  `THKQuery.ExecuteRaw`/`StartAfterRaw`/`Walk` with `THK_WalkCallback`.
- **JS** (`native.ts` both backends + `client.ts`, `tsc` clean):
  `queryExecuteRaw`, raw set accessors, `queryStartAfterRaw`,
  `rawDocToDoc`, plus `RawQuerySnapshot`/`getRaw`/`startAfterRaw` at the
  client level (scan-many/touch-few: page raw, resolve selected rows).
  Deliberately absent: `rawDocBytes`/`Id` (need unverifiable
  backend memory reads — resolve-or-anchor covers everything) and JS
  walk callbacks (a walk you can't touch rows in is a counting loop;
  use Go/C++/Pascal for byte-level walks).
- **Tauri gateway + `tauri.ts`**: `query_raw` (bytes cross msgpack as
  bin → Uint8Array) and `decode_raw` (selected-row resolve
  server-side). Pointers never cross into TS — raw crosses by value,
  which is exactly the honest shape: cheaper protocol (bytes vs JSON),
  never zero-copy.
- CLI untouched by design (`run_query`'s 24-arg threading for an
  id+len debug print isn't worth the churn).
- Ops note: `target/debug/incremental` had eaten the disk (32GB tree,
  110MB free); cleared, 14GB back. Watch it on CI.

### v0.8.7 — integrity matrix: two real bugs caught
- New `tests/codec_integrity.rs`: codec identity on every Value arm,
  then a mixed dataset (scalars, unicode, nested, blobs, overwrites,
  deletes, 300-row volume) read back through **every** path — get,
  scans both directions + paging, offset/limit, unordered limit,
  raw+decode, walk+resolve, projections — fresh AND reopened, with
  `encode(decode(bytes)) == bytes` on every stored row.
- **Bug 1 (genuine): `Value::PartialEq` missed Binary/Array/Reference
  arms** — any `==` on docs holding those values returned false while
  `Ord` ordered them fine. Fixed; eq/cmp consistent.
- **Bug 2 (genuine): `decode` accepted truncated buffers** as silently
  short docs (iterator ran dry mid-document, no error). Decode is now
  strict: field count AND exact consumption required. All 92 tests pass
  unchanged — no caller relied on lenient decode.
- **Documented semantic**: blob storage is type-erased bytes; inflation
  restores String iff valid UTF-8 else Binary. Locked both sides in the
  matrix (changing it needs per-blob type tags — separate decision).

### v0.8.6 — FFI walk: 1.64M docs/s over the ABI
- `hk_cursor_walk(engine, query, cb, userdata)` + `HkWalkCallback`
  typedef (header-regenerated): one FFI call per scan, borrowed
  `(id, id_len, bytes, bytes_len)` per row, `false` stops early,
  returns rows visited / -1 on error. C cannot unwind so the trampoline
  needs no per-row shield; same no-reentry contract as `db.walk`.
- Measured in-process release: **1.64M docs/s** over the ABI (vs
  2.1–2.5M native with-id — the delta is one indirect call per row).
  The dbbench backend can now grow its raw branch on this (caller side).

### v0.8.5 — decoded scans 150k → 560k (fetch machinery, not codec)
- **Measured first**: codec floors are decode 950ns / encode 209ns per
  100B doc — decode is only ~14% of the 6.7µs scan row. The rest was
  phases 3–5 (offset-sort, String re-clones, restore-order map) running
  even when the index already yields final order.
- **Satisfied fast paths**: order- + filter-satisfied scans now decode
  in scan order — sequential to 2000 rows, order-preserving parallel
  beyond (`execute_satisfied_parallel`: one guard, Arc staging, par
  decode). Small unsatisfied queries keep the old ≤250 path; big
  unsatisfied keep phases 3–6. No behavior change, just skipped work.
- **FullCollection limit pushdown**: unordered TOP-N no longer collects
  the whole collection per page.
- Result: decoded keyset scans **~150k → 564k fwd / 485k rev**;
  `benchmark --gate` PASS (one noise FAIL in three runs on the Batch≈
  Single margin — read-only batch, reruns green).

### v0.8.4 — zero-alloc walk: 2.1–3.3M docs/s
- New `db.walk(query, callback)`: the engine lends each row (`&str` id,
  `&[u8]` bytes, borrowed under one read lock), `false` stops early.
  No per-row String, Arc bump, Vec, or per-page plan — SortedKeys scans
  for now, index-satisfied filters/ordering required, callback must not
  re-enter the engine (documented, same rule as nested MDBX txns).
- Measured in-process release, 20k docs: **with-id 2.1–2.5M**,
  **count-only 3.35M vs MDBX 3.66M** (0.92x — effectively parity; our
  remainder is one HashMap lookup vs their cursor bump). The 2M bar from
  the design review, cleared with room to spare.

### v0.8.3 — raw FFI surface for byte-fair benchmarks
- `HK_RawDoc` / `HK_RawResultSet` + 7 functions (`hk_query_execute_raw`,
  `hk_rawresult_{count,get,free}`, `hk_rawdoc_{bytes,id}`,
  `hk_query_start_after_raw`, `hk_rawdoc_to_doc`). Same slab + borrowed
  contract as the decoded path; same `HK_Query` builders (raw forced
  internally). Measured in-process: **~670k docs/s** over the ABI vs
  ~1.03M native raw (the gap is per-row id copies on the caller side)
  vs MDBX 3.66M pointer bumps — remaining 5x is per-row allocs + HashMap
  that only a zero-alloc cursor-callback API would remove. Honest
  raw-vs-raw comparison is now possible; `t_hakodb.cc` needs its raw
  branch (caller side).

### v0.8.2 — inline-at-write + raw scans: 1M+ docs/s
- **Raw scans hit 1.03–1.20M docs/s** (was ~289k). New `Query.raw` /
  `db.query_raw()` stops after the index walk and shares buffers
  (`read_pointer_shared`, zero copies for inlined docs) — no decode, no
  rayon. Requires index-satisfied filters/ordering; bytes are opaque
  storage encoding (decode with `HakoDoc::decode`).
- **Inline-at-write** (the bigger lever): every put used to land as
  `BlobPending` and re-encode on *every read* until background
  conversion — small docs in Manual mode never converted at all. Writes
  now store `Inlined` with the bytes already encoded for WAL (durability
  identical, worker swap no-ops, pre-flush blob reads still resolve via
  the flush queue). Fresh-state reads run at steady-state speed:
  point-gets ~105k → ~235k native, FFI+JSON ~50k → ~90-103k.
  Always-profile writes unchanged (636 WPS, in-band).
- **Two pre-existing flakes fixed**: (1) queries issued before background
  index recovery silently plan `FullCollection` (cursor bounds then
  ignored, pages repeat) — tests now poll the new public
  `is_indexes_ready()` (mirrors the FFI); benchmark authors take note.
  (2) `bulk_json_matches_per_doc` raced the blob flush (3/6 fails on
  main) — now polls for persistence instead of luck.

### v0.8.1 — reverse-cursor parity + point-get push (dbbench vs MDBX)
- **Reverse cursor 27x → ~1.1x.** Descending `ORDER BY id` with cursor
  bounds fell through the id fast path into full-scan + sort per page.
  The planner now emits `SortedKeys{reverse + bound}` for any anchor and
  the executor walks backward from a binary search
  (`sorted_key_range_reverse`, O(log N + limit)) — same path as forward.
- **Point-get 46.8k → ~80k (harness-equivalent).** Split measured:
  engine floor ~105-140k (vs MDBX 136k raw memcpy — competitive given
  full doc decode), FFI wrapper tax ~2x, `hk_doc_to_json` Binary arm
  ~11µs (100 boxed Numbers per 100-byte value). Fixes, all
  byte-identical output: streaming JSON serializer (digits need no
  escaping; keys still via serde_json; sorted order kept), borrowed C
  strings in `hk_engine_get`, single version lookup + audit-gated allocs
  in `get()`. Remainder is real work-per-row (decode + JSON text vs
  pointer bumps) — see `tests/cursor_parity.rs`.

### v0.8.0 — sync hub/server release
- The sync batch graduates to minor: managed hub
  (auth, groups, data plane, SSE, admin console, TLS, systemd + Windows
  Service), mesh discovery modes (mDNS / broadcast / both), rejoin-safe
  local-only deletes, and fail-closed encrypted sync (Layer 0 handshake
  capabilities, sender/receiver/relay enforcement — no silent plaintext
  leaks to unverified peers). `net-sync` + `cloud-sync` ride in default
  features. Core engine behavior and performance unchanged (verified with
  `benchmark --gate`, see below).

### v0.7.14 — encrypted sync goes fail-closed (Layer 0)
- Encrypted collections no longer replicate as silent plaintext to
  unverified peers. Handshakes now carry key fingerprints (`SyncCaps` on
  mesh, `enc_fp`/`enc_cols` on cloud auth); senders skip, receivers drop,
  and relays filter per recipient — all with throttled loud warnings.
  Plaintext deployments are byte-identical; mixed-version meshes stay
  connected. See [Sync encryption posture](#sync-encryption-posture-read-this-before-encrypting).

### v0.7.13 — net-sync + cloud-sync in default features
- The release DLL now exports the full mesh + cloud surface
  (`hk_net_syncer_*`, `hk_cloud_sync_*`), matching what the Go/JS/Pascal
  SDKs already wrap. Previously those symbols existed only with explicit
  features — SDK calls against the default DLL failed at runtime, not at
  compile time. `tauri-gateway` stays opt-in.

### v0.7.12 — WAL reserve off by default
- **Measured, not theorized.** A/B on the fsync-bound Always profile:
  746 WPS with reserve 0 vs 626–764 across five reserve-4MB runs — inside
  the noise band. The fsync cost dominates so completely that file-growth
  metadata is unmeasurable at this scale.
- Default `wal_reserve_bytes` is now 0 (was 4 MB): no phantom size per
  shard, no surprise floors on mobile storage. Opt back in per workload
  via `hk_config_set_wal_reserve_bytes` if a long-soak test ever shows
  fragmentation-driven fsync decay.

### v0.7.11 — WAL history compaction for hot-small collections
- **Problem.** Small-but-hot collections (sync checkpoints, carts, sessions)
  appended WAL history nothing ever reclaimed: segments never spill at that
  volume, and `compact()` early-returned before the WAL rewrite when no
  segments needed merging — not even manual compact helped.
- **Fix.** `StorageEngine::compact()` now rewrites the WAL snapshot whenever
  stale history dominates (file past the compaction threshold *and* over ~3x
  live inlined bytes — O(1) check, tombstones count as zero), even with zero
  segments to merge. Same bounded rewrite runs once at open (best-effort).
  Existing `compact` CLI/FFI/app paths reclaim automatically.

### v0.7.10 — Pascal SDK install fixes + component polish
- Canonical runtime/designtime split (`HakoDBPkg` + `HakoDBDesign`);
  the single mixed package would not install.
- Package renamed `HakoDB` → `HakoDBPkg`: the IDE auto-generates a
  `<PackageName>.pas` stub that had overwritten the engine unit, causing a
  phantom circular reference.
- Palette icon (`thakodbcomponent.lrs`, built from `.xpm` via `lazres`).
- `NetSyncEnabled` / `CloudSyncEnabled` master switches (default off);
  sync properties are inert until enabled.
- `THKDiscoveryMode` + `SetDiscoveryMode` + component `NetSyncDiscovery`
  property surface the net_sync discovery choice.

### v0.7.9 — developer-chosen discovery: mDNS / broadcast / both
- **Why.** v0.7.8 proved UDP broadcast beacons (no MulticastLock, pure Rust),
  but gated them Android-only — where no desktop listens, so mixed groups
  could never meet. Discovery must be symmetric: this release puts the choice
  in the developer's hands on every platform.
- **How.** `NetSyncer::with_discovery(DiscoveryMode::{Mdns, Broadcast, Both})`
  (mirrors `with_relay`; CLI `serve --discovery <mdns|broadcast|both>`).
  Defaults preserve history: **mDNS on desktop, broadcast on mobile** — every
  existing deployment behaves bit-for-bit as before with zero config.
- **Mixed-group recipe.** A desktop joining mobile peers opts in once
  (`--discovery both`); one-directional discovery suffices per pair (whoever
  hears, dials) and beacon gossip spreads membership group-wide. Beacons carry
  `{id, room_hash, tcp_port, known_peers}`, receivers use the UDP source IP
  (multi-interface safe — the exact failure that killed the v0.6-era beacon,
  which advertised self-reported IPs, room-unaware, dial-per-packet).
- mDNS paths untouched; no wire-protocol change; no new API beyond the
  builder flag; no cloud_sync change.

### v0.7.8 — Android UDP broadcast discovery + membership gossip
- **Why.** Android's WiFi stack filters inbound multicast without a Java-side
  `MulticastLock`, so mDNS browsing silently hears nothing. Subnet *broadcast*
  is not filtered: no lock, no new permission, pure Rust on `INTERNET`.
- **How.** On `target_os = "android"` only, each node beacons
  `{id, room_hash, tcp_port, known_peers}` to `255.255.255.255:5354` every 5s
  and listens on the same port. Receivers take the sender address from the UDP
  source (multi-interface safe) and merge gossiped peers, so finding one peer
  bootstraps the group. Stale entries expire after 45s (broadcast has no
  leave event). Desktop binaries are unchanged: same code paths, mDNS only —
  the spawn sites are the only target-gated lines.
- No new API, no wire-protocol change, no cloud_sync change.

### v0.7.8 — Android UDP broadcast discovery + membership gossip

### v0.7.7 — rejoin-safe local scope: vacuum + delta-send filter
- **net_sync rejoin leak closed.** `handle_delta_send` (mesh bootstrap/catch-up)
  now filters local-only tombstones and collections, matching the live tailer
  and both cloud catch-up paths. No handshake path can transmit a local mark.
- **Vacuum.** `vacuum_collection` purges a collection's tombstones from the
  index with zero WAL traffic (FFI `hk_engine_vacuum_collection`, CLI `vacuum`,
  Tauri `vacuum` op). Version drops to the newest live doc, so the next
  handshake pulls peer state instead of defending local deletes.
- **Rejoin recipe (reset now, restore later, wipe nothing):**
  1. `delete_where_local` / `delete_local` (or `collection-local`) — reset stays
     local; fresh tombstones keep the version ahead so no ping restores early.
  2. Optionally turn sync off while reset.
  3. To restore: `vacuum_collection` + `replicate_collection` (or
     `collection-local --off`) — marks clear, tombstones gone, version drops.
  4. Next handshake pulls the room state; nothing is pushed outward at any step.
  Without vacuum, a fresh local tombstone correctly outranks older peer puts
  under LWW (the doc stays deleted — that *is* the local-only promise).
- `replicate_collection` clears a collection's flag plus all its key marks
  (prefix-safe: `c` never eats `c2`). SDKs: Go/JS/Pascal + Tauri `vacuum`.

### v0.7.6 — local-only deletes + tombstone catch-up fix
- **Local-only signal.** `delete_local` / `delete_where_local` / `delete_ids_local`
  (FFI `hk_engine_delete_local`, `hk_query_delete_local`, CLI `--local`) mark keys
  so no sync tailer or handshake ever transmits them — the app owns the deletion.
  `set_collection_local` scopes whole collections (CLI `collection-local`, shown in
  `collections`); `replicate_key` opts a key back in. Marks persist in
  `__hako_system/local_only` across restarts.
- **Handshake-stability rule.** Local-only ops keep fresh tombstone timestamps, so
  the deleter's version clock advances and no ping/catch-up can push the doc back.
  Resurrect rule: a genuinely *newer* remote put still applies (LWW); stale
  replays are rejected.
- **Tombstone catch-up fix.** Handshake catch-up (`send_catchup_deltas`,
  `push_client_deltas_upstream`) now replays tombstones as timestamped deletes —
  previously put-only, so a peer offline during a delete never learned of it.
  The cloud ingest Delete arm gained the missing LWW check; `BlobPending` docs
  (previously also skipped) are included too.
- SDKs: Go `DeleteLocal`/`DeleteWhereLocal`/`SetCollectionLocal`/`ReplicateKey`,
  JS `deleteLocal`/`deleteWhereLocal`/`setCollectionLocal`, Tauri
  `local_only` op flag + `localOnly()` constraint, Pascal `DeleteLocal`.

### v0.7.5 — deferred blobs, parallel inflation, bulk JSON
- **`defer_blobs` query flag** — queries can skip blob inflation and return a `{"__blob__": {"len", "offset"}}` placeholder per blob field instead of the bytes. A 20-doc query over 50 KB images drops from ~1.2 ms to ~240 µs (~5×; more for larger blobs).
- **`hk_doc_resolve_blobs`** — fetch the real blob bytes for a deferred doc on demand (point-get path, stays fast).
- **Parallel blob inflation** — multi-blob docs inflate link targets on the rayon pool; link-free docs skip the scan entirely.
- **`hk_result_set_to_json`** — stream a whole result set to one JSON array in a single call (~1.92× vs per-doc `hk_doc_to_json`).
- **CLI `--defer-blobs`** on `query` (one-shot and serve mode); `get` stays eager. Tauri `QueryInput.defer_blobs` supported end to end.
- SDK surface: Go `DeferBlobs`/`ToJSON`/`ResolveBlobs`, JS `query.deferBlobs()`, Pascal `THKQuery.DeferBlobs`.
- `build.rs` refreshes the Windows import lib on every build (stale `.lib` after header regen is gone).

### v0.7.4 — durability fix + perf gate
- **Linux fdatasync restore** — WAL append/flush uses `sync_data` (fdatasync) instead of `sync_all`; codespace `Always` write throughput 535 → 1115 WPS (+109%). No-op on Windows (`FlushFileBuffers` covers both).
- **`benchmark --gate`** — CI-enforced regression gate (`.github/workflows/perf.yml`): Qry ≥ Cmp, Off/Cur within 2×, point-get > 5× query, batch ≥ single, plus smoke floors.
- `benches/read_path.rs` + `benches/write_path.rs` criterion benches tracked in git (were ignored).

### v0.7.3 — read/write path optimization
- **Zero-copy decode** — `Pointer::Inlined(Arc<Vec<u8>>)`, byte-compare matcher (`unified_match_decode`), scratch-buffer scalar encode, field-name interning.
- **Limit pushdown** — `range_scan_limit` and unordered single-`Eq` yield to the secondary index instead of the composite path; empty-`ORDER BY` limit pushdown with offset-safe limits.
- **Id-cursor fast path** — `start_at`/`start_after` on document id resolves to a `SortedKeys` Vec range instead of a composite scan.
- **Plan-cache key fix** — cursor bound tags (`start_at` vs `start_after`) included in the key; previously colliding plans could return wrong pages.
- **Write fast path** — `put_owned` / `hk_engine_insert_take` (no clone on owned docs), shard-lookup hoist, `ChangeEvent.path: Arc<str>`, 8192-entry version-stamped hot doc cache.
- **WAL headroom** — `wal_reserve_bytes` (default 4MB since v0.8.18, was
  opt-in 0 since v0.7.12; sparse, internal collections skip it) via
  `HakoConfig::wal_reserve_bytes` / `hk_config_set_wal_reserve_bytes`.
- **Write-phase timers** — `WRITE_STATS` + `write_stats_report()` / `hk_debug_write_stats()`; `benchmark --profile=<mode> --wstats` attributes write latency (Manual ~11.7 µs after shard hoist, −24%).

### v0.7.2 — pagination, WAL hardening, FFI slab
- **O(1) offset pagination** — `sorted_key_range` slice + `offset_to_apply_later`; descending order via `SortedKeys` reverse ranges.
- **WAL decoder + recovery fixes** — padding-safe replay/tail/reset, Manual-mode double-size fix, recovery-vs-write race fix (`entry().or_insert`).
- **FFI slab allocator** — `Vec<HK_Doc>` slab refactor for result sets; `eprintln!` → log sink; `tests/ffi_roundtrip.rs` + `tests/write_path.rs`.
- **Fair benchmark** — all four query shapes decode the same 20 docs (limits raised 5 → 20), so Qry/Cmp vs Off/Cur numbers are comparable.
# HakoDB

**HakoDB is an embedded, Firestore-style document database written in Rust.**

It stores typed JSON-like documents in binary form, runs **fully in-process** like SQLite (no server process, no daemon, no network config), and exposes a **flat C ABI** so it can be embedded in applications written in Rust, C/C++, Go, JavaScript/TypeScript (Node.js + Bun), Pascal/Lazarus, and more.

HakoDB speaks "documents", not tables: collections of flexible, schemaless objects with a query API that feels like Google Firestore (`collection().doc().set()`, `.where().orderBy().limit()`), while keeping the zero-deploy footprint of an embedded engine.

> **Current status: v0.8.21 (production-candidate).** The core engine supports physical data sharding, zero-copy field projection, near-instant recovery, composite + full-text + secondary indexing, encryption at rest, deferred blob fetching, bulk JSON result export, and high-throughput local or cloud synchronization capable of **50,000+ OPS** under heavy concurrent workloads.

---

## What's new (0.7.2 → 0.8.21)

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

---

## Table of Contents

- [What is HakoDB?](#what-is-hakodb)
- [What's new (0.7.2 → 0.8.21)](#whats-new-072--0821)
- [When to use HakoDB (sync vs non-sync)](#when-to-use-hakodb-sync-vs-non-sync)
- [Key features](#key-features)
- [Quick Start (Rust)](#quick-start-rust)
- [Rust usage](#rust-usage)
- [Multi-language platform support (C ABI)](#multi-language-platform-support-c-abi)
- [Net Sync (LAN replication)](#net-sync-lan-replication)
- [Cloud Sync (centralized replication)](#cloud-sync-centralized-replication)
- [Sync encryption posture (read this before encrypting)](#sync-encryption-posture-read-this-before-encrypting)
- [Guide: choosing a read path](docs/reads.md)
- [Repositories](#repositories)
- [Architecture](#architecture)
- [Implementation status](#implementation-status)
- [Contribution notes](#contribution-notes)
- [License](#license)

---

## What is HakoDB?

HakoDB is a **document-oriented embedded database** for applications that want:

- **Firestore-like ergonomics** — collections, documents, `set/get/delete`, fluent queries, real-time change streams.
- **SQLite-style embedding** — link a library into your process and open a database file; there is nothing to install or operate.
- **Durability without sacrifice** — a WAL + tiered-segment storage engine with configurable durability (from full `fsync` per write to group-commit `on-commit` batches) and crash recovery.
- **Encryption at rest** — ChaCha20-Poly1305 encryption of WAL and segment payloads via a master key.
- **Real-time locally** — `watch_collection` streams document changes (`put` / `delete`) to subscribers in-process.
- **Synchronization when you need it** — two optional replication layers:
  - **Net Sync** (`net-sync` feature): peer-to-peer mesh replication over LAN with mDNS discovery.
  - **Cloud Sync** (`cloud-sync` feature): centralized client-server replication over WebSockets + MessagePack.

Because it is a library, HakoDB has no "database server" to manage. Your app *is* the database host. This makes it ideal for local-first and offline-first products, desktop and CLI tooling, edge devices, games, and apps that occasionally need to sync with the cloud or with each other.

---

## When to use HakoDB (sync vs non-sync)

| Scenario | Recommended mode | Why |
|---|---|---|
| Desktop / CLI / local tool needs a real DB with zero setup | **Embedded (no sync)** | In-process, single file, no services. |
| Offline-first mobile/edge/desktop app that syncs to a central backend | **Cloud Sync** (client) | Local reads/writes keep working offline; deltas sync over `ws://`/`wss://`. |
| Central hub collecting writes from many devices | **Cloud Sync** (server) | Thousands of concurrent WebSocket clients, 5ms micro-batched writes, WAL tailer broadcast. |
| Real-time multiplayer / collaborative session on one LAN | **Net Sync** (mesh) | mDNS discovery, room-key isolation, delta replication across peers. |
| Local app that must *also* be reachable by other processes/languages | **Embedded + FFI** | C ABI with Go/JS/Pascal gateways; watch streams for reactive UIs. |
| Analytics / ad-hoc queries over large datasets | **Embedded** | Composite indexes, FTS, aggregates, zero-copy projection, parallel scans. |

**In short:** use HakoDB **without sync** when your data is local to one process. Turn on **Net Sync** when you need peer-to-peer replication across devices on a network you control. Turn on **Cloud Sync** when you need offline-first clients to converge through a central server (or to build a real-time multi-client hub).

---

## Key features

- **Core engine API** — `open`, `put`, `get`, `delete`, `query`, `compact`, `flush`, `write_batch`.
- **Durable storage stack** — shard-oriented segment storage (`segment-l{level}-{id}.dat`) with tiered compaction; WAL with transactional markers and group-commit batching; committed-op recovery replay; index snapshot + WAL rewrite after compaction/recovery.
- **Durability tuning** — `DurabilityMode`: `Always`, `Interval`, `Manual`, `OnCommit`; group-commit control via `group_commit_max_ops`.
- **Encryption at rest** — optional key-based encryption (ChaCha20-Poly1305) for WAL and segment payloads; selectable encrypted collections.
- **Transactions** — `begin_transaction` + staged mutations + `commit`; `begin_serializable_transaction` with conflict-aware commit validation (read/write version checks).
- **Queries** — filters (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `notIn`, `arrayContains`, `arrayContainsAny`, `match`, `contains`, `startsWith`); ordering, limit, offset, cursor bounds (`startAt/startAfter/endAt/endBefore`); cost-aware parallel planner; predicate + projection pushdown; `count/sum/avg` aggregates.
- **Indexing** — single-field secondary B-Tree indexes, composite indexes, inverted (full-text) indexes; async background index building.
- **Zero-copy projection** — queries cherry-pick fields directly from memory-mapped binary slices without inflating full documents.
- **Real-time watch streams** — `watch_collection` with change events; FFI bridge for cross-language reactive UIs.
- **Subcollections** — `put/get/delete/query` on hierarchical nested collections.
- **Security & operations** — collection-prefix allow/deny policy rules; in-memory and file-backed audit logging (`audit.log`).
- **Multi-language FFI** — opaque handle C ABI with document builder, CRUD, query, batch, transaction, watch, result-set and cloud-sync functions; gateways for Go, JavaScript/TypeScript and Pascal.

---

## Quick Start (Rust)

```rust
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = HakoConfig::default();
    cfg.encryption_key = Some("change-me-secret".to_string());

    let db = Hako::open(".hakodb-example", cfg)?;

    let mut doc = HakoDoc::default();
    doc.insert("name", Value::String("alice".to_string()));
    doc.insert("age", Value::Int(30));

    db.put("users", "1", &doc)?;
    db.flush()?;
    Ok(())
}
```

---

## Rust usage

```rust
use hakodb::config::HakoConfig;
use hakodb::engine::{BatchMutation, Hako};

let db = Hako::open("./data", HakoConfig::default())?;

// atomic batch write
db.write_batch(vec![
    BatchMutation::Put {
        collection: "users".into(),
        doc_id: "1".into(),
        doc: /* HakoDoc */ doc1,
    },
    BatchMutation::Delete { collection: "users".into(), doc_id: "2".into() },
])?;

// serializable transaction
let mut tx = db.begin_serializable_transaction();
tx.get(&db, "accounts", "alice")?;
tx.put("accounts", "alice", updated_balance_doc);
let ids = tx.commit(&db)?;

// reactive watch
let rx = db.watch_collection("users");
while let Ok(event) = rx.recv() {
    println!("{:?} {}", event.kind, event.path);
}
```

See [`example/rust/basic`](example/rust/basic) for a complete, working example of the engine API.

> More Rust examples: [`example/rust/basic`](example/rust/basic) — a runnable
> Cargo project (`cargo run`) covering open, CRUD, query, aggregation, batch,
> transaction and the real-time watch channel.

---

## Multi-language platform support (C ABI)

HakoDB exposes a flat C ABI for Node.js/Python/C++/C# and other integration layers. Opaque handle types are defined in `include/hako.h`.

### Build artifacts

- Cargo crate types: `cdylib` (dynamic library consumers) and `rlib` (Rust consumers).
- Auto-generated C header via `build.rs` + `cbindgen.toml`: `include/hako.h`.

```bash
cargo build --release
```

Platform outputs:

- Linux: `target/release/libhakodb.so`
- macOS: `target/release/libhakodb.dylib`
- Windows: `target\release\hakodb.dll`

### Opaque handle types

- `HK_Engine` — main database instance
- `HK_Doc` — document builder / result handle
- `HK_Batch` — atomic write-batch container
- `HK_Query` — query definition builder
- `HK_Config` — advanced configuration builder
- `HK_Watch` — real-time subscription handle
- `HK_Transaction` — serializable transaction handle
- `HK_ResultSet` — query result handle set
- `HK_NetSyncer` — LAN net-sync handle
- `HK_CloudSync` — cloud-sync handle

### C API highlights

- **Engine / memory:** `hk_engine_open`, `hk_engine_open_with_config`, `hk_engine_is_indexes_ready`, `hk_engine_free`, `hk_engine_backup`, `hk_engine_compact`, `hk_engine_list_collections`, `hk_engine_list_indexes`, `hk_engine_get_stats`, `hk_engine_get_audit_log`, `hk_engine_snapshot_indices`, `hk_last_error`, `hk_string_free`.
- **Configuration:** `hk_config_new/free`, `hk_config_set_durability`, `hk_config_set_encryption_key`, `hk_config_set_encrypted_collections`, `hk_config_set_audit_log`, `hk_config_set_query_workers`, `hk_config_set_memory_limits`, `hk_config_set_storage_tuning`, `hk_config_set_blob_threshold`, `hk_config_set_compression`, `hk_config_set_wal_reserve_bytes`.
- **Real-time:** `hk_engine_watch`, `hk_watch_free`.
- **Documents:** `hk_doc_new/free`, `hk_doc_insert_str/int/float/bool/null/bin/timestamp/server_timestamp/doc/array/reference`, `hk_doc_to_json`, `hk_doc_resolve_blobs` (materialize deferred `__blob__` placeholders).
- **CRUD:** `hk_engine_insert`, `hk_engine_insert_take` (owned doc, no clone), `hk_engine_get`, `hk_engine_delete`, `hk_engine_patch`, `hk_engine_get_by_ref`, `hk_engine_insert_subdoc`.
- **Batches:** `hk_batch_new/free`, `hk_batch_set`, `hk_batch_delete`, `hk_batch_commit`.
- **Transactions:** `hk_transaction_begin/get/set/commit/free`.
- **Queries:** `hk_query_new/free`, all `hk_query_where_*` filters, `hk_query_order_by`, `hk_query_limit/offset`, `hk_query_select_field`, `hk_query_defer_blobs`, cursor functions (`start_at/start_after/end_at/end_before`), `hk_query_execute`, `hk_query_execute_to_handles`, `hk_query_delete`, `hk_query_patch`, aggregates (`hk_query_aggregate_count/sum/avg`, `hk_query_execute_aggregation`).
- **Result sets:** `hk_result_set_count/get_doc/free`, `hk_result_set_to_json` (bulk single-call export).
- **Diagnostics:** `hk_debug_write_stats` (write-phase timing breakdown; see `--wstats`).
- **Indexing:** `hk_engine_create_index` (composite JSON), `hk_engine_create_simple_index`, `hk_engine_create_fts_index`.
- **Net Sync:** `hk_net_syncer_new/start/status/free`.
- **Cloud Sync:** `hk_cloud_sync_new/start/status/stop/free`, plus the room-agnostic `hk_cloud_sync_server_new` and the room-bound `hk_cloud_sync_client_new`.

All FFI gateways (Go / JS-TS / Pascal) wrap these APIs.

---

## Net Sync (LAN replication)

The `net-sync` feature provides peer-to-peer replication over the local network:

- mDNS-based peer discovery for local mesh clusters.
- Room-key isolation using SHA-256 room hashing (`Identify` handshake validation).
- Delta replication via WAL operation payloads (`Replication` packets).
- Live node status telemetry (`idle` / `connected` / `syncing`, peer count, known peers).
- Relay/mesh fan-out controls for multi-hop LAN topologies.

Core FFI functions: `hk_net_syncer_new`, `hk_net_syncer_start`, `hk_net_syncer_status`, `hk_net_syncer_free`. All FFI-based gateways include wrappers for these APIs.

### Enable the feature

```toml
[dependencies]
hakodb = { version = "0.7.5", features = ["net-sync"] }
tokio = { version = "1", features = ["full"] }
```

### Android notes

On Android, discovery defaults to UDP subnet broadcast (no `MulticastLock`
needed — broadcast bypasses the WiFi multicast filter). Override with
`with_discovery()` if the app holds a lock and prefers mDNS as well.
Requirements live on the app side: `INTERNET` permission, same WiFi as the
group, and realistic expectations about Doze (the mesh stalls with the screen
off; use a foreground service for always-on sync). Mixed groups: the desktop
side must opt into `Both` — a default-configured desktop never hears
broadcast-only mobile peers. The core library needs no Java glue.

---

## Cloud Sync (centralized replication)

The `cloud-sync` feature provides cloud-level, **bi-directional synchronization** over WebSockets and MessagePack. HakoDB instances can act as a **central cloud server** or as an **offline-first cloud client**.

### Enable the feature

```toml
[dependencies]
hakodb = { version = "0.7.5", features = ["cloud-sync"] }
tokio = { version = "1", features = ["full"] }
```

### Architecture overview

- **Server mode (`CloudSyncMode::Server`)** — central hub handling thousands of concurrent WebSocket connections. An ingress batch flusher coalesces incoming stream mutations into 5ms micro-batches (preventing write-lock bottlenecks); a server outbound WAL tailer captures local server-side writes (CLI, FFI, or `db.put()`) and broadcasts them to all room subscribers.
- **Client mode (`CloudSyncMode::Client`)** — connects to the cloud server over `ws://` or `wss://` (with automatic `https://` → `wss://` conversion). An outbound WAL tailer streams local embedded changes upstream; incoming remote changes are applied locally using LWW (last-write-wins) timestamp filtering.
- **Symmetrical 2-way handshake (`VersionPing`)** — exchanged automatically on connection so that either side (server or client) catches up any deltas missed while offline or restarting.
- **TLS-friendly URLs** — `https://`/`wss://` connect seamlessly through cloud proxies (GitHub Codespaces, Cloudflare Tunnels, AWS ALB, Heroku).
- **High-throughput flusher** — drains and coalesces incoming client mutations every 5ms or 512 ops, reducing HakoDB write-lock acquisitions by ~500x.
- **Anti-echo & deduplication** — self-pruning echo cache plus `msg_id` deduplication prevents infinite loopbacks and stale re-transmissions.

### Rooms and storage layout

A **room** is uniquely identified by the pair `(room_name, room_key)`. Clients
that share a room name but use a different security key are treated as being in
different rooms, so their data is fully isolated on the server.

- `CloudSync::new(db, mode, client_id, room_name, room_key, auth_token)` — the
  `room_name` argument was added in **v0.7.0** (inserted before `room_key`).
  Prefer the dedicated constructors:
  - `CloudSync::server(db, server_id, auth_token)` — a **room-agnostic cloud
    server** ("big cloud server storage"). It is *not* bound to any room: it
    accepts and persists any `(room_name, room_key)` pair its clients ask for.
  - `CloudSync::client(db, client_id, room_name, room_key, auth_token)` — an
    offline-first client that decides which room (and, via `start(server_url)`,
    which server) to sync with. Every client using the same `(room_name,
    room_key)` on the same server forms one sync group.
- The server is **multi-room and room-agnostic**: it accepts any `(room_name,
  room_key)` pair and hosts all of them in one database.
- On the server, every room owns a **storage prefix**:
  - first distinct `(name, key)` for a name → `roomname`
  - each additional distinct key → `roomname_1`, `roomname_2`, …
- Client collections are stored server-side as `<prefix>_<collection>` (e.g. a
  client's `users` collection lands in `game_users`) and are presented back to
  clients as plain `<collection>`, so data never mixes across rooms even when
  clients use identical collection names.
- The room→prefix mapping is kept in the internal, hidden
  `__hako_rooms` collection and is re-read periodically by the server, so
  new rooms are picked up without a restart.
- Peer routing on the server is keyed by `(room, client_id)`, so two clients in
  different rooms may safely reuse the same `client_id` without clobbering each
  other's connections.

### Cloud Sync server example (Rust)

```rust
use std::sync::Arc;
use hakodb::config::HakoConfig;
use hakodb::engine::Hako;
use hakodb::cloud_sync::CloudSync;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Hako::open("./data/cloud_server_db", HakoConfig::default())?);

    // Room-agnostic server: not bound to any room, hosts any (room, key).
    let cloud_server = CloudSync::server(db.clone(), "server_node_01", "master_jwt_secret");

    cloud_server.start("0.0.0.0:8080").await?;
    println!("HakoDB Cloud Sync Server listening on ws://0.0.0.0:8080");

    tokio::signal::ctrl_c().await?;
    cloud_server.stop();
    Ok(())
}
```

### Cloud Sync client example (Rust)

```rust
use std::sync::Arc;
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::cloud_sync::CloudSync;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Hako::open("./data/client_db", HakoConfig::default())?);

    // The client picks its room (room_name + room_key) and the server URL.
    let cloud_client = CloudSync::client(
        db.clone(),
        "user_client_42",          // Client ID
        "game",                    // Room name (must match other clients of the room)
        "secret_game_room_key",    // Must match the room key
        "user_jwt_token_123",      // Authentication Token
    );

    cloud_client.start("wss://my-cloud-server.app.dev").await?;
    println!("Connected to Cloud Sync Server");

    // Local writes are synced to the server in the background.
    let mut doc = HakoDoc::default();
    doc.insert("username", Value::String("player_one".to_string()));
    doc.insert("score", Value::Int(9500));
    db.put("players", "user_42", &doc)?;

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    cloud_client.stop();
    Ok(())
}
```

---

## Sync encryption posture (read this before encrypting)

Encryption at rest (WAL/segments, via `encryption_key` + `encrypted_cols`)
and sync-time plaintext are **independent properties**. The sync tailers
read through the storage decryption layer and emit decoded documents, so
an encrypted collection replicates as **plaintext on the wire** unless the
rules below refuse the transfer. There is deliberately no silent path.

### What is (and isn't) protected

| Threat | Status |
|---|---|
| Disk / backup theft on any node | Protected when the node holds the key (at-rest encryption). |
| Keyless or wrong-key peer receiving your encrypted docs | **Refused, loudly.** Both directions enforce: senders skip the room per peer, receivers drop the batch. |
| Old (pre-capability) peer in the room | Treated as unverified: encrypted rooms pause for it (it keeps syncing plaintext rooms). Upgrade the peer to resume. |
| Cloud server operator / DB thief | **Not protected.** The hub sees whatever clients send (and needs `room_key` in the clear to route). If the operator must not read a room, that room needs end-to-end encryption (future work), not at-rest keys. |
| Passive LAN observer (mesh) | **Not protected.** Packets between verified key-holders are still plaintext. Use mTLS/`wss://` segments or E2E rooms for observer resistance. |
| Active impersonator replaying a fingerprint | **Not protected.** Fingerprints are assertions, not proofs (no challenge-response yet). Mitigated by group admission + room keys, not eliminated. |

Admission control (groups, API keys, room keys) is **not** confidentiality:
it decides *who may join*, these rules decide *what may leave*.

### How it works

Every sync handshake now carries encryption capabilities alongside auth:

- **Mesh**: a `SyncCaps` packet (`key_fp` = SHA-256 of the at-rest secret,
  plus the node's encrypted collection list) right after `Identify`.
  Appended as the last enum variant, so old peers fail the decode and
  skip it silently — mixed-version meshes stay connected.
- **Cloud**: `enc_fp` / `enc_cols` fields on `Authenticate` (serde
  defaults; old clients omit them and parse fine both directions).

The decision (`caps_allow`, in `src/sync_guard.rs`) is one rule everywhere:
a locally-encrypted collection flows to/from a peer **iff** that peer
presented the same non-zero fingerprint. Everything else — old peers,
keyless peers, wrong-key peers — is refused per collection with a
throttled loud warning naming peer, collection, and key prefix
(`[sync-guard] ...`), plus an info line when caps arrive. Plaintext
collections behave byte-identically to before (zero behavior delta when
nothing is encrypted).

Enforcement points: mesh tailer (per-peer fan-out), bootstrap, delta
sender, inbound apply, and mesh relay (origin-aware: an encrypted room's
bytes are forwarded only to fingerprint-matched peers, original bytes
untouched); cloud relay fan-out, server apply, server tailer, and both
catch-up senders. The cloud **client trusts its configured hub** (it
sends everything upstream; the server enforces on receipt and relay) —
otherwise encrypted rooms could never use the standard keyless hub.
Catch-up and version-driven deltas self-heal anything skipped while a
peer was unknown: skipped ops stay behind the receiver's version vector
and are re-sent on the next exchange (including the re-`Ping` triggered
by every caps announcement).

### Operating it

1. Put the **same** `encryption_key` on every node that must share the
   room; list the room in `encrypted_cols` (or leave the list empty for
   global encryption).
2. On the cloud server, configure `encrypted_cols` with **storage** names
   (`<prefix>_<collection>`); clients use plain names. Either side
   matching counts.
3. Expect `[sync-guard]` warnings while any peer is keyless, wrong-keyed,
   or old — fix by distributing the key / upgrading, not by relaxing.
4. There is intentionally **no override flag**: a downgrade switch would
   reintroduce the exact silent leak this removes. Mixed-version rooms
   keep working for plaintext collections throughout the upgrade.

---

## Repositories

HakoDB lives under the [`hakodb`](https://github.com/hakodb) organization
— one repo per deliverable, each versioned independently:

| Repo | Delivers | Version |
|---|---|---|
| [`hakodb/hakodb`](https://github.com/hakodb/hakodb) | Core library: engine, storage, query, FFI (`hako.h`), net/cloud sync | 0.8.21 |
| [`hakodb/hako-cli`](https://github.com/hakodb/hako-cli) | Command-line manager + serve REPL | 0.2.1 |
| [`hakodb/hako-cloudserver`](https://github.com/hakodb/hako-cloudserver) | Managed sync hub + admin console | 0.1.1 |
| [`hakodb/hako-tauri`](https://github.com/hakodb/hako-tauri) | Tauri gateway crate (Rust) | 0.1.1 |
| [`hakodb/hako-tauri-ts`](https://github.com/hakodb/hako-tauri-ts) | Tauri client (`@hakodb/tauri`) | 0.1.1 |
| [`hakodb/hako-bench`](https://github.com/hakodb/hako-bench) | C++ benchmark harnesses + SQLite duel | 0.1.1 |
| [`hakodb/hako-go`](https://github.com/hakodb/hako-go) | Go SDK (cgo) | 0.1.1 |
| [`hakodb/hako-js`](https://github.com/hakodb/hako-js) | JS/TS SDK (`@hakodb/client`, Node + Bun) | 0.5.12 |
| [`hakodb/hako-pascal`](https://github.com/hakodb/hako-pascal) | Lazarus/FPC wrapper + components | 0.1.1 |

Branches: **`main`** (stable — merged releases only) and **`cloud_sync`**
(active development). The pre-rebrand history remains archived, read-only,
at [`rizaptk/firelite`](https://github.com/rizaptk/firelite) for existing
consumers (v0.8.19 and below).

---

## Architecture

```text
API (HakoDB + FFI + SDKs)
  -> Query (Parallel sharded executor + projection pushdown)
    -> Index (Async background manager + lock-free metadata)
      -> Storage (WAL Inlining + RAM-to-Disk Checkpointing + Tiered Segments)
        -> Hardware (Positional I/O + Atomic Sync Coordination)
```

---

## Implementation status

### Performance & durability

| Area | Status | Technical detail |
|---|---|---|
| **Write performance** | Elite | WAL inlining (small docs bypass segments); single hardware flush per write. |
| **Read performance** | Elite | Zero-lock positional reads allow many parallel readers without contention. |
| **Indexing** | Async | B-Tree updates offloaded to a background worker thread to keep main-thread latency low. |
| **Memory management** | Adaptive | Inlined RAM data spills to tiered segments when thresholds are met. |
| **Concurrency** | Atomic | Atomic versioning and async auditing remove global mutex bottlenecks. |
| **Durability** | ACID | `OnCommit` mode mirrors Firestore `batch.commit()` semantics. |
| **Encryption** | Optimized | ChaCha20-Poly1305 with thread-local RNG for high-frequency encrypted I/O. |
| **Real-time** | Threaded | FFI-compatible background listener bridge with `user_data` context passing. |

### Feature status vs Firestore-style target

| Area | HakoDB status | Notes |
|---|---|---|
| Embedded engine | Implemented | High-concurrency Rust runtime with FFI bridge |
| Durable WAL + recovery | Implemented | Fully encrypted WAL with committed-op filter and crash recovery |
| Encryption at rest | Implemented | ChaCha20-Poly1305 on both WAL and segment layers |
| Atomic batches | Implemented | Single-I/O memory buffer writes for maximum throughput |
| Transactions | Implemented | Serializable snapshot isolation with conflict detection |
| Composite indexes | Implemented | Background-updated B-Trees with equality scan support |
| Query engine | Implemented | Cost-aware planner + predicate pushdown doc-view filter |
| Zero-copy pipeline | Implemented | Direct field projection from binary views across all FFI layers |
| Real-time listeners | Implemented | Non-blocking cross-language callback architecture |
| Subcollections | Implemented | Prefix-based hierarchical document nesting |
| Multi-platform FFI | Implemented | Windows (`.dll`), Linux (`.so`), macOS (`.dylib`) |
| Compaction | Implemented | Tiered LSM-style background merging + memory checkpointing |
| Parity (Cloud) | Non-goal | No remote authentication or globally distributed state |

### Module map

| Module | Path | Status |
|---|---|---|
| Engine API | `src/engine/*` | Implemented |
| Storage + crypto | `src/storage/*` | Implemented |
| Indexing | `src/index/*` | Implemented |
| Query planner/executor | `src/query/*` | Implemented |
| Document model | `src/document/*` | Implemented |
| C-FFI | `src/ffi.rs`, `include/hako.h` | Implemented |

---

## Contribution notes

- Keep module boundaries aligned with `STRUCTURE.md`.
- Add recovery tests when touching storage/WAL/indexing.
- Document binary format or compatibility-impacting changes.
- Keep C ABI additions reflected in cbindgen config + the generated header.

---

## License

TBD

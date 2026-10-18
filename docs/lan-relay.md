# LAN relay for cloud sync (issue #72, phase 1)

One active cloud client per LAN per room, instead of N duplicate hub
connections. This document covers the phase-1 carriers and the manual
mode operators can deploy today; the lease-based auto leader is phase 2.

## Manual relay (mode 1, deployable today)

One node per LAN runs both planes:

- cloud tailer -> hub room (`CloudSync::client`)
- mesh server -> LAN peers (`NetSyncer`)

Other LAN peers open the same room mesh-only; they never dial the hub.
Mesh-absorbed writes become part of the relay's WAL and reach the cloud
through its normal push. Zero code, one config decision. Downside: if
the relay is down, the LAN has no cloud path until an operator flips
config.

## Protocol carriers (additive, mixed-version safe)

Two new trailing `NetPacket` variants (mesh plane, bincode — appended
after every pre-existing variant, so old peers fail-decode and silently
skip them, the same rule `SyncCaps` established):

- `CloudLease { client_id, last_exchange_ts }` — the relay's cloud
  freshness (ms-epoch of its last successful cloud round-trip).
  Announced at the same cadence as the mesh ping.
- `RelayTake { ts, client_id }` — sent the moment a peer takes the
  relay role; observers converge fast without waiting for the next
  lease refresh.

NOTE: the issue text sketched the lease as a field inside `Ping`. Under
bincode that breaks old peers decoding the `Ping` variant itself (all
version exchange would die in a mixed mesh). Separate trailing variants
keep the compat property the design actually needs.

## Admission control (hub side)

`CloudSync::set_max_clients_per_room(n)` — hard cap on distinct
connected clients per room (0 = unlimited, historic default).

- A client reconnecting with its own id always re-admits (replaces its
  stale entry; the cap counts distinct clients, not retries).
- A new client beyond the cap gets the typed loud
  `AuthResult { success: false, error: "room at capacity" }` and retries
  with backoff. Mesh-only LAN peers are unaffected: they never dial.
- Runtime-tunable on Arc'd handles (FFI/SDK included).

## Lag meter

`hakodb::cloud_sync::cloud_last_exchange_millis() -> Option<i64>` —
ms-epoch of the last successful cloud exchange on any client face in
this process (auth round-trip or any decoded server packet). `None` =
never reached the cloud. Observers receive the relay's value via
`CloudLease`; alert on `now - value` exceeding your freshness budget.

## Phase 2 (not yet implemented)

Lease-based auto leader: peers connect to the cloud only after
`now - last_known_exchange > GRACE`, spread by deterministic
`hash(client_id) % J` jitter, announce `RelayTake`, and step down when a
steady lease appears. Requires runtime start/stop of the cloud client
without an engine restart. Overlapping leaders inside the GRACE window
are allowed by design and self-resolve.

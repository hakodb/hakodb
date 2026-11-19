#![doc = include_str!("../README.md")]
// Public API keeps rustdoc coverage. Internal plumbing modules are hidden
// from the rendered docs (and from this lint) rather than padded with prose.
#![warn(missing_docs)]

/// Cancellation token for long-running scans and maintenance.
#[allow(missing_docs)]
pub mod cancel;
/// Engine configuration: durability, encryption, audit, and memory limits.
/// Field-level defaults are documented in `HakoConfig` and the manual.
#[allow(missing_docs)]
pub mod config;
/// Typed documents and values, including references and blobs.
#[allow(missing_docs)]
pub mod document;
/// The embedded engine handle and its write/read APIs. Advanced maintenance
/// helpers are documented in the mdBook manual, not repeated per method here.
#[allow(missing_docs)]
pub mod engine;
/// Error types and the FFI-facing error classification.
#[allow(missing_docs)]
pub mod error;
/// Flat C ABI. Symbols are generated from this module for non-Rust clients;
/// parameter layouts are documented in `hakodb.h`, not repeated here.
#[allow(missing_docs)]
pub mod ffi;
/// Secondary, composite, full-text, vector, and traversal indexes.
#[allow(missing_docs)]
pub mod index;
/// In-memory storage engine details (internal).
#[doc(hidden)]
#[allow(missing_docs)]
pub mod memory;
/// Query builder and planner.
#[allow(missing_docs)]
pub mod query;
/// On-disk storage layout and WAL (internal).
#[doc(hidden)]
#[allow(missing_docs)]
pub mod storage;
/// Transport-independent sync primitives (internal).
#[doc(hidden)]
#[allow(missing_docs)]
pub mod sync_core;
/// Fail-closed handshake guards for encrypted collections (internal).
#[doc(hidden)]
#[allow(missing_docs)]
pub mod sync_guard;

#[cfg(any(feature = "net-sync", feature = "socket-sync"))]
/// Length-prefixed framing for sync transports (internal).
#[doc(hidden)]
#[allow(missing_docs)]
pub mod sync_frame;

/// General-purpose helpers (internal).
#[doc(hidden)]
#[allow(missing_docs)]
pub mod util;

#[cfg(feature = "net-sync")]
/// LAN mesh sync (mDNS discovery, rooms, echo suppression).
#[allow(missing_docs)]
pub mod net_sync;

#[cfg(feature = "cloud-sync")]
/// Cloud hub sync (client/server rooms, version vectors).
#[allow(missing_docs)]
pub mod cloud_sync;

#[cfg(all(unix, feature = "socket-sync"))]
/// Co-located instance sync over unix sockets.
#[allow(missing_docs)]
pub mod socket_sync;

#[cfg(feature = "cloud-sync")]
pub use cloud_sync::{CloudPacket, CloudStatus, CloudStatusRedacted, CloudSync, CloudSyncMode};

pub use cancel::CancelToken;
/// The embedded database handle.
pub use engine::Hako;
/// Result of [`Hako::await_quiescent`] polling.
pub use engine::QuiescenceStatus;
/// Per-collection schema validation.
#[allow(missing_docs)]
pub mod schema;

#![doc = include_str!("../README.md")]

/// Cancellation token for long-running scans and maintenance.
pub mod cancel;
/// Engine configuration: durability, encryption, audit, and memory limits.
pub mod config;
/// Typed documents and values, including references and blobs.
pub mod document;
/// The embedded engine handle and its write/read APIs.
pub mod engine;
/// Error types and the FFI-facing error classification.
pub mod error;
/// Flat C ABI. Symbols are generated from this module for non-Rust clients.
pub mod ffi;
/// Secondary, composite, full-text, vector, and traversal indexes.
pub mod index;
/// In-memory storage engine details.
pub mod memory;
/// Query builder and planner.
pub mod query;
/// On-disk storage layout and WAL.
pub mod storage;
/// Transport-independent sync primitives.
pub mod sync_core;
/// Fail-closed handshake guards for encrypted collections.
pub mod sync_guard;

#[cfg(any(feature = "net-sync", feature = "socket-sync"))]
/// Length-prefixed framing for sync transports.
pub mod sync_frame;

/// General-purpose helpers.
pub mod util;

#[cfg(feature = "net-sync")]
/// LAN mesh sync (mDNS discovery, rooms, echo suppression).
pub mod net_sync;

#[cfg(feature = "cloud-sync")]
/// Cloud hub sync (client/server rooms, version vectors).
pub mod cloud_sync;

#[cfg(all(unix, feature = "socket-sync"))]
/// Co-located instance sync over unix sockets.
pub mod socket_sync;

#[cfg(feature = "cloud-sync")]
pub use cloud_sync::{CloudPacket, CloudStatus, CloudStatusRedacted, CloudSync, CloudSyncMode};

pub use cancel::CancelToken;
/// The embedded database handle.
pub use engine::Hako;
/// Result of [`Hako::await_quiescent`] polling.
pub use engine::QuiescenceStatus;
/// Per-collection schema validation.
pub mod schema;

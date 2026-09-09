pub mod config;
pub mod document;
pub mod engine;
pub mod error;
pub mod ffi;
pub mod index;
pub mod memory;
pub mod query;
pub mod storage;
pub mod sync_guard;

#[cfg(feature = "tauri-gateway")]
pub mod tauri_gateway;
pub mod util;

#[cfg(feature = "net-sync")]
pub mod net_sync; 

#[cfg(feature = "cloud-sync")]
pub mod cloud_sync;

#[cfg(feature = "cloud-sync")]
pub use cloud_sync::{CloudPacket, CloudStatus, CloudSync, CloudSyncMode};

pub use engine::FireLite;

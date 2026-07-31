pub mod config;
pub mod document;
pub mod engine;
pub mod error;
pub mod ffi;
pub mod index;
pub mod memory;
pub mod query;
pub mod storage;
#[cfg(feature = "tauri-gateway")]
pub mod tauri_gateway;
pub mod util;
#[cfg(feature = "net-sync")]
pub mod net_sync; 
#[cfg(feature = "cloud-sync")]
pub mod cloud_sync;

// Re-exports
pub use config::EngineConfig;
pub use document::FireLiteDoc;
pub use engine::Engine;
pub use error::{Error, Result};

#[cfg(feature = "cloud-sync")]
pub use cloud_sync::{
    auth::AuthClient,
    broadcaster::SyncBroadcaster,
    client::{CloudClient, CloudSyncConfig},
};

pub use engine::FireLite;

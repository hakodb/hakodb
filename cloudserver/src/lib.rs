//! firelite-cloudserver library: shared app surface (config, routes, state).
//! The binary (`main.rs`) is a thin CLI wrapper so integration tests and
//! future consumers can drive the app directly.

pub mod app;
pub mod auth;
pub mod config;
pub mod data;
pub mod events;
pub mod groups;
pub mod users;

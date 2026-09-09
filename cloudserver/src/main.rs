//! firelite-cloudserver: standalone cloud-sync server + admin console.
//!
//! Phase 1: crate skeleton, layered config, logging, health endpoint.
//! Later phases add auth/users/groups, the sync plane, data-plane admin
//! APIs, SSE realtime, the embedded UI, TLS and service integration.

use clap::Parser;
use firelite::config::FireLiteConfig;
use firelite::engine::FireLite;
use firelite_cloudserver::app::{build_router, AppState};
use firelite_cloudserver::config::{load_config, ConfigLayer};
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(name = "firelite-cloudserver", about = "Standalone FireLite cloud-sync server")]
struct Cli {
    /// Config file (TOML). Defaults to ./firelite-cloud.toml when present.
    #[arg(long)]
    config: Option<String>,
    /// Database directory.
    #[arg(long)]
    db_path: Option<String>,
    /// Admin HTTP bind address.
    #[arg(long)]
    admin_bind: Option<String>,
    /// Sync WebSocket bind address (wired up in a later phase).
    #[arg(long)]
    sync_bind: Option<String>,
    /// Log level (error|warn|info|debug|trace).
    #[arg(long)]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let env_vars: HashMap<String, String> = std::env::vars().collect();
    let cfg = load_config(
        cli.config.as_deref(),
        ConfigLayer {
            db_path: cli.db_path,
            admin_bind: cli.admin_bind,
            sync_bind: cli.sync_bind,
            log_level: cli.log_level,
        },
        &env_vars,
    )?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&cfg.log_level)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db = FireLite::open(&cfg.db_path, FireLiteConfig::default())
        .map_err(|e| format!("open db {}: {e}", cfg.db_path))?;
    tracing::info!(db_path = %cfg.db_path, admin_bind = %cfg.admin_bind, sync_bind = %cfg.sync_bind, "firelite-cloudserver starting (sync plane arrives in a later phase)");

    let state = std::sync::Arc::new(AppState::new(db));
    let listener = tokio::net::TcpListener::bind(&cfg.admin_bind)
        .await
        .map_err(|e| format!("bind {}: {e}", cfg.admin_bind))?;
    axum::serve(listener, build_router(state))
        .await
        .map_err(|e| format!("serve: {e}"))?;
    Ok(())
}

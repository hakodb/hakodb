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
    /// Emit Secure on session cookies (enable with TLS).
    #[arg(long)]
    secure_cookies: bool,
    /// Sync-plane server id shown to peers.
    #[arg(long)]
    server_id: Option<String>,
    /// Sync-plane shared token presented by clients.
    #[arg(long)]
    sync_token: Option<String>,
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
            secure_cookies: cli.secure_cookies.then_some(true),
            server_id: cli.server_id,
            sync_token: cli.sync_token,
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
    tracing::info!(db_path = %cfg.db_path, admin_bind = %cfg.admin_bind, sync_bind = %cfg.sync_bind, server_id = %cfg.server_id, "firelite-cloudserver starting");

    // Sync plane: the shared room-agnostic server. Group admission policy
    // (__groups) is enforced inside the handshake; see cloud_sync.
    let db = std::sync::Arc::new(db);
    let sync = firelite::cloud_sync::CloudSync::server(
        db.clone(),
        &cfg.server_id,
        &cfg.sync_token,
    );
    let sync_bind = cfg.sync_bind.clone();
    let sync_task = tokio::spawn(async move {
        sync.start(&sync_bind)
            .await
            .map_err(|e| format!("sync serve {sync_bind}: {e}"))
    });

    let state = std::sync::Arc::new(AppState::new(db, cfg.secure_cookies));
    let listener = tokio::net::TcpListener::bind(&cfg.admin_bind)
        .await
        .map_err(|e| format!("bind {}: {e}", cfg.admin_bind))?;
    let admin_task = tokio::spawn(async move {
        axum::serve(
            listener,
            build_router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .map_err(|e| format!("admin serve: {e}"))
    });
    // Either plane dying takes the process down (fail-fast: the service
    // manager restarts us clean rather than half-serving).
    match tokio::join!(admin_task, sync_task) {
        (Ok(Ok(())), Ok(Ok(()))) => Ok(()),
        (Ok(Err(e)), _) | (_, Ok(Err(e))) => Err(e),
        (Err(e), _) | (_, Err(e)) => Err(format!("task panicked: {e}")),
    }
}

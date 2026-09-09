//! Server runner shared by console mode and the Windows Service entry.
//!
//! `run(cfg, shutdown)` boots both planes and resolves when either plane
//! fails, or when `shutdown` fires — in which case the sync plane is
//! stopped and `Ok` is returned for a clean exit.

use std::future::Future;

use crate::app::{build_router, AppState};
use crate::config::ServerConfig;
use firelite::config::FireLiteConfig;
use firelite::engine::FireLite;

pub async fn run(cfg: ServerConfig, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    // try_init: safe to call from console and service paths alike (second
    // call is a no-op error, not a panic).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&cfg.log_level)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
    if let Some(e) = cfg.tls_error() {
        return Err(e);
    }
    let tls = cfg.tls_enabled();
    if (cfg.secure_cookies || tls) && !cfg.secure_cookies {
        tracing::info!("TLS active: session cookies forced Secure");
    }
    // Loud warning for the dangerous combination: world-reachable console
    // without transport encryption. Default bind stays loopback.
    let bind_warn = match cfg.admin_bind.parse::<std::net::SocketAddr>() {
        Ok(a) => !a.ip().is_loopback(),
        Err(_) => true,
    };
    if bind_warn && !tls {
        tracing::warn!(
            admin_bind = %cfg.admin_bind,
            "admin console bound to a non-loopback address WITHOUT TLS — credentials cross the network in cleartext; use --tls-cert/--tls-key or a reverse proxy"
        );
    }

    let db = FireLite::open(&cfg.db_path, FireLiteConfig::default())
        .map_err(|e| format!("open db {}: {e}", cfg.db_path))?;
    tracing::info!(db_path = %cfg.db_path, admin_bind = %cfg.admin_bind, sync_bind = %cfg.sync_bind, server_id = %cfg.server_id, "firelite-cloudserver starting");

    let db = std::sync::Arc::new(db);
    let sync = std::sync::Arc::new(firelite::cloud_sync::CloudSync::server(
        db.clone(),
        &cfg.server_id,
        &cfg.sync_token,
    ));
    let sync_bind = cfg.sync_bind.clone();
    let sync_task = tokio::spawn({
        let sync = sync.clone();
        async move {
            sync.start(&sync_bind)
                .await
                .map_err(|e| format!("sync serve {sync_bind}: {e}"))
        }
    });

    let state = std::sync::Arc::new(
        AppState::new(db, cfg.secure_cookies || cfg.tls_enabled())
            .with_sync(sync.clone())
            .with_config(cfg.clone()),
    );
    let app =
        build_router(state).into_make_service_with_connect_info::<std::net::SocketAddr>();

    let admin_task = tokio::spawn(async move {
        if cfg.tls_enabled() {
            let cert = cfg.tls_cert.clone().expect("checked");
            let key = cfg.tls_key.clone().expect("checked");
            let rustls_cfg =
                axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                    .await
                    .map_err(|e| format!("load TLS cert/key: {e}"))?;
            let addr: std::net::SocketAddr = cfg
                .admin_bind
                .parse()
                .map_err(|e| format!("parse admin bind {}: {e}", cfg.admin_bind))?;
            axum_server::bind_rustls(addr, rustls_cfg)
                .serve(app)
                .await
                .map_err(|e| format!("admin serve (tls): {e}"))
        } else {
            let listener = tokio::net::TcpListener::bind(&cfg.admin_bind)
                .await
                .map_err(|e| format!("bind {}: {e}", cfg.admin_bind))?;
            axum::serve(listener, app)
                .await
                .map_err(|e| format!("admin serve: {e}"))
        }
    });

    tokio::select! {
        // Either plane dying takes the process down (fail-fast: the service
        // manager restarts us clean rather than half-serving).
        joined = async { tokio::join!(admin_task, sync_task) } => {
            match joined {
                (Ok(Ok(())), Ok(Ok(()))) => Ok(()),
                (Ok(Err(e)), _) | (_, Ok(Err(e))) => Err(e),
                (Err(e), _) | (_, Err(e)) => Err(format!("task panicked: {e}")),
            }
        }
        _ = shutdown => {
            sync.stop();
            Ok(())
        }
    }
}

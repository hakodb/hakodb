//! Command-line surface (shared by console mode and the Windows Service
//! entry, which re-parses its baked argv).

use clap::Parser;
use std::collections::HashMap;

use crate::config::{load_config, ConfigLayer, ServerConfig};

#[derive(Parser, Debug)]
#[command(name = "firelite-cloudserver", about = "Standalone FireLite cloud-sync server")]
pub struct Cli {
    /// Config file (TOML). Defaults to ./firelite-cloud.toml when present.
    #[arg(long)]
    pub config: Option<String>,
    /// Database directory.
    #[arg(long)]
    pub db_path: Option<String>,
    /// Admin HTTP bind address.
    #[arg(long)]
    pub admin_bind: Option<String>,
    /// Sync WebSocket bind address.
    #[arg(long)]
    pub sync_bind: Option<String>,
    /// Log level (error|warn|info|debug|trace).
    #[arg(long)]
    pub log_level: Option<String>,
    /// Emit Secure on session cookies (enable with TLS).
    #[arg(long)]
    pub secure_cookies: bool,
    /// Sync-plane server id shown to peers.
    #[arg(long)]
    pub server_id: Option<String>,
    /// Sync-plane shared token presented by clients.
    #[arg(long)]
    pub sync_token: Option<String>,
    /// TLS certificate PEM for the admin plane (requires --tls-key).
    #[arg(long)]
    pub tls_cert: Option<String>,
    /// TLS private key PEM for the admin plane (requires --tls-cert).
    #[arg(long)]
    pub tls_key: Option<String>,
    /// Install as a Windows Service and exit (requires --db-path absolute).
    #[cfg(windows)]
    #[arg(long)]
    pub install_service: bool,
    /// Remove the Windows Service and exit.
    #[cfg(windows)]
    #[arg(long)]
    pub uninstall_service: bool,
    /// Run as the service image (set automatically on install; not for
    /// interactive use).
    #[cfg(windows)]
    #[arg(long)]
    pub run_service: bool,
    /// Windows Service name.
    #[cfg(windows)]
    #[arg(long, default_value = "firelite-cloudserver")]
    pub service_name: String,
}

pub fn load_cfg(cli: &Cli) -> Result<ServerConfig, String> {
    let env_vars: HashMap<String, String> = std::env::vars().collect();
    load_config(
        cli.config.as_deref(),
        ConfigLayer {
            db_path: cli.db_path.clone(),
            admin_bind: cli.admin_bind.clone(),
            sync_bind: cli.sync_bind.clone(),
            log_level: cli.log_level.clone(),
            secure_cookies: cli.secure_cookies.then_some(true),
            server_id: cli.server_id.clone(),
            sync_token: cli.sync_token.clone(),
            tls_cert: cli.tls_cert.clone(),
            tls_key: cli.tls_key.clone(),
        },
        &env_vars,
    )
}

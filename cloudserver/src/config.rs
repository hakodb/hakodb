//! Layered server configuration: defaults < TOML file < `FL_*` env < CLI flags.
//!
//! The env source is injected as a map (rather than read from the process)
//! so precedence is unit-testable without racy `set_var` calls.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const DEFAULT_DB_PATH: &str = "./firelite-cloud.db";
pub const DEFAULT_ADMIN_BIND: &str = "127.0.0.1:8081";
pub const DEFAULT_SYNC_BIND: &str = "0.0.0.0:8080";
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Resolved, fully-defaulted configuration the server runs with.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub db_path: String,
    pub admin_bind: String,
    pub sync_bind: String,
    pub log_level: String,
    /// Emit `Secure` on session cookies. Enable with TLS (phase 7);
    /// until then the loopback default bind would only break logins.
    pub secure_cookies: bool,
}

/// Partial file/env/flag layer. Every field optional; `None` inherits.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigLayer {
    pub db_path: Option<String>,
    pub admin_bind: Option<String>,
    pub sync_bind: Option<String>,
    pub log_level: Option<String>,
    pub secure_cookies: Option<bool>,
}

impl ConfigLayer {
    /// Overlay `over` on top of `self`; set fields win.
    fn merged(mut self, over: ConfigLayer) -> Self {
        if over.db_path.is_some() {
            self.db_path = over.db_path;
        }
        if over.admin_bind.is_some() {
            self.admin_bind = over.admin_bind;
        }
        if over.sync_bind.is_some() {
            self.sync_bind = over.sync_bind;
        }
        if over.log_level.is_some() {
            self.log_level = over.log_level;
        }
        if over.secure_cookies.is_some() {
            self.secure_cookies = over.secure_cookies;
        }
        self
    }

    fn resolve(self) -> ServerConfig {
        ServerConfig {
            db_path: self.db_path.unwrap_or_else(|| DEFAULT_DB_PATH.into()),
            admin_bind: self
                .admin_bind
                .unwrap_or_else(|| DEFAULT_ADMIN_BIND.into()),
            sync_bind: self.sync_bind.unwrap_or_else(|| DEFAULT_SYNC_BIND.into()),
            log_level: self.log_level.unwrap_or_else(|| DEFAULT_LOG_LEVEL.into()),
            secure_cookies: self.secure_cookies.unwrap_or(false),
        }
    }
}

/// `FL_*` environment layer (`FL_DB_PATH`, `FL_ADMIN_BIND`, `FL_SYNC_BIND`,
/// `FL_LOG_LEVEL`, `FL_SECURE_COOKIES=1`). Only non-empty values count.
fn env_layer(vars: &HashMap<String, String>) -> ConfigLayer {
    let get = |k: &str| {
        vars.get(k)
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.trim().to_string())
    };
    ConfigLayer {
        db_path: get("FL_DB_PATH"),
        admin_bind: get("FL_ADMIN_BIND"),
        sync_bind: get("FL_SYNC_BIND"),
        log_level: get("FL_LOG_LEVEL"),
        secure_cookies: get("FL_SECURE_COOKIES").map(|v| v == "1" || v.eq_ignore_ascii_case("true")),
    }
}

/// Resolve final config. `config_path`: explicit `--config`, else
/// `./firelite-cloud.toml` when present, else no file layer.
pub fn load_config(
    config_path: Option<&str>,
    cli: ConfigLayer,
    vars: &HashMap<String, String>,
) -> Result<ServerConfig, String> {
    let mut base = ConfigLayer::default();
    let path = match config_path {
        Some(p) => Some(p.to_string()),
        None if std::path::Path::new("./firelite-cloud.toml").exists() => {
            Some("./firelite-cloud.toml".to_string())
        }
        None => None,
    };
    if let Some(p) = path {
        let text =
            std::fs::read_to_string(&p).map_err(|e| format!("read config {p}: {e}"))?;
        let file: ConfigLayer =
            toml::from_str(&text).map_err(|e| format!("parse config {p}: {e}"))?;
        base = base.merged(file);
    }
    Ok(base.merged(env_layer(vars)).merged(cli).resolve())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn defaults_when_nothing_set() {
        let cfg = load_config(None, ConfigLayer::default(), &vars(&[])).unwrap();
        assert_eq!(cfg.db_path, DEFAULT_DB_PATH);
        assert_eq!(cfg.admin_bind, DEFAULT_ADMIN_BIND);
        assert_eq!(cfg.sync_bind, DEFAULT_SYNC_BIND);
        assert_eq!(cfg.log_level, DEFAULT_LOG_LEVEL);
    }

    #[test]
    fn cli_beats_env_beats_file() {
        let dir = std::env::temp_dir().join("fl-cfg-precedence.toml");
        std::fs::write(
            &dir,
            "db_path = \"/file.db\"\nadmin_bind = \"1.1.1.1:1\"\nsync_bind = \"2.2.2.2:2\"\nlog_level = \"debug\"\n",
        )
        .unwrap();
        let env = vars(&[("FL_DB_PATH", "/env.db"), ("FL_ADMIN_BIND", "3.3.3.3:3")]);
        let cli = ConfigLayer {
            admin_bind: Some("4.4.4.4:4".into()),
            ..Default::default()
        };
        let cfg = load_config(Some(dir.to_str().unwrap()), cli, &env).unwrap();
        assert_eq!(cfg.db_path, "/env.db"); // env over file
        assert_eq!(cfg.admin_bind, "4.4.4.4:4"); // cli over env
        assert_eq!(cfg.sync_bind, "2.2.2.2:2"); // file survives where nothing overrides
        assert_eq!(cfg.log_level, "debug");
        std::fs::remove_file(&dir).ok();
    }

    #[test]
    fn empty_env_values_ignored_and_missing_file_errors() {
        let env = vars(&[("FL_DB_PATH", "   ")]);
        let cfg = load_config(None, ConfigLayer::default(), &env).unwrap();
        assert_eq!(cfg.db_path, DEFAULT_DB_PATH);
        assert!(load_config(
            Some("/no/such/file.toml"),
            ConfigLayer::default(),
            &vars(&[])
        )
        .is_err());
    }

    #[test]
    fn malformed_toml_errors() {
        let dir = std::env::temp_dir().join("fl-cfg-bad.toml");
        std::fs::write(&dir, "db_path = [unclosed").unwrap();
        assert!(load_config(
            Some(dir.to_str().unwrap()),
            ConfigLayer::default(),
            &vars(&[])
        )
        .is_err());
        std::fs::remove_file(&dir).ok();
    }
}

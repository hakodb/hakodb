//! Windows Service integration (Windows only).
//!
//! Installs `firelite-cloudserver` as an SCM-managed service so it runs at
//! boot without a console session:
//!   firelite-cloudserver --db-path C:\data\fl.db --install-service [--service-name NAME]
//!   firelite-cloudserver --uninstall-service [--service-name NAME]
//! The installed image runs `... --run-service` with the same operational
//! flags baked in (services start in System32, so all paths must be
//! absolute — enforced at install). Stop/Shutdown from the SCM drains via
//! the shared run loop and exits 0.

#[cfg(windows)]
pub mod imp {
    use std::ffi::OsString;
    use std::sync::atomic::{AtomicBool, Ordering};
    use windows_service::{
        define_windows_service,
        service::{
            ServiceAccess, ServiceControl, ServiceDependency, ServiceErrorControl, ServiceInfo,
            ServiceStartType, ServiceType,
        },
        service_control_handler::{self, ServiceControlHandlerResult},
        service_dispatcher,
        service_manager::{ServiceManager, ServiceManagerAccess},
    };

    use crate::cli::{load_cfg, Cli};
    use crate::config::ServerConfig;

    pub const DEFAULT_SERVICE_NAME: &str = "firelite-cloudserver";

    /// Args baked into the installed image (minus --install-service itself).
    /// Everything the server needs at boot must be explicit: no CWD, no tty.
    pub fn service_argv(name: &str, cfg: &ServerConfig) -> Result<Vec<OsString>, String> {
        if !std::path::Path::new(&cfg.db_path).is_absolute() {
            return Err("--db-path must be absolute for --install-service".into());
        }
        let mut args = vec![
            OsString::from("--run-service"),
            OsString::from("--service-name"),
            OsString::from(name),
            OsString::from("--db-path"),
            OsString::from(&cfg.db_path),
            OsString::from("--admin-bind"),
            OsString::from(&cfg.admin_bind),
            OsString::from("--sync-bind"),
            OsString::from(&cfg.sync_bind),
            OsString::from("--server-id"),
            OsString::from(&cfg.server_id),
        ];
        if !cfg.sync_token.is_empty() {
            args.push(OsString::from("--sync-token"));
            args.push(OsString::from(&cfg.sync_token));
        }
        if let (Some(cert), Some(key)) = (&cfg.tls_cert, &cfg.tls_key) {
            args.push(OsString::from("--tls-cert"));
            args.push(OsString::from(cert));
            args.push(OsString::from("--tls-key"));
            args.push(OsString::from(key));
        }
        Ok(args)
    }

    pub fn install(name: &str, cfg: &ServerConfig) -> Result<(), String> {
        let manager = ServiceManager::local_computer(
            None::<&str>,
            ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
        )
        .map_err(|e| format!("open SCM (need admin): {e}"))?;
        let exe = std::env::current_exe().map_err(|e| format!("current exe: {e}"))?;
        let info = ServiceInfo {
            name: name.into(),
            display_name: "FireLite Cloud Server".into(),
            service_type: ServiceType::OWN_PROCESS,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: exe,
            launch_arguments: service_argv(name, cfg)?,
            dependencies: Vec::<ServiceDependency>::new(),
            account_name: None,
            account_password: None,
        };
        let service = manager
            .create_service(&info, ServiceAccess::CHANGE_CONFIG)
            .map_err(|e| format!("create service: {e}"))?;
        service
            .set_description("FireLite offline-first sync hub + admin console")
            .map_err(|e| format!("set description: {e}"))?;
        Ok(())
    }

    pub fn uninstall(name: &str) -> Result<(), String> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .map_err(|e| format!("open SCM (need admin): {e}"))?;
        let service = manager
            .open_service(name, ServiceAccess::DELETE)
            .map_err(|e| format!("open service: {e}"))?;
        service.delete().map_err(|e| format!("delete: {e}"))?;
        Ok(())
    }

    static SHUTDOWN: AtomicBool = AtomicBool::new(false);

    fn service_handler(control: ServiceControl) -> ServiceControlHandlerResult {
        match control {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                SHUTDOWN.store(true, Ordering::Relaxed);
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    }

    define_windows_service!(ffi_service_main, service_main_impl);

    fn service_main_impl(_args: Vec<OsString>) {
        use clap::Parser;
        // Baked argv carries --service-name, so the control handler
        // registers under the exact name the SCM addresses (custom names
        // work, including Stop dispatch).
        let cli = Cli::parse();
        let cfg = match load_cfg(&cli) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("service config: {e}");
                std::process::exit(1);
            }
        };
        if let Err(e) = service_control_handler::register(&cli.service_name, service_handler) {
            eprintln!("register control handler: {e}");
            std::process::exit(1);
        }
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("runtime: {e}");
                std::process::exit(1);
            }
        };
        let code = rt.block_on(async {
            match crate::server::run(cfg, async {
                while !SHUTDOWN.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            })
            .await
            {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("server: {e}");
                    1
                }
            }
        });
        std::process::exit(code);
    }

    /// Entry point when launched by the SCM (`--run-service`). The dispatcher
    /// runs `service_main_impl` (which re-parses the baked argv, including
    /// `--service-name`) and returns when the service stops.
    pub fn run_service_main(service_name: &str) -> ! {
        match service_dispatcher::start(service_name, ffi_service_main) {
            Ok(()) => std::process::exit(0),
            Err(e) => {
                eprintln!("service dispatcher (run outside SCM?): {e}");
                std::process::exit(1);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn cfg() -> ServerConfig {
            ServerConfig {
                db_path: if cfg!(windows) {
                    r"C:\data\fl.db".into()
                } else {
                    "/data/fl.db".into()
                },
                admin_bind: "127.0.0.1:8081".into(),
                sync_bind: "0.0.0.0:8080".into(),
                log_level: "info".into(),
                secure_cookies: false,
                server_id: "hub".into(),
                sync_token: String::new(),
                tls_cert: None,
                tls_key: None,
            }
        }

        #[test]
        fn argv_bakes_name_and_flags() {
            let a = service_argv("custom", &cfg())
                .unwrap()
                .into_iter()
                .map(|s| s.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            assert_eq!(
                &a[..5],
                &[
                    "--run-service",
                    "--service-name",
                    "custom",
                    "--db-path",
                    if cfg!(windows) {
                        r"C:\data\fl.db"
                    } else {
                        "/data/fl.db"
                    },
                ]
            );
            assert!(!a.iter().any(|x| x == "--sync-token"));
        }

        #[test]
        fn argv_rejects_relative_db_path() {
            let mut c = cfg();
            c.db_path = "relative/path.db".into();
            assert!(service_argv("x", &c).is_err());
        }

        #[test]
        fn argv_carries_token_and_tls() {
            let mut c = cfg();
            c.sync_token = "tok".into();
            c.tls_cert = Some("/c.pem".into());
            c.tls_key = Some("/k.pem".into());
            let a = service_argv("custom", &c)
                .unwrap()
                .into_iter()
                .map(|s| s.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            assert!(a.windows(2).any(|w| w == ["--sync-token", "tok"]));
            assert!(a.windows(2).any(|w| w == ["--tls-cert", "/c.pem"]));
            assert!(a.windows(2).any(|w| w == ["--tls-key", "/k.pem"]));
        }
    }
}

//! firelite-cloudserver: standalone cloud-sync server + admin console.

use clap::Parser;
use firelite_cloudserver::cli::{load_cfg, Cli};

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();

    #[cfg(windows)]
    {
        use firelite_cloudserver::service::imp as svc;
        if cli.install_service {
            let cfg = load_cfg(&cli)?;
            svc::install(&cli.service_name, &cfg)?;
            println!("installed service '{}' (starts automatically at boot)", cli.service_name);
            return Ok(());
        }
        if cli.uninstall_service {
            svc::uninstall(&cli.service_name)?;
            println!("uninstalled service '{}'", cli.service_name);
            return Ok(());
        }
        if cli.run_service {
            svc::run_service_main(&cli.service_name);
        }
    }

    let cfg = load_cfg(&cli)?;
    firelite_cloudserver::server::run(cfg, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

use std::{net::SocketAddr, path::Path};

use anyhow::Result;
use chirpstack_provisioner::{build_router, AppState, Config};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let config_path = std::env::var("CHIRPSTACK_PROVISIONER_CONFIG")
        .unwrap_or_else(|_| "/etc/chirpstack-provisioner/config.yaml".into());
    let config = Config::load(Path::new(&config_path))?;
    let bind: SocketAddr = config.server.bind.parse()?;
    let state = AppState::load(config).await?;
    let app = build_router(state);
    let listener = TcpListener::bind(bind).await?;
    info!(%bind, "chirpstack-provisioner listening");
    axum::serve(listener, app).await?;
    Ok(())
}

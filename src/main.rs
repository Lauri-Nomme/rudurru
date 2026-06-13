use anyhow::Context;
use rudurru::server;
use rudurru::storage::Store;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rudurru=info".parse().unwrap()),
        )
        .init();

    let wal_path = std::env::var("RUDURRU_WAL")
        .unwrap_or_else(|_| "/tmp/rudurru.wal".to_string());

    let listen_addr = std::env::var("RUDURRU_LISTEN")
        .unwrap_or_else(|_| "[::]:2379".to_string());

    let store = Arc::new(Store::open(&wal_path).await?);

    let addr: SocketAddr = listen_addr.parse().context("parse listen address")?;

    tracing::info!("Rudurru listening on {addr}, WAL: {wal_path}");

    Server::builder()
        .add_service(server::new_kv(store.clone()))
        .add_service(server::new_watch(store.clone()))
        .add_service(server::new_lease(store.clone()))
        .add_service(server::new_cluster(store.clone()))
        .add_service(server::new_maintenance(store.clone()))
        .add_service(server::new_auth(store))
        .serve(addr)
        .await?;

    Ok(())
}

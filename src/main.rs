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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rudurru=info".parse().unwrap()),
        )
        .init();

    let wal_path = std::env::var("RUDURRU_WAL")
        .unwrap_or_else(|_| "/tmp/rudurru.wal".to_string());

    let listen_addr = std::env::var("RUDURRU_LISTEN")
        .unwrap_or_else(|_| "[::]:2379".to_string());

    let store = Arc::new(Store::open(&wal_path).await?);

    let addr: SocketAddr = listen_addr.parse().context("parse listen address")?;

    tracing::info!("Rudurru listening on {addr}, WAL: {wal_path}");

    // Periodic status logging (every 60s)
    let status_store = store.clone();
    let status_wal = wal_path.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let (keys, watchers, leases) = {
                let s = status_store.state.read().await;
                (s.keys.len(), s.watchers.len(), s.leases.len())
            };
            let wal_size = std::fs::metadata(&status_wal)
                .map(|m| m.len())
                .unwrap_or(0);
            let rev = rudurru::storage::current_revision();
            tracing::info!(rev, keys, watchers, leases, wal_size, "rudurru status");
        }
    });

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

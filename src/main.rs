use anyhow::Context;
use rudurru::server;
use rudurru::storage::Store;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rudurru=info".parse().unwrap()),
        )
        .init();

    let wal_path = std::env::var("RUDURRU_WAL").unwrap_or_else(|_| "/tmp/rudurru.wal".to_string());

    let listen_addr = std::env::var("RUDURRU_LISTEN").unwrap_or_else(|_| "[::]:2379".to_string());

    let store = Arc::new(Store::open(&wal_path).await?);

    let addr: SocketAddr = listen_addr.parse().context("parse listen address")?;

    tracing::info!(
        git_revision = env!("GIT_REVISION"),
        "Rudurru listening on {addr}, WAL: {wal_path}"
    );

    // Periodic status logging + malloc_trim (every 60s)
    let status_wal = wal_path.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let keys = rudurru::storage::KEY_COUNT.load(std::sync::atomic::Ordering::Relaxed);
            let watchers =
                rudurru::storage::WATCHER_COUNT.load(std::sync::atomic::Ordering::Relaxed);
            let leases = rudurru::storage::LEASE_COUNT.load(std::sync::atomic::Ordering::Relaxed);
            let wal_size = std::fs::metadata(&status_wal).map(|m| m.len()).unwrap_or(0);
            let rev = rudurru::storage::current_revision();
            tracing::info!(rev, keys, watchers, leases, wal_size, "rudurru status");
            // Release free memory cached by glibc back to the OS.
            // Without this, glibc holds freed mmap'd regions indefinitely,
            // causing RSS to remain high after bulk key deletion.
            unsafe { malloc_trim(0); }
        }
    });

    let shutdown = async {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("shutdown signal received, draining...");
    };

    Server::builder()
        .add_service(server::new_kv(store.clone()))
        .add_service(server::new_watch(store.clone()))
        .add_service(server::new_lease(store.clone()))
        .add_service(server::new_cluster(store.clone()))
        .add_service(server::new_maintenance(store.clone()))
        .add_service(server::new_auth(store))
        .serve_with_shutdown(addr, shutdown)
        .await?;

    tracing::info!("shutdown complete");
    Ok(())
}

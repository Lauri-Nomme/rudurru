use anyhow::Context;
use rudurru::server;
use std::net::SocketAddr;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rudurru=info".parse().unwrap()),
        )
        .with_test_writer()
        .init();

    let addr: SocketAddr = "[::]:2379".parse().context("parse listen address")?;

    tracing::info!("Rudurru listening on {addr}");

    Server::builder()
        .add_service(server::new_kv())
        .add_service(server::new_watch())
        .add_service(server::new_lease())
        .add_service(server::new_cluster())
        .add_service(server::new_maintenance())
        .add_service(server::new_auth())
        .serve(addr)
        .await?;

    Ok(())
}

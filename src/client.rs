use std::time::Duration;

pub async fn new_client() -> etcd_client::Client {
    let ep = std::env::var("ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:2379".to_string());
    etcd_client::Client::connect([&ep], None)
        .await
        .unwrap_or_else(|e| panic!("connect to etcd at {ep}: {e}"))
}

pub fn random_key(prefix: &str) -> String {
    use rand::Rng;
    let suffix: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    format!("{prefix}_{suffix}")
}

pub async fn setup() -> etcd_client::Client {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rudurru=info".parse().unwrap()),
        )
        .with_test_writer()
        .init();

    new_client().await
}

pub async fn setup_with(root_user: &str, root_pass: &str) -> etcd_client::Client {
    let _ = setup().await;

    let ep = std::env::var("ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:2379".to_string());
    etcd_client::Client::connect(
        [&ep],
        Some(
            etcd_client::ConnectOptions::new()
                .with_user(root_user, root_pass)
                .with_keep_alive(Duration::from_secs(30), Duration::from_secs(10)),
        ),
    )
    .await
    .expect("connect to etcd with auth")
}

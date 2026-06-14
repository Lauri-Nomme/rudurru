use std::sync::OnceLock;
use tracing_subscriber::EnvFilter;

static TRACING: OnceLock<()> = OnceLock::new();

fn init() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive("rudurru=info".parse().unwrap())
                    .add_directive("etcd_client=warn".parse().unwrap()),
            )
            .with_test_writer()
            .init();
    });
}

fn endpoint() -> String {
    std::env::var("ETCD_ENDPOINT").unwrap_or_else(|_| "http://localhost:2379".to_string())
}

pub async fn connect() -> etcd_client::Client {
    init();
    let ep = endpoint();
    etcd_client::Client::connect([&ep], None)
        .await
        .unwrap_or_else(|e| panic!("connect to {ep}: {e}"))
}

#[macro_export]
macro_rules! key {
    ($prefix:expr) => {{
        let suffix: String = rand::random::<u32>().to_string();
        format!("rudurru_test/{}/{}", $prefix, suffix)
    }};
}

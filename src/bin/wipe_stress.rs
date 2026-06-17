use etcd_client::*;

#[tokio::main]
async fn main() {
    let endpoint = std::env::var("ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let mut client = Client::connect([endpoint], None).await.unwrap();
    let resp = client
        .delete("stress/", Some(DeleteOptions::new().with_prefix()))
        .await
        .unwrap();
    println!("Deleted {} keys", resp.deleted());
}

use etcd_client::*;
use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: rudurru-compact <revision> [endpoint]");
        std::process::exit(1);
    }
    let rev: i64 = args[1].parse().expect("revision must be a number");
    let endpoint = args.get(2).map(|s| s.as_str()).unwrap_or("http://127.0.0.1:2379");

    let mut client = Client::connect([endpoint], None).await.expect("connect");
    client.compact(rev, None).await.expect("compact");
    println!("Compacted at revision {rev} on {endpoint}");
}

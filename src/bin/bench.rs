use etcd_client::*;
use std::time::Duration;
use tokio::time::Instant;

fn endpoint() -> String {
    std::env::var("ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into())
}
const KEY_PREFIX: &str = "rudurru_bench/";

fn make_key(i: u64) -> String {
    format!("{KEY_PREFIX}{i:020}")
}

fn make_value(n: usize) -> Vec<u8> {
    vec![b'x'; n]
}

async fn connect() -> Client {
    Client::connect([endpoint()], None)
        .await
        .expect("connect to Rudurru")
}

fn compute_stats(latencies: &[Duration]) -> (f64, f64, f64) {
    let mut ms: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = ms.len();
    if n == 0 { return (0.0, 0.0, 0.0); }
    let avg = ms.iter().sum::<f64>() / n as f64;
    let p50 = ms[n / 2];
    let p99_index = ((n as f64 * 0.99) as usize).min(n - 1);
    let p99 = ms[p99_index];
    (avg, p50, p99)
}

#[tokio::main]
async fn main() {
    println!("=== Rudurru Performance Benchmark ===\n");
    println!("Endpoint: {}\n", endpoint());

    // ── Warmup ─────────────────────────────────────────────────────
    let mut client = connect().await;
    for i in 0..100u64 {
        let key = make_key(i);
        client.put(key.as_str(), b"warmup", None).await.unwrap();
    }
    println!("Warmup: 100 keys written\n");

    // ── Single-operation latency (1000 puts, 128B values) ──────────
    println!("── Single-operation latency (1000 ops, 128B values) ──");
    let mut latencies = Vec::with_capacity(1000);
    let val = make_value(128);
    let start = Instant::now();
    for i in 0..1000u64 {
        let key = make_key(i + 10_000_000);
        let t0 = Instant::now();
        client.put(key.as_str(), val.clone(), None).await.unwrap();
        latencies.push(t0.elapsed());
    }
    let elapsed = start.elapsed().as_secs_f64();
    let (avg, p50, p99) = compute_stats(&latencies);
    println!("  Put:   avg={avg:.3}ms  p50={p50:.3}ms  p99={p99:.3}ms  ({:.0} ops/s)", 1000.0 / elapsed);

    let mut latencies = Vec::with_capacity(1000);
    for i in 0..1000u64 {
        let key = make_key(i);
        let t0 = Instant::now();
        client.get(key.as_str(), None).await.unwrap();
        latencies.push(t0.elapsed());
    }
    let (avg, p50, p99) = compute_stats(&latencies);
    println!("  Get:   avg={avg:.3}ms  p50={p50:.3}ms  p99={p99:.3}ms");

    // Txn
    let mut latencies = Vec::with_capacity(100);
    for i in 0..100u64 {
        let key = make_key(i);
        let t0 = Instant::now();
        let get = client.get(key.as_str(), None).await.unwrap();
        let rev = get.kvs().first().map(|kv| kv.mod_revision()).unwrap_or(0);
        let cmp = Compare::mod_revision(key.as_str(), CompareOp::Equal, rev);
        let txn = Txn::new().when(vec![cmp]).and_then(vec![TxnOp::put(key.as_str(), "txn", None)]);
        client.txn(txn).await.unwrap();
        latencies.push(t0.elapsed());
    }
    let (avg, p50, p99) = compute_stats(&latencies);
    println!("  Txn:   avg={avg:.3}ms  p50={p50:.3}ms  p99={p99:.3}ms");

    // ── Value size scaling ─────────────────────────────────────────
    println!("\n── Value size scaling (500 puts each) ──");
    for &size in &[64, 256, 1024, 4096, 16384] {
        let val = make_value(size);
        let mut latencies = Vec::with_capacity(500);
        let start = Instant::now();
        for i in 0..500u64 {
            let key = make_key(i + 20_000_000);
            let t0 = Instant::now();
            client.put(key.as_str(), val.clone(), None).await.unwrap();
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed().as_secs_f64();
        let (avg, p50, p99) = compute_stats(&latencies);
        println!("  {size:>5}B:  avg={avg:.3}ms  p50={p50:.3}ms  p99={p99:.3}ms  ({:.0} ops/s)", 500.0 / elapsed);
    }

    // ── Prefix scan scaling ─────────────────────────────────────────
    println!("\n── Prefix scan scaling (prefix '{KEY_PREFIX}') ──");
    for &count in &[10, 100, 1000] {
        let opts = GetOptions::new().with_prefix().with_limit(count);
        let t0 = Instant::now();
        client.get(KEY_PREFIX, Some(opts)).await.unwrap();
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!("  {count:>5} keys:  {ms:.3}ms");
    }
    // Scan warmup keys (limit 2000 to stay under gRPC 4MB limit)
    let opts = GetOptions::new().with_prefix().with_limit(2000);
    let t0 = Instant::now();
    match client.get(KEY_PREFIX, Some(opts)).await {
        Ok(resp) => {
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            println!("  {:>5} keys:  {ms:.3}ms", resp.count());
        }
        Err(e) => println!("  scan (prefix) failed: {e}"),
    }

    // ── Concurrent throughput ───────────────────────────────────────
    println!("\n── Concurrent put throughput (2000 total ops) ──");
    for &workers in &[1, 4, 8, 16, 32] {
        let val = make_value(128);
        let start = Instant::now();
        let mut handles = Vec::new();
        let per_task = 2000 / workers;
        for w in 0..workers {
            let val = val.clone();
            handles.push(tokio::spawn(async move {
                let mut c = connect().await;
                let base = w as u64 * per_task + 30_000_000;
                for i in 0..per_task {
                    let key = make_key(base + i as u64);
                    c.put(key.as_str(), val.as_slice(), None).await.unwrap();
                }
            }));
        }
        for h in handles { h.await.unwrap(); }
        let elapsed = start.elapsed().as_secs_f64();
        println!("  {workers:>2} workers:  {:.0} ops/s ({:.3}s)", 2000.0 / elapsed, elapsed);
    }

    // ── Memory usage ────────────────────────────────────────────────
    println!("\n── Memory usage (bench process, not server) ──");
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") || line.starts_with("VmPeak:") {
                println!("  {line}");
            }
        }
    }

    println!("\n── Done ──");
}

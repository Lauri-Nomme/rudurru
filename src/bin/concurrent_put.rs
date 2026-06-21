use etcd_client::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::time::Duration;
use tokio::time::Instant;

fn endpoint() -> String {
    std::env::var("ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into())
}

const KEY_PREFIX: &str = "rwlock_bench/";
const PREPOP: u64 = 50_000;
const OPS_PER_WORKER: u64 = 2_000;
const WORKERS: &[u64] = &[1, 2, 4, 8, 16, 32, 64, 128];
// const WORKERS: &[u64] = &[16, 32]; // fast smoke test

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

async fn warmup(client: &mut Client) {
    let val = make_value(128);
    for i in 0..PREPOP {
        let key = make_key(i);
        client
            .put(key.as_str(), val.clone(), None)
            .await
            .expect("warmup put");
        if (i + 1) % 10_000 == 0 {
            eprintln!("  warmup: {}/{} keys", i + 1, PREPOP);
        }
    }
    eprintln!("  warmup done: {PREPOP} keys");
}

fn compute_stats(latencies: &[Duration]) -> (f64, f64, f64, f64) {
    let mut us: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1_000_000.0).collect();
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = us.len();
    if n == 0 {
        return (0.0, 0.0, 0.0, 0.0);
    }
    let avg = us.iter().sum::<f64>() / n as f64;
    let p50 = us[n / 2];
    let p99_index = ((n as f64 * 0.99) as usize).min(n - 1);
    let p99 = us[p99_index];
    let max = us[n - 1];
    (avg, p50, p99, max)
}

#[tokio::main]
async fn main() {
    println!("=== RwLock Contention Benchmark ===");
    println!("endpoint={}", endpoint());
    println!("prepopulated_keys={PREPOP}  ops_per_worker={OPS_PER_WORKER}");
    println!();

    // Connect and warmup
    let mut client = connect().await;
    warmup(&mut client).await;
    drop(client);

    let val = make_value(128);
    let key_pool: Vec<u64> = (0..PREPOP).collect();

    for &workers in WORKERS {
        let total_ops = workers * OPS_PER_WORKER;
        let mut handles = Vec::new();

        // Pre-generate per-worker random key sequences to avoid contention on rng
        let per_worker_keys: Vec<Vec<u64>> = (0..workers)
            .map(|_| {
                let mut rng = thread_rng();
                (0..OPS_PER_WORKER)
                    .map(|_| *key_pool.choose(&mut rng).unwrap())
                    .collect()
            })
            .collect();

        let t_start = Instant::now();

        for w in 0..workers {
            let val = val.clone();
            let keys = per_worker_keys[w as usize].clone();

            handles.push(tokio::spawn(async move {
                let mut c = connect().await;
                let mut latencies = Vec::with_capacity(OPS_PER_WORKER as usize);
                for key_idx in keys {
                    let key = make_key(key_idx);
                    let t0 = Instant::now();
                    c.put(key.as_str(), val.clone(), None).await.unwrap();
                    latencies.push(t0.elapsed());
                }
                latencies
            }));
        }

        // Collect results
        let mut all_latencies = Vec::with_capacity(total_ops as usize);
        for h in handles {
            if let Ok(lats) = h.await {
                all_latencies.extend(lats);
            }
        }

        let elapsed = t_start.elapsed().as_secs_f64();
        let throughput = total_ops as f64 / elapsed;
        let (avg_us, p50_us, p99_us, max_us) = compute_stats(&all_latencies);

        println!(
            "workers={workers:>3}  throughput={throughput:>8.0} ops/s  ",
        );
        println!(
            "         avg={avg_us:>7.1}µs  p50={p50_us:>7.1}µs  p99={p99_us:>7.1}µs  max={max_us:>7.1}µs"
        );

        // Small delay between runs to let server settle
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

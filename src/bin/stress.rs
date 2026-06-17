use etcd_client::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn endpoint() -> String {
    std::env::var("ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into())
}

async fn connect() -> Client {
    Client::connect([endpoint()], None)
        .await
        .expect("connect to Rudurru")
}

#[tokio::main]
async fn main() {
    let workers: u64 = std::env::var("WORKERS")
        .unwrap_or_else(|_| "64".into())
        .parse()
        .unwrap();
    let duration_secs: u64 = std::env::var("DURATION")
        .unwrap_or_else(|_| "120".into())
        .parse()
        .unwrap();
    let val_size: usize = std::env::var("VAL_SIZE")
        .unwrap_or_else(|_| "256".into())
        .parse()
        .unwrap();

    println!(
        "=== Rudurru Load Generator ===\n  endpoint={}  workers={}  duration={}s  val_size={}B\n",
        endpoint(),
        workers,
        duration_secs,
        val_size,
    );

    let val = vec![b'x'; val_size];
    let running = Arc::new(AtomicBool::new(true));
    let ops_counter = Arc::new(AtomicU64::new(0));
    let err_counter = Arc::new(AtomicU64::new(0));
    let r = running.clone();
    let oc = ops_counter.clone();
    let ec = err_counter.clone();

    // Reporter
    let report_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let mut prev = 0u64;
        let start = Instant::now();
        loop {
            interval.tick().await;
            if !r.load(Ordering::Relaxed) {
                break;
            }
            let total = oc.load(Ordering::Relaxed);
            let errs = ec.load(Ordering::Relaxed);
            let rate = (total - prev) as f64 / 5.0;
            prev = total;
            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "  [{elapsed:6.1}s]  {total:>8} ops  {rate:>8.0} ops/s  errors={errs}"
            );
        }
    });

    // Worker spawner
    let mut handles = Vec::new();
    for w in 0..workers {
        let val = val.clone();
        let running = running.clone();
        let oc = ops_counter.clone();
        let ec = err_counter.clone();
        handles.push(tokio::spawn(async move {
            let mut c = connect().await;
            let key_prefix = format!("stress/{w:04}/");
            let mut i = 0u64;
            while running.load(Ordering::Relaxed) {
                let key = format!("{key_prefix}{i:020}");
                let put_res = c.put(key.as_str(), val.clone(), None).await;
                match put_res {
                    Ok(_) => {
                        oc.fetch_add(1, Ordering::Relaxed);
                        if i % 10 == 0 {
                            let _ = c.get(key.as_str(), None).await;
                        }
                    }
                    Err(e) => {
                        ec.fetch_add(1, Ordering::Relaxed);
                        eprintln!("worker {w}: put error: {e}");
                    }
                }
                i += 1;
            }
        }));
    }

    sleep(Duration::from_secs(duration_secs)).await;
    running.store(false, Ordering::Relaxed);

    for h in handles {
        let _ = h.await;
    }
    report_handle.await.unwrap();

    let total = ops_counter.load(Ordering::Relaxed);
    let errs = err_counter.load(Ordering::Relaxed);
    println!(
        "\n--- Done ---\n  total ops={total}  errors={errs}  avg rate={:.0} ops/s",
        total as f64 / duration_secs as f64
    );
}

use crate::storage::state::StoreState;
use crate::storage::LEASE_COUNT;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use parking_lot::RwLock;

pub(crate) fn start_fsync_task(file: Arc<Mutex<std::fs::File>>, dirty: Arc<AtomicBool>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if dirty.swap(false, Ordering::AcqRel) {
                if let Ok(f) = file.lock() {
                    if let Err(e) = f.sync_all() {
                        tracing::error!("WAL background fsync failed: {e}");
                    }
                }
            }
        }
    });
}

pub(crate) fn start_expiry_task(state: Arc<RwLock<StoreState>>) {
    tokio::spawn(async move {
        let notify = {
            let s = state.read();
            s.expiry_notify.clone()
        };
        loop {
            let sleep_dur = {
                let s = state.read();
                s.leases
                    .values()
                    .map(|ls| ls.expires_at)
                    .min()
                    .map(|earliest| {
                        let now = tokio::time::Instant::now();
                        if earliest <= now {
                            Duration::ZERO
                        } else {
                            earliest - now
                        }
                    })
                    .unwrap_or(Duration::MAX)
            };

            tokio::select! {
                _ = tokio::time::sleep(sleep_dur) => {}
                _ = notify.notified() => {}
            }

            {
                let mut s = state.write();
                let now = tokio::time::Instant::now();
                let expired: Vec<i64> = s
                    .leases
                    .iter()
                    .filter(|(_, ls)| ls.expires_at <= now)
                    .map(|(id, _)| *id)
                    .collect();
                if expired.is_empty() {
                    continue;
                }
                for id in &expired {
                    s.leases.remove(id);
                    LEASE_COUNT.fetch_sub(1, Ordering::Relaxed);
                    if let Err(e) = s.delete_keys_for_lease(*id) {
                        tracing::error!(lease_id = id, error = %e, "expiry_task: lease key deletion failed");
                    }
                }
            }
            notify.notify_one();
        }
    });
}

pub(crate) fn start_compaction_task(store: crate::storage::Store, wal_path: String) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        interval.tick().await;
        loop {
            interval.tick().await;
            let size = match std::fs::metadata(&wal_path) {
                Ok(m) => m.len(),
                Err(_) => continue,
            };
            if size < 64 * 1024 * 1024 {
                continue;
            }
            tracing::info!(wal_size = size, "wal_compaction_triggered");
            if let Err(e) = store.compact_wal().await {
                tracing::error!(error = %e, "wal_compaction_failed");
            }
        }
    });
}

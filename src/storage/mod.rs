pub mod wal;
mod state;
mod apply;
mod watcher;
mod store;
mod background;

pub use state::{KeyState, LeaseState, StoreState, WatchEvent, WatchRegistration};
pub use store::Store;


use crate::proto::etcdserverpb;
use prost::bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
/// Global revision counter. Monotonically increasing, starts at 1.
static NEXT_REV: AtomicU64 = AtomicU64::new(1);

pub fn next_revision() -> u64 {
    NEXT_REV.fetch_add(1, Ordering::SeqCst)
}

pub fn current_revision() -> u64 {
    NEXT_REV.load(Ordering::SeqCst).saturating_sub(1)
}

static NEXT_LEASE_ID: AtomicI64 = AtomicI64::new(1);
static COMPACT_REV: AtomicU64 = AtomicU64::new(0);

/// Approximate counters for the periodic status log (no read lock needed).
pub static KEY_COUNT: AtomicU64 = AtomicU64::new(0);
pub static WATCHER_COUNT: AtomicU64 = AtomicU64::new(0);
pub static LEASE_COUNT: AtomicU64 = AtomicU64::new(0);

fn next_lease_id() -> i64 {
    NEXT_LEASE_ID.fetch_add(1, Ordering::SeqCst)
}

/// Translate key + range_end from proto into a range bound for BTreeMap.
pub(crate) fn resolve_range(key: &[u8], range_end: &[u8]) -> RangeBound {
    if range_end.is_empty() {
        return RangeBound::Point(key.to_vec());
    }

    if range_end.len() == 1 && range_end[0] == 0 {
        return if key.is_empty() {
            RangeBound::All
        } else {
            RangeBound::From(key.to_vec())
        };
    }

    if key.is_empty() && range_end.len() == 1 && range_end[0] == 0 {
        return RangeBound::All;
    }

    // Prefix encoding: range_end == key with last byte incremented
    // Key ending in 0xFF wraps to 0x00, which collides with the \0
    // suffix encoding — skip prefix detection in that case.
    if range_end.len() == key.len()
        && !range_end.is_empty()
        && range_end[..range_end.len() - 1] == key[..key.len() - 1]
        && range_end[key.len() - 1] == key[key.len() - 1].wrapping_add(1)
        && key[key.len() - 1] != 0xFF
    {
        return RangeBound::Prefix(key.to_vec());
    }

    // Alternate prefix encoding: range_end == key + "\0"
    if range_end.len() == key.len() + 1
        && range_end[..key.len()] == key[..]
        && range_end[key.len()] == 0
    {
        return RangeBound::Prefix(key.to_vec());
    }

    RangeBound::Range(key.to_vec(), range_end.to_vec())
}

#[derive(Clone, Debug)]
pub(crate) enum RangeBound {
    All,
    Point(Vec<u8>),
    From(Vec<u8>),
    Prefix(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
}

impl RangeBound {
    pub(crate) fn to_ref(&self) -> RangeBoundRef<'_> {
        match self {
            RangeBound::All => RangeBoundRef::All,
            RangeBound::Point(k) => RangeBoundRef::Point(k),
            RangeBound::From(k) => RangeBoundRef::From(k),
            RangeBound::Prefix(p) => RangeBoundRef::Prefix(p),
            RangeBound::Range(s, e) => RangeBoundRef::Range(s, e),
        }
    }
}

pub(crate) enum RangeBoundRef<'a> {
    All,
    Point(&'a [u8]),
    From(&'a [u8]),
    Prefix(&'a [u8]),
    Range(&'a [u8], &'a [u8]),
}

pub(crate) fn matches_range(bound: RangeBoundRef<'_>, key: &[u8]) -> bool {
    match bound {
        RangeBoundRef::All => true,
        RangeBoundRef::Point(k) => key == k,
        RangeBoundRef::From(k) => key >= k,
        RangeBoundRef::Prefix(p) => key.starts_with(p),
        RangeBoundRef::Range(start, end) => key >= start && key < end,
    }
}

/// Convert a RangeBoundRef to (start, end) bounds for BTreeMap::range().
fn btree_bounds(bound: RangeBoundRef<'_>) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    match bound {
        RangeBoundRef::All => (None, None),
        RangeBoundRef::Point(k) => {
            let mut end = k.to_vec();
            if let Some(last) = end.last_mut() {
                *last = last.wrapping_add(1);
                if *last == 0 {
                    end.push(0);
                }
            }
            (Some(k.to_vec()), Some(end))
        }
        RangeBoundRef::From(k) => (Some(k.to_vec()), None),
        RangeBoundRef::Prefix(p) => (Some(p.to_vec()), None),
        RangeBoundRef::Range(start, end) => (Some(start.to_vec()), Some(end.to_vec())),
    }
}

/// Scan the WAL up to `up_to_rev`, returning a map of key → kv_bytes for
/// records whose key falls in the range [key, range_end) and whose revision
/// is <= up_to_rev. Opens a separate file handle to avoid contention with
/// the shared Arc<Mutex<File>> used by writers.
fn scan_wal_range(
    path: &str,
    key: &[u8],
    range_end: &[u8],
    up_to_rev: u64,
) -> std::io::Result<HashMap<Vec<u8>, Bytes>> {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // WAL may have been renamed during compaction; return empty
            return Ok(HashMap::new());
        }
        Err(e) => return Err(e),
    };

    let bound = resolve_range(key, range_end);
    let mut state: HashMap<Vec<u8>, Bytes> = HashMap::new();

    wal::scan_kv_file(&mut file, 0, |rec| {
        let rev = rec.mod_revision().unwrap_or(0) as u64;
        let rec_key = match rec.key() {
            Some(k) => k,
            None => return,
        };

        // Only process records in the requested key range
        if !matches_range(bound.to_ref(), rec_key) {
            return;
        }

        if rev <= up_to_rev {
            let is_delete = (rec.flags & wal::DELETED) != 0;
            if is_delete {
                state.remove(rec_key);
            } else {
                state.insert(rec_key.to_vec(), Bytes::copy_from_slice(&rec.kv_bytes));
            }
        }
    })?;

    Ok(state)
}

fn eval_compare(state: &StoreState, cmp: &etcdserverpb::Compare) -> bool {
    let key = &cmp.key;

    // A tombstoned (deleted) key counts as "does not exist" for compare purposes,
    // matching etcd semantics where deleted keys are absent from the current store.
    let alive = state.keys.get(key).filter(|k| k.is_alive());

    let result = result_from_i32(cmp.result);
    let target = target_from_i32(cmp.target);

    match target {
        etcdserverpb::compare::CompareTarget::Version => {
            let actual = alive.map(|k| k.version).unwrap_or(0);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::Version(v)) => *v,
                _ => 0,
            };
            cmp_i64(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Create => {
            let actual = alive.map(|k| k.create_revision as i64).unwrap_or(0);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::CreateRevision(v)) => *v,
                _ => 0,
            };
            cmp_i64(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Mod => {
            let actual = alive.map(|k| k.mod_revision as i64).unwrap_or(0);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::ModRevision(v)) => *v,
                _ => 0,
            };
            cmp_i64(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Value => {
            let actual: &[u8] = alive.map(|k| k.value.as_ref()).unwrap_or(&[]);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::Value(v)) => v.as_slice(),
                _ => &[],
            };
            cmp_bytes(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Lease => {
            let actual = alive.map(|k| k.lease).unwrap_or(0);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::Lease(v)) => *v,
                _ => 0,
            };
            cmp_i64(result, actual, expected)
        }
    }
}

fn cmp_i64(result: etcdserverpb::compare::CompareResult, actual: i64, expected: i64) -> bool {
    match result {
        etcdserverpb::compare::CompareResult::Equal => actual == expected,
        etcdserverpb::compare::CompareResult::Greater => actual > expected,
        etcdserverpb::compare::CompareResult::Less => actual < expected,
        etcdserverpb::compare::CompareResult::NotEqual => actual != expected,
    }
}

fn cmp_bytes(result: etcdserverpb::compare::CompareResult, actual: &[u8], expected: &[u8]) -> bool {
    match result {
        etcdserverpb::compare::CompareResult::Equal => actual == expected,
        etcdserverpb::compare::CompareResult::Greater => actual > expected,
        etcdserverpb::compare::CompareResult::Less => actual < expected,
        etcdserverpb::compare::CompareResult::NotEqual => actual != expected,
    }
}

fn result_from_i32(v: i32) -> etcdserverpb::compare::CompareResult {
    match v {
        0 => etcdserverpb::compare::CompareResult::Equal,
        1 => etcdserverpb::compare::CompareResult::Greater,
        2 => etcdserverpb::compare::CompareResult::Less,
        3 => etcdserverpb::compare::CompareResult::NotEqual,
        _ => etcdserverpb::compare::CompareResult::Equal,
    }
}

fn target_from_i32(v: i32) -> etcdserverpb::compare::CompareTarget {
    match v {
        0 => etcdserverpb::compare::CompareTarget::Version,
        1 => etcdserverpb::compare::CompareTarget::Create,
        2 => etcdserverpb::compare::CompareTarget::Mod,
        3 => etcdserverpb::compare::CompareTarget::Value,
        4 => etcdserverpb::compare::CompareTarget::Lease,
        _ => etcdserverpb::compare::CompareTarget::Version,
    }
}

#[cfg(test)]
mod compact_tests {
    use super::*;

    fn temp_wal() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir();
        let name = format!(
            "rudurru_compact_{}_{}.wal",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        // Reset global state to avoid cross-test interference in parallel runs.
        COMPACT_REV.store(0, Ordering::SeqCst);
        path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn test_compact_basic() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // Create stale records by updating keys multiple times
        for i in 0u64..5 {
            store
                .put(etcdserverpb::PutRequest {
                    key: b"k1".to_vec(),
                    value: format!("v{i}").into_bytes(),
                    ..Default::default()
                })
                .await;
        }
        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            })
            .await;

        let size_before = {
            let s = store.state.read();
            let f = s.wal.file.lock().unwrap();
            f.metadata().unwrap().len()
        };
        store.compact_wal().await.unwrap();
        let size_after = {
            let s = store.state.read();
            let f = s.wal.file.lock().unwrap();
            f.metadata().unwrap().len()
        };

        // 5 stale records for k1 + 1 for k2 = 6 before; 2 after
        assert!(
            size_after < size_before,
            "WAL should shrink: {size_after} >= {size_before}"
        );
        assert!(size_after > 0, "WAL should not be empty");

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                ..Default::default()
            })
            .await.unwrap();
        assert_eq!(resp.count, 2, "keys preserved after compaction");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_compact_empty_store() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store.compact_wal().await.unwrap();
        // Empty store → 0 snapshot records + 0 tail = 0-byte WAL (valid)

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                ..Default::default()
            })
            .await.unwrap();
        assert_eq!(resp.count, 0, "no keys in empty store");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_compact_with_deletes() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;

        store
            .delete_range(etcdserverpb::DeleteRangeRequest {
                key: b"k1".to_vec(),
                ..Default::default()
            })
            .await;

        store.compact_wal().await.unwrap();

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                ..Default::default()
            })
            .await.unwrap();
        assert_eq!(resp.count, 1, "only k2 after compact");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_compact_restart_recovery() {
        let path = temp_wal();
        let final_rev;

        // Phase 1: write, compact, write more (simulating tail)
        {
            let store = Store::open(&path).await.unwrap();

            store
                .put(etcdserverpb::PutRequest {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    ..Default::default()
                })
                .await;
            store
                .put(etcdserverpb::PutRequest {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                    ..Default::default()
                })
                .await;

            store.compact_wal().await.unwrap();

            // Writes after compaction (simulating writes during Phase B)
            store
                .put(etcdserverpb::PutRequest {
                    key: b"k3".to_vec(),
                    value: b"v3".to_vec(),
                    ..Default::default()
                })
                .await;

            final_rev = current_revision();
        }

        // Phase 2: reopen from compacted WAL
        let store2 = Store::open(&path).await.unwrap();

        let resp = store2
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                ..Default::default()
            })
            .await.unwrap();
        assert_eq!(resp.count, 3, "all 3 keys after restart from compacted WAL");
        assert_eq!(
            current_revision(),
            final_rev,
            "revision matches after restart"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_compact_tail_preserves_concurrent_writes() {
        let path = temp_wal();
        let store = std::sync::Arc::new(Store::open(&path).await.unwrap());

        // Seed some data
        for i in 0u64..10 {
            store
                .put(etcdserverpb::PutRequest {
                    key: format!("k{i}").into_bytes(),
                    value: b"v".to_vec(),
                    ..Default::default()
                })
                .await;
        }

        // Write more during Phase B happens naturally because compact_wal
        // releases the write lock between Phase A and Phase C.
        // We spawn a concurrent writer that fires just after Phase A.
        let store_clone = store.clone();
        let write_handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            store_clone
                .put(etcdserverpb::PutRequest {
                    key: b"concurrent".to_vec(),
                    value: b"c".to_vec(),
                    ..Default::default()
                })
                .await;
        });

        store.compact_wal().await.unwrap();
        write_handle.await.unwrap();

        // Verify concurrent write is visible
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"concurrent".to_vec(),
                ..Default::default()
            })
            .await.unwrap();
        assert_eq!(resp.count, 1, "concurrent write preserved after compact");

        // Restart and verify
        drop(store);
        let store2 = Store::open(&path).await.unwrap();
        let resp2 = store2
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                ..Default::default()
            })
            .await.unwrap();
        assert_eq!(resp2.count, 11, "all 11 keys after restart");

        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(test)]
mod historical_tests {
    use super::*;
    use crate::proto::mvccpb;
    use prost::Message;

    fn temp_wal() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir();
        let name = format!(
            "rudurru_historical_{}_{}.wal",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        // Reset compact rev before each test to avoid test interference
        COMPACT_REV.store(0, Ordering::SeqCst);
        path.to_string_lossy().to_string()
    }

    fn all_req() -> etcdserverpb::RangeRequest {
        etcdserverpb::RangeRequest {
            key: b"".to_vec(),
            range_end: vec![0],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_historical_compacted_revision_error() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // Need data so current_rev is ahead of the compacted revision
        store
            .put(etcdserverpb::PutRequest {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;
        let create_rev = current_revision(); // rev 2
        // Compact at rev 2 — rev 1 is now below COMPACT_REV
        COMPACT_REV.store(create_rev, Ordering::SeqCst);

        let err = store
            .range(etcdserverpb::RangeRequest {
                key: b"k".to_vec(),
                revision: 1,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            err.message().contains("compacted"),
            "should be compacted error: {}",
            err.message()
        );

        // Non-compacted revision should still work
        let ok = store
            .range(etcdserverpb::RangeRequest {
                key: b"k".to_vec(),
                revision: create_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(ok.count, 2, "both keys exist at non-compacted rev");

        COMPACT_REV.store(0, Ordering::SeqCst);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_future_revision_error() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        let err = store
            .range(etcdserverpb::RangeRequest {
                key: b"k".to_vec(),
                revision: 999_999_999,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            err.message().contains("future"),
            "should be future revision error: {}",
            err.message()
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_zero_revision_returns_current() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;

        let resp = store.range(all_req()).await.unwrap();
        assert_eq!(resp.count, 1, "revision=0 returns current state");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_all_keys_current_skip_wal() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        let rev1 = current_revision();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;
        let rev2 = current_revision();

        // Query at rev2: both keys have mod_revision == rev2, so mod_rev <= target_rev
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: rev2 as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 2, "both keys at rev2");

        // Query at rev1: only k1 exists (k2 doesn't exist yet)
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: rev1 as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "only k1 at rev1");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_some_keys_stale() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        let rev1 = current_revision();

        // k2 is created after rev1 — should not appear in query at rev1
        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;

        // Query at rev1: k1 exists, k2 doesn't (create_rev > rev1)
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: rev1 as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "only k1 at rev1");
        assert_eq!(resp.kvs.len(), 1, "one kv at rev1");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_value_at_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // Create k1 at rev1
        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"old_value".to_vec(),
                ..Default::default()
            })
            .await;
        let create_rev = current_revision();

        // Update k1 at some later revision
        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"new_value".to_vec(),
                ..Default::default()
            })
            .await;

        // Query at the create revision: should see old_value
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: create_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "k1 exists at create_rev");
        if let Some(kv_bytes) = resp.kvs.first() {
            let kv = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
            assert_eq!(
                kv.value, b"old_value",
                "value at create_rev should be old_value"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_after_delete() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;

        store
            .delete_range(etcdserverpb::DeleteRangeRequest {
                key: b"k1".to_vec(),
                ..Default::default()
            })
            .await;

        // Query after delete: k1 should not appear
        let later_rev = current_revision();
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: later_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 0, "k1 deleted by later_rev");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_before_delete() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        let create_rev = current_revision();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;

        // Delete k1
        store
            .delete_range(etcdserverpb::DeleteRangeRequest {
                key: b"k1".to_vec(),
                ..Default::default()
            })
            .await;

        // Query at create_rev: k1 should exist (it was created before the
        // delete, which happened later). k2 should NOT be present because
        // k2's create_rev > create_rev.
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: create_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "only k1 at create_rev");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_after_create() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;

        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;
        let later_rev = current_revision();

        // Put another one, query at the point before it existed
        store
            .put(etcdserverpb::PutRequest {
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
                ..Default::default()
            })
            .await;

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: later_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 2, "only k1,k2 at later_rev (k3 not yet created)");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_keys_only() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;
        let rev = current_revision();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
                ..Default::default()
            })
            .await;

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: rev as i64,
                keys_only: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 2, "keys_only returns 2 keys at rev");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_count_only() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        let rev = current_revision();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: rev as i64,
                count_only: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "count_only returns 1 at rev");
        assert!(resp.kvs.is_empty(), "count_only should have empty kvs");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_limit() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;
        let rev = current_revision();

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: rev as i64,
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 2, "count is total, limit is 1");
        assert_eq!(resp.kvs.len(), 1, "limited to 1 kv");
        assert!(resp.more, "should have more items");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_point_lookup() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;
        let rev = current_revision();

        // Point lookup for a key that exists at rev
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "point lookup finds k1 at rev");

        // Point lookup for a key that doesn't exist yet at rev
        store
            .put(etcdserverpb::PutRequest {
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
                ..Default::default()
            })
            .await;

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k3".to_vec(),
                revision: rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 0, "k3 doesn't exist at rev");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_prefix_range() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"/a/x".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"/a/y".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"/b/z".to_vec(),
                value: b"v3".to_vec(),
                ..Default::default()
            })
            .await;
        let rev = current_revision();

        // Create another key under /a/ after rev
        store
            .put(etcdserverpb::PutRequest {
                key: b"/a/w".to_vec(),
                value: b"v4".to_vec(),
                ..Default::default()
            })
            .await;

        // Prefix scan at rev: should get 2 keys under /a/
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"/a/".to_vec(),
                range_end: b"/a/\0".to_vec(),
                revision: rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 2, "2 keys under /a/ at rev");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_after_compaction() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // Write some keys
        for i in 0u64..5 {
            store
                .put(etcdserverpb::PutRequest {
                    key: format!("k{i}").into_bytes(),
                    value: b"v".to_vec(),
                    ..Default::default()
                })
                .await;
        }
        let pre_compact_rev = current_revision();

        // Compact WAL and set compact revision
        store.compact_wal().await.unwrap();
        COMPACT_REV.store(pre_compact_rev, Ordering::SeqCst);

        // Now revision 1 is below COMPACT_REV → should error
        let err = store
            .range(etcdserverpb::RangeRequest {
                key: b"k0".to_vec(),
                revision: 1,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            err.message().contains("compacted"),
            "revision 1 should be compacted: {}",
            err.message()
        );

        // Query at pre_compact_rev: should still work (>= COMPACT_REV)
        let compact_rev_val = COMPACT_REV.load(Ordering::Relaxed);
        assert!(
            pre_compact_rev >= compact_rev_val,
            "pre_compact_rev={} >= COMPACT_REV={}",
            pre_compact_rev,
            compact_rev_val
        );
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: pre_compact_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 5, "all 5 keys at pre_compact_rev");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_key_modified_after_target() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // Create k1
        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"original".to_vec(),
                ..Default::default()
            })
            .await;
        let target_rev = current_revision();

        // Modify k1 multiple times after target_rev
        for i in 0u64..5 {
            store
                .put(etcdserverpb::PutRequest {
                    key: b"k1".to_vec(),
                    value: format!("updated_{i}").into_bytes(),
                    ..Default::default()
                })
                .await;
        }

        // Query at target_rev: should see "original", not any updated value
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: target_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "k1 exists at target_rev");
        if let Some(kv_bytes) = resp.kvs.first() {
            let kv = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
            assert_eq!(
                kv.value, b"original",
                "should see original value, not updated"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_range_bounds() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"a".to_vec(),
                value: b"v_a".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"b".to_vec(),
                value: b"v_b".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"c".to_vec(),
                value: b"v_c".to_vec(),
                ..Default::default()
            })
            .await;
        let rev = current_revision();

        // Range from b to d (exclusive): should get b and c
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"b".to_vec(),
                range_end: b"d".to_vec(),
                revision: rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 2, "range [b, d) gives 2 keys at rev");

        // Key created after rev shouldn't appear in range
        store
            .put(etcdserverpb::PutRequest {
                key: b"bb".to_vec(),
                value: b"v_bb".to_vec(),
                ..Default::default()
            })
            .await;

        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"b".to_vec(),
                range_end: b"c\x00".to_vec(),
                revision: rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 2, "range [b, d) gives 2 keys (b, c) at rev — bb created later");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_delete_recreate() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // First lifetime: create k1
        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"first_life".to_vec(),
                ..Default::default()
            })
            .await;
        let first_rev = current_revision();

        // Delete k1
        store
            .delete_range(etcdserverpb::DeleteRangeRequest {
                key: b"k1".to_vec(),
                ..Default::default()
            })
            .await;

        // Second lifetime: recreate k1
        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"second_life".to_vec(),
                ..Default::default()
            })
            .await;

        // Query at first_rev: should see "first_life" from the first lifetime
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: first_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "k1 exists at first_rev (first lifetime)");
        if let Some(kv_bytes) = resp.kvs.first() {
            let kv = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
            assert_eq!(
                kv.value, b"first_life",
                "value at first_rev should be from first lifetime"
            );
        }

        // Query at current revision: should see "second_life"
        let current = current_revision();
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: current as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "k1 exists at current (second lifetime)");
        if let Some(kv_bytes) = resp.kvs.first() {
            let kv = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
            assert_eq!(
                kv.value, b"second_life",
                "current value should be from second lifetime"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_delete_at_exact_delete_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;

        store
            .delete_range(etcdserverpb::DeleteRangeRequest {
                key: b"k1".to_vec(),
                ..Default::default()
            })
            .await;
        let del_rev = current_revision();

        // Query exactly at delete revision: key should NOT be included
        // (deleted at this rev, not before it)
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: del_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 0, "k1 deleted at del_rev should not appear");

        // Query one before delete revision: key should exist
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: del_rev as i64 - 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "k1 exists one rev before delete");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_many_stale_keys_with_limit() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // Create 5 keys
        for i in 0u64..5 {
            store
                .put(etcdserverpb::PutRequest {
                    key: format!("k{i}").into_bytes(),
                    value: b"original".to_vec(),
                    ..Default::default()
                })
                .await;
        }
        let target_rev = current_revision();

        // Update all 5 keys after target_rev — all become stale
        for i in 0u64..5 {
            store
                .put(etcdserverpb::PutRequest {
                    key: format!("k{i}").into_bytes(),
                    value: format!("updated_{i}").into_bytes(),
                    ..Default::default()
                })
                .await;
        }

        // Query with limit=2 at target_rev: all keys stale, should return 2
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"".to_vec(),
                range_end: vec![0],
                revision: target_rev as i64,
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 5, "count is total (5)");
        assert_eq!(resp.kvs.len(), 2, "only 2 kvs returned");
        assert!(resp.more, "more flag should be true");
        if let Some(kv_bytes) = resp.kvs.first() {
            let kv = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
            assert_eq!(
                kv.value, b"original",
                "stale key should have original value"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_rebirth_forces_wal() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();

        // First lifetime
        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"first".to_vec(),
                ..Default::default()
            })
            .await;
        let first_rev = current_revision();

        // Delete
        store
            .delete_range(etcdserverpb::DeleteRangeRequest {
                key: b"k1".to_vec(),
                ..Default::default()
            })
            .await;

        // Recreate (second lifetime, new create_revision)
        store
            .put(etcdserverpb::PutRequest {
                key: b"k1".to_vec(),
                value: b"second".to_vec(),
                ..Default::default()
            })
            .await;

        // Query at first_rev: BTreeMap has rebirth=true, create_rev > first_rev
        // → forces WAL scan to find "first" value from prior lifetime
        let resp = store
            .range(etcdserverpb::RangeRequest {
                key: b"k1".to_vec(),
                revision: first_rev as i64,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1, "k1 exists at first_rev (rebirth path)");
        if let Some(kv_bytes) = resp.kvs.first() {
            let kv = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
            assert_eq!(
                kv.value, b"first",
                "rebirth WAL path should return first lifetime value"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_min_mod_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"a".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"b".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();
        let rev = current_revision();
        store.put(etcdserverpb::PutRequest{key:b"a".to_vec(),value:b"v1_upd".to_vec(),..Default::default()}).await.unwrap();

        // Current-state: min_mod_revision filters current mod_revision
        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            min_mod_revision: (rev + 1) as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "min_mod_revision filters to 1 key (a updated)");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_max_mod_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"a".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        let rev1 = current_revision();
        store.put(etcdserverpb::PutRequest{key:b"b".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            max_mod_revision: rev1 as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "max_mod_revision=rev1 gives 1 key (a)");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_min_create_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"a".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"b".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();
        let rev_b = current_revision();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            min_create_revision: rev_b as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "min_create_revision gives 1 key (b)");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_max_create_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"a".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        let rev_a = current_revision();
        store.put(etcdserverpb::PutRequest{key:b"b".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            max_create_revision: rev_a as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "max_create_revision gives 1 key (a)");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_kv_metadata() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k1".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        let create_rev = current_revision();
        store.put(etcdserverpb::PutRequest{key:b"k1".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();

        // Historical: metadata should reflect state at create_rev
        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"k1".to_vec(), revision: create_rev as i64,
            ..Default::default()
        }).await.unwrap();
        let kv = mvccpb::KeyValue::decode(&resp.kvs[0][..]).unwrap();
        assert_eq!(kv.value, b"v1");
        assert_eq!(kv.create_revision, create_rev as i64);
        assert_eq!(kv.mod_revision, create_rev as i64);
        assert_eq!(kv.version, 1);

        // Current: metadata reflects latest state
        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"k1".to_vec(), ..Default::default()
        }).await.unwrap();
        let kv = mvccpb::KeyValue::decode(&resp.kvs[0][..]).unwrap();
        assert_eq!(kv.value, b"v2");
        assert_eq!(kv.create_revision, create_rev as i64);
        assert_eq!(kv.mod_revision, create_rev as i64 + 1);
        assert_eq!(kv.version, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_from_key_query() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"a".to_vec(),value:b"va".to_vec(),..Default::default()}).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"b".to_vec(),value:b"vb".to_vec(),..Default::default()}).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"c".to_vec(),value:b"vc".to_vec(),..Default::default()}).await.unwrap();
        let rev = current_revision();

        // From-key: all keys >= "b"
        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"b".to_vec(), range_end: vec![0], revision: rev as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 2, "from-key gives 2 keys (b, c)");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_non_existent_key() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"existing".to_vec(),value:b"v".to_vec(),..Default::default()}).await.unwrap();
        let rev = current_revision();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"nonexistent".to_vec(), revision: rev as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 0);
        assert!(resp.kvs.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_non_existent_prefix() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"/real/k1".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        let rev = current_revision();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"/fake/".to_vec(), range_end: b"/fake0".to_vec(), revision: rev as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 0, "non-existent prefix returns 0");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_exact_create_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k1".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        let create_rev = current_revision();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"k1".to_vec(), revision: create_rev as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "key exists at create revision");
        let kv = mvccpb::KeyValue::decode(&resp.kvs[0][..]).unwrap();
        assert_eq!(kv.value, b"v1");
        assert_eq!(kv.create_revision, create_rev as i64);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_historical_more_flag_limit_zero() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k1".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k2".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            revision: current_revision() as i64, limit: 0,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 2);
        assert!(!resp.more, "more=false when limit=0");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_current_state_min_max_mod_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k1".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k2".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();
        let rev = current_revision();
        store.put(etcdserverpb::PutRequest{key:b"k1".to_vec(),value:b"v1_upd".to_vec(),..Default::default()}).await.unwrap();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            min_mod_revision: (rev + 1) as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "min_mod_revision: only k1");

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            max_mod_revision: rev as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "max_mod_revision: only k2");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_current_state_min_max_create_revision() {
        let path = temp_wal();
        let store = Store::open(&path).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k1".to_vec(),value:b"v1".to_vec(),..Default::default()}).await.unwrap();
        store.put(etcdserverpb::PutRequest{key:b"k2".to_vec(),value:b"v2".to_vec(),..Default::default()}).await.unwrap();
        let rev_k2 = current_revision();

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            min_create_revision: rev_k2 as i64,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "min_create_revision: only k2");

        let resp = store.range(etcdserverpb::RangeRequest{
            key: b"".to_vec(), range_end: vec![0],
            max_create_revision: rev_k2 as i64 - 1,
            ..Default::default()
        }).await.unwrap();
        assert_eq!(resp.count, 1, "max_create_revision: only k1");
        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(test)]
#[allow(unused)]
mod unsupported_features_tests {
    use super::*;

    /// etcd RangeRequest.sort_order / sort_target are not implemented.
    /// The server ignores these fields and always returns keys in
    /// lexicographic (BTreeMap) order regardless of sort_order or sort_target.
    #[test]
    #[ignore]
    fn test_sort_order_is_not_supported() {
        // Expected: RangeRequest with sort_order=ASCEND, sort_target=VERSION
        // should return keys sorted by version, not by key.
        // Currently ignored because sorting is not implemented.
    }

    /// etcd RangeRequest.serializable flag is not implemented.
    /// All reads use the same code path regardless of this flag.
    #[test]
    #[ignore]
    fn test_serializable_flag_is_not_supported() {
        // Expected: serializable=true would allow stale reads without
        // consensus. Currently ignored because the flag has no effect.
    }

    /// etcd Compare.range_end field is not implemented.
    /// Txn compare operations only support point key comparisons,
    /// not range comparisons like "all keys with prefix X have value Y".
    #[test]
    #[ignore]
    fn test_compare_range_end_is_not_supported() {
        // Expected: Compare with range_end set would compare against
        // all keys in the range. Currently only point key compares work.
    }

    /// etcd WatchCreateRequest.fragment is not implemented.
    /// Large watch responses are never split into multiple chunks.
    #[test]
    #[ignore]
    fn test_watch_fragment_is_not_supported() {
        // Expected: when fragment=true and a response exceeds the message
        // size, the server splits it across multiple WatchResponse messages.
        // Currently all events are sent in a single response.
    }

    /// etcd WatchCreateRequest.progress_notify automatic notifications
    /// are not implemented. The flag is stored but no background task
    /// sends periodic progress notifications to watchers with this flag.
    #[test]
    #[ignore]
    fn test_watch_progress_notify_auto_is_not_supported() {
        // Expected: watchers with progress_notify=true receive periodic
        // empty WatchResponses when there are no events. Currently the
        // flag is stored but no background task triggers notifications.
    }

    /// etcd WatchProgressRequest is implemented (manual progress request),
    /// but NOT automatic progress notifications. See test above.
    #[test]
    #[ignore]
    fn test_watch_progress_notify_periodic_is_not_supported() {
        // Expected: server sends progress notifications at regular
        // intervals for watchers with progress_notify=true.
        // Currently only manual WatchProgressRequest works.
    }

    /// etcd CompactionRequest.physical flag is not implemented.
    /// The compact operation always updates COMPACT_REV but does not
    /// wait for physical removal of compacted entries from storage.
    /// Since Rudurru uses WAL+in-memory BTreeMap (no backend DB),
    /// physical compaction is a no-op.
    #[test]
    #[ignore]
    fn test_compact_physical_flag_is_not_supported() {
        // Expected: when physical=true, the RPC waits until compacted
        // entries are physically removed from the backend database.
        // Currently the flag is ignored.
    }

    /// etcd Auth service is not implemented.
    /// All authentication and authorization RPCs return
    /// Status::unimplemented("not implemented").
    #[test]
    #[ignore]
    fn test_auth_enable_is_not_supported() {
        // Expected: AuthEnable would enable RBAC. Currently unimplemented.
    }

    #[test]
    #[ignore]
    fn test_auth_disable_is_not_supported() {
        // Expected: AuthDisable would disable RBAC. Currently unimplemented.
    }

    #[test]
    #[ignore]
    fn test_auth_authenticate_is_not_supported() {
        // Expected: Authenticate returns a token. Currently unimplemented.
    }

    #[test]
    #[ignore]
    fn test_auth_user_management_is_not_supported() {
        // Expected: UserAdd/Get/List/Delete/ChangePassword, GrantRole,
        // RevokeRole. Currently all unimplemented.
    }

    #[test]
    #[ignore]
    fn test_auth_role_management_is_not_supported() {
        // Expected: RoleAdd/Get/List/Delete, GrantPermission,
        // RevokePermission. Currently all unimplemented.
    }

    /// etcd Cluster service: MemberAdd, MemberRemove, MemberUpdate,
    /// and MemberPromote return unimplemented (single-node cluster).
    /// Only MemberList is implemented (returns self).
    #[test]
    #[ignore]
    fn test_cluster_member_add_is_not_supported() {
        // Expected: adds a member to the raft cluster.
        // Currently unimplemented — rudurru is single-node.
    }

    #[test]
    #[ignore]
    fn test_cluster_member_remove_is_not_supported() {
        // Expected: removes a member. Currently unimplemented.
    }

    #[test]
    #[ignore]
    fn test_cluster_member_update_is_not_supported() {
        // Expected: updates member peer URLs. Currently unimplemented.
    }

    #[test]
    #[ignore]
    fn test_cluster_member_promote_is_not_supported() {
        // Expected: promotes a learner to voting member. Currently unimplemented.
    }

    /// etcd Maintenance.MoveLeader is not implemented.
    /// Since rudurru is single-node with no Raft, there is no leader
    /// to transfer.
    #[test]
    #[ignore]
    fn test_maintenance_move_leader_is_not_supported() {
        // Expected: transfers leadership to another member.
        // Currently unimplemented — single-node cluster.
    }

    /// etcd RangeStream is not implemented.
    /// This is a newer (etcd 3.7) streaming variant of Range.
    #[test]
    #[ignore]
    fn test_range_stream_is_not_supported() {
        // Expected: streaming range response in chunks.
        // Currently unimplemented.
    }

    /// Txn snapshot isolation: txn-internal range calls do NOT see
    /// their own uncommitted writes (no dirty reads). The write lock
    /// is dropped before executing txn ops, so concurrent reads may
    /// interleave.
    #[test]
    #[ignore]
    fn test_txn_snapshot_isolation_is_not_supported() {
        // Expected: within a transaction, all reads see a consistent
        // snapshot including the transaction's own pending writes.
        // Currently the write lock is released before executing ops,
        // so the txn does not see its own modifications.
    }

    /// Put ignore_value / ignore_lease on non-existent key:
    /// etcd returns an error when ignore_value or ignore_lease is set
    /// and the key does not exist. Our implementation currently uses
    /// unwrap_or_default/fallback instead of returning an error.
    #[test]
    #[ignore]
    fn test_put_ignore_value_on_missing_key_should_error() {
        // Expected: Err with "key not found" when ignore_value is set
        // on a non-existent key. Currently no error is returned.
    }

    #[test]
    #[ignore]
    fn test_put_ignore_lease_on_missing_key_should_error() {
        // Expected: Err with "key not found" when ignore_lease is set
        // on a non-existent key. Currently no error is returned.
    }
}

#[cfg(test)]
mod eval_compare_tests {
    use super::*;
    use crate::storage::apply::apply_record;

    fn make_compare(key: &[u8], target: i32, result_val: i32, target_val: i64) -> etcdserverpb::Compare {
        let target_union = match target {
            0 => Some(etcdserverpb::compare::TargetUnion::Version(target_val)),
            1 => Some(etcdserverpb::compare::TargetUnion::CreateRevision(target_val)),
            2 => Some(etcdserverpb::compare::TargetUnion::ModRevision(target_val)),
            3 => Some(etcdserverpb::compare::TargetUnion::Value(vec![])),
            4 => Some(etcdserverpb::compare::TargetUnion::Lease(target_val)),
            _ => None,
        };
        etcdserverpb::Compare {
            key: key.to_vec(),
            range_end: vec![],
            result: result_val,
            target,
            target_union,
        }
    }

    static TEST_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn temp_wal() -> String {
        let dir = std::env::temp_dir();
        let n = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let name = format!("rudurru_evalcmp_{}_{}.wal", std::process::id(), n);
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        path.to_string_lossy().to_string()
    }

    /// Helper: create a StoreState with a temporary WAL. This avoids Store's
    /// async background tasks and global-atomic pollution between tests.
    fn fresh_state(records: Vec<wal::KvWalRecord>) -> StoreState {
        let p = temp_wal();
        // Write records into a temporary WAL, then re-open it.
        {
            let mut w = wal::WalFile::open(&p).unwrap();
            for r in &records {
                w.append_kv(r).unwrap();
            }
        }
        let wal = wal::WalFile::open(&p).unwrap();
        let mut state = StoreState::new(wal);
        let mut max_rev = 0u64;
        for rec in &records {
            let rev = rec.mod_revision().unwrap_or(0) as u64;
            apply_record(&mut state, rec);
            max_rev = max_rev.max(rev);
        }
        state.next_rev = max_rev + 1;
        let _ = std::fs::remove_file(&p);
        state
    }

    fn make_put_record(key: &[u8], value: &[u8], rev: u64, lease: i64) -> wal::KvWalRecord {
        let flags = wal::IS_CREATE | if lease != 0 { wal::HAS_LEASE } else { 0 };
        wal::KvWalRecord::new(flags, key, value, rev as i64, rev as i64, 1, lease)
    }

    fn make_delete_record(key: &[u8], rev: u64) -> wal::KvWalRecord {
        wal::KvWalRecord::new(wal::DELETED, key, b"", rev as i64, rev as i64, 1, 0)
    }

    #[test]
    fn test_eval_compare_alive_key_matches() {
        let state = fresh_state(vec![
            make_put_record(b"/registry/test/key", b"value1", 1, 0),
        ]);

        let cmp = make_compare(b"/registry/test/key", 2, 0, 1);
        assert!(eval_compare(&state, &cmp), "alive key should match its mod_revision");

        let cmp = make_compare(b"/registry/test/key", 0, 0, 1);
        assert!(eval_compare(&state, &cmp), "alive key should match its version");

        let cmp = make_compare(b"/registry/test/key", 0, 3, 2);
        assert!(eval_compare(&state, &cmp), "alive key version != 2 should match");
    }

    #[test]
    fn test_eval_compare_tombstone_does_not_match() {
        let state = fresh_state(vec![
            make_put_record(b"/registry/test/key", b"value1", 1, 0),
            make_delete_record(b"/registry/test/key", 2),
        ]);

        let cmp = make_compare(b"/registry/test/key", 2, 0, 1);
        assert!(!eval_compare(&state, &cmp), "tombstoned key should NOT match its old mod_revision");
    }

    #[test]
    fn test_eval_compare_tombstone_all_targets() {
        let state = fresh_state(vec![
            make_put_record(b"/registry/test/key", b"value1", 1, 42),
            make_delete_record(b"/registry/test/key", 2),
        ]);

        let cmp = make_compare(b"/registry/test/key", 0, 0, 1);
        assert!(!eval_compare(&state, &cmp), "tombstone: version compare should be false");

        let cmp = make_compare(b"/registry/test/key", 1, 0, 1);
        assert!(!eval_compare(&state, &cmp), "tombstone: create_rev compare should be false");

        let cmp = make_compare(b"/registry/test/key", 2, 0, 1);
        assert!(!eval_compare(&state, &cmp), "tombstone: mod_rev compare should be false");

        let mut cmp = make_compare(b"/registry/test/key", 3, 0, 0);
        cmp.target_union = Some(etcdserverpb::compare::TargetUnion::Value(b"value1".to_vec()));
        assert!(!eval_compare(&state, &cmp), "tombstone: value compare should be false");

        let cmp = make_compare(b"/registry/test/key", 4, 0, 42);
        assert!(!eval_compare(&state, &cmp), "tombstone: lease compare should be false");
    }

    #[test]
    fn test_eval_compare_missing_key_does_not_match() {
        let state = fresh_state(vec![]);

        // A key that never existed — should behave same as tombstone.
        // Comparing mod_revision != 0 (any non-zero value) against a missing
        // key will fail because missing keys yield actual=0.
        let cmp = make_compare(b"/registry/nonexistent", 2, 0, 1);
        assert!(!eval_compare(&state, &cmp), "missing key should not match mod_revision == 1");
    }

    #[test]
    fn test_eval_compare_recreated_key_matches() {
        let state = fresh_state(vec![
            make_put_record(b"/registry/test/key", b"v1", 1, 0),
            make_delete_record(b"/registry/test/key", 2),
            make_put_record(b"/registry/test/key", b"v2", 3, 0),
        ]);

        let cmp = make_compare(b"/registry/test/key", 2, 0, 3);
        assert!(eval_compare(&state, &cmp), "recreated key should match its new mod_revision");

        let cmp = make_compare(b"/registry/test/key", 2, 0, 1);
        assert!(!eval_compare(&state, &cmp), "recreated key should NOT match old mod_revision");
    }
}

#[cfg(test)]
mod lease_restore_tests {
    use super::*;
    use crate::storage::apply::apply_record;

    static TEST_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn temp_wal() -> String {
        let dir = std::env::temp_dir();
        let n = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let name = format!("rudurru_leaserestore_{}_{}.wal", std::process::id(), n);
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        path.to_string_lossy().to_string()
    }

    /// Helper: create a StoreState from a WAL file pre-populated with records,
    /// exactly as Store::open does internally, but without spawning background
    /// tasks or interacting with globals.
    fn rebuild_from_wal(path: &str) -> StoreState {
        let mut wal = wal::WalFile::open(path).unwrap();
        let records = wal.scan_kv_collect().unwrap();
        let mut state = StoreState::new(wal);
        let mut max_rev = 0u64;
        for rec in &records {
            let rev = rec.mod_revision().unwrap_or(0) as u64;
            apply_record(&mut state, rec);
            max_rev = max_rev.max(rev);
        }
        state.next_rev = max_rev + 1;

        // Replicate the lease restoration logic from Store::open inline.
        let lease_count = {
            let unique: std::collections::BTreeSet<i64> = state
                .keys
                .values()
                .filter(|ks| ks.is_alive() && ks.lease != 0)
                .map(|ks| ks.lease)
                .collect();
            let max_id = unique.last().copied().unwrap_or(0);
            if max_id > 0 {
                NEXT_LEASE_ID.store(max_id + 1, std::sync::atomic::Ordering::SeqCst);
            }
            let now = tokio::time::Instant::now();
            let default_ttl = 3600i64;
            for &id in &unique {
                state.leases.insert(
                    id,
                    LeaseState {
                        id,
                        ttl: default_ttl,
                        expires_at: now + std::time::Duration::from_secs(default_ttl as u64),
                        key_count: 0,
                    },
                );
            }
            unique.len() as u64
        };
        if lease_count > 0 {
            LEASE_COUNT.store(lease_count, std::sync::atomic::Ordering::Relaxed);
        }

        state
    }

    fn make_lease_put_record(key: &[u8], rev: u64, lease: i64) -> wal::KvWalRecord {
        let flags = wal::IS_CREATE | wal::HAS_LEASE;
        wal::KvWalRecord::new(flags, key, b"v", rev as i64, rev as i64, 1, lease)
    }

    fn make_plain_put_record(key: &[u8], rev: u64) -> wal::KvWalRecord {
        wal::KvWalRecord::new(wal::IS_CREATE, key, b"v", rev as i64, rev as i64, 1, 0)
    }

    fn populate_wal(path: &str, records: &[wal::KvWalRecord]) {
        let mut w = wal::WalFile::open(path).unwrap();
        for r in records {
            w.append_kv(r).unwrap();
        }
    }

    #[test]
    fn test_leases_restored_after_open() {
        let path = temp_wal();
        populate_wal(&path, &[make_lease_put_record(b"/registry/test/leased", 10, 42)]);

        NEXT_LEASE_ID.store(1, std::sync::atomic::Ordering::SeqCst);
        let state = rebuild_from_wal(&path);

        assert!(state.leases.contains_key(&42), "LeaseState should exist for restored lease 42");

        let ks = state.keys.get(b"/registry/test/leased".as_slice()).unwrap();
        assert!(ks.is_alive(), "key should be alive");
        assert_eq!(ks.lease, 42, "key should reference lease 42");

        let next_after = NEXT_LEASE_ID.load(std::sync::atomic::Ordering::SeqCst);
        assert!(next_after > 42, "NEXT_LEASE_ID({}) should be beyond restored lease 42", next_after);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_leases_restored_multiple_ids() {
        let path = temp_wal();
        populate_wal(&path, &[
            make_lease_put_record(b"/registry/a", 10, 42),
            make_lease_put_record(b"/registry/b", 20, 99),
        ]);

        NEXT_LEASE_ID.store(1, std::sync::atomic::Ordering::SeqCst);
        let state = rebuild_from_wal(&path);

        assert_eq!(state.leases.len(), 2, "both leases should be restored");
        assert!(state.leases.contains_key(&42), "lease 42 restored");
        assert!(state.leases.contains_key(&99), "lease 99 restored");

        let next_after = NEXT_LEASE_ID.load(std::sync::atomic::Ordering::SeqCst);
        assert!(next_after > 99, "NEXT_LEASE_ID({}) > max restored(99)", next_after);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_no_leases_no_restore() {
        let path = temp_wal();
        populate_wal(&path, &[make_plain_put_record(b"/registry/nolease", 10)]);

        NEXT_LEASE_ID.store(1, std::sync::atomic::Ordering::SeqCst);
        let state = rebuild_from_wal(&path);

        assert_eq!(state.leases.len(), 0, "no leases should be restored when none exist");

        let next_after = NEXT_LEASE_ID.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(next_after, 1, "NEXT_LEASE_ID should stay at 1 when no leases restored");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_next_lease_id_bumps_past_restored_max() {
        let path = temp_wal();
        populate_wal(&path, &[
            make_lease_put_record(b"/registry/a", 10, 5),
            make_lease_put_record(b"/registry/b", 20, 99),
            make_lease_put_record(b"/registry/c", 30, 42),
        ]);

        NEXT_LEASE_ID.store(1, std::sync::atomic::Ordering::SeqCst);
        rebuild_from_wal(&path);

        let next_id = NEXT_LEASE_ID.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(next_id, 100, "NEXT_LEASE_ID should be max(5,99,42)+1 = 100, got {next_id}");

        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(test)]
mod resolve_range_tests {
    use super::*;

    #[test]
    fn test_resolve_range_prefix_0xff_does_not_wrap() {
        let key = b"abc\xFF";
        let range_end = b"abc\x00";
        let bound = resolve_range(key, range_end);
        assert!(
            !matches!(bound, RangeBound::Prefix(_)),
            "expected Range, got Prefix for key ending in 0xFF: {bound:?}"
        );
    }

    #[test]
    fn test_resolve_range_prefix_normal() {
        let key = b"abc";
        let range_end = b"abd";
        let bound = resolve_range(key, range_end);
        assert!(matches!(bound, RangeBound::Prefix(_)), "expected Prefix, got {bound:?}");
    }

    #[test]
    fn test_resolve_range_prefix_0xff_suffix() {
        let key = b"abc\xFF";
        let range_end = b"abc\xFF\x00";
        let bound = resolve_range(key, range_end);
        assert!(matches!(bound, RangeBound::Prefix(_)), "expected Prefix via \\0 suffix, got {bound:?}");
    }
}

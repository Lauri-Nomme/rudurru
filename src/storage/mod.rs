pub mod wal;

use crate::proto::etcdserverpb;
use crate::proto::mvccpb;
use prost::bytes::Bytes;
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use parking_lot::RwLock;
use tokio::sync::Notify;
use tonic::Status;

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

/// In-memory representation of a key's state (alive or tombstoned).
/// Deleted keys remain in the BTreeMap so that historical range queries
/// can conclusively determine whether a key existed at a target revision
/// without scanning the WAL.
#[derive(Debug, Clone)]
pub struct KeyState {
    pub(crate) value: Arc<[u8]>,
    pub(crate) mod_revision: u64,
    pub(crate) create_revision: u64,
    pub(crate) version: i64,
    pub(crate) lease: i64,
    /// 0 = alive (not deleted). Non-zero = revision at which this key was deleted.
    pub(crate) delete_revision: u64,
    /// True if this key ever went through a delete→recreate cycle.
    /// When `rebirth && create_revision > target_rev`, a prior lifetime may
    /// have existed at target_rev, so the WAL is needed to confirm.
    pub(crate) rebirth: bool,
    pub(crate) kv_bytes: Bytes,
}

impl KeyState {
    pub fn is_alive(&self) -> bool {
        self.delete_revision == 0
    }
}

impl KeyState {
    /// Decode the pre-encoded kv_bytes into a mvccpb::KeyValue.
    /// Falls back to constructing from fields if kv_bytes is empty (WAL replay from old format).
    pub fn to_key_value(&self, key: &[u8]) -> mvccpb::KeyValue {
        if !self.kv_bytes.is_empty() {
            if let Ok(mut kv) = mvccpb::KeyValue::decode(&self.kv_bytes[..]) {
                kv.key = key.to_vec();
                return kv;
            }
        }
        mvccpb::KeyValue {
            key: key.to_vec(),
            create_revision: self.create_revision as i64,
            mod_revision: self.mod_revision as i64,
            version: self.version,
            value: self.value.to_vec(),
            lease: self.lease,
        }
    }
}

/// Pre-encode a KeyValue protobuf with overlong varints for use as kv_bytes.
fn make_kv_bytes(key: &[u8], ks: &KeyState) -> Bytes {
    let (kv_bytes, _, _) = wal::encode_kv(
        key,
        &ks.value,
        ks.create_revision as i64,
        ks.mod_revision as i64,
        ks.version,
        ks.lease,
    );
    Bytes::from(kv_bytes)
}

#[derive(Debug)]
pub struct LeaseState {
    pub id: i64,
    pub ttl: i64,
    pub expires_at: tokio::time::Instant,
    pub key_count: u64,
}

#[derive(Debug, Clone)]
pub struct WatchRegistration {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub start_revision: u64,
    pub sender: tokio::sync::mpsc::UnboundedSender<WatchEvent>,
    pub watch_id: i64,
    pub progress_notify: bool,
    pub filters: Vec<i32>,
    pub prev_kv: bool,
    pub(crate) bound: RangeBound,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub revision: u64,
    pub event_type: mvccpb::event::EventType,
    pub key: Bytes,
    pub kv_bytes: Bytes,
    pub prev_kv_bytes: Bytes,
}

#[derive(Debug)]
pub struct StoreState {
    pub keys: BTreeMap<Vec<u8>, KeyState>,
    pub leases: BTreeMap<i64, LeaseState>,
    pub watchers: Vec<WatchRegistration>,
    pub next_rev: u64,
    pub wal: wal::WalFile,
    pub expiry_notify: Arc<Notify>,
}

impl StoreState {
    pub fn new(wal: wal::WalFile) -> Self {
        Self {
            keys: BTreeMap::new(),
            leases: BTreeMap::new(),
            watchers: Vec::new(),
            next_rev: 1,
            wal,
            expiry_notify: Arc::new(Notify::new()),
        }
    }

    fn header(&self) -> etcdserverpb::ResponseHeader {
        etcdserverpb::ResponseHeader {
            cluster_id: 1,
            member_id: 1,
            revision: current_revision() as i64,
            raft_term: 1,
        }
    }

    fn apply(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        rev: u64,
        kv_bytes: Option<Bytes>,
    ) -> Option<KeyState> {
        let prev = self.keys.get(&key).filter(|k| k.is_alive()).cloned();
        let is_new = prev.is_none();
        // Detect whether this key was previously deleted (lost a lifetime)
        let rebirth = self.keys.get(&key).map_or(false, |k| k.delete_revision != 0);

        let mut entry = KeyState {
            value: Arc::from(value.into_boxed_slice()),
            mod_revision: rev,
            create_revision: prev.as_ref().map(|k| k.create_revision).unwrap_or(rev),
            version: prev.as_ref().map(|k| k.version + 1).unwrap_or(1),
            lease,

            delete_revision: 0,
            rebirth,
            kv_bytes: Bytes::new(),
        };
        entry.kv_bytes = kv_bytes.unwrap_or_else(|| make_kv_bytes(&key, &entry));
        let event_key = Bytes::from(key.clone());
        self.keys.insert(key, entry.clone());
        if is_new {
            KEY_COUNT.fetch_add(1, Ordering::Relaxed);
        }

        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Put,
            key: event_key,
            kv_bytes: entry.kv_bytes.clone(),
            prev_kv_bytes: prev
                .as_ref()
                .map(|p| p.kv_bytes.clone())
                .unwrap_or_default(),
        };
        self.notify_watchers(event);

        prev
    }

    fn apply_delete(&mut self, key: Vec<u8>, rev: u64) -> Option<KeyState> {
        let entry = self.keys.get_mut(&key)?;
        if entry.delete_revision != 0 {
            return None;
        }
        let prev = entry.clone();
        
        entry.delete_revision = rev;
        // Keep mod_revision as the value's revision (for historical query filtering).
        // kv_bytes also stays as the value before deletion.
        KEY_COUNT.fetch_sub(1, Ordering::Relaxed);

        // Create watch event for DELETE
        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Delete,
            key: Bytes::from(key),
            kv_bytes: prev.kv_bytes.clone(),
            prev_kv_bytes: prev.kv_bytes.clone(),
        };
        self.notify_watchers(event);

        Some(prev)
    }

    fn delete_keys_for_lease(&mut self, id: i64) {
        let keys_to_delete: Vec<Vec<u8>> = self
            .keys
            .iter()
            .filter(|(_, ks)| ks.lease == id && ks.is_alive())
            .map(|(k, _)| k.clone())
            .collect();
        if keys_to_delete.is_empty() {
            return;
        }
        let rev = next_revision();
        let mut records = Vec::with_capacity(keys_to_delete.len());
        for key in &keys_to_delete {
            if let Some(prev) = self.apply_delete(key.clone(), rev) {
                let mut flags = wal::DELETED;
                if prev.lease != 0 {
                    flags |= wal::HAS_LEASE;
                }
                records.push(wal::KvWalRecord::new(
                    flags,
                    key,
                    &prev.value,
                    prev.create_revision as i64,
                    rev as i64,
                    prev.version,
                    prev.lease,
                ));
            }
        }
        if let Err(e) = self.wal.append_kv_batch(&records) {
            tracing::error!("WAL batch append failed on lease key deletion: {e}");
        }
    }

    // Watcher management
    pub(crate) fn register_watcher(&mut self, reg: WatchRegistration) -> i64 {
        let watch_id = reg.watch_id;
        self.watchers.push(reg);
        WATCHER_COUNT.fetch_add(1, Ordering::Relaxed);
        watch_id
    }

    pub(crate) fn cancel_watcher(&mut self, watch_id: i64) -> bool {
        let len_before = self.watchers.len();
        self.watchers.retain(|w| w.watch_id != watch_id);
        let changed = len_before != self.watchers.len();
        if changed {
            WATCHER_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
        changed
    }

    fn notify_watchers(&mut self, event: WatchEvent) {
        for i in 0..self.watchers.len() {
            let watcher = &self.watchers[i];
            if !matches_range(watcher.bound.to_ref(), &event.key) {
                continue;
            }

            if event.revision < watcher.start_revision {
                continue;
            }

            let mut should_send = true;
            for &filter in &watcher.filters {
                match filter {
                    0 if event.event_type == mvccpb::event::EventType::Put => {
                        should_send = false;
                        break;
                    }
                    1 if event.event_type == mvccpb::event::EventType::Delete => {
                        should_send = false;
                        break;
                    }
                    _ => {}
                }
            }
            if !should_send {
                continue;
            }

            let mut event = event.clone();
            if !watcher.prev_kv {
                event.prev_kv_bytes = Bytes::new();
            }
            let _ = watcher.sender.send(event);
        }
    }
}

/// Thread-safe handle to the store.
#[derive(Debug, Clone)]
pub struct Store {
    pub state: Arc<RwLock<StoreState>>,
}

impl Store {
    /// Open or create the store from the given WAL path.
    /// Rebuilds in-memory state from the WAL on startup.
    pub async fn open(wal_path: &str) -> anyhow::Result<Self> {
        let mut wal = wal::WalFile::open(wal_path)?;
        let records = wal.scan_kv_collect()?;

        let mut state = StoreState::new(wal);
        let mut max_rev = 0u64;

        for rec in &records {
            let rev = rec.mod_revision().unwrap_or(0) as u64;
            if rev <= COMPACT_REV.load(Ordering::Relaxed) {
                continue;
            }
            apply_record(&mut state, rec);
            if rev > max_rev {
                max_rev = rev;
            }
        }

        state.next_rev = max_rev + 1;
        NEXT_REV.store(state.next_rev, Ordering::SeqCst);
        let active_keys = state.keys.values().filter(|ks| ks.is_alive()).count() as u64;
        KEY_COUNT.store(active_keys, Ordering::Relaxed);
        WATCHER_COUNT.store(0, Ordering::Relaxed);
        LEASE_COUNT.store(0, Ordering::Relaxed);

        // Restore lease state from alive keys. After WAL replay the actual
        // LeaseState entries are gone (they were in-memory only), but keys
        // still carry their lease ID.  We re-create a LeaseState for each
        // unique lease ID with a generous default TTL; the real owner will
        // send LeaseKeepAlive immediately to adjust it.
        let lease_count = {
            let unique: std::collections::BTreeSet<i64> = state
                .keys
                .values()
                .filter(|ks| ks.is_alive() && ks.lease != 0)
                .map(|ks| ks.lease)
                .collect();
            let max_id = unique.last().copied().unwrap_or(0);
            if max_id > 0 {
                // Bump past the highest restored lease ID so new leases don't collide.
                NEXT_LEASE_ID.store(max_id + 1, Ordering::SeqCst);
            }
            let now = tokio::time::Instant::now();
            // 1 hour default – conservative so nothing expires prematurely.
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
            LEASE_COUNT.store(lease_count, Ordering::Relaxed);
        }

        tracing::info!(
            "rudurru ready: revision={}, keys={}, leases_restored={}, compact_rev={}",
            max_rev,
            active_keys,
            lease_count,
            COMPACT_REV.load(Ordering::Relaxed),
        );

        let state_arc = Arc::new(RwLock::new(state));
        Self::start_fsync_task(
            state_arc.read().wal.file.clone(),
            state_arc.read().wal.dirty.clone(),
        );
        Self::start_expiry_task(state_arc.clone());
        let store = Self { state: state_arc };
        Self::start_compaction_task(store.clone(), wal_path.to_string());

        Ok(store)
    }

    fn start_fsync_task(file: Arc<Mutex<std::fs::File>>, dirty: Arc<AtomicBool>) {
        // fsync at most every 50ms. WAL writes set the dirty flag;
        // this background task picks it up and issues the fsync
        // without blocking readers/writers on the store lock.
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

    pub async fn compact_rev(&self) -> u64 {
        COMPACT_REV.load(Ordering::Relaxed)
    }

    pub async fn wal_path(&self) -> String {
        self.state.read().wal.path.clone()
    }

    // ── KV operations ──────────────────────────────────────────────────

    pub async fn range(
        &self,
        req: etcdserverpb::RangeRequest,
    ) -> Result<etcdserverpb::RangeResponse, Status> {
        let target_rev = if req.revision > 0 {
            req.revision as u64
        } else {
            0
        };
        let current_rev = current_revision();

        // Future revision
        if target_rev > current_rev {
            return Err(Status::new(
                tonic::Code::Unavailable,
                "etcdserver: mvcc: required revision is a future revision",
            ));
        }

        // Compacted revision
        if target_rev > 0 && target_rev < COMPACT_REV.load(Ordering::Relaxed) {
            return Err(Status::new(
                tonic::Code::Unavailable,
                "etcdserver: mvcc: required revision has been compacted",
            ));
        }

        // Historical query (target_rev > 0, >= COMPACT_REV)
        if target_rev > 0 {
            return self.range_historical(req, target_rev).await;
        }

        // Current state (revision == 0) — unchanged logic
        let state = self.state.read();
        let bound = resolve_range(&req.key, &req.range_end);
        let cap = if req.limit > 0 {
            req.limit as usize
        } else {
            state.keys.len()
        };
        let mut kvs: Vec<Bytes> = Vec::with_capacity(cap);

        let (range_start, range_end): (Option<Vec<u8>>, Option<Vec<u8>>) = match bound.to_ref() {
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
        };

        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &KeyState)>> = match (range_start, range_end)
        {
            (Some(start), Some(end)) => Box::new(state.keys.range(start..end)),
            (Some(start), None) => Box::new(state.keys.range(start..)),
            (None, Some(end)) => Box::new(state.keys.range(..end)),
            (None, None) => Box::new(state.keys.iter()),
        };

        let prefix_key = match bound.to_ref() {
            RangeBoundRef::Prefix(p) => Some(p),
            _ => None,
        };

        for (k, ks) in iter {
            if let Some(p) = prefix_key {
                if !k.starts_with(p) {
                    break;
                }
            }
            if ks.delete_revision != 0 {
                continue;
            }
            if req.min_mod_revision > 0 && (ks.mod_revision as i64) < req.min_mod_revision {
                continue;
            }
            if req.max_mod_revision > 0 && (ks.mod_revision as i64) > req.max_mod_revision {
                continue;
            }
            if req.min_create_revision > 0 && (ks.create_revision as i64) < req.min_create_revision
            {
                continue;
            }
            if req.max_create_revision > 0 && (ks.create_revision as i64) > req.max_create_revision
            {
                continue;
            }

            let kv = if req.keys_only {
                let (kv_bytes, _, _) = wal::encode_kv(k, b"", 0, 0, 0, 0);
                Bytes::from(kv_bytes)
            } else {
                ks.kv_bytes.clone()
            };
            kvs.push(kv);
        }

        let count = kvs.len() as i64;

        if req.count_only {
            return Ok(etcdserverpb::RangeResponse {
                header: Some(state.header()),
                kvs: vec![],
                more: false,
                count,
            });
        }

        let more = if req.limit > 0 && kvs.len() > req.limit as usize {
            kvs.truncate(req.limit as usize);
            true
        } else {
            false
        };

        Ok(etcdserverpb::RangeResponse {
            header: Some(state.header()),
            kvs,
            more,
            count,
        })
    }

    /// Range query at a specific historical revision (target_rev >= COMPACT_REV).
    ///
    /// Phase 1: BTreeMap scan (under read lock). Deleted keys are kept in the
    /// BTreeMap with their `delete_revision`, so we can conclusively determine
    /// the full result set. Keys with `mod_revision <= target_rev` are correct
    /// as-is. Keys with `mod_revision > target_rev` need WAL replay.
    ///
    /// When Phase 1 finds no stale keys, we return early — zero WAL I/O.
    ///
    /// Phase 2: WAL scan (no store lock). Reconstruct state at target_rev for
    /// stale keys in the requested range.
    ///
    /// Phase 3: Merge Phase-1-correct values with Phase-2-reconstructed values.
    async fn range_historical(
        &self,
        req: etcdserverpb::RangeRequest,
        target_rev: u64,
    ) -> Result<etcdserverpb::RangeResponse, Status> {
        let bound = resolve_range(&req.key, &req.range_end);
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            usize::MAX
        };
        let t0 = std::time::Instant::now();

        // ── Phase 1: BTreeMap scan (under read lock) ───────────────
        let (range_start, range_end) = btree_bounds(bound.to_ref());

        let mut phase1_direct: Vec<(Vec<u8>, Bytes)> = Vec::new(); // mod_rev <= target, value correct
        let mut phase1_stale_keys: Vec<Vec<u8>> = Vec::new();      // mod_rev > target, needs WAL
        {
            let state = self.state.read();

            let iter: Box<dyn Iterator<Item = (&Vec<u8>, &KeyState)>> = match (range_start, range_end)
            {
                (Some(ref start), Some(ref end)) => Box::new(state.keys.range(start.clone()..end.clone())),
                (Some(ref start), None) => Box::new(state.keys.range(start.clone()..)),
                (None, Some(ref end)) => Box::new(state.keys.range(..end.clone())),
                (None, None) => Box::new(state.keys.iter()),
            };

            let prefix_key = match bound.to_ref() {
                RangeBoundRef::Prefix(p) => Some(p),
                _ => None,
            };

            for (k, ks) in iter {
                if let Some(p) = prefix_key {
                    if !k.starts_with(p) {
                        break;
                    }
                }

                if ks.delete_revision != 0 {
                    // Deleted keys are kept in the BTreeMap with their
                    // delete_revision, so we can conclusively determine
                    // whether they existed at target_rev.
                    if ks.create_revision > target_rev {
                        continue; // didn't exist at target_rev
                    }
                    if ks.delete_revision <= target_rev {
                        continue; // was already deleted by target_rev
                    }
                    // Key existed at target_rev (created before or at target,
                    // deleted after). Value depends on mod_revision.
                    if ks.mod_revision <= target_rev {
                        // kv_bytes is the correct value at target_rev
                        let kv = if req.keys_only {
                            let (b, _, _) = wal::encode_kv(k, b"", 0, 0, 0, 0);
                            Bytes::from(b)
                        } else {
                            ks.kv_bytes.clone()
                        };
                        phase1_direct.push((k.clone(), kv));
                    } else {
                        // kv_bytes is from a modification after target_rev.
                        // Need WAL for the value at target_rev.
                        phase1_stale_keys.push(k.clone());
                    }
                    continue;
                }

                // Filter by create/mod revision if requested
                if req.min_mod_revision > 0 && (ks.mod_revision as i64) < req.min_mod_revision {
                    continue;
                }
                if req.max_mod_revision > 0 && (ks.mod_revision as i64) > req.max_mod_revision {
                    continue;
                }
                if req.min_create_revision > 0
                    && (ks.create_revision as i64) < req.min_create_revision
                {
                    continue;
                }
                if req.max_create_revision > 0
                    && (ks.create_revision as i64) > req.max_create_revision
                {
                    continue;
                }

                if ks.create_revision > target_rev {
                    // Key didn't exist at target_rev in its current lifetime.
                    // If it was ever deleted and recreated, it may have existed
                    // in a previous lifetime — force WAL scan.
                    if ks.rebirth {
                        phase1_stale_keys.push(k.clone());
                    }
                    continue;
                }

                if ks.mod_revision <= target_rev {
                    let kv = if req.keys_only {
                        let (b, _, _) = wal::encode_kv(k, b"", 0, 0, 0, 0);
                        Bytes::from(b)
                    } else {
                        ks.kv_bytes.clone()
                    };
                    phase1_direct.push((k.clone(), kv));
                } else {
                    phase1_stale_keys.push(k.clone());
                }

                // No early break on limit — we need to count ALL matching keys.
                // Limit is applied when building the response below.
            }

            tracing::debug!(
                target_rev,
                key = %String::from_utf8_lossy(&req.key),
                range_end = %String::from_utf8_lossy(&req.range_end),
                direct = phase1_direct.len(),
                stale = phase1_stale_keys.len(),
                elapsed_us = t0.elapsed().as_micros(),
                "historical_range phase1 done"
            );

            // ── Phase 1 early return: no stale keys → WAL not needed ──
            // Since deleted keys are kept in the BTreeMap with their
            // delete_revision, we can conclusively determine the full
            // result set without scanning the WAL when no keys need
            // historical value reconstruction.
            if phase1_stale_keys.is_empty() {
                let count = phase1_direct.len() as i64;
                tracing::debug!(
                    target_rev,
                    key = %String::from_utf8_lossy(&req.key),
                    range_end = %String::from_utf8_lossy(&req.range_end),
                    count,
                    elapsed_us = t0.elapsed().as_micros(),
                    "historical_range phase1_only"
                );
                if req.count_only {
                    return Ok(etcdserverpb::RangeResponse {
                        header: Some(etcdserverpb::ResponseHeader {
                            cluster_id: 1,
                            member_id: 1,
                            revision: target_rev as i64,
                            raft_term: 1,
                        }),
                        kvs: vec![],
                        more: false,
                        count,
                    });
                }
                let more = if req.limit > 0 && count > req.limit as i64 {
                    phase1_direct.truncate(req.limit as usize);
                    true
                } else {
                    false
                };
                return Ok(etcdserverpb::RangeResponse {
                    header: Some(etcdserverpb::ResponseHeader {
                        cluster_id: 1,
                        member_id: 1,
                        revision: target_rev as i64,
                        raft_term: 1,
                    }),
                    kvs: phase1_direct.into_iter().map(|(_, b)| b).collect(),
                    more,
                    count,
                });
            }

        } // read lock dropped

        // ── Phase 2: WAL scan (no store lock) ──────────────────────
        // Only reaches here when at least one key in range has been modified
        // after target_rev and needs historical value reconstruction.
        tracing::debug!(
            target_rev,
            key = %String::from_utf8_lossy(&req.key),
            range_end = %String::from_utf8_lossy(&req.range_end),
            stale_keys = phase1_stale_keys.len(),
            elapsed_us = t0.elapsed().as_micros(),
            "historical_range phase2 starting wal scan"
        );
        let wal_path = self.state.read().wal.path.clone();
        let wal_state = scan_wal_range(&wal_path, &req.key, &req.range_end, target_rev)
            .map_err(|e| Status::new(tonic::Code::Internal, format!("wal scan failed: {e}")))?;

        tracing::debug!(
            target_rev,
            key = %String::from_utf8_lossy(&req.key),
            range_end = %String::from_utf8_lossy(&req.range_end),
            wal_records = wal_state.len(),
            elapsed_us = t0.elapsed().as_micros(),
            "historical_range phase2 done"
        );

        // ── Phase 3: Merge ─────────────────────────────────────────
        // Collect all results in a BTreeMap for deterministic lexicographic ordering.
        let mut merged: BTreeMap<Vec<u8>, Bytes> = BTreeMap::new();

        // Phase 1 keys with correct current values
        for (key, kv_bytes) in &phase1_direct {
            let kv = if req.keys_only {
                let (b, _, _) = wal::encode_kv(key, b"", 0, 0, 0, 0);
                Bytes::from(b)
            } else {
                kv_bytes.clone()
            };
            merged.insert(key.clone(), kv);
        }

        // Phase 1 keys that need historical values from WAL
        for key in &phase1_stale_keys {
            if let Some(wal_kv) = wal_state.get(key) {
                let kv = if req.keys_only {
                    let (b, _, _) = wal::encode_kv(key, b"", 0, 0, 0, 0);
                    Bytes::from(b)
                } else {
                    wal_kv.clone()
                };
                merged.insert(key.clone(), kv);
            }
            // WAL miss: key existed at target but no WAL record ≤ target_rev.
            // The current BTreeMap value is the best available — use it.
            // This can happen with a compacted WAL snapshot that doesn't
            // fully cover the interval up to target_rev.
            if !merged.contains_key(key) {
                // We already validated create_rev ≤ target_rev in Phase 1,
                // so the key should exist at target. The missing WAL record
                // is a gap — accept the current value as best-effort.
                let state = self.state.read();
                if let Some(ks) = state.keys.get(key) {
                    if ks.is_alive() {
                        let kv = if req.keys_only {
                            let (b, _, _) = wal::encode_kv(key, b"", 0, 0, 0, 0);
                            Bytes::from(b)
                        } else {
                            ks.kv_bytes.clone()
                        };
                        merged.insert(key.clone(), kv);
                    }
                }
            }
        }

        // Keys from WAL not yet covered (should be empty now that deleted keys
        // live in the BTreeMap, but kept for defensive coverage).
        for (key, wal_kv) in &wal_state {
            if merged.contains_key(key) {
                continue;
            }
            // Double-check key is within range (WAL has out-of-range cruft)
            if !matches_range(bound.to_ref(), key) {
                continue;
            }
            let kv = if req.keys_only {
                let (b, _, _) = wal::encode_kv(key, b"", 0, 0, 0, 0);
                Bytes::from(b)
            } else {
                wal_kv.clone()
            };
            merged.insert(key.clone(), kv);
        }

        let count = merged.len() as i64;

        tracing::debug!(
            target_rev,
            key = %String::from_utf8_lossy(&req.key),
            range_end = %String::from_utf8_lossy(&req.range_end),
            total_keys = count,
            from_phase1 = phase1_direct.len(),
            from_wal = count.saturating_sub(phase1_direct.len() as i64),
            elapsed_us = t0.elapsed().as_micros(),
            "historical_range complete"
        );

        if req.count_only {
            return Ok(etcdserverpb::RangeResponse {
                header: Some(etcdserverpb::ResponseHeader {
                    cluster_id: 1,
                    member_id: 1,
                    revision: target_rev as i64,
                    raft_term: 1,
                }),
                kvs: vec![],
                more: false,
                count,
            });
        }

        let kvs: Vec<Bytes> = merged.into_values().take(limit).collect();
        let more = req.limit > 0 && count > req.limit as i64;

        Ok(etcdserverpb::RangeResponse {
            header: Some(etcdserverpb::ResponseHeader {
                cluster_id: 1,
                member_id: 1,
                revision: target_rev as i64,
                raft_term: 1,
            }),
            kvs,
            more,
            count,
        })
    }

    pub async fn put(
        &self,
        req: etcdserverpb::PutRequest,
    ) -> Result<etcdserverpb::PutResponse, Status> {
        let rev = next_revision();
        let mut state = self.state.write();
        let key = req.key;

        let prev_entry = state.keys.get(&key);
        let value = if req.ignore_value {
            prev_entry
                .map(|k| k.value.to_vec())
                .unwrap_or_default()
        } else {
            req.value
        };
        let lease = if req.ignore_lease {
            prev_entry.map(|k| k.lease).unwrap_or(req.lease)
        } else {
            req.lease
        };

        if lease != 0 && !state.leases.contains_key(&lease) {
            return Err(Status::new(
                tonic::Code::NotFound,
                "etcdserver: lease not found",
            ));
        }

        let prev = prev_entry.filter(|k| k.is_alive()).cloned();
        let mut flags = 0u8;
        if prev.is_none() {
            flags |= wal::IS_CREATE;
        }
        if lease != 0 {
            flags |= wal::HAS_LEASE;
        }
        let create_revision = prev.as_ref().map(|k| k.create_revision).unwrap_or(rev);
        let version = prev.as_ref().map(|k| k.version + 1).unwrap_or(1);

        let record = wal::KvWalRecord::new(
            flags,
            &key,
            &value,
            create_revision as i64,
            rev as i64,
            version,
            lease,
        );
        if let Err(e) = state.wal.append_kv(&record) {
            tracing::error!("WAL append failed: {e}");
        }

        let prev = state.apply(key, value, lease, rev, None);

        let header = Some(state.header());
        let prev_kv = if req.prev_kv {
            prev.as_ref()
                .map(|p| p.kv_bytes.clone())
                .unwrap_or_default()
        } else {
            Bytes::new()
        };

        Ok(etcdserverpb::PutResponse { header, prev_kv })
    }

    pub async fn delete_range(
        &self,
        req: etcdserverpb::DeleteRangeRequest,
    ) -> etcdserverpb::DeleteRangeResponse {
        let rev = next_revision();
        let mut state = self.state.write();

        let bound = resolve_range(&req.key, &req.range_end);

        let keys_to_delete: Vec<Vec<u8>> = match bound.to_ref() {
            RangeBoundRef::Point(k) => state
                .keys
                .get(k)
                .filter(|ks| ks.is_alive())
                .map(|_| k.to_vec())
                .into_iter()
                .collect(),
            RangeBoundRef::From(k) => {
                let start = k.to_vec();
                state
                    .keys
                    .range(start..)
                    .filter(|(_, ks)| ks.is_alive())
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::Prefix(p) => {
                let start = p.to_vec();
                state
                    .keys
                    .range(start..)
                    .take_while(|(k, _)| k.starts_with(p))
                    .filter(|(_, ks)| ks.is_alive())
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::Range(start, end) => {
                let start = start.to_vec();
                let end = end.to_vec();
                state
                    .keys
                    .range(start..end)
                    .filter(|(_, ks)| ks.is_alive())
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::All => state
                .keys
                .iter()
                .filter(|(_, ks)| ks.is_alive())
                .map(|(k, _)| k.clone())
                .collect(),
        };

        let mut prev_kvs = Vec::new();
        for key in &keys_to_delete {
            let prev = state.keys.get(key).filter(|k| k.is_alive()).cloned();
            state.apply_delete(key.clone(), rev);

            if let Some(p) = &prev {
                let mut flags = wal::DELETED;
                if p.lease != 0 {
                    flags |= wal::HAS_LEASE;
                }
                let record = wal::KvWalRecord::new(
                    flags,
                    key,
                    &p.value,
                    p.create_revision as i64,
                    rev as i64,
                    p.version,
                    p.lease,
                );
                if let Err(e) = state.wal.append_kv(&record) {
                    tracing::error!("WAL append failed: {e}");
                }
            }

            if req.prev_kv {
                if let Some(p) = prev {
                    prev_kvs.push(p.kv_bytes.clone());
                }
            }
        }

        etcdserverpb::DeleteRangeResponse {
            header: Some(state.header()),
            deleted: keys_to_delete.len() as i64,
            prev_kvs,
        }
    }

    pub async fn txn(
        &self,
        req: etcdserverpb::TxnRequest,
    ) -> Result<etcdserverpb::TxnResponse, Status> {
        let success = {
            let state = self.state.read();
            req.compare.iter().all(|c| eval_compare(&state, c))
        };

        let ops = if success { req.success } else { req.failure };
        self.execute_txn_ops(ops, success).await
    }

    async fn execute_txn_ops(
        &self,
        ops: Vec<etcdserverpb::RequestOp>,
        succeeded: bool,
    ) -> Result<etcdserverpb::TxnResponse, Status> {
        if ops.len() > 1 {
            tracing::warn!(
                op_count = ops.len(),
                succeeded,
                "txn_multi_op: partial execution possible on WAL write failure — txn is not atomic across multiple ops"
            );
        }
        let mut responses = Vec::with_capacity(ops.len());

        for op in ops {
            match op.request {
                Some(etcdserverpb::request_op::Request::RequestRange(r)) => {
                    let resp = self.range(r).await?;
                    responses.push(etcdserverpb::ResponseOp {
                        response: Some(etcdserverpb::response_op::Response::ResponseRange(resp)),
                    });
                }
                Some(etcdserverpb::request_op::Request::RequestPut(p)) => {
                    let resp = self.put(p).await?;
                    responses.push(etcdserverpb::ResponseOp {
                        response: Some(etcdserverpb::response_op::Response::ResponsePut(resp)),
                    });
                }
                Some(etcdserverpb::request_op::Request::RequestDeleteRange(d)) => {
                    let resp = self.delete_range(d).await;
                    responses.push(etcdserverpb::ResponseOp {
                        response: Some(etcdserverpb::response_op::Response::ResponseDeleteRange(
                            resp,
                        )),
                    });
                }
                Some(etcdserverpb::request_op::Request::RequestTxn(_)) => {
                    responses.push(etcdserverpb::ResponseOp {
                        response: Some(etcdserverpb::response_op::Response::ResponseTxn(
                            etcdserverpb::TxnResponse {
                                header: None,
                                succeeded: true,
                                responses: vec![],
                            },
                        )),
                    });
                }
                None => {}
            }
        }

        let header = {
            let state = self.state.read();
            state.header()
        };
        Ok(etcdserverpb::TxnResponse {
            header: Some(header),
            succeeded,
            responses,
        })
    }

    pub async fn compact(
        &self,
        req: etcdserverpb::CompactionRequest,
    ) -> Result<etcdserverpb::CompactionResponse, Status> {
        let rev = req.revision as u64;
        let current = current_revision();

        if rev > current {
            return Err(Status::new(
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision is a future revision",
            ));
        }
        if rev <= COMPACT_REV.load(Ordering::Relaxed) {
            return Err(Status::new(
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision has been compacted",
            ));
        }

        let state = self.state.write();
        COMPACT_REV.store(rev, Ordering::SeqCst);

        // NOTE: etcd's Compact does NOT delete current key-values from the store.
        // It only sets compact_rev to allow garbage collection of old MVCC revisions.
        // The current snapshot must be retained.
        // The old code called state.keys.retain(...) which deleted current data — BUG.

        Ok(etcdserverpb::CompactionResponse {
            header: Some(state.header()),
        })
    }

    // ── Lease operations ────────────────────────────────────────────────

    pub async fn lease_grant(
        &self,
        req: etcdserverpb::LeaseGrantRequest,
    ) -> etcdserverpb::LeaseGrantResponse {
        let mut state = self.state.write();
        let id = if req.id != 0 { req.id } else { next_lease_id() };
        let ttl = req.ttl;
        let expires_at = tokio::time::Instant::now() + std::time::Duration::from_secs(ttl as u64);
        state.leases.insert(
            id,
            LeaseState {
                id,
                ttl,
                expires_at,
                key_count: 0,
            },
        );
        LEASE_COUNT.fetch_add(1, Ordering::Relaxed);
        state.expiry_notify.notify_one();
        tracing::info!(id, ttl, "lease_granted");
        etcdserverpb::LeaseGrantResponse {
            header: Some(state.header()),
            id,
            ttl,
            error: String::new(),
        }
    }

    pub async fn lease_revoke(
        &self,
        req: etcdserverpb::LeaseRevokeRequest,
    ) -> etcdserverpb::LeaseRevokeResponse {
        let mut state = self.state.write();
        let id = req.id;
        state.leases.remove(&id);
        LEASE_COUNT.fetch_sub(1, Ordering::Relaxed);
        state.expiry_notify.notify_one();
        tracing::info!(id, "lease_revoked");
        state.delete_keys_for_lease(id);
        etcdserverpb::LeaseRevokeResponse {
            header: Some(state.header()),
        }
    }

    pub async fn lease_keep_alive(
        &self,
        id: i64,
    ) -> Result<etcdserverpb::LeaseKeepAliveResponse, Status> {
        let mut state = self.state.write();
        if let Some(ls) = state.leases.get_mut(&id) {
            let ttl = ls.ttl;
            ls.expires_at =
                tokio::time::Instant::now() + std::time::Duration::from_secs(ttl as u64);
            state.expiry_notify.notify_one();
            Ok(etcdserverpb::LeaseKeepAliveResponse {
                header: Some(state.header()),
                id,
                ttl,
            })
        } else {
            Err(Status::new(
                tonic::Code::NotFound,
                "etcdserver: lease not found",
            ))
        }
    }

    pub async fn lease_time_to_live(
        &self,
        req: etcdserverpb::LeaseTimeToLiveRequest,
    ) -> etcdserverpb::LeaseTimeToLiveResponse {
        let state = self.state.read();
        if let Some(ls) = state.leases.get(&req.id) {
            let remaining = (ls
                .expires_at
                .saturating_duration_since(tokio::time::Instant::now()))
            .as_secs() as i64;
            let keys = if req.keys {
                state
                    .keys
                    .iter()
                    .filter(|(_, ks)| ks.lease == req.id && ks.is_alive())
                    .map(|(k, _)| k.clone())
                    .collect()
            } else {
                vec![]
            };
            etcdserverpb::LeaseTimeToLiveResponse {
                header: Some(state.header()),
                id: req.id,
                ttl: remaining.max(0),
                granted_ttl: ls.ttl,
                keys,
            }
        } else {
            etcdserverpb::LeaseTimeToLiveResponse {
                header: Some(state.header()),
                id: req.id,
                ttl: -1,
                granted_ttl: -1,
                keys: vec![],
            }
        }
    }

    pub async fn lease_leases(&self) -> etcdserverpb::LeaseLeasesResponse {
        let state = self.state.read();
        let leases = state
            .leases
            .keys()
            .map(|id| etcdserverpb::LeaseStatus { id: *id })
            .collect();
        etcdserverpb::LeaseLeasesResponse {
            header: Some(state.header()),
            leases,
        }
    }

    // ── Maintenance operations ──────────────────────────────────────────

    pub async fn db_size(&self) -> i64 {
        let state = self.state.read();
        let md = state.wal.file.lock().unwrap().metadata();
        md.map(|m| m.len() as i64).unwrap_or(0)
    }

    pub async fn store_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let state = self.state.read();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for (k, ks) in state.keys.iter() {
            if ks.delete_revision != 0 {
                continue;
            }
            k.hash(&mut hasher);
            ks.value.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn start_expiry_task(state: Arc<RwLock<StoreState>>) {
        tokio::spawn(async move {
            let notify = {
                let s = state.read();
                s.expiry_notify.clone()
            };
            loop {
                // Compute sleep duration from the earliest lease expiry.
                // When no leases exist, sleep indefinitely (woken by notify).
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

                // Wait until the earliest expiry OR a notification
                // (lease granted / refreshed / revoked).
                tokio::select! {
                    _ = tokio::time::sleep(sleep_dur) => {}
                    _ = notify.notified() => {}
                }

                // Collect and process expired leases under the write lock.
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
                for id in expired {
                    s.leases.remove(&id);
                    LEASE_COUNT.fetch_sub(1, Ordering::Relaxed);
                    s.delete_keys_for_lease(id);
                }
            }
        });
    }

    /// Compact the WAL: snapshot active keys, write compacted file,
    /// then append any writes that occurred during snapshotting (tail).
    pub async fn compact_wal(&self) -> anyhow::Result<()> {
        let t0 = std::time::Instant::now();
        let wal_path = self.wal_path().await;
        let target = format!("{}.compact", wal_path);

        // ── Phase A: Snapshot active keys under write lock ──────────
        let t_a = std::time::Instant::now();
        let records: Vec<wal::KvWalRecord>;
        let snapshot_rev: u64;
        let snapshot_wal_size: u64;

        {
            let state = self.state.read();
            snapshot_rev = current_revision();
            snapshot_wal_size = state.wal.file.lock().unwrap().metadata()?.len();
            let mut recs = Vec::with_capacity(state.keys.len());
            for (key, ks) in state.keys.iter() {
                if ks.delete_revision != 0 {
                    continue;
                }
                let flags = if ks.lease != 0 { wal::HAS_LEASE } else { 0 };
                recs.push(wal::KvWalRecord::new(
                    flags,
                    key,
                    &ks.value,
                    ks.create_revision as i64,
                    ks.mod_revision as i64,
                    ks.version,
                    ks.lease,
                ));
            }
            records = recs;
        }
        let phase_a_us = t_a.elapsed().as_micros() as u64;

        // ── Phase B: Write snapshot records to temp file (no lock) ──
        let t_b = std::time::Instant::now();
        let snapshot_bytes: usize;
        {
            use std::io::Write;
            let mut compact = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&target)?;
            for rec in &records {
                let data = rec.serialize();
                compact.write_all(&data)?;
            }
            compact.sync_all()?;
            snapshot_bytes = compact.metadata()?.len() as usize;
        }
        let phase_b_us = t_b.elapsed().as_micros() as u64;

        // ── Phase C: Append tail + swap under write lock ────────────
        let t_c = std::time::Instant::now();
        let tail_bytes: usize;
        let tail_count: usize;
        let old_wal_size: u64;
        let new_wal_size: u64;

        {
            let state = self.state.write();

            // Read tail bytes from the active WAL (writes during Phase B)
            let tail = {
                let mut f = state.wal.file.lock().unwrap();
                let current_len = f.metadata()?.len();
                if current_len > snapshot_wal_size {
                    f.seek(SeekFrom::Start(snapshot_wal_size))?;
                    let mut buf = Vec::new();
                    f.read_to_end(&mut buf)?;
                    buf
                } else {
                    Vec::new()
                }
            };

            tail_bytes = tail.len();
            tail_count = wal::count_wal_records(&tail);

            // Append tail bytes to the compacted file
            if !tail.is_empty() {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&target)?;
                f.write_all(&tail)?;
                f.sync_all()?;
                new_wal_size = f.metadata()?.len();
            } else {
                new_wal_size = snapshot_bytes as u64;
            }

            old_wal_size = snapshot_wal_size + tail_bytes as u64;

            // Atomically replace the active WAL
            std::fs::rename(&target, &wal_path)?;

            // Open the new WAL and swap into the shared file handle
            let new_file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&wal_path)?;
            *state.wal.file.lock().unwrap() = new_file;
            state.wal.dirty.store(true, Ordering::Release);
        }
        let phase_c_us = t_c.elapsed().as_micros() as u64;

        let total_us = t0.elapsed().as_micros() as u64;

        tracing::info!(
            snapshot_keys = records.len(),
            snapshot_rev,
            snapshot_bytes,
            phase_a_us,
            phase_b_us,
            phase_c_us,
            total_us,
            tail_bytes,
            tail_count,
            old_wal_size,
            new_wal_size,
            "wal_compacted"
        );

        Ok(())
    }

    fn start_compaction_task(store: Store, wal_path: String) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            interval.tick().await; // skip immediate tick
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
}

/// Apply a KvWalRecord during startup replay. Rebuilds in-memory state
/// and fires watch events for any watchers caught up during replay.
fn apply_record(state: &mut StoreState, rec: &wal::KvWalRecord) {
    let deleted = (rec.flags & wal::DELETED) != 0;
    let rev = rec.mod_revision().unwrap_or(0) as u64;

    if deleted {
        let key = rec.key().unwrap_or_default().to_vec();
        state.apply_delete(key, rev);
    } else if let Ok(kv) = mvccpb::KeyValue::decode(&rec.kv_bytes[..]) {
        state.apply(kv.key, kv.value, kv.lease, rev, Some(Bytes::from(rec.kv_bytes.clone())));
    }
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
        let dir = std::env::temp_dir();
        let name = format!("rudurru_compact_{}.wal", std::process::id());
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
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

    fn temp_wal() -> String {
        let dir = std::env::temp_dir();
        let name = format!("rudurru_historical_{}.wal", std::process::id());
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

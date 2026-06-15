pub mod wal;

use crate::proto::etcdserverpb;
use crate::proto::mvccpb;
use prost::bytes::Bytes;
use prost::Message;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

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

/// In-memory representation of a key's current state.
#[derive(Debug, Clone)]
pub struct KeyState {
    pub value: Arc<[u8]>,
    pub mod_revision: u64,
    pub create_revision: u64,
    pub version: i64,
    pub lease: i64,
    pub deleted: bool,
    pub kv_bytes: Bytes,
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
    pub key: Vec<u8>,
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

    fn apply(&mut self, key: Vec<u8>, value: Vec<u8>, lease: i64, rev: u64) -> Option<KeyState> {
        let prev = self.keys.get(&key).filter(|k| !k.deleted).cloned();

        let mut entry = KeyState {
            value: Arc::from(value.into_boxed_slice()),
            mod_revision: rev,
            create_revision: prev.as_ref().map(|k| k.create_revision).unwrap_or(rev),
            version: prev.as_ref().map(|k| k.version + 1).unwrap_or(1),
            lease,
            deleted: false,
            kv_bytes: Bytes::new(),
        };
        entry.kv_bytes = make_kv_bytes(&key, &entry);
        let old = self.keys.insert(key.clone(), entry.clone());
        if old.is_none() {
            KEY_COUNT.fetch_add(1, Ordering::Relaxed);
        }

        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Put,
            key: key.clone(),
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
        let prev = self.keys.remove(&key)?;
        KEY_COUNT.fetch_sub(1, Ordering::Relaxed);
        if prev.deleted {
            return None;
        }

        // Create watch event for DELETE
        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Delete,
            key: key.clone(),
            kv_bytes: prev.kv_bytes.clone(),
            prev_kv_bytes: prev.kv_bytes.clone(),
        };
        self.notify_watchers(event);

        Some(prev)
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
        KEY_COUNT.store(state.keys.len() as u64, Ordering::Relaxed);
        WATCHER_COUNT.store(0, Ordering::Relaxed);
        LEASE_COUNT.store(0, Ordering::Relaxed);

        tracing::info!(
            "rudurru ready: revision={}, keys={}, compact_rev={}",
            max_rev,
            state.keys.len(),
            COMPACT_REV.load(Ordering::Relaxed),
        );

        let state_arc = Arc::new(RwLock::new(state));
        Self::start_fsync_task(
            state_arc.read().await.wal.file.clone(),
            state_arc.read().await.wal.dirty.clone(),
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
        self.state.read().await.wal.path.clone()
    }

    // ── KV operations ──────────────────────────────────────────────────

    pub async fn range(&self, req: etcdserverpb::RangeRequest) -> etcdserverpb::RangeResponse {
        let state = self.state.read().await;
        if req.revision > 0 && (req.revision as u64) < COMPACT_REV.load(Ordering::Relaxed) {
            return etcdserverpb::RangeResponse {
                header: Some(state.header()),
                kvs: vec![],
                more: false,
                count: 0,
            };
        }

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
                // Point: produce a range that matches exactly one key.
                // Use (start..end) where 'end' is the successor of k.
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

        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &KeyState)>> = match (range_start, range_end) {
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
            if ks.deleted {
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
            return etcdserverpb::RangeResponse {
                header: Some(state.header()),
                kvs: vec![],
                more: false,
                count,
            };
        }

        let more = if req.limit > 0 && kvs.len() > req.limit as usize {
            kvs.truncate(req.limit as usize);
            true
        } else {
            false
        };

        etcdserverpb::RangeResponse {
            header: Some(state.header()),
            kvs,
            more,
            count,
        }
    }

    pub async fn put(&self, req: etcdserverpb::PutRequest) -> etcdserverpb::PutResponse {
        let rev = next_revision();
        let mut state = self.state.write().await;
        let key = req.key.clone();
        let value = if req.ignore_value {
            state
                .keys
                .get(&key)
                .map(|k| k.value.to_vec())
                .unwrap_or_default()
        } else {
            req.value
        };
        let lease = if req.ignore_lease {
            state.keys.get(&key).map(|k| k.lease).unwrap_or(req.lease)
        } else {
            req.lease
        };

        let prev = state.keys.get(&key).filter(|k| !k.deleted).cloned();
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

        let prev = state.apply(key.clone(), value, lease, rev);

        let header = Some(state.header());
        let prev_kv = if req.prev_kv {
            prev.as_ref()
                .map(|p| p.kv_bytes.clone())
                .unwrap_or_default()
        } else {
            Bytes::new()
        };

        etcdserverpb::PutResponse { header, prev_kv }
    }

    pub async fn delete_range(
        &self,
        req: etcdserverpb::DeleteRangeRequest,
    ) -> etcdserverpb::DeleteRangeResponse {
        let rev = next_revision();
        let mut state = self.state.write().await;

        let bound = resolve_range(&req.key, &req.range_end);

        let keys_to_delete: Vec<Vec<u8>> = match bound.to_ref() {
            RangeBoundRef::Point(k) => state
                .keys
                .get(k)
                .filter(|ks| !ks.deleted)
                .map(|_| k.to_vec())
                .into_iter()
                .collect(),
            RangeBoundRef::From(k) => {
                let start = k.to_vec();
                state
                    .keys
                    .range(start..)
                    .filter(|(_, ks)| !ks.deleted)
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::Prefix(p) => {
                let start = p.to_vec();
                state
                    .keys
                    .range(start..)
                    .take_while(|(k, _)| k.starts_with(p))
                    .filter(|(_, ks)| !ks.deleted)
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::Range(start, end) => {
                let start = start.to_vec();
                let end = end.to_vec();
                state
                    .keys
                    .range(start..end)
                    .filter(|(_, ks)| !ks.deleted)
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::All => state
                .keys
                .iter()
                .filter(|(_, ks)| !ks.deleted)
                .map(|(k, _)| k.clone())
                .collect(),
        };

        let mut prev_kvs = Vec::new();
        for key in &keys_to_delete {
            let prev = state.keys.get(key).filter(|k| !k.deleted).cloned();
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

    pub async fn txn(&self, req: etcdserverpb::TxnRequest) -> etcdserverpb::TxnResponse {
        let state = self.state.write().await;

        let success = req.compare.iter().all(|c| eval_compare(&state, c));

        drop(state);

        let ops = if success { req.success } else { req.failure };
        self.execute_txn_ops(ops, success).await
    }

    async fn execute_txn_ops(
        &self,
        ops: Vec<etcdserverpb::RequestOp>,
        succeeded: bool,
    ) -> etcdserverpb::TxnResponse {
        let mut responses = Vec::with_capacity(ops.len());

        for op in ops {
            match op.request {
                Some(etcdserverpb::request_op::Request::RequestRange(r)) => {
                    let resp = self.range(r).await;
                    responses.push(etcdserverpb::ResponseOp {
                        response: Some(etcdserverpb::response_op::Response::ResponseRange(resp)),
                    });
                }
                Some(etcdserverpb::request_op::Request::RequestPut(p)) => {
                    let resp = self.put(p).await;
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
            let state = self.state.read().await;
            state.header()
        };

        etcdserverpb::TxnResponse {
            header: Some(header),
            succeeded,
            responses,
        }
    }

    pub async fn compact(
        &self,
        req: etcdserverpb::CompactionRequest,
    ) -> etcdserverpb::CompactionResponse {
        let state = self.state.write().await;
        COMPACT_REV.store(req.revision as u64, Ordering::SeqCst);

        // NOTE: etcd's Compact does NOT delete current key-values from the store.
        // It only sets compact_rev to allow garbage collection of old MVCC revisions.
        // The current snapshot must be retained.
        // The old code called state.keys.retain(...) which deleted current data — BUG.

        etcdserverpb::CompactionResponse {
            header: Some(state.header()),
        }
    }

    // ── Lease operations ────────────────────────────────────────────────

    pub async fn lease_grant(
        &self,
        req: etcdserverpb::LeaseGrantRequest,
    ) -> etcdserverpb::LeaseGrantResponse {
        let mut state = self.state.write().await;
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
        let mut state = self.state.write().await;
        let id = req.id;
        state.leases.remove(&id);
        LEASE_COUNT.fetch_sub(1, Ordering::Relaxed);
        state.expiry_notify.notify_one();
        tracing::info!(id, "lease_revoked");
        let keys_to_delete: Vec<Vec<u8>> = state
            .keys
            .iter()
            .filter(|(_, ks)| ks.lease == id && !ks.deleted)
            .map(|(k, _)| k.clone())
            .collect();
        let mut records = Vec::with_capacity(keys_to_delete.len());
        for key in &keys_to_delete {
            let rev = next_revision();
            let prev = state.keys.get(key).filter(|k| !k.deleted).cloned();
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
                records.push(record);
            }
        }
        if let Err(e) = state.wal.append_kv_batch(&records) {
            tracing::error!("WAL batch append failed on lease revoke: {e}");
        }
        etcdserverpb::LeaseRevokeResponse {
            header: Some(state.header()),
        }
    }

    pub async fn lease_keep_alive(&self, id: i64) -> etcdserverpb::LeaseKeepAliveResponse {
        let mut state = self.state.write().await;
        if let Some(ls) = state.leases.get_mut(&id) {
            let ttl = ls.ttl;
            ls.expires_at =
                tokio::time::Instant::now() + std::time::Duration::from_secs(ttl as u64);
            state.expiry_notify.notify_one();
            etcdserverpb::LeaseKeepAliveResponse {
                header: Some(state.header()),
                id,
                ttl,
            }
        } else {
            etcdserverpb::LeaseKeepAliveResponse {
                header: Some(state.header()),
                id,
                ttl: -1,
            }
        }
    }

    pub async fn lease_time_to_live(
        &self,
        req: etcdserverpb::LeaseTimeToLiveRequest,
    ) -> etcdserverpb::LeaseTimeToLiveResponse {
        let state = self.state.read().await;
        if let Some(ls) = state.leases.get(&req.id) {
            let remaining = (ls
                .expires_at
                .saturating_duration_since(tokio::time::Instant::now()))
            .as_secs() as i64;
            let keys = if req.keys {
                state
                    .keys
                    .iter()
                    .filter(|(_, ks)| ks.lease == req.id && !ks.deleted)
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
        let state = self.state.read().await;
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
        let state = self.state.read().await;
        let md = state.wal.file.lock().unwrap().metadata();
        md.map(|m| m.len() as i64).unwrap_or(0)
    }

    pub async fn store_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let state = self.state.read().await;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for (k, ks) in state.keys.iter() {
            if ks.deleted {
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
                let s = state.read().await;
                s.expiry_notify.clone()
            };
            loop {
                // Compute sleep duration from the earliest lease expiry.
                // When no leases exist, sleep indefinitely (woken by notify).
                let sleep_dur = {
                    let s = state.read().await;
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
                let mut s = state.write().await;
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
                    let keys_to_delete: Vec<Vec<u8>> = s
                        .keys
                        .iter()
                        .filter(|(_, ks)| ks.lease == id && !ks.deleted)
                        .map(|(k, _)| k.clone())
                        .collect();
                    for key in &keys_to_delete {
                        let rev = next_revision();
                        let prev = s.keys.get(key).filter(|k| !k.deleted).cloned();
                        s.apply_delete(key.clone(), rev);

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
                            if let Err(e) = s.wal.append_kv(&record) {
                                tracing::error!("WAL append failed on lease expiry: {e}");
                            }
                        }
                    }
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
            let state = self.state.read().await;
            snapshot_rev = current_revision();
            snapshot_wal_size = state.wal.file.lock().unwrap().metadata()?.len();
            let mut recs = Vec::with_capacity(state.keys.len());
            for (key, ks) in state.keys.iter() {
                if ks.deleted {
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
            let state = self.state.write().await;

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

    if deleted {
        let key = rec.key().unwrap_or_default().to_vec();
        let event = WatchEvent {
            revision: rec.mod_revision().unwrap_or(0) as u64,
            event_type: mvccpb::event::EventType::Delete,
            key: key.clone(),
            kv_bytes: Bytes::from(rec.kv_bytes.clone()),
            prev_kv_bytes: Bytes::from(rec.kv_bytes.clone()),
        };
        state.keys.remove(&key);
        state.notify_watchers(event);
    } else if let Ok(kv) = mvccpb::KeyValue::decode(&rec.kv_bytes[..]) {
        let key = kv.key.clone();
        let value = kv.value.clone();
        let entry = KeyState {
            value: Arc::from(value.into_boxed_slice()),
            mod_revision: kv.mod_revision as u64,
            create_revision: kv.create_revision as u64,
            version: kv.version,
            lease: kv.lease,
            deleted: false,
            kv_bytes: Bytes::from(rec.kv_bytes.clone()),
        };
        state.keys.insert(key.clone(), entry);

        let event = WatchEvent {
            revision: rec.mod_revision().unwrap_or(0) as u64,
            event_type: mvccpb::event::EventType::Put,
            key,
            kv_bytes: Bytes::from(rec.kv_bytes.clone()),
            prev_kv_bytes: Bytes::new(),
        };
        state.notify_watchers(event);
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
    if range_end.len() == key.len()
        && !range_end.is_empty()
        && range_end[..range_end.len() - 1] == key[..key.len() - 1]
        && range_end[key.len() - 1] == key[key.len() - 1].wrapping_add(1)
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

fn eval_compare(state: &StoreState, cmp: &etcdserverpb::Compare) -> bool {
    let key = &cmp.key;
    let ks = state.keys.get(key);

    let result = result_from_i32(cmp.result);
    let target = target_from_i32(cmp.target);

    match target {
        etcdserverpb::compare::CompareTarget::Version => {
            let actual = ks.map(|k| k.version).unwrap_or(0);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::Version(v)) => *v,
                _ => 0,
            };
            cmp_i64(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Create => {
            let actual = ks.map(|k| k.create_revision as i64).unwrap_or(0);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::CreateRevision(v)) => *v,
                _ => 0,
            };
            cmp_i64(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Mod => {
            let actual = ks.map(|k| k.mod_revision as i64).unwrap_or(0);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::ModRevision(v)) => *v,
                _ => 0,
            };
            cmp_i64(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Value => {
            let actual: &[u8] = ks.map(|k| k.value.as_ref()).unwrap_or(&[]);
            let expected = match &cmp.target_union {
                Some(etcdserverpb::compare::TargetUnion::Value(v)) => v.as_slice(),
                _ => &[],
            };
            cmp_bytes(result, actual, expected)
        }
        etcdserverpb::compare::CompareTarget::Lease => {
            let actual = ks.map(|k| k.lease).unwrap_or(0);
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
            let s = store.state.read().await;
            let f = s.wal.file.lock().unwrap();
            f.metadata().unwrap().len()
        };
        store.compact_wal().await.unwrap();
        let size_after = {
            let s = store.state.read().await;
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
            .await;
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
            .await;
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
            .await;
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
            .await;
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
            .await;
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
            .await;
        assert_eq!(resp2.count, 11, "all 11 keys after restart");

        let _ = std::fs::remove_file(&path);
    }
}

pub mod wal;

use crate::proto::etcdserverpb;
use crate::proto::mvccpb;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, RwLock};

/// Global revision counter. Monotonically increasing, starts at 1.
static NEXT_REV: AtomicU64 = AtomicU64::new(1);

pub fn next_revision() -> u64 {
    NEXT_REV.fetch_add(1, Ordering::SeqCst)
}

pub fn current_revision() -> u64 {
    NEXT_REV.load(Ordering::SeqCst).saturating_sub(1)
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
}

impl KeyState {
    pub fn to_key_value(&self, key: &[u8]) -> mvccpb::KeyValue {
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
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub revision: u64,
    pub event_type: mvccpb::event::EventType,
    pub kv: mvccpb::KeyValue,
    pub prev_kv: Option<mvccpb::KeyValue>,
}

#[derive(Debug)]
pub struct StoreState {
    pub keys: BTreeMap<Vec<u8>, KeyState>,
    pub leases: BTreeMap<i64, LeaseState>,
    pub watchers: Vec<WatchRegistration>,
    pub compact_rev: u64,
    pub next_rev: u64,
    pub wal: wal::WalFile,
}

impl StoreState {
    pub fn new(wal: wal::WalFile) -> Self {
        Self {
            keys: BTreeMap::new(),
            leases: BTreeMap::new(),
            watchers: Vec::new(),
            compact_rev: 0,
            next_rev: 1,
            wal,
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

        let entry = KeyState {
            value: Arc::from(value.into_boxed_slice()),
            mod_revision: rev,
            create_revision: prev.as_ref().map(|k| k.create_revision).unwrap_or(rev),
            version: prev.as_ref().map(|k| k.version + 1).unwrap_or(1),
            lease,
            deleted: false,
        };
        self.keys.insert(key.clone(), entry.clone());
        
        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Put,
            kv: mvccpb::KeyValue {
                key: key.clone(),
                create_revision: entry.create_revision as i64,
                mod_revision: entry.mod_revision as i64,
                version: entry.version,
                value: entry.value.to_vec(),
                lease: entry.lease,
            },
            prev_kv: prev.as_ref().map(|p| p.to_key_value(&key)),
        };
        self.notify_watchers(event);
        
        prev
    }

    fn apply_delete(&mut self, key: Vec<u8>, rev: u64) -> Option<KeyState> {
        let prev = self.keys.remove(&key)?;
        if prev.deleted {
            return None;
        }
        
        // Create watch event for DELETE
        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Delete,
            kv: mvccpb::KeyValue {
                key: key.clone(),
                create_revision: prev.create_revision as i64,
                mod_revision: prev.mod_revision as i64,
                version: prev.version,
                value: prev.value.to_vec(),
                lease: prev.lease,
            },
            prev_kv: Some(prev.to_key_value(&key)),
        };
        self.notify_watchers(event);
        
        Some(prev)
    }

    // Watcher management
    pub(crate) fn register_watcher(&mut self, key: Vec<u8>, range_end: Vec<u8>, start_revision: u64,
                       sender: mpsc::UnboundedSender<WatchEvent>, watch_id: i64,
                       progress_notify: bool, filters: Vec<i32>, prev_kv: bool) -> i64 {
        let registration = WatchRegistration {
            key,
            range_end,
            start_revision,
            sender,
            watch_id,
            progress_notify,
            filters,
            prev_kv,
        };
        self.watchers.push(registration);
        watch_id
    }

    pub(crate) fn cancel_watcher(&mut self, watch_id: i64) -> bool {
        let len_before = self.watchers.len();
        self.watchers.retain(|w| w.watch_id != watch_id);
        len_before != self.watchers.len()
    }

    fn notify_watchers(&mut self, event: WatchEvent) {
        let watchers: Vec<WatchRegistration> = self.watchers.clone();
        for watcher in watchers {
            let bound = resolve_range(&watcher.key, &watcher.range_end);
            if !matches_range(bound.to_ref(), &event.kv.key) {
                continue;
            }
            
            if event.revision < watcher.start_revision {
                continue;
            }
            
            let mut should_send = true;
            for &filter in &watcher.filters {
                match filter {
                    0 => {
                        if event.event_type == mvccpb::event::EventType::Put {
                            should_send = false;
                            break;
                        }
                    }
                    1 => {
                        if event.event_type == mvccpb::event::EventType::Delete {
                            should_send = false;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            
            if !should_send {
                continue;
            }
            
            let _ = watcher.sender.send(event.clone());
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
        let records = wal.scan_collect()?;

        let mut state = StoreState::new(wal);
        let mut max_rev = 0u64;

        for rec in &records {
            if rec.revision <= state.compact_rev {
                continue;
            }
            apply_record(&mut state, rec);
            if rec.revision > max_rev {
                max_rev = rec.revision;
            }
        }

        state.next_rev = max_rev + 1;
        NEXT_REV.store(state.next_rev, Ordering::SeqCst);

            tracing::info!(
            "rudurru ready: revision={}, keys={}, compact_rev={}",
            max_rev,
            state.keys.len(),
            state.compact_rev,
        );

        Ok(Self {
            state: Arc::new(RwLock::new(state)),
        })
    }

    pub async fn wal_path(&self) -> String {
        let state = self.state.read().await;
        state.wal.path.clone()
    }

    // ── KV operations ──────────────────────────────────────────────────

    pub async fn range(&self, req: etcdserverpb::RangeRequest) -> etcdserverpb::RangeResponse {
        let state = self.state.read().await;
        if req.revision > 0 && (req.revision as u64) < state.compact_rev {
            return etcdserverpb::RangeResponse {
                header: Some(state.header()),
                kvs: vec![],
                more: false,
                count: 0,
            };
        }

        let bound = resolve_range(&req.key, &req.range_end);
        let mut kvs: Vec<mvccpb::KeyValue> = Vec::new();

        for (k, ks) in state.keys.iter() {
            if ks.deleted {
                continue;
            }
            if !matches_range(bound.to_ref(), k) {
                continue;
            }
            if req.min_mod_revision > 0 && (ks.mod_revision as i64) < req.min_mod_revision {
                continue;
            }
            if req.max_mod_revision > 0 && (ks.mod_revision as i64) > req.max_mod_revision {
                continue;
            }
            if req.min_create_revision > 0 && (ks.create_revision as i64) < req.min_create_revision {
                continue;
            }
            if req.max_create_revision > 0 && (ks.create_revision as i64) > req.max_create_revision {
                continue;
            }

            let kv = if req.keys_only {
                mvccpb::KeyValue {
                    key: k.clone(),
                    ..Default::default()
                }
            } else {
                ks.to_key_value(k)
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
            state.keys.get(&key).map(|k| k.value.to_vec()).unwrap_or_default()
        } else {
            req.value
        };
        let lease = if req.ignore_lease {
            state.keys.get(&key).map(|k| k.lease).unwrap_or(req.lease)
        } else {
            req.lease
        };

        let mut flags = wal::IS_CREATE;
        if lease != 0 {
            flags |= wal::HAS_LEASE;
        }
        let record = wal::WalRecord {
            revision: rev,
            key: key.clone(),
            value: value.clone(),
            flags,
            lease_id: if lease != 0 { Some(lease) } else { None },
        };
        if let Err(e) = state.wal.append(&record) {
            tracing::error!("WAL append failed: {e}");
        }

        let prev = state.apply(key.clone(), value, lease, rev);

        let header = Some(state.header());
        let prev_kv = if req.prev_kv {
            prev.map(|p| p.to_key_value(&key))
        } else {
            None
        };

        etcdserverpb::PutResponse { header, prev_kv }
    }

    pub async fn delete_range(&self, req: etcdserverpb::DeleteRangeRequest) -> etcdserverpb::DeleteRangeResponse {
        let rev = next_revision();
        let mut state = self.state.write().await;

        let bound = resolve_range(&req.key, &req.range_end);

        let keys_to_delete: Vec<Vec<u8>> = state.keys.iter()
            .filter(|(k, ks)| {
                if ks.deleted {
                    return false;
                }
                matches_range(bound.to_ref(), k)
            })
            .map(|(k, _)| k.clone())
            .collect();

        let mut prev_kvs = Vec::new();
        for key in &keys_to_delete {
            let prev = state.apply_delete(key.clone(), rev);

            if req.prev_kv {
                if let Some(p) = prev {
                    prev_kvs.push(p.to_key_value(key));
                }
            }

            let record = wal::WalRecord {
                revision: rev,
                key: key.clone(),
                value: vec![],
                flags: wal::DELETED,
                lease_id: None,
            };
            if let Err(e) = state.wal.append(&record) {
                tracing::error!("WAL append failed: {e}");
            }
        }

        etcdserverpb::DeleteRangeResponse {
            header: Some(state.header()),
            deleted: keys_to_delete.len() as i64,
            prev_kvs,
        }
    }

    pub async fn txn(&self, req: etcdserverpb::TxnRequest) -> etcdserverpb::TxnResponse {
        let state_read = self.state.read().await;

        let success = req.compare.iter().all(|c| eval_compare(&state_read, c));

        drop(state_read);

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
                        response: Some(etcdserverpb::response_op::Response::ResponseDeleteRange(resp)),
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

    pub async fn compact(&self, req: etcdserverpb::CompactionRequest) -> etcdserverpb::CompactionResponse {
        let mut state = self.state.write().await;
        state.compact_rev = req.revision as u64;

        let compact_rev = state.compact_rev;
        state.keys.retain(|_, ks| {
            if ks.deleted {
                return false;
            }
            ks.mod_revision >= compact_rev || ks.create_revision >= compact_rev
        });

        etcdserverpb::CompactionResponse {
            header: Some(state.header()),
        }
    }
}

fn apply_record(state: &mut StoreState, rec: &wal::WalRecord) {
    let deleted = (rec.flags & wal::DELETED) != 0;
    let has_lease = (rec.flags & wal::HAS_LEASE) != 0;
    let lease = if has_lease { rec.lease_id.unwrap_or(0) } else { 0 };

    if deleted {
        // Create watch event for DELETE during replay
        let event = WatchEvent {
            revision: rec.revision,
            event_type: mvccpb::event::EventType::Delete,
            kv: mvccpb::KeyValue {
                key: rec.key.clone(),
                create_revision: rec.revision as i64,
                mod_revision: rec.revision as i64,
                version: 1,
                value: vec![],
                lease: 0,
            },
            prev_kv: None,
        };
        state.keys.remove(&rec.key);
        state.notify_watchers(event);
    } else {
        let event = WatchEvent {
            revision: rec.revision,
            event_type: mvccpb::event::EventType::Put,
            kv: mvccpb::KeyValue {
                key: rec.key.clone(),
                create_revision: rec.revision as i64,
                mod_revision: rec.revision as i64,
                version: 1, // During replay, we don't have original version
                value: rec.value.clone(),
                lease,
            },
            prev_kv: None, // During replay, we don't have previous KV
        };
        let entry = KeyState {
            value: Arc::from(rec.value.clone().into_boxed_slice()),
            mod_revision: rec.revision,
            create_revision: rec.revision,
            version: 1,
            lease,
            deleted: false,
        };
        state.keys.insert(rec.key.clone(), entry);
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

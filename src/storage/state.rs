use crate::proto::etcdserverpb;
use crate::proto::mvccpb;
use crate::storage::RangeBound;
use crate::storage::wal;
use prost::bytes::Bytes;
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Notify;

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
pub(crate) fn make_kv_bytes(key: &[u8], ks: &KeyState) -> Bytes {
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
    /// Next lease id to hand out. Restored past the highest lease id in the WAL.
    pub next_lease_id: i64,
    /// Highest revision released for garbage collection by `compact`.
    pub compact_rev: u64,
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
            next_lease_id: 1,
            compact_rev: 0,
            wal,
            expiry_notify: Arc::new(Notify::new()),
        }
    }

    /// Allocate the next revision for this store.
    ///
    /// Must be called with the store write lock held, so the revision order
    /// matches the order records reach the WAL. Keeping the clock on the state
    /// rather than in a process-wide static also makes independently opened
    /// stores (i.e. tests) independent of each other.
    pub fn alloc_revision(&mut self) -> u64 {
        let rev = self.next_rev;
        self.next_rev += 1;
        rev
    }

    /// Highest revision allocated so far (0 for a fresh store).
    pub fn current_revision(&self) -> u64 {
        self.next_rev.saturating_sub(1)
    }

    /// Allocate the next lease id for this store.
    pub fn alloc_lease_id(&mut self) -> i64 {
        let id = self.next_lease_id;
        self.next_lease_id += 1;
        id
    }

    pub(crate) fn header(&self) -> etcdserverpb::ResponseHeader {
        etcdserverpb::ResponseHeader {
            cluster_id: 1,
            member_id: 1,
            revision: self.current_revision() as i64,
            raft_term: 1,
        }
    }
}

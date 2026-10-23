use crate::proto::etcdserverpb;
use crate::proto::mvccpb;
use crate::storage::RangeBound;
use crate::storage::wal;
use prost::bytes::{Bytes, BytesMut};
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

/// Byte offset of the `mod_revision` varint inside a blob produced by
/// [`wal::encode_kv`], i.e. just past its field tag.
///
/// The encoder emits a fixed field order, so the offset is simply the preceding
/// lengths added up — no tag checks, and the `create_revision` varint in between
/// only has to be skipped, never decoded:
///
/// ```text
/// 0       tag 0x0a
/// 1 .. 6  5-byte overlong key length
/// 6 ..    key
/// +1      tag 0x10                               (create_revision)
/// +1..10  create_revision varint, minimal width  (skipped)
/// +1      tag 0x18                               (mod_revision)   <- returned
/// then 10 bytes of value
/// ```
#[inline]
fn mod_revision_field_offset(kv_bytes: &[u8]) -> usize {
    let key_len = ((kv_bytes[1] as u32) & 0x7f)
        | (((kv_bytes[2] as u32) & 0x7f) << 7)
        | (((kv_bytes[3] as u32) & 0x7f) << 14)
        | (((kv_bytes[4] as u32) & 0x7f) << 21)
        | (((kv_bytes[5] as u32) & 0x0f) << 28);

    // tag + overlong length + key + create_revision tag
    let mut i = 1 + 5 + key_len as usize + 1;
    // advance to the varint's terminating byte (high bit clear)
    while kv_bytes[i] & 0x80 != 0 {
        i += 1;
    }
    i + 2 // past that byte, past the mod_revision tag
}

/// Copy `kv_bytes` with its `mod_revision` replaced by `rev`.
///
/// `mod_revision` is encoded with the fixed-width "overlong" varint
/// ([`wal::encode_overlong_u64`]), so the field is overwritten in place: one
/// exact-size allocation and a 10-byte store, no re-encoding, and nothing after
/// it shifts. Every `kv_bytes` in a [`KeyState`] originates from
/// [`make_kv_bytes`] or from a WAL record built by [`wal::encode_kv`], so the
/// layout is guaranteed.
#[inline]
pub(crate) fn with_mod_revision(kv_bytes: &Bytes, rev: u64) -> Bytes {
    let offset = mod_revision_field_offset(kv_bytes);
    let mut out = BytesMut::from(&kv_bytes[..]);
    out[offset..offset + 10].copy_from_slice(&wal::encode_overlong_u64(rev));
    out.freeze()
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
    /// Number of keys currently alive (tombstones excluded). Maintained by
    /// `apply`/`apply_delete`, so it is O(1) to read and needs no separate
    /// process-wide counter.
    pub key_count: u64,
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
            key_count: 0,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Patching the fixed-width `mod_revision` in place must produce exactly
    /// what a full re-encode would, for every key length and varint width of the
    /// fields that precede it.
    #[test]
    fn with_mod_revision_matches_re_encoding() {
        let keys: [&[u8]; 4] = [
            b"",
            b"k",
            b"/registry/replicasets/default/immich-microservices-85fc7f49bf",
            &[0xff; 200],
        ];
        // (create_revision, previous mod_revision, new mod_revision)
        let cases: [(i64, u64, u64); 5] = [
            (1, 1, 2),                          // narrow, one varint byte each
            (1, 1, 1_000_000),                  // new revision much wider
            (200, 300, 301),                    // 2-byte create_revision shifts the field
            (16_384, 30_000, 4_000_000_000),    // 3-byte create_revision
            (i64::MAX, u64::MAX / 2, u64::MAX), // extreme values
        ];

        for key in keys {
            for (create_rev, prev_rev, new_rev) in cases {
                let (original, ..) =
                    wal::encode_kv(key, b"some value", create_rev, prev_rev as i64, 7, 0);
                let patched = with_mod_revision(&Bytes::from(original), new_rev);
                let (expected, ..) =
                    wal::encode_kv(key, b"some value", create_rev, new_rev as i64, 7, 0);

                assert_eq!(
                    &patched[..],
                    &expected[..],
                    "patched blob differs from re-encode (key len {}, create_rev {}, prev {}, new {})",
                    key.len(),
                    create_rev,
                    prev_rev,
                    new_rev
                );
            }
        }
    }

    /// Decoding the patched blob proves nothing but `mod_revision` moved.
    #[test]
    fn with_mod_revision_preserves_other_fields() {
        let key = b"/registry/pods/default/foo";
        let (original, ..) = wal::encode_kv(key, b"payload", 42, 43, 5, 9);

        let patched = with_mod_revision(&Bytes::from(original), 99);
        let kv = mvccpb::KeyValue::decode(&patched[..]).unwrap();

        assert_eq!(kv.key, key);
        assert_eq!(kv.value, b"payload");
        assert_eq!(kv.create_revision, 42);
        assert_eq!(kv.version, 5);
        assert_eq!(kv.lease, 9);
        assert_eq!(kv.mod_revision, 99);
    }

    /// The hardcoded offset arithmetic must agree with the offset `encode_kv`
    /// computes for itself, across key lengths and every `create_revision`
    /// varint width. If the encoder's layout ever changes, this fails.
    #[test]
    fn mod_revision_offset_matches_encoder() {
        let keys: [&[u8]; 4] = [
            b"",
            b"k",
            b"/registry/replicasets/default/immich-microservices-85fc7f49bf",
            &[0xff; 200],
        ];

        for key in keys {
            for create_rev in [0i64, 1, 127, 128, 16_383, 16_384, 1 << 35, i64::MAX] {
                let (blob, _, mod_rev_offset) = wal::encode_kv(key, b"v", create_rev, 7, 1, 0);
                assert_eq!(
                    mod_revision_field_offset(&blob),
                    mod_rev_offset as usize,
                    "hardcoded offset disagrees with encode_kv (key len {}, create_rev {})",
                    key.len(),
                    create_rev
                );
            }
        }
    }
}

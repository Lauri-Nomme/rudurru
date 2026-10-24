use crate::proto::etcdserverpb;
use crate::storage::apply::apply_record;
use crate::storage::background::{start_compaction_task, start_expiry_task, start_fsync_task};
use crate::storage::state::{KeyState, LeaseState, StoreState};
use crate::storage::wal;
use crate::storage::{
    btree_bounds, eval_compare, matches_range, resolve_range, scan_wal_range, RangeBoundRef,
};
use prost::bytes::Bytes;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use parking_lot::RwLock;
use tonic::Status;

/// A cursor over an encoded `mvccpb.KeyValue`.
///
/// `wal::encode_kv` is the only producer of these blobs, so the field layout is
/// known and the tags can be skipped instead of interpreted: length prefixes are
/// fixed-width "overlong" varints (5 bytes for key/value, 10 for `mod_revision`),
/// which decode without scanning for a terminator.
struct KvCursor<'a> {
    buf: &'a [u8],
    i: usize,
}

impl<'a> KvCursor<'a> {
    #[inline]
    fn skip(&mut self, n: usize) -> Option<()> {
        self.i = self.i.checked_add(n)?;
        (self.i <= self.buf.len()).then_some(())
    }

    #[inline]
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.i.checked_add(n)?;
        let slice = self.buf.get(self.i..end)?;
        self.i = end;
        Some(slice)
    }

    #[inline]
    fn varint(&mut self) -> Option<u64> {
        let buf = self.buf.get(self.i..)?;
        let mut value = 0u64;
        let mut shift = 0;
        for (n, &b) in buf.iter().enumerate() {
            value |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                self.i += n + 1;
                return Some(value);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
        None
    }

    /// Fixed 5-byte overlong key/value length.
    #[inline]
    fn overlong_u32(&mut self) -> Option<u32> {
        wal::decode_overlong_u32(self.take(5)?)
    }

    /// Fixed 10-byte overlong `mod_revision`.
    #[inline]
    fn overlong_u64(&mut self) -> Option<u64> {
        wal::decode_overlong_u64(self.take(10)?)
    }
}

/// The fields of an encoded `mvccpb.KeyValue`, borrowed from the blob.
pub(crate) struct KvFields<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) create_revision: u64,
    pub(crate) mod_revision: u64,
    pub(crate) version: u64,
    pub(crate) value: &'a [u8],
}

/// Pull every sortable field out of a blob in **one** pass, without decoding a
/// `KeyValue` and without allocating.
///
/// Layout written by `wal::encode_kv` (tags are skipped, not checked):
///
/// ```text
/// 0x0a | overlong u32 len (5B) | key
/// 0x10 | varint create_revision
/// 0x18 | overlong u64 (10B)    | mod_revision
/// 0x20 | varint version
/// 0x2a | overlong u32 len (5B) | value
/// 0x30 | varint lease
/// ```
#[inline]
pub(crate) fn parse_kv(buf: &[u8]) -> Option<KvFields<'_>> {
    let mut c = KvCursor { buf, i: 0 };
    c.skip(1)?; // key tag
    let key_len = c.overlong_u32()? as usize;
    let key = c.take(key_len)?;
    c.skip(1)?; // create_revision tag
    let create_revision = c.varint()?;
    c.skip(1)?; // mod_revision tag
    let mod_revision = c.overlong_u64()?;
    c.skip(1)?; // version tag
    let version = c.varint()?;
    c.skip(1)?; // value tag
    let value_len = c.overlong_u32()? as usize;
    let value = c.take(value_len)?;
    Some(KvFields {
        key,
        create_revision,
        mod_revision,
        version,
        value,
    })
}

/// The compared value for a sort target.
enum SortValue<'a> {
    Int(u64),
    Bytes(&'a [u8]),
}

fn cmp_sort_value(a: &SortValue<'_>, b: &SortValue<'_>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (SortValue::Int(x), SortValue::Int(y)) => x.cmp(y),
        (SortValue::Bytes(x), SortValue::Bytes(y)) => x.cmp(y),
        (SortValue::Int(_), SortValue::Bytes(_)) => Ordering::Less,
        (SortValue::Bytes(_), SortValue::Int(_)) => Ordering::Greater,
    }
}

/// Sort a range result like etcd (`sort_order` over `sort_target`), with the key
/// as a deterministic tiebreak.
///
/// `limit` is a hint: when it is smaller than the result set, the sort finds the
/// top `limit` with a linear `select_nth_unstable_by` and sorts only those,
/// instead of a full `O(n log n)` sort over the whole range. Callers still
/// truncate afterwards (and derive `more` from the pre-sort count).
///
/// Bytes fields are borrowed from the blobs, so sorting never copies a value:
/// peak extra memory is one `(SortValue, &[u8])` per element.
pub(crate) fn sort_kvs(kvs: &mut Vec<Bytes>, order: i32, target: i32, limit: usize) {
    use crate::proto::etcdserverpb::range_request::{SortOrder, SortTarget};

    if order != SortOrder::Ascend as i32 && order != SortOrder::Descend as i32 {
        return;
    }

    // Every range result is produced by iterating a BTreeMap, so it is already
    // ascending by key. Sorting on the key axis needs no parsing at all:
    // ASCEND is a no-op and DESCEND is a reverse.
    if target == SortTarget::Key as i32 {
        if order == SortOrder::Descend as i32 {
            kvs.reverse();
        }
        return;
    }

    let descending = order == SortOrder::Descend as i32;
    // Unknown target: leave the (key-ordered) result alone.
    if target != SortTarget::Create as i32
        && target != SortTarget::Mod as i32
        && target != SortTarget::Version as i32
        && target != SortTarget::Value as i32
    {
        return;
    }

    // Sort the entries themselves rather than indices into a side table: the
    // comparator then walks a compact, sequentially-built array instead of
    // chasing `entries[idx]` for every comparison.
    struct Entry<'a> {
        value: SortValue<'a>,
        key: &'a [u8],
        idx: u32,
    }

    let mut entries: Vec<Entry<'_>> = kvs
        .iter()
        .enumerate()
        .map(|(idx, blob)| match parse_kv(blob) {
            Some(f) => {
                let value = match target {
                    t if t == SortTarget::Create as i32 => SortValue::Int(f.create_revision),
                    t if t == SortTarget::Version as i32 => SortValue::Int(f.version),
                    t if t == SortTarget::Value as i32 => SortValue::Bytes(f.value),
                    _ => SortValue::Int(f.mod_revision),
                };
                Entry {
                    value,
                    key: f.key,
                    idx: idx as u32,
                }
            }
            None => Entry {
                value: SortValue::Int(0),
                key: &[],
                idx: idx as u32,
            },
        })
        .collect();

    let cmp = |a: &Entry<'_>, b: &Entry<'_>| {
        let ord = cmp_sort_value(&a.value, &b.value).then_with(|| a.key.cmp(b.key));
        if descending {
            ord.reverse()
        } else {
            ord
        }
    };

    // Top-k: partition in O(n), then sort only the k selected entries. Only
    // worth it when the limit is a small fraction of the range — otherwise
    // `sort_unstable_by` is as fast (measured; parsing dominates either way).
    if limit > 0 && limit.saturating_mul(64) <= entries.len() {
        entries.select_nth_unstable_by(limit - 1, cmp);
        entries.truncate(limit);
    }
    entries.sort_unstable_by(cmp);

    let order: Vec<usize> = entries.iter().map(|e| e.idx as usize).collect();
    drop(entries);

    let mut out = Vec::with_capacity(order.len());
    for i in order {
        out.push(std::mem::take(&mut kvs[i]));
    }
    *kvs = out;
}

/// etcd's lease TTL bounds.
///
/// `lessor.MaxLeaseTTL = math.MaxInt64 / int64(time.Second) - 1` and
/// `LeaseMinTTL = 2`: shorter TTLs are silently raised to the minimum, longer
/// ones are rejected. This also keeps a client-supplied integer out of
/// `Instant::now() + Duration::from_secs(ttl as u64)`, where a negative TTL
/// casts to ~1.8e19 seconds and the addition overflows — with `panic = "abort"`
/// in the release profile that aborts the process.
const LEASE_MAX_TTL: i64 = i64::MAX / 1_000_000_000 - 1;
const LEASE_MIN_TTL: i64 = 2;

/// etcd rejects a zero-length key on every KV RPC with this exact status.
/// ("all keys" is spelled `key = "\0"`, not `key = ""`.)
fn check_key(key: &[u8]) -> Result<(), Status> {
    if key.is_empty() {
        return Err(Status::new(
            tonic::Code::InvalidArgument,
            "etcdserver: key is not provided",
        ));
    }
    Ok(())
}

fn check_lease_ttl(ttl: i64) -> Result<i64, Status> {
    if ttl > LEASE_MAX_TTL {
        return Err(Status::new(
            tonic::Code::OutOfRange,
            "etcdserver: too large lease TTL",
        ));
    }
    Ok(ttl.max(LEASE_MIN_TTL))
}

/// etcd rejects transactions whose **writes** touch overlapping keys: puts are
/// point intervals, delete ranges are intervals, and no two may intersect.
/// Range (read) ops do not participate — reading a key the same txn writes is
/// legal, and the read sees the txn's own write.
fn check_txn_duplicate_keys(req: &etcdserverpb::TxnRequest) -> Result<(), Status> {
    check_txn_branch(&req.success)?;
    check_txn_branch(&req.failure)
}

fn check_txn_branch(ops: &[etcdserverpb::RequestOp]) -> Result<(), Status> {
    use etcdserverpb::request_op::Request;

    let mut intervals: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for op in ops {
        match &op.request {
            Some(Request::RequestPut(p)) => {
                check_key(&p.key)?;
                intervals.push(point_interval(&p.key));
            }
            Some(Request::RequestDeleteRange(d)) => {
                check_key(&d.key)?;
                intervals.push(bound_interval(resolve_range(&d.key, &d.range_end).to_ref()));
            }
            Some(Request::RequestRange(r)) => check_key(&r.key)?,
            Some(Request::RequestTxn(t)) => {
                check_txn_branch(&t.success)?;
                check_txn_branch(&t.failure)?;
            }
            None => {}
        }
    }

    for i in 0..intervals.len() {
        for j in (i + 1)..intervals.len() {
            if intervals_overlap(&intervals[i], &intervals[j]) {
                return Err(Status::new(
                    tonic::Code::InvalidArgument,
                    "etcdserver: duplicate key given in txn request",
                ));
            }
        }
    }
    Ok(())
}

fn point_interval(key: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let mut end = key.to_vec();
    end.push(0);
    (key.to_vec(), Some(end))
}

/// Smallest key greater than every key with `prefix`; `None` when unbounded
/// (prefix consists only of 0xFF).
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut v = prefix.to_vec();
    while let Some(last) = v.last_mut() {
        if *last == 0xff {
            v.pop();
        } else {
            *last += 1;
            return Some(v);
        }
    }
    None
}

fn bound_interval(bound: RangeBoundRef<'_>) -> (Vec<u8>, Option<Vec<u8>>) {
    match bound {
        RangeBoundRef::All => (Vec::new(), None),
        RangeBoundRef::Point(k) => point_interval(k),
        RangeBoundRef::Prefix(p) => (p.to_vec(), prefix_end(p)),
        RangeBoundRef::From(k) => (k.to_vec(), None),
        RangeBoundRef::Range(s, e) => (s.to_vec(), Some(e.to_vec())),
    }
}

fn intervals_overlap(a: &(Vec<u8>, Option<Vec<u8>>), b: &(Vec<u8>, Option<Vec<u8>>)) -> bool {
    if let Some(end) = &a.1 {
        if a.0 >= *end {
            return false;
        }
    }
    if let Some(end) = &b.1 {
        if b.0 >= *end {
            return false;
        }
    }
    let starts_before_end = |start: &[u8], end: &Option<Vec<u8>>| match end {
        Some(e) => start < e.as_slice(),
        None => true,
    };
    starts_before_end(&a.0, &b.1) && starts_before_end(&b.0, &a.1)
}

/// Place a deferred historical-range response at its slot in the txn response
/// tree (`path` is a chain of response indices through nested txns).
fn patch_deferred_range(
    responses: &mut [etcdserverpb::ResponseOp],
    path: &[usize],
    resp: etcdserverpb::RangeResponse,
) {
    let Some((&first, rest)) = path.split_first() else {
        return;
    };
    if rest.is_empty() {
        responses[first].response =
            Some(etcdserverpb::response_op::Response::ResponseRange(resp));
        return;
    }
    if let Some(etcdserverpb::response_op::Response::ResponseTxn(nested)) =
        &mut responses[first].response
    {
        patch_deferred_range(&mut nested.responses, rest, resp);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StoreStats {
    pub revision: u64,
    pub keys: u64,
    pub watchers: u64,
    pub leases: u64,
}

#[derive(Clone, Debug)]
pub struct Store {
    pub state: Arc<RwLock<StoreState>>,
}
/// Validation and application helpers shared by the direct RPCs and by
/// transactions, which apply several writes under one lock at one revision.
impl StoreState {
    fn validate_put(&self, req: &etcdserverpb::PutRequest) -> Result<(), Status> {
        check_key(&req.key)?;
        if req.ignore_value && !req.value.is_empty() {
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "etcdserver: value is provided",
            ));
        }
        if req.ignore_lease && req.lease != 0 {
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "etcdserver: lease is provided",
            ));
        }

        // ignore_value/ignore_lease mean "reuse the stored value/lease", which
        // is an error when there is nothing stored (etcd: key not found).
        let prev_alive = self.keys.get(&req.key).filter(|k| k.is_alive());
        if (req.ignore_value || req.ignore_lease) && prev_alive.is_none() {
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "etcdserver: key not found",
            ));
        }

        let lease = if req.ignore_lease {
            prev_alive.map(|k| k.lease).unwrap_or(req.lease)
        } else {
            req.lease
        };
        if lease != 0 && !self.leases.contains_key(&lease) {
            return Err(Status::new(
                tonic::Code::NotFound,
                "etcdserver: requested lease not found",
            ));
        }
        Ok(())
    }

    /// Apply a validated put at `rev`. The caller holds the write lock and owns
    /// the revision, so a transaction can apply several writes at one revision.
    fn put_at(
        &mut self,
        req: etcdserverpb::PutRequest,
        rev: u64,
    ) -> Result<etcdserverpb::PutResponse, Status> {
        let key = req.key;

        // ignore_value/ignore_lease mean "reuse the stored value/lease", which
        // is an error when there is nothing stored (etcd: key not found).
        let prev_alive = self.keys.get(&key).filter(|k| k.is_alive()).cloned();
        if (req.ignore_value || req.ignore_lease) && prev_alive.is_none() {
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "etcdserver: key not found",
            ));
        }

        let value = if req.ignore_value {
            prev_alive
                .as_ref()
                .map(|k| k.value.to_vec())
                .unwrap_or_default()
        } else {
            req.value
        };
        let lease = if req.ignore_lease {
            prev_alive.as_ref().map(|k| k.lease).unwrap_or(req.lease)
        } else {
            req.lease
        };

        if lease != 0 && !self.leases.contains_key(&lease) {
            return Err(Status::new(
                tonic::Code::NotFound,
                "etcdserver: requested lease not found",
            ));
        }

        let prev = prev_alive;
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
        if let Err(e) = self.wal.append_kv(&record) {
            tracing::error!("WAL append failed: {e}");
            return Err(Status::new(
                tonic::Code::Internal,
                "etcdserver: wal write failed",
            ));
        }

        let prev = self.apply(key, value, lease, rev, None);

        let header = Some(self.header());
        let prev_kv = if req.prev_kv {
            prev.as_ref()
                .map(|p| p.kv_bytes.clone())
                .unwrap_or_default()
        } else {
            Bytes::new()
        };

        Ok(etcdserverpb::PutResponse { header, prev_kv })
    
    }

    /// Apply a delete range at `rev`, under the caller's write lock.
    fn delete_range_at(
        &mut self,
        req: etcdserverpb::DeleteRangeRequest,
        rev: u64,
    ) -> Result<etcdserverpb::DeleteRangeResponse, Status> {
        let bound = resolve_range(&req.key, &req.range_end);

        let keys_to_delete: Vec<Vec<u8>> = match bound.to_ref() {
            RangeBoundRef::Point(k) => self
                .keys
                .get(k)
                .filter(|ks| ks.is_alive())
                .map(|_| k.to_vec())
                .into_iter()
                .collect(),
            RangeBoundRef::From(k) => {
                let start = k.to_vec();
                self
                    .keys
                    .range(start..)
                    .filter(|(_, ks)| ks.is_alive())
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::Prefix(p) => {
                let start = p.to_vec();
                self
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
                self
                    .keys
                    .range(start..end)
                    .filter(|(_, ks)| ks.is_alive())
                    .map(|(k, _)| k.clone())
                    .collect()
            }
            RangeBoundRef::All => self
                .keys
                .iter()
                .filter(|(_, ks)| ks.is_alive())
                .map(|(k, _)| k.clone())
                .collect(),
        };

        // Build WAL records before mutating self.
        let mut records: Vec<wal::KvWalRecord> = Vec::with_capacity(keys_to_delete.len());
        let mut prevs: Vec<Option<KeyState>> = Vec::with_capacity(keys_to_delete.len());
        for key in &keys_to_delete {
            let prev = self.keys.get(key).filter(|k| k.is_alive()).cloned();
            if let Some(p) = &prev {
                let mut flags = wal::DELETED;
                if p.lease != 0 {
                    flags |= wal::HAS_LEASE;
                }
                records.push(wal::KvWalRecord::new(
                    flags,
                    key,
                    &p.value,
                    p.create_revision as i64,
                    rev as i64,
                    p.version,
                    p.lease,
                ));
            }
            prevs.push(prev);
        }

        if let Err(e) = self.wal.append_kv_batch(&records) {
            tracing::error!("WAL batch append on delete_range failed: {e}");
            return Err(Status::new(
                tonic::Code::Internal,
                "etcdserver: wal write failed",
            ));
        }

        // Apply deletions to self only after WAL write succeeds.
        let mut prev_kvs = Vec::new();
        for (i, key) in keys_to_delete.iter().enumerate() {
            self.apply_delete(key.clone(), rev);
            if req.prev_kv {
                if let Some(p) = &prevs[i] {
                    prev_kvs.push(p.kv_bytes.clone());
                }
            }
        }

        Ok(etcdserverpb::DeleteRangeResponse {
            header: Some(self.header()),
            deleted: keys_to_delete.len() as i64,
            prev_kvs,
        })
    
    }
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
            if rev <= state.compact_rev {
                continue;
            }
            apply_record(&mut state, rec);
            if rev > max_rev {
                max_rev = rev;
            }
        }

        state.next_rev = max_rev + 1;

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
                state.next_lease_id = max_id + 1;
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
        tracing::info!(
            "rudurru ready: revision={}, keys={}, leases_restored={}, compact_rev={}",
            max_rev,
            state.key_count,
            lease_count,
            state.compact_rev,
        );

        let state_arc = Arc::new(RwLock::new(state));
        start_fsync_task(
            state_arc.read().wal.file.clone(),
            state_arc.read().wal.dirty.clone(),
        );
        start_expiry_task(state_arc.clone());
        let store = Self { state: state_arc };
        start_compaction_task(store.clone(), wal_path.to_string());

        Ok(store)
    }


    pub async fn compact_rev(&self) -> u64 {
        self.state.read().compact_rev
    }

    /// Highest revision allocated by this store.
    pub fn current_revision(&self) -> u64 {
        self.state.read().current_revision()
    }

    /// Point-in-time counters for the status log and metrics.
    ///
    /// Derived from the state under one read lock rather than kept in separate
    /// atomics: there is nothing to keep in sync, and no counter can drift from
    /// the data it describes.
    pub fn stats(&self) -> StoreStats {
        let state = self.state.read();
        StoreStats {
            revision: state.current_revision(),
            keys: state.key_count,
            watchers: state.watchers.len() as u64,
            leases: state.leases.len() as u64,
        }
    }

    pub async fn wal_path(&self) -> String {
        self.state.read().wal.path.clone()
    }

    // ── KV operations ──────────────────────────────────────────────────

    pub async fn range(
        &self,
        req: etcdserverpb::RangeRequest,
    ) -> Result<etcdserverpb::RangeResponse, Status> {
        check_key(&req.key)?;
        let mut target_rev = if req.revision > 0 {
            req.revision as u64
        } else {
            0
        };
        let current_rev = self.current_revision();

        // etcd rejects any revision above the current one, however small the
        // gap: `ErrFutureRev` is `codes.OutOfRange`. Serving "current" instead
        // hides a broken client (a reader that asked for N and silently got
        // today's data cannot tell), so there is no tolerance here.
        if target_rev > current_rev {
            return Err(Status::new(
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision is a future revision",
            ));
        }

        // `ErrCompacted` is also `codes.OutOfRange` (kube-apiserver's
        // isCompactedErr matches on the code, not just the message).
        if target_rev > 0 && target_rev < self.compact_rev().await {
            return Err(Status::new(
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision has been compacted",
            ));
        }

        // Historical query (target_rev > 0, >= COMPACT_REV)
        if target_rev > 0 {
            return self.range_historical(req, target_rev).await;
        }

        // Current state (revision == 0)
        let state = self.state.read();
        Self::range_current(&state, &req)
    }

/// Range over the current state. Takes `&StoreState` so it can run either under
/// a read lock (normal path) or under the write lock held by a transaction —
/// which is what makes in-txn reads see the transaction's own earlier writes,
/// as etcd does.
fn range_current(
    state: &StoreState,
    req: &etcdserverpb::RangeRequest,
) -> Result<etcdserverpb::RangeResponse, Status> {
        let bound = resolve_range(&req.key, &req.range_end);

        // Point queries are exact-key lookups, so resolve them with a single
        // BTreeMap::get. This is both cheaper than materialising [key, key+1)
        // bounds and an iterator, and correct: a [key, key+1) scan also matches
        // unrelated keys that merely share this key as a prefix, which would
        // hand the caller a completely different object.
        if let RangeBoundRef::Point(key) = bound.to_ref() {
            let mut kvs: Vec<Bytes> = Vec::with_capacity(1);

            if let Some(ks) = state.keys.get(key) {
                if ks.delete_revision == 0
                    && (req.min_mod_revision <= 0
                        || (ks.mod_revision as i64) >= req.min_mod_revision)
                    && (req.max_mod_revision <= 0
                        || (ks.mod_revision as i64) <= req.max_mod_revision)
                    && (req.min_create_revision <= 0
                        || (ks.create_revision as i64) >= req.min_create_revision)
                    && (req.max_create_revision <= 0
                        || (ks.create_revision as i64) <= req.max_create_revision)
                {
                    kvs.push(if req.keys_only {
                        let (kv_bytes, _, _) = wal::encode_kv(key, b"", 0, 0, 0, 0);
                        Bytes::from(kv_bytes)
                    } else {
                        ks.kv_bytes.clone()
                    });
                }
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

            return Ok(etcdserverpb::RangeResponse {
                header: Some(state.header()),
                kvs,
                more: false,
                count,
            });
        }

        let cap = if req.limit > 0 {
            req.limit as usize
        } else {
            state.keys.len()
        };
        let mut kvs: Vec<Bytes> = Vec::with_capacity(cap);

        let (range_start, range_end): (Option<Vec<u8>>, Option<Vec<u8>>) = match bound.to_ref() {
            RangeBoundRef::All => (None, None),
            // Point queries are resolved and returned above.
            RangeBoundRef::Point(_) => unreachable!("point range handled before bounds"),
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

        let limit = if req.limit > 0 { req.limit as usize } else { 0 };
        sort_kvs(&mut kvs, req.sort_order, req.sort_target, limit);

        // `count` is the pre-sort length: sort_kvs may already have dropped the
        // tail, so `more` cannot be derived from kvs.len() any more.
        let more = req.limit > 0 && count > req.limit;
        if more {
            kvs.truncate(req.limit as usize);
        }

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
                let mut kvs: Vec<Bytes> = phase1_direct.into_iter().map(|(_, b)| b).collect();
                let limit = if req.limit > 0 { req.limit as usize } else { 0 };
                sort_kvs(&mut kvs, req.sort_order, req.sort_target, limit);
                let more = req.limit > 0 && count > req.limit;
                if more {
                    kvs.truncate(req.limit as usize);
                }
                return Ok(etcdserverpb::RangeResponse {
                    header: Some(etcdserverpb::ResponseHeader {
                        cluster_id: 1,
                        member_id: 1,
                        revision: target_rev as i64,
                        raft_term: 1,
                    }),
                    kvs,
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

        let mut kvs: Vec<Bytes> = merged.into_values().collect();
        sort_kvs(&mut kvs, req.sort_order, req.sort_target, limit);
        kvs.truncate(limit);
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
        let mut state = self.state.write();
        state.validate_put(&req)?;
        let rev = state.alloc_revision();
        state.put_at(req, rev)
    }

    pub async fn delete_range(
        &self,
        req: etcdserverpb::DeleteRangeRequest,
    ) -> Result<etcdserverpb::DeleteRangeResponse, Status> {
        check_key(&req.key)?;
        let mut state = self.state.write();
        let rev = state.alloc_revision();
        state.delete_range_at(req, rev)
    }

    pub async fn txn(
        &self,
        req: etcdserverpb::TxnRequest,
    ) -> Result<etcdserverpb::TxnResponse, Status> {
        // etcd validates the whole request before applying anything: writes in a
        // txn must touch disjoint keys ("duplicate key given in txn request").
        check_txn_duplicate_keys(&req)?;

        // One lock for the whole transaction, and one revision for all of its
        // writes. Readers therefore see the txn either entirely or not at all:
        // no intermediate revision exists. The lock lives in this block so it is
        // released before the deferred (async) historical ranges run.
        let (succeeded, header, mut responses, deferred) = {
            let mut state = self.state.write();
            let succeeded = req.compare.iter().all(|c| eval_compare(&state, c));
            let ops = if succeeded { req.success } else { req.failure };

            let mut rev: Option<u64> = None;
            let mut deferred: Vec<(Vec<usize>, etcdserverpb::RangeRequest)> = Vec::new();
            let responses = Self::apply_txn_ops(
                &mut state,
                ops,
                &mut rev,
                &mut Vec::new(),
                &mut deferred,
                0,
            )?;
            (succeeded, state.header(), responses, deferred)
        };

        // Historical ranges need the WAL and take read locks, so they run after
        // the write lock is released and are patched back into place. Their
        // requested revision is <= the txn's, so the result is unaffected by the
        // txn's own writes.
        for (path, req) in deferred {
            let resp = self.range(req).await?;
            patch_deferred_range(&mut responses, &path, resp);
        }

        Ok(etcdserverpb::TxnResponse {
            header: Some(header),
            succeeded,
            responses,
        })
    }

    /// Apply a txn's operations under the caller's write lock.
    ///
    /// `rev` is allocated lazily on the first write and shared by every write in
    /// the transaction (including nested ones). `path` tracks the response
    /// position so deferred historical ranges can be patched afterwards.
    fn apply_txn_ops(
        state: &mut StoreState,
        ops: Vec<etcdserverpb::RequestOp>,
        rev: &mut Option<u64>,
        path: &mut Vec<usize>,
        deferred: &mut Vec<(Vec<usize>, etcdserverpb::RangeRequest)>,
        depth: u8,
    ) -> Result<Vec<etcdserverpb::ResponseOp>, Status> {
        let mut responses = Vec::with_capacity(ops.len());

        for (i, op) in ops.into_iter().enumerate() {
            path.push(i);
            let response = match op.request {
                Some(etcdserverpb::request_op::Request::RequestRange(r)) => {
                    if r.revision > 0 {
                        deferred.push((path.clone(), r));
                        None
                    } else {
                        // Reads see the transaction's own earlier writes: the
                        // state is already mutated under this same lock.
                        Some(etcdserverpb::response_op::Response::ResponseRange(
                            Store::range_current(state, &r)?,
                        ))
                    }
                }
                Some(etcdserverpb::request_op::Request::RequestPut(p)) => {
                    state.validate_put(&p)?;
                    let r = *rev.get_or_insert_with(|| state.alloc_revision());
                    Some(etcdserverpb::response_op::Response::ResponsePut(
                        state.put_at(p, r)?,
                    ))
                }
                Some(etcdserverpb::request_op::Request::RequestDeleteRange(d)) => {
                    check_key(&d.key)?;
                    let r = *rev.get_or_insert_with(|| state.alloc_revision());
                    Some(etcdserverpb::response_op::Response::ResponseDeleteRange(
                        state.delete_range_at(d, r)?,
                    ))
                }
                Some(etcdserverpb::request_op::Request::RequestTxn(t)) => {
                    if depth >= 1 {
                        return Err(Status::new(
                            tonic::Code::InvalidArgument,
                            "etcdserver: nested txn is not allowed",
                        ));
                    }
                    let nested_succeeded =
                        t.compare.iter().all(|c| eval_compare(state, c));
                    let nested_ops = if nested_succeeded { t.success } else { t.failure };
                    let nested = Self::apply_txn_ops(
                        state,
                        nested_ops,
                        rev,
                        path,
                        deferred,
                        depth + 1,
                    )?;
                    Some(etcdserverpb::response_op::Response::ResponseTxn(
                        etcdserverpb::TxnResponse {
                            header: Some(state.header()),
                            succeeded: nested_succeeded,
                            responses: nested,
                        },
                    ))
                }
                None => None,
            };
            path.pop();
            responses.push(etcdserverpb::ResponseOp { response });
        }

        Ok(responses)
    }


    pub async fn compact(
        &self,
        req: etcdserverpb::CompactionRequest,
    ) -> Result<etcdserverpb::CompactionResponse, Status> {
        let rev = req.revision as u64;
        let mut state = self.state.write();
        let current = state.current_revision();

        if rev > current {
            return Err(Status::new(
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision is a future revision",
            ));
        }
        if rev <= state.compact_rev {
            return Err(Status::new(
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision has been compacted",
            ));
        }

        state.compact_rev = rev;

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
    ) -> Result<etcdserverpb::LeaseGrantResponse, Status> {
        // Validate before touching state (and before doing `Instant + Duration`
        // with a client-supplied integer).
        let ttl = check_lease_ttl(req.ttl)?;
        let mut state = self.state.write();
        let id = if req.id != 0 {
            req.id
        } else {
            state.alloc_lease_id()
        };
        if state.leases.contains_key(&id) {
            return Err(Status::new(
                tonic::Code::FailedPrecondition,
                "etcdserver: lease already exists",
            ));
        }
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
        state.expiry_notify.notify_one();
        tracing::debug!(id, ttl, "lease_granted");
        Ok(etcdserverpb::LeaseGrantResponse {
            header: Some(state.header()),
            id,
            ttl,
            error: String::new(),
        })
    }

    pub async fn lease_revoke(
        &self,
        req: etcdserverpb::LeaseRevokeRequest,
    ) -> Result<etcdserverpb::LeaseRevokeResponse, Status> {
        let mut state = self.state.write();
        let id = req.id;
        if !state.leases.contains_key(&id) {
            return Err(Status::new(
                tonic::Code::NotFound,
                "etcdserver: requested lease not found",
            ));
        }
        state.leases.remove(&id);
        state.expiry_notify.notify_one();
        tracing::info!(id, "lease_revoked");
        state.delete_keys_for_lease(id)?;
        Ok(etcdserverpb::LeaseRevokeResponse {
            header: Some(state.header()),
        })
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
    ) -> Result<etcdserverpb::LeaseTimeToLiveResponse, Status> {
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
            Ok(etcdserverpb::LeaseTimeToLiveResponse {
                header: Some(state.header()),
                id: req.id,
                ttl: remaining.max(0),
                granted_ttl: ls.ttl,
                keys,
            })
        } else {
            Ok(etcdserverpb::LeaseTimeToLiveResponse {
                header: Some(state.header()),
                id: req.id,
                ttl: -1,
                granted_ttl: -1,
                keys: vec![],
            })
        }
    }

    pub async fn lease_leases(
        &self,
    ) -> Result<etcdserverpb::LeaseLeasesResponse, Status> {
        let state = self.state.read();
        let leases = state
            .leases
            .keys()
            .map(|id| etcdserverpb::LeaseStatus { id: *id })
            .collect();
        Ok(etcdserverpb::LeaseLeasesResponse {
            header: Some(state.header()),
            leases,
        })
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
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        for (k, ks) in state.keys.iter() {
            if ks.delete_revision != 0 {
                continue;
            }
            k.hash(&mut hasher);
            ks.value.hash(&mut hasher);
        }
        hasher.finish()
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
            snapshot_rev = state.current_revision();
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

}

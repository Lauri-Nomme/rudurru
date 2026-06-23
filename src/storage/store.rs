use crate::proto::etcdserverpb;
use crate::storage::apply::apply_record;
use crate::storage::background::{start_compaction_task, start_expiry_task, start_fsync_task};
use crate::storage::state::{KeyState, LeaseState, StoreState};
use crate::storage::wal;
use crate::storage::{
    btree_bounds, current_revision, eval_compare, matches_range, next_lease_id, next_revision,
    resolve_range, scan_wal_range, RangeBoundRef, COMPACT_REV, KEY_COUNT, LEASE_COUNT,
    NEXT_LEASE_ID, NEXT_REV, WATCHER_COUNT,
};
use prost::bytes::Bytes;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use parking_lot::RwLock;
use tonic::Status;

#[derive(Clone, Debug)]
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
            return Err(Status::new(
                tonic::Code::Internal,
                "etcdserver: wal write failed",
            ));
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
    ) -> Result<etcdserverpb::DeleteRangeResponse, Status> {
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

        // Build WAL records before mutating state.
        let mut records: Vec<wal::KvWalRecord> = Vec::with_capacity(keys_to_delete.len());
        let mut prevs: Vec<Option<KeyState>> = Vec::with_capacity(keys_to_delete.len());
        for key in &keys_to_delete {
            let prev = state.keys.get(key).filter(|k| k.is_alive()).cloned();
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

        if let Err(e) = state.wal.append_kv_batch(&records) {
            tracing::error!("WAL batch append on delete_range failed: {e}");
            return Err(Status::new(
                tonic::Code::Internal,
                "etcdserver: wal write failed",
            ));
        }

        // Apply deletions to state only after WAL write succeeds.
        let mut prev_kvs = Vec::new();
        for (i, key) in keys_to_delete.iter().enumerate() {
            state.apply_delete(key.clone(), rev);
            if req.prev_kv {
                if let Some(p) = &prevs[i] {
                    prev_kvs.push(p.kv_bytes.clone());
                }
            }
        }

        Ok(etcdserverpb::DeleteRangeResponse {
            header: Some(state.header()),
            deleted: keys_to_delete.len() as i64,
            prev_kvs,
        })
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
                    let resp = self.delete_range(d).await?;
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
    ) -> Result<etcdserverpb::LeaseGrantResponse, Status> {
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
        state.leases.remove(&id);
        LEASE_COUNT.fetch_sub(1, Ordering::Relaxed);
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

}

# Historical Revision Queries

**Date:** 2026-06-20  
**Driver:** 14k/day k3s cache consistency errors, all `etcdDigest="cbf29ce484222325"` (FNV-1a offset basis = hash of empty input)  
**Root Cause:** `src/storage/mod.rs` returned empty response instead of `ErrCompacted` when `req.revision < COMPACT_REV`

---

## 1. Problem

k3s runs periodic cache consistency checks by issuing `LIST(resourceVersion=N)`. When N refers to a compacted revision, Rudurru was returning `200 OK` with zero items instead of an etcd `ErrCompacted`. This caused k3s to never rebuild its watch cache — the stale resource version persisted forever, and every subsequent consistency check failed.

```
CacheItem at RV=1111   →  LIST(revision=3172445)  →  empty response
digest(empty) = 0xcbf29ce484222325  (FNV-1a offset basis)
digest(cache) = 0xb9b8e6386ef52b11
            ↓
    "Cache consistency check failed"  ×14437/day
```

## 2. etcd Protocol Behavior

| Condition | etcd Behavior |
|-----------|---------------|
| `revision == 0` | Return latest state |
| `revision > 0 && revision >= COMPACT_REV && revision <= current_rev` | Return state at that revision |
| `revision > current_rev` | `ErrFutureRev` (`Unavailable`) |
| `revision < COMPACT_REV` | `ErrCompacted` (`Unavailable`) |

## 3. Implementation: WAL-Based Historical Reconstruction

### Design

Reconstruct state at revision X for the **requested key range** by combining the BTreeMap (current state) with a targeted WAL replay for keys where the current value isn't valid at revision X.

**Deleted keys remain in the BTreeMap** with their `delete_revision`. This is the key insight: Phase 1 can conclusively determine the full result set for any target revision without scanning the WAL, **except** when a key has been through a `delete→recreate` cycle (the `rebirth` edge case).

### Phase 1: BTreeMap Scan (under read lock)

```
For each (k, ks) in the key range:
  ── Deleted keys (delete_revision != 0) ──
    create_rev > target_rev               → skip (didn't exist)
    delete_rev <= target_rev              → skip (already deleted)
    delete_rev > target_rev && mod_rev <= target_rev → DIRECT (value correct)
    delete_rev > target_rev && mod_rev > target_rev  → WAL (value stale)

  ── Alive keys (delete_revision == 0) ──
    create_rev > target_rev:
      if rebirth:                         → WAL (may have existed in prior lifetime)
      else:                               → skip (didn't exist)
    mod_rev <= target_rev                 → DIRECT (value correct)
    mod_rev > target_rev                  → WAL (value stale)
```

**`rebirth` edge case:** When a key is deleted and then recreated, the BTreeMap entry has the *new* `create_revision`. If `target_rev < create_rev` but the key existed in a prior lifetime, only a WAL scan can confirm. The `rebirth: bool` flag on `KeyState` records this cycle.

**Early return:** If `phase1_stale_keys` is empty, **zero WAL I/O** — the BTreeMap has the complete answer. This is safe because all keys (including tombstoned) are visible in Phase 1.

### Phase 2: WAL Scan (no store lock)

```
- Open separate read-only file handle
- Read entire WAL into buffer
- For each record with revision <= target_rev:
    - PUT/UPDATE: insert key → kv_bytes into HashMap
    - DELETE: remove key from HashMap
- Result: HashMap<Vec<u8>, Bytes> of state at target_rev for stale keys
```

### Phase 3: Merge

```
1. Insert Phase 1 DIRECT keys into BTreeMap (for deterministic ordering)
2. For each Phase 1 STALE key:
     - Use WAL value if found (preferred)
     - Fallback to current BTreeMap value if WAL miss (compaction gap)
3. Apply limit, keys_only, count_only
4. Return RangeResponse with header.revision = target_rev
```

### Performance

| Scenario | WAL Scan | Time |
|----------|----------|------|
| All keys current (mod_rev <= target) | **Skipped** | ~0.01 ms |
| Some keys stale | Full WAL scan, filtered | 5–10 ms |

Logging at `tracing::debug!` records duration per phase and key counts served from each phase.

## 4. KeyState Structure

```rust
pub struct KeyState {
    pub value: Arc<[u8]>,
    pub mod_revision: u64,
    pub create_revision: u64,
    pub version: i64,
    pub lease: i64,
    pub delete_revision: u64,    // 0 = alive
    pub rebirth: bool,           // true after delete→recreate cycle
    pub kv_bytes: Bytes,
}
```

## 5. Error Handling

- `range()` returns `Result<etcdserverpb::RangeResponse, tonic::Status>`
- Compacted: `Err(Status::new(Code::Unavailable, "etcdserver: mvcc: required revision has been compacted"))`
- Future: `Err(Status::new(Code::Unavailable, "etcdserver: mvcc: required revision is a future revision"))`
- Server handler in `kv.rs` uses `?` to propagate errors to the etcd API
- Txn ranges use `.unwrap()` (internal, no user-supplied revision)

## 6. Production Validation

Measured from 460 consecutive historical queries on the live k3s cluster (10 min window after deploy):

| Metric | Value |
|--------|-------|
| **WAL scans triggered** | **0** — all 460 queries hit Phase 1 early return |
| BTreeMap scan time | 2–61 µs (median 8 µs) |
| Total query time (Phase 1 only) | 9–109 µs (median 45 µs) |
| Queries returning count=0 | 226 (no data at target RV for that key prefix) |
| Queries returning count≥1 | 234 |
| k3s consistency check failures | **0** (was 14k/day before fix) |

The `stale=0` across all queries confirms: keeping deleted keys in the BTreeMap eliminates WAL I/O entirely for this workload. Every query completed with zero disk reads beyond the BTreeMap walk.

### Debug Log Fields

Each `historical_range` log line includes:

| Field | Example | Description |
|-------|---------|-------------|
| `target_rev` | `3172445` | Revision being queried |
| `key` | `/registry/pods/` | Requested key prefix |
| `range_end` | `/registry/pods0` | Range end (prefix queries use `\0` suffix) |
| `direct` | `8` | Keys served from BTreeMap (mod_rev ≤ target) |
| `stale` | `0` | Keys needing WAL reconstruction (mod_rev > target) |
| `count` / `total_keys` | `8` | Total keys in result set |
| `from_phase1` | `8` | Keys sourced from Phase 1 (no WAL) |
| `from_wal` | `0` | Keys sourced from WAL |
| `elapsed_us` | `45` | Wall-clock microseconds |

## 7. Test Plan

| # | Test | What it covers |
|---|------|----------------|
| 1 | compacted_revision_error | Error guard: target_rev < COMPACT_REV → ErrCompacted |
| 2 | future_revision_error | Error guard: target_rev > current_rev → ErrFutureRev |
| 3 | zero_revision_returns_current | Zero revision returns current state (existing path) |
| 4 | after_create | Key created after target → not included |
| 5 | before_delete | Key deleted after target → included (Phase 2) |
| 6 | after_delete | Key deleted before target → not included |
| 7 | after_compaction | WAL-compacted range returns correct data |
| 8 | value_at_revision | Key modified after target → old value returned (Phase 2) |
| 9 | all_keys_current_skip_wal | No stale keys → zero WAL I/O |
| 10 | some_keys_stale | Mix of current + stale keys |
| 11 | key_modified_after_target | Point lookup, stale key → WAL value |
| 12 | prefix_range | Prefix query, mixed stale/current |
| 13 | range_bounds | Range from/to query |
| 14 | limit | Limit applied correctly |
| 15 | count_only | Count-only mode |
| 16 | keys_only | Keys-only mode |
| 17 | point_lookup | Single key, various timelines |
| 18 | delete_recreate | Key deleted then recreated, query at rev between lifetimes |
| 19 | delete_recreate_after_rebirth | After rebirth, query at pre-rebirth rev needs WAL |
| 20 | multiple_stale_keys_limit | Many stale keys with limit |

## 8. Files

| File | Contents |
|------|----------|
| `src/storage/mod.rs` | `range()`, `range_historical()`, `apply_delete()`, `scan_wal_range()`, `KeyState`, `btree_bounds()`, `resolve_range()` |
| `src/storage/wal.rs` | `KvWalRecord`, `WalFile`, record serialization |
| `src/server/kv.rs` | Range handler, propagates `Result` via `?` |
| `prd/historical-queries.md` | This document |

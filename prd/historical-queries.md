# Historical Revision Queries & the Compacted Guard Bug

**Date:** 2026-06-20  
**Driver:** 14k/day k3s cache consistency errors, all `etcdDigest="cbf29ce484222325"` (FNV-1a offset basis = hash of empty input)  
**Root Cause:** `src/storage/mod.rs:336` returns empty response instead of `ErrCompacted` when `req.revision < COMPACT_REV`  
**Fix:** Return proper etcd `ErrCompacted` / `ErrFutureRev` gRPC errors; for un-compacted historical queries return current state as best-effort

---

## 1. Current Bug

### 1.1 The Guard

```rust
// src/storage/mod.rs:336-343
if req.revision > 0 && (req.revision as u64) < COMPACT_REV.load(Ordering::Relaxed) {
    return etcdserverpb::RangeResponse {
        header: Some(state.header()),
        kvs: vec![],    // ← empty list, not an error
        more: false,
        count: 0,
    };
}
```

Returns a `200 OK` with **zero items** when the client asks for a compacted revision.

### 1.2 Consequence

k3s runs periodic cache consistency checks:

```
CacheItem at RV=1111   →  LIST(revision=3172445)  →  empty response

digest(empty) = 0xcbf29ce484222325 (FNV-1a offset basis)
digest(cache) = 0xb9b8e6386ef52b11
            ↓
    "Cache consistency check failed"  ×14437/day
```

Since the response is `200 OK` (not an error), k3s never rebuilds its watch cache. The stale resource version persists forever. Every consistency check fails. Permanently.

### 1.3 `cbf29ce484222325` Decoded

This is `0xcbf29ce484222325` — the FNV-1a 64-bit **offset basis**. It appears when hashing a **zero-length input**. Definitively proves our range response is empty.

---

## 2. Correct etcd Protocol Behavior

From `etcd/server/mvcc/kvstore_txn.go`:

```go
func (tr *storeTxnRead) rangeKeys(ctx context.Context, key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
    rev := ro.Rev
    if rev > curRev {
        return nil, ErrFutureRev       // revision doesn't exist yet
    }
    if rev <= 0 {
        rev = curRev                    // latest state
    }
    if rev < tr.s.compactMainRev {
        return nil, ErrCompacted        // revision was garbage-collected
    }
    // ... return historical state at rev
}
```

| Condition | etcd Behavior | Our Current Behavior |
|-----------|---------------|---------------------|
| `revision == 0` | Return latest state | Correct |
| `revision > 0 && revision >= compactMainRev && revision <= currentRev` | Return state at that revision | **Wrong** — returns current state, not historical |
| `revision > 0 && revision < compactMainRev` | `ErrCompacted` | **Wrong** — returns empty list, not error |
| `revision > currentRev` | `ErrFutureRev` | **Wrong** — returns current state, not error |

Error messages (exact strings clients check for):

```
ErrCompacted = errors.New("mvcc: required revision has been compacted")
ErrFutureRev = errors.New("mvcc: required revision is a future revision")
```

gRPC status code: `Unavailable` (etcd maps both to `codes.Unavailable`).

---

## 3. Historical Queries — The Real Gap

We do not have an MVCC store. Our `BTreeMap<Vec<u8>, KeyState>` holds only the **latest version** of each key. We cannot natively answer "what did the store look like at revision X?" for any X other than "now."

However, the WAL contains every mutation that has ever happened — puts with values, deletes with tombstone markers. By replaying the WAL up to revision X, we can reconstruct the exact state at X. After compaction, the WAL is a snapshot of all live keys (at `snapshot_rev = COMPACT_REV`) followed by tail bytes of post-compaction writes. This is a complete log from `t=0`.

### 3.1 Option D: WAL-Based Historical Reconstruction (Recommended)

Reconstruct state at revision X by scanning the WAL, filtered to records with `revision <= X`. No BTreeMap needed for correctness — the WAL alone is the source of truth for historical state.

**Algorithm — `reconstruct_state_at(target_rev: u64)`:**

```
1. Open separate read-only file handle to WAL (by path, not through
   the shared Arc<Mutex<File>> — avoids fsync lock contention)
2. Read entire WAL into buffer
3. For each KvWalRecord with revision <= target_rev:
   - Not DELETED → insert/update map[key] = kv_bytes
   - DELETED → remove key from map
4. Records with revision > target_rev → skip
5. Return HashMap<Vec<u8>, Bytes>
```

Then `range_at_revision()` applies range bounds, prefix filters, count/limit to the map.

**Optimization — BTreeMap short-circuit (optional):**

The current BTreeMap already has the latest state. For keys where `mod_revision <= target_rev` AND `NOT deleted`, the current value IS the value at `target_rev`. We can diff:

| Key State | Action |
|-----------|--------|
| `create_revision > target_rev` | Skip (key didn't exist yet) |
| `mod_revision <= target_rev` && `!deleted` | Use `kv_bytes` from BTreeMap directly (O(1), current value is correct) |
| `mod_revision <= target_rev` && `deleted` | Remove from result (key was deleted before target) |
| `mod_revision > target_rev` && `!deleted` | Need WAL — current value is newer, find value at `target_rev` |
| `deleted` and `mod_revision > target_rev` | Need WAL — key existed at target, was deleted after |

However, deleted keys are NOT in the BTreeMap (they're only retained briefly during transactions). Keys deleted before compaction are gone from memory. We'd still need the WAL to detect them.

**WAL-only approach is simpler and safer**: no diff logic, no edge cases with deleted keys. The WAL is already small enough.

### 3.2 Performance of WAL Scan

| WAL Size | Scan Time (warm page cache) | Notes |
|----------|----------------------------|-------|
| 5 MB (post-compact) | ~0.5–1 ms | Typical — compaction runs every ~10h |
| 30 MB | ~3–5 ms | Mid-cycle |
| 67 MB (pre-compact) | ~5–10 ms | Just before compaction triggers |

For the k3s consistency check workload (~10 queries/min across all resource types), this adds:
- Post-compaction: ~10 ms/min CPU → 0.02% of a core
- Pre-compaction: ~100 ms/min CPU → 0.17% of a core
- Well within the <3% CPU target

### 3.3 Concurrency & Safety

**WAL file race conditions:**

| Hazard | Mitigation |
|--------|-----------|
| `Arc<Mutex<File>>` contention | Open a separate `File::open(path)` — not through the shared mutex |
| WAL rename during compaction (Phase C) | Handle `ENOENT` with retry (compaction holds write lock briefly, <50ms) |
| Concurrent write appends to WAL | Append-only is safe — we may or may not see the latest write; records with `revision > target_rev` are skipped either way |
| File replaced under us | Linux inode semantics: our open handle still reads the old inode. New writes go to new inode. Correct for historical queries — we want the old WAL. |

**No store lock required:** The WAL scan runs without the store `RwLock`. Writers are not blocked. The only lock acquired is the brief `std::fs::File::open()` system call + sequential read of the file.

### 3.4 Correctness vs etcd

For `target_rev >= COMPACT_REV`:
- Post-compaction WAL = snapshot (all keys at `snapshot_rev`) + tail bytes (writes since)
- Replaying up to `target_rev` produces bit-exact state at `target_rev`
- Result matches what etcd would return for `Range(revision=target_rev)`

For `target_rev >= current_rev` → handled by `ErrFutureRev` check (never reaches WAL scan).

For `target_rev < COMPACT_REV` → handled by `ErrCompacted` check (never reaches WAL scan).

### 3.5 Edge Cases

| Case | Behavior |
|------|----------|
| Key created at rev 5, value changed at rev 10, query at rev 8 | Returns value at rev 5 (correct — last write before target) |
| Key created at rev 5, deleted at rev 10, query at rev 12 | Not included (correct — deleted before target) |
| Key created at rev 5, deleted at rev 10, query at rev 8 | Included with value at rev 5 (correct — alive at target) |
| Key created at rev 10, query at rev 8 | Not included (correct — didn't exist yet) |
| WAL has 64MB, compaction triggers mid-scan | Retry on ENOENT; scan old file if already opened (inode persists) |
| WAL corrupted at offset N | `scan_kv` stops at error, returns partial state up to last valid record |

### 3.6 Implementation Complexity

The WAL scan approach adds ~80 lines of Rust code:

| Component | Lines | Description |
|-----------|-------|-------------|
| `reconstruct_state_at()` | ~40 | Open file, read buffer, deserialize + filter by revision, build HashMap |
| `range_historical()` | ~30 | Call reconstruct, apply range bounds, count/limit, build response |
| Concurrency plumbing | ~10 | Read WAL path from `StoreState` (brief lock) or cache in `Store`, open handle |

---

## 4. Approach Comparison

| Aspect | A: ErrCompacted on all revision>0 | B: Return current state | C: Hybrid error+current | **D: WAL-based historical** |
|--------|-----------------------------------|------------------------|------------------------|----------------------------|
| **Correctness** | Wrong — rejects valid queries | Wrong — returns wrong data | Mostly wrong for window | **Bit-exact** for all queries |
| **Complexity** | Trivial (~10 lines) | Trivial (~remove guard) | Low (~20 lines) | Moderate (~80 lines) |
| **CPU cost** | Zero | Zero | Zero | 0.02-0.17% core (at k3s idle) |
| **k3s errors stop** | Yes (err → cache rebuild) | Partial (items may differ) | Yes (err → cache rebuild) | Yes (correct data → digest matches) |
| **etcd compatible** | No | No | No | **Yes** |
| **Future-proof** | No — breaks any client that queries historical revs | No | No | **Yes** — matches etcd contract |

**Recommendation: Option D.** The WAL scan is fast enough (<0.2% CPU for k3s workload), produces bit-exact state, and makes our range implementation fully etcd-compatible for historical queries within the compaction window.

---

## 5. The Fix

### 4.1 Changes Required

**`src/storage/mod.rs` — `range()` method:**

```rust
pub async fn range(&self, req: etcdserverpb::RangeRequest) -> Result<etcdserverpb::RangeResponse, Status> {
    let state = self.state.read().await;
    let current_rev = state.next_rev - 1;

    if req.revision > current_rev as i64 {
        return Err(Status::new(
            Code::Unavailable,
            "etcdserver: mvcc: required revision is a future revision",
        ));
    }

    if req.revision > 0 && (req.revision as u64) < COMPACT_REV.load(Ordering::Relaxed) {
        return Err(Status::new(
            Code::Unavailable,
            "etcdserver: mvcc: required revision has been compacted",
        ));
    }

    // ... rest of range logic (same as today, returns current state)
}
```

The `req.revision > 0` guard that falls through (un-compacted historical) proceeds into the existing code which returns current state. This is the best-effort path.

**`src/server/kv.rs` — handler:**

```rust
async fn range(&self, req: Request<...>) -> Result<Response<RangeResponse>, Status> {
    self.store.range(req.into_inner()).await
    // propagate Result directly — no wrapping in Ok()
}
```

Same for `txn()` if it also passes revision to its internal range calls.

### 4.2 Exact Error Strings

Must match etcd verbatim — k8s/k3s may parse the error message:

| Error | String |
|-------|--------|
| Compacted | `"etcdserver: mvcc: required revision has been compacted"` |
| FutureRev | `"etcdserver: mvcc: required revision is a future revision"` |

gRPC status code: `Code::Unavailable` (maps to gRPC status code `14` / `UNAVAILABLE`).

---

## 6. k3s Recovery Flow

With Option D, the consistency checker receives the **correct data** at revision X instead of an empty response or an error.

1. k3s does `LIST(resourceVersion=3172445)` as a consistency check
2. Rudurru replays the WAL up to revision 3,172,445
3. Returns all keys that existed at that revision, with their values at that revision
4. k3s computes `etcdDigest` of the response — matches `cacheDigest` (because the data is correct)
5. Consistency check **passes** — no error logged
6. Subsequent checks at the same revision also pass

After a compaction sets a new `COMPACT_REV`:
1. If the old resourceVersion is now below `COMPACT_REV` → `ErrCompacted`
2. k3s's `interpretListError()` converts it to a Kubernetes API error
3. This signals k3s to rebuild the watch cache at the current revision
4. From then on, consistency checks use the new (current) revision
5. Zero errors going forward

**Result:** After one compact-rebuild cycle, consistency errors drop to zero permanently.

---

## 7. Verification

### 7.1 Unit Tests

Add tests to `src/storage/tests/mod.rs`:

1. **compacted_revision_returns_error** — `range(revision=1)` after compact → expect `Err` with "compacted"
2. **future_revision_returns_error** — `range(revision=u64::MAX)` → expect `Err` with "future"
3. **zero_revision_returns_current** — `range(revision=0)` → expect current state
4. **uncompacted_historical_best_effort** — `range(revision=N)` where `N >= COMPACT_REV` → expect current state with no error

### 7.2 Integration Check

After deploy:
- Watch journal for `"Cache consistency check failed"` — should drop to 0
- Confirm k3s pods remain Running (35 expected)
- Confirm Rudurru status logs show no errors

---

## 8. Prior Art

This bug was introduced in the same optimization pass that added WAL compaction. The `COMPACT_REV` guard was meant to "reject queries for lost data" but used the wrong mechanism (empty success instead of error).

Related etcd source:
- `etcd/server/mvcc/kvstore_txn.go:rangeKeys()` — revision checks
- `etcd/server/etcdserver/api/v3rpc/key.go` — `togRPCError()` error conversion
- `etcd/api/v3rpc/rpctypes/error.go` — `ErrCompacted`, `ErrFutureRev` definitions
- `k8s.io/apiserver/pkg/storage/etcd3/store.go` — `interpretListError()` handling in k8s

---

## 9. Implementation Plan

### 9.1 Code Changes

| Step | File | Change |
|------|------|--------|
| 1 | `src/storage/mod.rs` | Change `range()` return type: `RangeResponse` → `Result<RangeResponse, Status>`. Add revision checks + WAL-based `range_historical()` path. |
| 2 | `src/storage/wal.rs` | Add `scan_records_up_to(rev: u64) -> io::Result<Vec<(Vec<u8>, Vec<u8>, u32)>>` — WAL scan filtered by revision, returns (key, value, flags) tuples. Or export `KvWalRecord` fields for external use. |
| 3 | `src/server/kv.rs` | Update handler: propagate `Result` directly from `store.range()` |
| 4 | `src/storage/mod.rs` | Same revision-error treatment in `txn()` if it passes revision to internal range operations |
| 5 | tests | Add unit tests for compacted/future/zero historical queries |
| 6 | Deploy | Replace binary, restart service |
| 7 | Verify | Monitor k3s errors drop to 0 |

### 9.2 `range()` Method — New Structure

```rust
pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse, Status> {
    let target_rev = if req.revision > 0 { req.revision as u64 } else { 0 };
    let current_rev = current_revision();

    // ── Error cases (no lock needed) ──────────────────────────────────
    if target_rev > current_rev {
        return Err(Status::new(Code::Unavailable,
            "etcdserver: mvcc: required revision is a future revision"));
    }
    if target_rev > 0 && target_rev < COMPACT_REV.load(Ordering::Relaxed) {
        return Err(Status::new(Code::Unavailable,
            "etcdserver: mvcc: required revision has been compacted"));
    }

    // ── Historical query (target_rev > 0, >= COMPACT_REV) ────────────
    if target_rev > 0 {
        return self.range_historical(req, target_rev).await;
    }

    // ── Current state (revision == 0) — existing code path ────────────
    let state = self.state.read().await;
    // ... (current range logic, unchanged) ...
}
```

### 9.3 `range_historical()` — WAL Reconstruction

```rust
async fn range_historical(&self, req: RangeRequest, target_rev: u64) -> Result<RangeResponse, Status> {
    // Get WAL path (brief read lock, released before WAL scan)
    let wal_path = self.wal_path().await;

    // Reconstruct state at target_rev from WAL (NO store lock held)
    let state_at = reconstruct_state_at(&wal_path, target_rev)
        .map_err(|e| Status::new(Code::Internal, format!("wal scan: {e}")))?;

    // Apply range bounds (resolve_range-like logic on HashMap)
    let bound = resolve_range(&req.key, &req.range_end);
    let mut kvs: Vec<Bytes> = Vec::new();
    for (key, kv_bytes) in &state_at {
        if !matches_range(bound.to_ref(), key) { continue; }
        if req.keys_only {
            let (encoded, _, _) = wal::encode_kv(key, b"", 0, 0, 0, 0);
            kvs.push(Bytes::from(encoded));
        } else {
            kvs.push(kv_bytes.clone());
        }
        if req.limit > 0 && kvs.len() >= req.limit as usize { break; }
    }

    let rev = target_rev as i64;
    Ok(RangeResponse {
        header: Some(ResponseHeader {
            cluster_id: 1,
            member_id: 1,
            revision: rev,
            raft_term: 0,
        }),
        kvs,
        more: false,
        count: kvs.len() as i64,
    })
}
```

### 9.4 `reconstruct_state_at()` — WAL Replay Engine

```rust
fn reconstruct_state_at(path: &str, up_to: u64) -> io::Result<HashMap<Vec<u8>, Bytes>> {
    let mut file = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    drop(file);

    let mut state: HashMap<Vec<u8>, Bytes> = HashMap::new();
    let mut ofs = 0;
    while ofs < buf.len() {
        match KvWalRecord::deserialize(&buf[ofs..]) {
            Ok((rec, consumed)) => {
                let rev = rec.mod_revision().unwrap_or(0) as u64;
                if rev > up_to {
                    ofs += consumed;
                    continue; // hasn't happened yet at target_rev
                }
                if (rec.flags & DELETED) != 0 {
                    state.remove(rec.key().unwrap_or(&[]));
                } else {
                    state.insert(rec.key().unwrap_or(&[]).to_vec(), Bytes::copy_from_slice(&rec.kv_bytes));
                }
                ofs += consumed;
            }
            Err(_) => break, // partial/corrupt tail
        }
    }
    Ok(state)
}
```

### 9.5 Test Plan

| # | Test | Input | Expected |
|---|------|-------|----------|
| 1 | compacted revision | `range(revision=1)` after compact to 1000 | `Err` with "compacted" |
| 2 | future revision | `range(revision=9999999999)` | `Err` with "future" |
| 3 | zero revision | `range(revision=0)` | Current state (unchanged) |
| 4 | historical — exact rev | Put key at rev 5, `range(revision=5)` | Key included with correct value |
| 5 | historical — before mod | Put key at rev 5, value change at rev 10, `range(revision=8)` | Key included with value at rev 5 |
| 6 | historical — after delete | Put key at rev 5, delete at rev 10, `range(revision=12)` | Key NOT included |
| 7 | historical — before delete | Put key at rev 5, delete at rev 10, `range(revision=8)` | Key included |
| 8 | historical — after create | Key created at rev 10, `range(revision=8)` | Key NOT included |
| 9 | historical — concurrent compact | range hits WAL during Phase C rename | Retry succeeds |

### 9.6 Deploy & Verify

```bash
# Build
cargo build --release
sudo systemctl stop rudurru
sudo cp target/release/rudurru /usr/local/bin/
sudo systemctl start rudurru

# Verify
sleep 120  # wait for first consistency check cycle
sudo journalctl -u k3s -o cat | grep "Cache consistency" | tail -5
# Expected: no new errors

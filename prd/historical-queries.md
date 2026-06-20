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

We do not have an MVCC store. Our `BTreeMap<Vec<u8>, KeyState>` holds only the **latest version** of each key. We cannot answer "what did the store look like at revision X?" for any X other than "now."

There are three options to handle `revision > 0 && revision >= COMPACT_REV` (un-compacted historical queries):

### Option A: ErrCompacted anyway (strict, simplest)

Treat any `revision > 0` as compacted. k3s rebuilds cache on first consistency check. After that it only uses `revision == 0` for normal operations.

- Pro: Trivial to implement, matches etcd error surface for all practical client behavior
- Con: A client that legitimately queries an un-compacted historical revision gets a false error

### Option B: Return current state (best-effort)

Ignore the revision parameter and return latest state. Set `header.revision` to `current_rev` (not the requested one).

- Pro: Works for clients that just need data
- Con: Clients that check `revision == requested_revision` will be confused
- Con: Items that didn't exist at the requested revision may appear; items that were deleted may be missing

### Option C: Hybrid — error when revision has compacted, current state otherwise

- `revision < COMPACT_REV` → `ErrCompacted`  
- `revision > current_rev` → `ErrFutureRev`  
- `revision == 0` → current state  
- `revision >= COMPACT_REV && revision <= current_rev` → return current state as best-effort

This is what we'll implement. It matches etcd error semantics for the well-defined cases, and degrades gracefully for the unsupported window.

**Impact of the "graceful degradation" window:** COMPACT_REV is always set to `snapshot_rev` from the last compaction, which is within ~64MB of WAL writes from `current_rev`. For a k3s idle workload (~6 MB/hour), the window is ~10 hours. k3s never requests historical revisions within this window — its consistency checks use the watch start revision which is always older.

---

## 4. The Fix

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

## 5. k3s Recovery Flow

Once we return `ErrCompacted`:

1. k3s's `delegator.go` receives the error from its `List` call
2. `interpretListError(...)` converts it to a Kubernetes API error
3. The watch cache's consistency checker detects the error, logs it
4. k3s does NOT automatically rebuild the cache from the error alone — it continues with its existing cache
5. However, on the **next LIST from a controller** (which uses `revision==0`), k3s gets a correct response and the cache starts receiving current data
6. The consistency checker stops because it either:
   a. Checks less frequently, or
   b. Receives matching digests when it re-checks at a newer (non-compacted) revision

In practice, after returning `ErrCompacted`:
- The error log changes from `"Cache consistency check failed"` to `"etcdserver: mvcc: required revision has been compacted"` (a less frequent, less alarming log)
- k3s continues normal operation within seconds
- Cache consistency errors drop from ~14k/day to ~0

### 5.1 Why This Works

The key insight: k3s uses `revision==0` (latest) for all **normal LIST operations** — only the consistency checker uses the historical revision. When the consistency checker gets `ErrCompacted`, it logs once and backs off. Normal k3s operation is unaffected.

---

## 6. Verification

### 6.1 Unit Tests

Add tests to `src/storage/tests/mod.rs`:

1. **compacted_revision_returns_error** — `range(revision=1)` after compact → expect `Err` with "compacted"
2. **future_revision_returns_error** — `range(revision=u64::MAX)` → expect `Err` with "future"
3. **zero_revision_returns_current** — `range(revision=0)` → expect current state
4. **uncompacted_historical_best_effort** — `range(revision=N)` where `N >= COMPACT_REV` → expect current state with no error

### 6.2 Integration Check

After deploy:
- Watch journal for `"Cache consistency check failed"` — should drop to 0
- Confirm k3s pods remain Running (35 expected)
- Confirm Rudurru status logs show no errors

---

## 7. Prior Art

This bug was introduced in the same optimization pass that added WAL compaction. The `COMPACT_REV` guard was meant to "reject queries for lost data" but used the wrong mechanism (empty success instead of error).

Related etcd source:
- `etcd/server/mvcc/kvstore_txn.go:rangeKeys()` — revision checks
- `etcd/server/etcdserver/api/v3rpc/key.go` — `togRPCError()` error conversion
- `etcd/api/v3rpc/rpctypes/error.go` — `ErrCompacted`, `ErrFutureRev` definitions
- `k8s.io/apiserver/pkg/storage/etcd3/store.go` — `interpretListError()` handling in k8s

---

## 8. Implementation Plan

| Step | File | Change |
|------|------|--------|
| 1 | `src/storage/mod.rs` | Change `range()` return type: `RangeResponse` → `Result<RangeResponse, Status>`. Add revision checks at top. |
| 2 | `src/server/kv.rs` | Update handler: propagate `Result` directly from `store.range()` |
| 3 | `src/storage/mod.rs` | Same revision-error treatment in `txn()` if it passes revision to internal range |
| 4 | tests | Add unit tests for compacted/future/zero/uncompacted revision scenarios |
| 5 | Deploy | Replace binary, restart service |
| 6 | Verify | Monitor k3s errors drop to 0 |

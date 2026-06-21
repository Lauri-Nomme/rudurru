# RwLock Optimization Analysis

**Date:** 2026-06-21

## Current Architecture

Single `tokio::sync::RwLock<StoreState>` guarding all state — keys BTreeMap, leases BTreeMap, watchers Vec, WAL handle. Lock is **never held across `.await`** in any method (verified: `put`, `range`, `txn`, `delete_range`, `lease_grant`, `lease_revoke`, `lease_keep_alive`, `compact_wal`).

### Already optimized (13 of 16 items from `prd/optimization.md`)

| Item | Change | Status |
|------|--------|--------|
| Deferred WAL fsync | Background thread fsyncs every 50ms, not inside write lock | ✅ Done |
| Atomic revision counter | `NEXT_REV` as `AtomicU64`, no read lock for revision | ✅ Done |
| Atomic counters | `KEY_COUNT`, `LEASE_COUNT`, `WATCHER_COUNT` as `AtomicU64` | ✅ Done |
| Event-driven lease expiry | `tokio::time::sleep_until(earliest_expiring)` not periodic polling | ✅ Done |
| Cross-stream watcher batching | 140 lock acquisitions → 1 for watcher registration | ✅ Done |
| Arc clones | `Arc<[u8]>` for values, `Bytes` for kv_bytes (cheap atomic increment) | ✅ Done |

### Known ceiling

`perf-test.md` identifies **~100K writes/sec** as the single-RwLock ceiling. Target k3s workload is **~72 writes/sec** — 3 orders of magnitude headroom.

---

## Option 1: Switch to `parking_lot::RwLock`

`tokio::sync::RwLock` is designed for locks held across `.await` points. Since our lock is **never held across `.await`**, we pay overhead for features we don't use.

### Comparison

| Metric | `tokio::sync::RwLock` | `parking_lot::RwLock` |
|--------|----------------------|----------------------|
| Uncontended read lock | ~15-25ns (async fn + waker allocation) | **~2-5ns** (futex, no allocation) |
| Uncontended write lock | ~20-30ns | **~5-10ns** |
| Fairness | FIFO (extra context switches under contention) | Writer-priority variant available |
| Held across `.await` | Safe | UB (panics in debug mode) |
| API | `state.read().await` / `state.write().await` | `state.read()` / `state.write()` |

### Code change

```diff
-use tokio::sync::RwLock;
+use parking_lot::RwLock;

-pub state: Arc<RwLock<StoreState>>,
+pub state: Arc<parking_lot::RwLock<StoreState>>,

-let state = self.state.read().await;
+let state = self.state.read();

-let mut state = self.state.write().await;
+let mut state = self.state.write();
```

No `spawn_blocking` needed — critical sections are ~1-11µs, well below the ~50µs threshold where blocking the async runtime matters.

### Pros/Cons

| Pro | Con |
|-----|-----|
| Reduces lock overhead 3-5x | Must never hold across `.await` (existing code already satisfies this) |
| Eliminates async waker allocations on every lock acquisition | Slightly invasive change (replace all `.read().await` / `.write().await`) |
| No dependency change already in Cargo.lock (parking_lot is transitive through tokio) | |

**Effort:** ~10 minutes, ~10 line changes across `src/storage/mod.rs`

---

## Option 2: Shard Keys by Prefix

Partition the key space into N shards (e.g., 256 by first byte or hash prefix), each with its own `parking_lot::RwLock`.

### Design sketch

```rust
struct ShardedStore {
    shards: [parking_lot::RwLock<Shard>; 256],
    wal: wal::WalFile,  // single global WAL
}

struct Shard {
    keys: BTreeMap<Vec<u8>, KeyState>,
}

fn shard_idx(key: &[u8]) -> usize {
    key.first().map(|b| *b as usize).unwrap_or(0) % 256
}
```

### Range query complication

Range/prefix queries must query all shards in key order and merge sorted results:

```rust
let mut merged = BTreeMap::new();
for i in 0..256 {
    let shard = self.shards[i].read();
    for (k, v) in shard.keys.range(start..end) {
        merged.insert(k.clone(), v.clone());
    }
}
```

This loses BTreeMap's natural ordering across shards and adds an O(k log k) merge cost.

### Pros/Cons

| Pro | Con |
|-----|-----|
| Eliminates write lock contention entirely (>1M ops/s theoretical) | Range/prefix queries must scan all shards and merge sorted results |
| Independent shards don't contend | Prefix scans lose natural ordering |
| Scales linearly with shard count | WAL ordering must be global (single revision counter) |
| | Lease operations need cross-shard coordination |
| | Significant code complexity (weeks of work) |

---

## Option 3: RCU / Lock-Free Reads

Use epoch-based reclamation (`crossbeam-epoch`) or `arc_swap` to allow lock-free reads while writes atomically swap the entire data structure (or a generation).

### Pros/Cons

| Pro | Con |
|-----|-----|
| Zero-contention reads | Writes must clone-and-modify entire BTreeMap (O(n) per write) |
| Readers never block | Memory reclamation is subtle and hard to get right |

**Verdict:** Not viable for a write-heavy workload. O(n) clone per write is ~1ms at 2.35M keys — disastrous at 105K writes/sec.

---

## Recommendation

| Option | Effort | Impact | Recommended? |
|--------|--------|--------|-------------|
| `tokio::sync::RwLock` → `parking_lot::RwLock` | **10 min** | 3-5x less lock overhead, no async waker allocations | **Yes** — low risk, measurable improvement |
| Shard by prefix | **Weeks** | Removes lock as bottleneck entirely | **No** — 1000x headroom already exists for target workload |
| RCU / lock-free | **Months** | Removes read lock entirely | **No** — O(n) clone per write is unworkable |

**The `parking_lot::RwLock` switch is still worthwhile** (mechanical, no behavioral impact), but the impact is smaller than initially hypothesized. The benchmark (`prd/concurrent-benchmark-design.md`) confirms the single-RwLock ceiling is ~80K ops/s, set by the **critical section length (~11µs per write)** — not lock acquisition overhead. `parking_lot::RwLock` would eliminate waker allocations, yielding **at most 5-10% throughput improvement** at 128 workers. To significantly exceed 80K ops/s would require reducing the critical section or sharding the store.

# Code Review — 2026-06-22

After one week of development, a fresh review of the entire rudurru codebase
covering design, performance, correctness, Rust best practices, and
module structure.

---

## 1. Design Issues

### 1.1 Txn eval acquires write lock for read-only comparisons

**File:** `src/storage/mod.rs:1001`

```rust
let success = {
    let state = self.state.write();   // ← should be .read()
    req.compare.iter().all(|c| eval_compare(&state, c))
};
```

`txn()` acquires the **write** lock just to evaluate comparisons. Comparisons
are read-only — they inspect `state.keys` without mutation. This needlessly
blocks ALL writers (put, delete, lease_revoke, expiry) during comparison
evaluation. For k3s's single-op CAS pattern this is fast, but it's semantically
wrong and wastes the reader/writer asymmetry of parking_lot's RwLock.

**Fix:** Use `self.state.read()`. The TOCTOU race between eval (under read
lock) and execution (under write lock) is a pre-existing concern — holding
the write lock for eval doesn't fix it because the lock is released between
eval and `execute_txn_ops`.

### 1.2 Error handling asymmetry

| Operation | WAL failure behavior | Client sees |
|-----------|---------------------|-------------|
| `range` | Returns `Result` with proper `Status` | Error propagated |
| `put` | `tracing::error!` at line 890 | "ok" response |
| `delete_range` | `tracing::error!` at line 980 | "ok" response |
| `lease_revoke` | `tracing::error!` at line 1158 | "ok" response |
| Expiry task | `tracing::error!` at line 1333 | No response (background) |

Every mutation path logs WAL failures but returns success to the client.
This is an explicit availability-over-consistency trade-off, but it should be
documented as such and/or made configurable. In the current code it looks
like an oversight — the error is caught, logged, and ignored.

### 1.3 `compact()` RPC doesn't validate revision bounds

**File:** `src/storage/mod.rs:1072-1087`

```rust
pub async fn compact(&self, req: etcdserverpb::CompactionRequest) -> etcdserverpb::CompactionResponse {
    let state = self.state.write();
    COMPACT_REV.store(req.revision as u64, Ordering::SeqCst);
    // ...
}
```

etcd validates:
- `revision <= current_rev` (can't compact a future revision)
- `revision > compact_rev` (can't compact below already-compacted)

Rudurru silently accepts any revision. A client could compact at `rev=2^63`
and clamp `COMPACT_REV` to nonsense, causing all subsequent queries to
return `ErrCompacted`.

### 1.4 `lease_keep_alive` returns TTL=-1 instead of error

**File:** `src/storage/mod.rs:1182`

When the lease ID doesn't exist, etcd returns `ErrLeaseNotFound`. Rudurru
returns TTL=-1. The etcd client library may trigger a `NotFound` gRPC error
on its side, but the gRPC-level response is `ok` rather than an error status.
The test `test_lease_keepalive` would need updating if this is fixed.

### 1.5 Lease-based key deletions use independent revisions

**File:** `src/storage/mod.rs:1137`, `1315`

```rust
// Inside lease_revoke and expiry task:
let rev = next_revision();  // ← called per key, inside the loop
```

Each key on a revoked/expired lease gets a **different** revision. etcd gives
all deletions from a lease revocation the **same** revision, making the
revocation atomically visible. This inconsistency means:
- A watcher may see some keys deleted from the lease and not others
- The revision timeline doesn't reflect the atomic nature of lease revocation

Contrast with `delete_range` which correctly uses a single revision (line 911).

### 1.6 No lease existence validation on put

**File:** `src/storage/mod.rs:851-867`

`put()` checks `ignore_lease` but never validates that the lease ID actually
exists. etcd returns `ErrLeaseNotFound`. A put with a non-existent lease
succeeds, leaving the key with a dangling lease reference.

### 1.7 `resolve_range` prefix detection last-byte edge case

**File:** `src/storage/mod.rs:1578-1584`

The prefix detection checks if `range_end == key` with last byte incremented.
For a key ending in `\xFF`, incrementing wraps to `\x00`, which would be
misinterpreted as the `From` bound (single `\x00` byte). The `\0` suffix
alternative (lines 1587-1592) handles this, but not all clients send it.
If a client sends the incremented form for a key like `foo\xFF`, the code
would produce a `RangeBound::Point` pointing at `foo\x00` rather than a
`Prefix` bound.

---

## 2. Performance Issues

### 2.1 WAL scan loads entire file into memory

**File:** `src/storage/wal.rs:336-337`

```rust
let mut buf = Vec::new();
file.read_to_end(&mut buf)?;
```

`scan_kv` allocates a `Vec<u8>` the size of the entire WAL file. This is
called:
- During startup (`scan_kv_collect` at line 290 — unavoidable, need all records)
- During watch Phase 1 replay (`flush_global_batch_at` at line 393 — once per
  batch, but still allocates ~270MB at current WAL size)
- During historical range Phase 2 (`scan_wal_range` at line 1678 — rare in
  practice, but when triggered, the full WAL is loaded)

A streaming reader using `read_exact` with `rec_len` would read records one
at a time, bounded to record-sized allocations. The `rec_len` field in the
9-byte header enables O(1) knowledge of each record's size before reading it.

### 2.2 Double key clone in put path

**File:** `src/storage/mod.rs:853`, `178`

```rust
// In put():
let key = req.key.clone();           // first clone
// ...
let prev = state.apply(key.clone(), value, lease, rev);  // second clone in apply's self.keys.insert(key.clone(), ...)
```

The key `Vec<u8>` is cloned at the call site, then cloned again inside
`apply()`. `apply()` could take ownership of the key:

```rust
fn apply(&mut self, key: Vec<u8>, value: Vec<u8>, lease: i64, rev: u64) -> Option<KeyState> {
    // ...
    self.keys.insert(key, entry);  // no .clone() needed
    // ...
}
```

This is a minor per-write cost (~100ns for a 100-byte key at k3s's
72 writes/sec = ~7µs/day saved), but it's a trivial fix and sets a good
pattern.

### 2.3 `compact_wal` calls `count_wal_records` for logging only

**File:** `src/storage/mod.rs:1424`

```rust
tail_count = wal::count_wal_records(&tail);
```

`count_wal_records` iterates and deserializes every tail record just to log
the count. The count is informative but not critical — could be skipped or
computed from the tail byte length divided by average record size (approximate).

### 2.4 Duplicate delete-under-lease code

**File:** `src/storage/mod.rs:1129-1160` and `1308-1337`

The lease revocation and lease expiry paths share ~30 identical lines:
iterate keys matching lease ID, call `apply_delete`, build WAL records,
append batch. Both paths do:

```
for key in keys_to_delete {
    let rev = next_revision();
    prev = state.keys.get(key);
    state.apply_delete(key, rev);
    record = KvWalRecord::new(DELETED, key, ...);
    records.push(record);
}
state.wal.append_kv_batch(&records);
```

Extract into `fn delete_keys_for_lease(&mut self, id: i64)`.

### 2.5 `scan_wal_range` reads from byte 0

**File:** `src/storage/mod.rs:1660-1724`

When Phase 1 of a historical range query finds stale keys, `scan_wal_range`
opens the WAL and reads from byte 0, scanning ALL records. For large WALs
this is expensive (though rare — 0 of 460 production queries triggered it).
An optimization: since we know `target_rev`, skip records until we reach
a revision near `target_rev`. But `KvWalRecord` doesn't embed the revision
in the header — it's in `kv_bytes` at `mod_rev_offset`. A pre-scan for the
starting offset would itself require reading records. Given the rarity,
this is low priority.

### 2.6 Watcher list is scanned O(W) per write

**File:** `src/storage/mod.rs:241-276`

Every `put` and `delete` iterates ALL watchers and calls `matches_range` for
each. With 265 watchers, this is ~5µs per write. For k3s's 72 writes/sec,
this is ~360µs/sec = 0.036% CPU. Acceptable but worth noting as an O(W)
pattern.

---

## 3. Correctness Issues

### 3.1 `apply_record` (startup) vs `apply`/`apply_delete` (runtime) — diverged

**Files:** `src/storage/mod.rs:1500-1556` (replay) vs `160-221` (runtime)

The startup replay path and the runtime mutation path implement similar logic
independently:
- Both insert/update keys in the BTreeMap
- Both fire watch events
- Both handle tombstoned keys

But there are subtle differences:
- Replay uses `rec.kv_bytes.clone()`; runtime uses `make_kv_bytes()` which
  re-encodes from `KeyState` fields
- Replay doesn't compute `rebirth` flag (it's always `false` on replay, which
  is correct since historical queries during replay are meaningless)
- Replay deserializes `mvccpb::KeyValue` from `kv_bytes`; runtime constructs
  from field data

A shared `fn apply_wal_record(&mut self, rec: &KvWalRecord)` would guarantee
that both paths produce identical state.

### 3.2 `store_hash()` uses `DefaultHasher`

**File:** `src/storage/mod.rs:1249`

```rust
let mut hasher = std::collections::hash_map::DefaultHasher::new();
```

`DefaultHasher` is explicitly documented as not having stable algorithm across
Rust versions. For a single-node store this is harmless, but the `HashKV`
maintenance RPC is expected to produce consistent hashes. If a user depends
on hash stability (e.g., for health checks that compare hashes), it could
break on Rust upgrade.

**Fix:** Use a fixed algorithm like `cityhash` or `xxhash`. Low priority.

### 3.3 `watch_id` collision with client-assigned IDs

**File:** `src/server/watch.rs:129-133`

```rust
let watch_id = if create.watch_id != 0 {
    create.watch_id          // client-specified — no uniqueness check
} else {
    next_watch_id()          // server-assigned — atomic counter
};
```

If a client manually assigns a `watch_id` that happens to match an existing
watcher (either from the same stream or another stream), the watcher is
registered with a duplicate ID. `cancel_watcher` would then cancel only the
first match (due to `retain`), potentially leaving a stale watcher.

etcd requires client-assigned watch IDs to be unique within a stream.
Rudurru doesn't enforce this, but it also doesn't check for duplicates.

### 3.4 `Notify` coalescing in expiry task

**File:** `src/storage/mod.rs:1260-1340`

```rust
tokio::select! {
    _ = tokio::time::sleep(sleep_dur) => {}
    _ = notify.notified() => {}
}
```

If `notify_one()` is called multiple times while the expiry task is processing
expired leases (inside the `state.write()` block), only one notification is
consumed — the rest are coalesced away. When the task finishes processing and
goes back to sleep, it sleeps for `sleep_dur` (the next expiry) rather than
waking immediately to process the new leases.

This is a **latency issue, not a correctness issue** — the new leases will
eventually be processed when the timer fires. Worst-case: 500ms delay for a
lease expiry if grant/revoke notifications arrive during processing.

**Fix:** Use standard `notify_one()` after releasing the write lock, or use
`notify_waiters()` to ensure at least one wake-up is queued.

---

## 4. Rust Best Practices & Code Cleanliness

### 4.1 Visibility — too many `pub` fields

Throughout `src/storage/mod.rs`, struct fields are liberally marked `pub`:

```rust
pub struct KeyState {
    pub value: Arc<[u8]>,         // could be pub(crate)
    pub mod_revision: u64,        // could be pub(crate)
    pub create_revision: u64,     // could be pub(crate)
    pub version: i64,             // could be pub(crate)
    pub lease: i64,               // could be pub(crate)
    pub delete_revision: u64,     // could be pub(crate)
    pub rebirth: bool,            // could be pub(crate)
    pub kv_bytes: Bytes,          // could be pub(crate)
}
```

All `KeyState` fields are `pub`, but only `StoreState` and its methods should
access them directly. External consumers (server handlers) go through `Store`
methods. Narrowing visibility to `pub(crate)` would:
- Document the intended API surface
- Prevent accidental field access from binary tools or tests outside `src/`

Similarly, `WalFile.file` is `pub` (line 311) but is accessed via locking
methods — external mutation could corrupt the file state.

### 4.2 Large `impl` blocks not split by domain

`impl Store` (line 285+) is a single monolithic block spanning ~1200 lines
(up to line 1495). It covers:
- `open()` — startup
- `range()` + `range_historical()` — queries
- `put()`, `delete_range()` — mutations
- `txn()`, `execute_txn_ops()` — transactions
- `compact()` — compaction
- `lease_grant` through `lease_leases` — 5 lease methods
- `db_size()`, `store_hash()` — maintenance
- `start_expiry_task()` — background tasks
- `compact_wal()`, `start_compaction_task()` — WAL compaction

Rust convention allows multiple `impl Store { }` blocks. Splitting by domain:

```rust
// Query operations
impl Store {
    pub async fn range(...) { }
    async fn range_historical(...) { }
}

// Mutation operations
impl Store {
    pub async fn put(...) { }
    pub async fn delete_range(...) { }
    pub async fn txn(...) { }
    pub async fn compact(...) { }
}

// Lease operations
impl Store {
    pub async fn lease_grant(...) { }
    // ...
}

// Background tasks (these are free functions, not Store methods)
fn start_expiry_task(...) { }
fn start_compaction_task(...) { }
```

This improves navigation without changing any behavior.

### 4.3 `Result` type inconsistency

Some methods return `Result<T, Status>` (`range()`), others return the
response type directly (`put()`, `delete_range()`, all lease methods).
This is partly because the etcd protocol defines error responses differently
for different RPCs, but it makes error handling at call sites inconsistent.

### 4.4 `let _ = ...` for ignored results

Widespread pattern:

```rust
let _ = ctx.event_tx.send(event.clone());   // watch.rs:425
let _ = c.reply.send(Ok(resp));              // watch.rs:358
let _ = tx.send(Ok(resp)).await;             // watch.rs:168
```

In many cases the send failure is expected (client disconnected), so ignoring
is correct. But some call sites should log or act on the error:

- `watch.rs:274` — `let _ = watcher.sender.send(event);` — dropping a
  notification to a watcher means the watcher missed an event. Should this
  be logged or the watcher canceled?
- `storage/mod.rs:889` — `let _ = state.wal.append_kv(&record);` — the error
  is logged inside `append_kv` but not propagated. If WAL append fails, the
  response still says "ok".

### 4.5 `WatchEvent.clone()` per matching watcher

**File:** `src/storage/mod.rs:270`

```rust
let mut event = event.clone();
```

For each matching watcher (potentially 265), the entire `WatchEvent` is
cloned. With `Bytes` for `kv_bytes` and `prev_kv_bytes`, the data copies
are cheap (refcount increments), but the `key: Vec<u8>` is deep-copied per
watcher.

**Fix:** Store `key` as `Bytes` (like `kv_bytes`) to get shared ownership
and cheap clones. Or change `WatchEvent.key` to `Arc<[u8]>`.

### 4.6 No span-based tracing context

All logging uses structured fields in `tracing::info!()`, `tracing::warn!()`,
etc. This works well and produces clean JSON output. However, there's no use
of `tracing::span!` to group related operations (e.g., a Txn and its
individual Range/Put/Delete operations). Span context would make it easier
to correlate log lines in distributed tracing systems.

### 4.7 Only one `unsafe` block

**File:** `src/main.rs:71`

```rust
unsafe { malloc_trim(0); }
```

Used to release glibc-cached memory back to the OS in the periodic status
task. This is safe in practice (single-threaded call, no aliasing concerns).
Worth noting but not a concern.

---

## 5. Module Structure — Splitting `src/storage/mod.rs`

At **3786 lines**, `src/storage/mod.rs` is the largest file in the codebase
(3× the next largest, `wal.rs` at 959 lines). It combines:
- Core data types (`KeyState`, `LeaseState`, `StoreState`, `WatchRegistration`)
- All KV operations (range, put, delete, txn, compact)
- All lease operations (grant, revoke, keepalive, ttl, list)
- Background tasks (fsync, expiry, compaction)
- Historical range queries with WAL reconstruction
- Range matching logic (`resolve_range`, `matches_range`, `btree_bounds`)
- Helper functions (`eval_compare`, `scan_wal_range`, `apply_record`)
- Unit tests (compact, historical queries)

### Proposed split

```
src/storage/
├── mod.rs          — Re-exports + free functions (resolve_range, matches_range,
│                     btree_bounds, eval_compare, scan_wal_range) + type aliases
├── state.rs        — KeyState, LeaseState, WatchRegistration, WatchEvent, RangeBound,
│                     StoreState (struct defs only, no ops)
├── store.rs        — Store struct, open(), all impl Store { ... } blocks
│                     (range, put, delete_range, txn, compact, all lease methods,
│                      db_size, store_hash, compact_wal)
├── background.rs   — start_fsync_task, start_expiry_task, start_compaction_task
├── apply.rs        — apply(), apply_delete(), apply_record() — the three state
│                     mutation paths
└── wal.rs          — Already separate (959 lines, WAL I/O + overlong varints)
```

Alternatively, a less granular but still meaningful split:

```
src/storage/
├── mod.rs          — Re-exports + resolve_range, matches_range, btree_bounds,
│                     eval_compare, scan_wal_range, KeyState, LeaseState,
│                     WatchRegistration, WatchEvent, RangeBound, StoreState
├── store.rs        — All Store methods (open + operations), background tasks
└── wal.rs          — Already separate
```

### Which to choose

The aggressive split (6 files) is cleaner but introduces many intra-crate
visibility considerations. Methods on `StoreState` that are currently `fn`
in `impl StoreState` would need to stay in one file or be split across files.

The simpler split (mod.rs + store.rs) requires minimal refactoring:
- Move `impl Store { }` and all associated code to `store.rs`
- Move background tasks to `store.rs` (they're called from `Store::open`)
- Keep types and free functions in `mod.rs`

This alone reduces `mod.rs` from 3786 to ~900 lines, improving navigation
without breaking anything.

### Recommendation

Start with the simpler split. If `store.rs` itself grows past ~1500 lines,
further split into domain-specific files:

```
src/storage/
├── mod.rs          — Types, free functions, re-exports  (~900 lines → stable)
├── store.rs        — Store struct, open(), KV ops       (~1200 lines → will grow)
├── background.rs   — Background tasks                   (~200 lines)
└── wal.rs          — Already separate
```

---

## 6. Things Considered but Not Mentioned Above

### 6.1 `next_revision()` uses `SeqCst` ordering

All atomic loads/stores on `NEXT_REV` use `SeqCst`. For a single-writer
paradigm (writes are serialized through the RwLock), `Relaxed` would suffice
for the counter — there's no cross-thread ordering constraint beyond
uniqueness. `SeqCst` is the safest default but slightly more expensive on
ARM (cheap on x86 where all stores are release).

### 6.2 `current_revision() == 0` when no writes have happened

Returns `NEXT_REV.load(..).saturating_sub(1)` which is 0 before any write.
This matches etcd behavior (revision 0 means "latest" / "no writes").

### 6.3 Progress_notify timer always exists

The event forwarding loop always creates a `tokio::time::interval` even when
`progress_notify` is false. On each tick (every 5 minutes if no events), the
`continue` skips the progress response. This is ~8760 extra wakeups per year
per watcher, each lasting <1µs — negligible.

### 6.4 `PendingCreate` and `GlobalCreate` have nearly identical fields

`PendingCreate` has `key`, `range_end`, `start_revision`, `progress_notify`,
`filters`, `prev_kv`. `GlobalCreate` wraps `PendingCreate` plus `event_tx`,
`reply`, `stream_id`, `remote_addr`, `watch_id`. The nesting is clean —
separates transport concern from watch parameters. Could inline `PendingCreate`
into `GlobalCreate` but the current structure is fine.

### 6.5 WalFile uses `std::sync::Mutex`, rest uses `parking_lot::RwLock`

Two different locking primitives. The `std::sync::Mutex` on `WalFile.file` is
held briefly (write/read the file descriptor), so contention is minimal.
Using parking_lot here too would be more consistent but provides no measurable
benefit for a file-level mutex.

### 6.6 Lease expiry task holds write lock during batch revocation

When multiple leases expire simultaneously (e.g., after a long pause), the
expiry task iterates expired leases, deletes all their keys, and writes WAL
records — all under the write lock. This could block writes for milliseconds.
Documented as acceptable for k3s workload.

### 6.7 `lease_time_to_live` with `keys=true` scans the full BTreeMap

Scans `state.keys` to find keys matching the lease ID. O(N) where N = total
keys. At 2,251 keys this is ~10µs. Acceptable.

### 6.8 Server-side `compact_rev` check catches stale watcher start revisions

When a new watcher has `start_revision < compact_rev`, it's rejected with
`compact_revision` in the WatchResponse. This correctly prevents hung
watchers. Already documented as done.

### 6.9 `biased` in `tokio::select!` ensures fair prioritization

The event forwarding loop uses `biased` to prioritize event delivery over
progress notifications. Without `biased`, tokio would randomly choose between
branches when both are ready, potentially delaying event delivery. `biased`
ensures events are always delivered first.

### 6.10 Discovery: `tracing::trace!` vs `tracing::info!` split

Some operations are logged at `info!` (Txn, watch lifecycle, lease grants),
others at `trace!` (Range, Put, Delete). This is intentional — `info!` is for
important lifecycle events, `trace!` for high-frequency per-request logging.
The split is clean and practical.

---

## 7. Summary — Priority for Action

| Priority | Issue | Effort | Impact |
|----------|-------|--------|--------|
| **P1** | Txn eval uses write lock (1.1) | 1 line | Low (k3s uses single-op CAS) |
| **P1** | `compact()` no validation (1.3) | 5 lines | Low (no malicious clients) |
| **P1** | Double key clone in put (2.2) | 1 line | Negligible |
| **P2** | Lease revocations use per-key revisions (1.5) | 2 lines | Low (order of events differs from etcd) |
| **P2** | Extract delete-under-lease helper (2.4) | 20 lines | Medium (DRY + maintainability) |
| **P2** | Simplify module split (5) | 2–4 hours | Medium (navigation + onboarding) |
| **P3** | Stream WAL reader (2.1) | ~3 days | High (eliminates 270MB allocation) |
| **P3** | Unify `apply_record` + `apply` (3.1) | 1 day | Medium (correctness risk) |
| **P3** | Lease validation on put (1.6) | 3 lines | Low (correctness edge case) |
| **P3** | `lease_keep_alive` return error (1.4) | 5 lines | Low (etcd compat) |
| **P4** | `KeyState` visibility (4.1) | 15 min | Low (API hygiene) |
| **P4** | Split `impl Store` blocks (4.2) | 30 min | Low (navigation) |
| **P4** | `WatchEvent.key` → `Bytes` (4.5) | 2 hours | Low (per-write alloc reduction) |
| **P4** | `resolve_range` prefix edge case (1.7) | 5 lines | Low (rare key pattern) |

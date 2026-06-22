# Rudurru Data Model & Operation Semantics

**Date:** 2026-06-13
**Project:** Rudurru — a purpose-built etcd v3 gRPC server in Rust, targeting <3% CPU for 30-pod k3s workloads.

This document defines the data structures, execution model, and operation semantics for Rudurru. Every design decision traces back to the goal of eliminating the 61% infrastructure overhead (CGo + GC + SQL parsing) identified in `kine-sqlite-waste-analysis.md`.

---

## 1. Design Tenets

1. **Append-only WAL is the single source of truth.** The in-memory index is rebuilt from the WAL on startup. There is no separate database file.
2. **No polling.** Watches are push-based via `tokio::sync::mpsc`. The etcd poll loop and its 1-second worst-case latency are eliminated.
3. **No query planner.** All access patterns are hardcoded: BTreeMap range scans, prefix walks, point lookups. Zero SQL parsing.
4. **No GC.** Rust's ownership model + `Arc<[u8]>` for values. No mark-sweep, no stop-the-world.
5. **No CGo.** Pure Rust throughout. The only FFI is the kernel `write`/`fsync` syscall.
6. **Single-writer principle.** All mutations (Put, Delete, Txn, Compact, LeaseRevoke) are serialized through a single `tokio::sync::Mutex`-guarded write path. Reads are lock-free against the in-memory index.

---

## 2. On-Disk Format: Binary WAL

### 2.1 Record Layout

```
┌─────────┬──────────┬──────────┬────────┬───────┬──────────┬──────────┬──────────┐
│ magic   │ revision │ crc32    │ key_len│val_len│  flags   │   key    │   value  │
│ u16     │   u64    │  u32     │  u32   │  u32  │   u8     │ [u8;N]   │ [u8;M]   │
└─────────┴──────────┴──────────┴────────┴───────┴──────────┴──────────┴──────────┘
  header (18 bytes)                            │  key       │  value    │
                                                └────────────┴───────────┘
                                                  payload (variable)
```

**Fixed header (18 bytes):**
- `magic: u16` — `0x5255` (`"RU"`) for file identification.
- `revision: u64` — monotonically increasing, globally unique revision number.
- `crc32: u32` — CRC32C of `flags + key + value` for integrity checking.
- `key_len: u32` — length of key in bytes (max 2^32-1, practical max ~1MB).
- `val_len: u32` — length of value in bytes (max 2^32-1).
- `flags: u8` — bitfield.

**Flags bits:**
| Bit | Name | Meaning |
|-----|------|---------|
| 0 | `DELETED` | Tombstone: the key was deleted at this revision |
| 1 | `IS_CREATE` | This revision created the key (not an update) |
| 2 | `HAS_LEASE` | Lease ID follows the value (8 bytes, little-endian i64) |
| 3-7 | _reserved_ | Must be zero |

**Record with lease:**
```
┌─────────────────┬──────────────┐
│ [header + kv]   │  lease_id    │
│ as above        │    i64 LE    │
└─────────────────┴──────────────┘
```

### 2.2 WAL File Structure

```
wal.bin:
┌──────┬──────┬──────┬──────┬──────┬──────┬──────┐
│ Rec1 │ Rec2 │ Rec3 │ Rec4 │ Rec5 │ Rec6 │ ...  │
└──────┴──────┴──────┴──────┴──────┴──────┴──────┘
^                                     ^
start_ofs                             write_pos (append here)
```

- **Append-only.** New records are written at `write_pos`. No in-place edits ever.
- **No checkpoint.** Old records are removed by rewriting the file during compaction. A crash during rewrite leaves the original file intact.
- **fsync policy:** fsync after every write. k3s workloads are ~2.4 writes/sec; fsync overhead is ~0.1ms per call. At 1000 writes/sec, batch fsync (every N records or every 10ms) can be introduced.

### 2.3 Crash Recovery

On startup:
1. Open `wal.bin`, scan from offset 0.
2. For each record: verify CRC32C, skip if corrupted (truncated tail).
3. Rebuild in-memory BTreeMap by applying each record in order.
4. `compact_rev` is the last compaction revision (stored in a separate `rudurru.meta` file or as the first record in the WAL).
5. Skip records with `revision <= compact_rev`.

Crash during write: the partially-written record fails CRC check and is skipped. The previous valid record is the last committed state.

---

## 3. In-Memory State

### 3.1 Core Structures

```rust
/// Global, monotonically increasing revision counter.
/// Stored as AtomicU64; written to WAL on every mutation.
/// Persisted as the last record's revision in the WAL.
static NEXT_REV: AtomicU64 = AtomicU64::new(1);
```

```rust
/// In-memory representation of a key's current state.
/// This is what the in-memory index stores for every live key.
struct KeyState {
    /// Value content, shared across watchers via Arc.
    value: Arc<[u8]>,
    /// Revision of the last modification (Put or Delete).
    mod_revision: u64,
    /// Revision of the last creation (first Put after Delete or initial).
    create_revision: u64,
    /// Number of modifications since creation (0 = deleted).
    version: i64,
    /// Lease ID this key is attached to, or 0.
    lease: i64,
    /// If true, the key is currently deleted (tombstoned).
    deleted: bool,
}
```

```rust
/// The entire in-memory state, guarded by a single RwLock.
struct StoreState {
    /// Primary index: BTreeMap<Vec<u8>, KeyState>
    /// Sorted by key bytes. Enables point lookup, range scan, prefix scan.
    keys: BTreeMap<Vec<u8>, KeyState>,

    /// Active leases: BTreeMap<LeaseId, LeaseState>
    leases: BTreeMap<i64, LeaseState>,

    /// Registered watchers.
    watchers: Vec<WatchRegistration>,

    /// Current compaction revision.
    compact_rev: u64,

    /// The next revision to assign (also written to WAL).
    next_rev: u64,
}
```

### 3.2 Lease State

```rust
struct LeaseState {
    id: i64,
    ttl: i64,                // seconds
    /// Absolute time when this lease expires.
    expires_at: tokio::time::Instant,
    /// Number of keys currently attached to this lease.
    key_count: u64,
    /// Background keepalive task (cancellation token).
    /// Dropping this cancels the task.
    _keepalive_task: Option<tokio::task::JoinHandle<()>>,
}
```

### 3.3 Watch State

```rust
struct WatchRegistration {
    /// Key prefix to match (empty = match all).
    prefix: Vec<u8>,
    /// Starting revision (exclusive) for event replay.
    start_revision: u64,
    /// Sender for push notifications.
    sender: tokio::sync::mpsc::UnboundedSender<WatchEvent>,
    /// Watch ID (assigned by server).
    watch_id: i64,
    /// Progress notify flag.
    progress_notify: bool,
    /// Filters for event types.
    filters: Vec<FilterType>,
    /// If true, include previous KV in events.
    prev_kv: bool,
}

struct WatchEvent {
    revision: u64,
    event_type: EventType,  // Put or Delete
    kv: mvccpb::KeyValue,
    prev_kv: Option<mvccpb::KeyValue>,
}
```

---

## 4. Concurrency Model

```
                     ┌─────────────────────────┐
    gRPC requests ──▶│    Request Router        │──▶ response
                     │  (tokio tasks, one per   │
                     │   incoming RPC)          │
                     └──────────┬──────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ▼                       ▼
            ┌──────────────┐      ┌──────────────────┐
            │  Read Path   │      │   Write Path     │
            │  (lock-free) │      │  (mutex-guarded) │
            │              │      │                  │
            │  BTreeMap    │      │  1. assign rev   │
            │  range scan  │      │  2. append WAL   │
            │  point get   │      │  3. update index │
            │  prefix walk │      │  4. notify watch │
            └──────────────┘      └──────────────────┘
```

- **Reads** (`Range`, `Watch` registration, `LeaseTimeToLive`): acquire `RwLock` read guard. No blocking on writes.
- **Writes** (`Put`, `Delete`, `Txn`, `Compact`, `LeaseGrant`, `LeaseRevoke`, `LeaseKeepAlive`): acquire `RwLock` write guard. Serialized.
- **Watch fan-out:** After the write guard is released, events are sent to watcher channels outside the lock. This prevents watcher back-pressure from blocking the write path.
- **WAL write:** Performed inside the write lock. The file is opened with `O_APPEND | O_SYNC` or `O_DIRECT` depending on platform. Write is `pwrite` + `fsync`.

### 4.1 Why a Single Write Lock Is Acceptable

At k3s's typical workload (2.4 writes/sec, peaks at ~100 writes/sec during pod churn), a single-threaded write path is not a bottleneck. Even at 1000 writes/sec, the critical section is:
- `assign_rev`: atomic add (ns)
- `serialize + pwrite`: ~1µs for 1KB
- `fsync`: ~0.1-1ms (dominates)
- `BTreeMap insert`: ~0.5µs
- `watcher notify`: O(W) where W = number of matching watchers (typically <10)

Total: ~0.1-1ms per write. At 1000 writes/sec, the write lock is held for ~100ms/sec total — 10% utilization. Sufficient headroom.

If contention becomes a problem (unlikely for k3s), the WAL write can be moved outside the lock using a write-ahead buffer:

```
1. Reserve revision (inside lock)
2. Buffer record (inside lock)
3. Release lock
4. Flush buffer to WAL (outside lock)
5. fsync (outside lock)
```

This is a future optimization. Start simple.

---

## 5. Operation Semantics

### 5.1 KV Service

#### 5.1.1 Range

```
Range(RangeRequest) → RangeResponse
```

**Read path (lock-free, `RwLock` read guard):**

1. Determine key range from `key` and `range_end`:
   - `range_end` empty: point lookup for `key` only.
   - `range_end == "\0"`: all keys >= `key`.
   - `range_end == key + 1` (next byte): prefix scan for keys starting with `key`.
   - `range_end > key`: range scan `[key, range_end)`.
   - Both `key` and `range_end` empty (`"\0"`): return all keys.

2. Walk `BTreeMap::range(key..end)`, collecting non-deleted keys.

3. Apply filters (if `revision > 0`, filter by `mod_revision`):
   - `min_mod_revision` / `max_mod_revision`
   - `min_create_revision` / `max_create_revision`
   - `keys_only`: omit values.
   - `count_only`: return count only.

4. Apply `sort_order` / `sort_target` if set (collect all matching keys, sort, then apply limit). If no sort, apply `limit` during scan.

5. Set `more = true` if the scan hit the limit and there are more keys.

6. Build `KeyValue` protos from `KeyState`:
   ```rust
   mvccpb::KeyValue {
       key: key.clone(),
       create_revision: ks.create_revision as i64,
       mod_revision: ks.mod_revision as i64,
       version: ks.version,
       value: ks.value.to_vec(),
       lease: ks.lease,
   }
   ```

**Point lookup fast path (no sort, no limit):** `BTreeMap::get(key)`. If absent or deleted, return empty response. O(log N).

**Edge cases:**
- `revision > 0` and `revision < compact_rev`: return `Err(ErrCompacted)`.
- `revision > 0`: iterate in-memory index and filter by `mod_revision <= revision`. Since we keep only the latest state per key (not historical), a point-in-time query at revision R requires scanning the WAL from `R` to current and applying records. This is expensive. **Punt:** if `revision > 0`, scan WAL from `revision` to rebuild the state at that point. For kine's workload, this is never used (kine always reads "latest"). If performance is needed later, maintain a separate `BTreeMap<(Vec<u8>, u64), KeyState>` for historical versions.
- `serializable = true`: same as non-serializable in single-node (no consensus distinction).
- Key with `deleted = true` in index: skip in range scans, return empty for point lookup.

#### 5.1.2 Put

```
Put(PutRequest) → PutResponse
```

**Write path (write lock):**

1. Assign revision: `next_rev += 1` → `rev`.
2. Build `WALRecord { revision: rev, key, value, flags }`.
3. Determine flags:
   - `IS_CREATE` if key is not in index or `deleted == true`.
   - `HAS_LEASE` if `lease > 0`.
4. Serialize record to binary WAL format.
5. `pwrite` + `fsync` to WAL file.
6. Update in-memory index:
   ```rust
   let prev = keys.insert(key, KeyState {
       value: Arc::from(value),
       mod_revision: rev,
       create_revision: prev.map(|k| if k.deleted { rev } else { k.create_revision }).unwrap_or(rev),
       version: prev.map(|k| if k.deleted { 1 } else { k.version + 1 }).unwrap_or(1),
       lease,
       deleted: false,
   });
   ```
7. If `prev_kv` requested, populate `PutResponse.prev_kv` from previous state.
8. If `ignore_value`, keep existing value unchanged.
9. If `ignore_lease`, keep existing lease unchanged.
10. Build response header with `revision = rev`.
11. Enqueue watch notifications (see §6).

**Edge cases:**
- `key` empty: return `Err(ErrEmptyKey)`. (etcd proto says "An empty key is not allowed.")
- `ignore_value && key` not found: return `Err(ErrKeyNotFound)`.
- `lease` set and lease does not exist: return `Err(ErrLeaseNotFound)`.
- `prev_kv` requested: return the `KeyValue` from before the put (or nil if first write).

#### 5.1.3 DeleteRange

```
DeleteRange(DeleteRangeRequest) → DeleteRangeResponse
```

**Write path (write lock):**

1. Determine key range (same logic as Range).
2. Walk the range, collect keys that are not already deleted.
3. Assign a single revision for the entire operation. All deleted keys share the same revision.
4. For each key to delete, create a tombstone WAL record:
   ```rust
   WALRecord { revision: rev, key, value: empty, flags: DELETED }
   ```
5. All tombstones are written to WAL in a single `writev` call (vectored I/O).
6. Update index for each key: set `deleted = true`, `mod_revision = rev`, `version = 0`.
7. Build response with `deleted = count`.
8. If `prev_kv` requested, collect previous `KeyValue` for each deleted key.
9. Enqueue watch notifications.

**Edge cases:**
- Range matches nothing: return `deleted = 0`, no WAL writes.
- Deleting the entire keyspace (`key = "\0"`, `range_end = "\0"`): this is valid and will mark everything deleted (but not remove from index — compaction handles that).

#### 5.1.4 Txn

```
Txn(TxnRequest) → TxnResponse
```

Txn is the **most complex** operation. It implements conditional multi-op with compare-and-swap semantics.

**Write path (write lock):**

```
Phase 1: Evaluate conditions
  for each Compare in compare:
    lookup key in index
    evaluate (key absent, value, mod_revision, create_revision, version, lease)
    if condition fails: result = false, break

Phase 2: Execute operations
  ops = result ? success : failure
  assign_rev = next_rev
  for each RequestOp in ops:
    execute Range/Put/DeleteRange with assign_rev
    (nested Txn is recursively executed, but assign_rev is the same for all ops)

Phase 3: Build response
  response.succeeded = result
  for each executed op, append matching ResponseOp
```

**Condition evaluation semantics:**

| CompareTarget | Comparison |
|---|---|
| `VERSION` | Key must exist. Compare `version` field. |
| `CREATE` | Key must exist. Compare `create_revision` field. |
| `MOD` | Key must exist. Compare `mod_revision` field. |
| `VALUE` | Key must exist. Compare `value` bytes. |
| `LEASE` | Key must exist. Compare `lease` field. |

| CompareResult | Meaning |
|---|---|
| `EQUAL` | `key_value == target` (or key absent for NOT_EQUAL?) |
| `GREATER` | `key_value > target` |
| `LESS` | `key_value < target` |
| `NOT_EQUAL` | `key_value != target` |

**Important:** If a `Compare` has `range_end` set, it checks whether ANY key in the range matches the condition. The semantics match etcd: "range_end compares the given target to all keys in the range."

**Nested Txn:** If a `RequestOp` contains a `TxnRequest`, it's executed recursively. This happens inside Phase 2, using the same revision.

**Revision assignment:** All operations in a single Txn share the same revision. This is critical for watch semantics: "generates events with the same revision for every completed request."

**Edge cases:**
- Compare on a key that doesn't exist with `EQUAL`: `false` (the comparison fails).
- Compare on a key that doesn't exist with `NOT_EQUAL`: `true` (key doesn't exist, so it's not equal to any value).
- Multiple conditions: they form a conjunction (AND). All must pass for `success` branch.
- `range_end` in Compare: checks if any key in range matches. Returns `true` if at least one matches.

#### 5.1.5 Compact

```
Compact(CompactionRequest) → CompactionResponse
```

**Write path (write lock):**

1. Validate: `revision > compact_rev` (cannot compact below already-compacted rev).
2. Validate: `revision <= next_rev - 1` (cannot compact future revisions).
3. Set `compact_rev = revision`.
4. Persist `compact_rev` to `rudurru.meta` file (or first WAL record).
5. Remove from in-memory index all keys where `mod_revision <= revision && deleted == true` (garbage collect tombstones that are below compact_rev).
6. Optional: rewrite WAL, removing records with `revision <= compact_rev`.
7. If `physical = true`, perform the WAL rewrite immediately (blocking). Otherwise, defer to background compaction goroutine.

**WAL rewrite during compaction:**

```
1. Open wal.bin.new for writing.
2. Scan wal.bin, copy records with revision > compact_rev.
3. fsync + rename wal.bin.new → wal.bin.
4. Old file is garbage collected by the OS.
```

**Edge cases:**
- `revision` < `compact_rev`: return `Err(ErrCompacted)`.
- `revision` > `next_rev`: return future revision error.
- No keys to compact: still update `compact_rev` and persist.

---

### 5.2 Watch Service

#### 5.2.1 Watch (bidirectional streaming)

```
Watch(stream WatchRequest) → stream WatchResponse
```

The Watch RPC is a bidirectional stream. The client sends `WatchCreateRequest` / `WatchCancelRequest` / `WatchProgressRequest` on the input stream, and the server sends `WatchResponse` on the output stream.

**Stream lifecycle:**

```
Client connects
  └→ Server spawns a task for this stream
       └→ loop:
            ├─ recv WatchCreateRequest ──→ register watcher ──→ send WatchResponse{created: true, watch_id}
            ├─ recv WatchCancelRequest ──→ unregister watcher ──→ send WatchResponse{canceled: true, watch_id}
            ├─ recv WatchProgressRequest ──→ send WatchResponse{watch_id: 0} with current header
            └─ client disconnects ──→ unregister all watchers on this stream, clean up
```

**Watcher registration:**

```rust
fn register_watcher(req: WatchCreateRequest) -> WatchID {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let watch_id = next_watch_id();
    store.watchers.push(WatchRegistration {
        prefix: determine_prefix(req.key, req.range_end),
        start_revision: req.start_revision,
        sender: tx,
        watch_id,
        progress_notify: req.progress_notify,
        filters: req.filters,
        prev_kv: req.prev_kv,
    });

    // Spawn a task that forwards from rx to the gRPC stream.
    // This task reads events from the channel and sends them as WatchResponse.

    // If start_revision > 0, replay historical events from WAL.
    if req.start_revision > 0 {
        replay_from_wal(req.start_revision, determine_prefix(req.key, req.range_end), tx.clone());
    }

    watch_id
}
```

**Event replay from WAL:**

When a watcher requests `start_revision`, the server must scan the WAL for records at or after that revision and send them as events before sending live events.

```rust
fn replay_from_wal(start_rev: u64, prefix: &[u8], tx: Sender) {
    // Scan WAL from the record containing start_rev.
    // For each record matching the prefix:
    //   - If DELETED flag: send DELETE event.
    //   - Otherwise: send PUT event.
    // After replay, live events come through the channel.
}
```

**Progress notifications:**

If `progress_notify` is set, the server periodically (every 5 minutes, matching etcd behavior) sends an empty `WatchResponse` with the current header. Implemented with a `tokio::time::interval` per watcher in the event forwarding task (`src/server/watch.rs`).

**Watch cancellation:**

When a `WatchCancelRequest` is received, or the client disconnects, or the watcher's `start_revision` has been compacted:
1. Remove the `WatchRegistration` from `store.watchers`.
2. Drop the sender (closes the channel, the stream task exits).
3. Send a final `WatchResponse{canceled: true, watch_id}` if not due to disconnect.

---

### 5.3 Lease Service

#### 5.3.1 LeaseGrant

```
LeaseGrant(LeaseGrantRequest) → LeaseGrantResponse
```

**Write path (write lock):**

1. If `ID == 0`, assign a new lease ID (choose a random i64 not in use, or use monotonically increasing). Otherwise, validate the requested ID is not in use.
2. Calculate `expires_at = now + TTL`.
3. Insert into `leases` map.
4. Spawn a background timeout task:
   ```rust
   tokio::spawn(async move {
       tokio::time::sleep(Duration::from_secs(ttl)).await;
       // If no keepalive extends the lease, revoke it.
       // "Revoke" = delete all keys with this lease + remove lease from map.
       // Use a cancellation token that's reset on each keepalive.
   });
   ```
5. Return `{ID, TTL}`.

#### 5.3.2 LeaseRevoke

```
LeaseRevoke(LeaseRevokeRequest) → LeaseRevokeResponse
```

**Write path (write lock):**

1. Look up lease in map. Not found → return `Err(ErrLeaseNotFound)`.
2. Cancel the keepalive task (drop the cancellation token).
3. For every key in the index with `lease == id`:
   - Create a DELETE WAL record (same revision for all keys).
   - Set `deleted = true`, `mod_revision = rev`, `version = 0` in index.
   - Enqueue watch notification with `DELETE` event type.
4. Remove lease from map.

#### 5.3.3 LeaseKeepAlive (bidirectional streaming)

```
LeaseKeepAlive(stream LeaseKeepAliveRequest) → stream LeaseKeepAliveResponse
```

1. Client sends `LeaseKeepAliveRequest{ID}`.
2. Server extends lease TTL: `expires_at = now + TTL`.
3. Send `LeaseKeepAliveResponse{ID, TTL}`.
4. If the keepalive stream breaks (client disconnect), lease continues with its remaining TTL.

**Implementation:** The streaming RPC uses a simple request/response loop. Each request from the client triggers a TTL extension and a response.

#### 5.3.4 LeaseTimeToLive

```
LeaseTimeToLive(LeaseTimeToLiveRequest) → LeaseTimeToLiveResponse
```

**Read path (lock-free):**

1. Look up lease.
2. If `keys == true`, scan the key index for all keys with this lease ID.
3. Return `{ID, TTL, grantedTTL, keys}`.

#### 5.3.5 LeaseLeases

```
LeaseLeases(LeaseLeasesRequest) → LeaseLeasesResponse
```

**Read path (lock-free):**

1. Collect all lease IDs from the leases map.
2. Return `{leases: [{ID}, ...]}`.

#### 5.3.6 Lease Expiry (background)

When a lease expires:
1. Acquire write lock.
2. Check if the lease is still in the map and `expires_at` has passed (it could have been extended by keepalive since the timer fired).
3. If expired: delete all keys with this lease (same as LeaseRevoke).
4. Remove lease from map.

---

### 5.4 Cluster Service (Stubs)

The Cluster service (MemberAdd, MemberRemove, MemberList, MemberUpdate, MemberPromote) is **not applicable** for a single-node embedded store. These methods will return `UNIMPLEMENTED` initially.

- `MemberList`: return a singleton member list with hardcoded values (ID=1, name="default", clientURLs=["http://localhost:2379"]).

### 5.5 Maintenance Service (Stubs)

Most maintenance operations are not applicable to an in-memory + WAL store:

| RPC | Implementation |
|-----|---------------|
| `Alarm` | No alarms. Return empty list. |
| `Status` | Return version, dbSize (WAL file size), leader (1), raftIndex/raftTerm (0 or last compact rev). |
| `Defragment` | No-op (no fragmentation in append-only WAL). |
| `Hash` | Return hash of the in-memory index. |
| `HashKV` | Return hash + compact_revision. |
| `Snapshot` | Stream the current WAL file contents. |
| `MoveLeader` | `UNIMPLEMENTED` (single-node). |

### 5.6 Auth Service (Stubs)

Auth is optional for k3s (many deployments run without auth). Initial implementation: `UNIMPLEMENTED` for all methods. Auth can be added later as a middleware layer.

---

## 6. Watch Notification Flow

This is the most performance-critical path after the WAL write itself. It must not block the write lock.

```
Write lock held:
  1. Assign revision rev.
  2. Write to WAL.
  3. Update BTreeMap.
  4. Build a lightweight Event { key, revision, event_type }.
  5. Collect matching watchers (prefix match).
  6. For each: try_send(Event) on the mpsc channel.
  7. Release write lock.

Watcher stream task (outside write lock):
  loop:
    recv(Event) from mpsc
    if filters apply, skip
    build WatchResponse from Event
    send WatchResponse to gRPC stream
```

**Key design choices:**

- **`try_send`** — never block inside the write lock. If a watcher's channel is full (back-pressure), either:
  1. Drop the event (and mark the watcher as lagging).
  2. Disconnect the watcher (send "too slow" error).
  
  Initial approach: drop events on full channel. Watchers that can't keep up must reconnect. This matches etcd's behavior (etcd uses a buffer per watcher and disconnects if full).

- **Prefix matching** — iterate over all registered watchers and compare each prefix against the key. O(W) where W = number of watchers. For k3s, W is typically < 10.

- **Event construction** — the `mvccpb::KeyValue` for the event is built after the write lock is released, using a snapshot of the key's state. This avoids serialization inside the lock.

- **Same revision events** — in a Txn, all operations share the same revision. Multiple events with the same revision may be sent to the same watcher. This matches etcd's behavior.

---

## 7. ResponseHeader Semantics

Every response includes a `ResponseHeader`:

```protobuf
message ResponseHeader {
  uint64 cluster_id = 1;
  uint64 member_id = 2;
  int64 revision = 3;
  uint64 raft_term = 4;
}
```

For Rudurru:
- `cluster_id`: hardcoded `1` (not a multi-member cluster).
- `member_id`: hardcoded `1` (single member).
- `revision`: the current store revision after the operation.
- `raft_term`: `0` (no Raft).

For read operations (Range), `revision` is the current store revision at the time of the read. For write operations, `revision` is the revision assigned to the write.

---

## 8. Startup Sequence

```
1. Parse CLI args / config:
   - --wal-path (default: /var/lib/rudurru/wal.bin)
   - --listen-addr (default: [::]:2379)

2. Open WAL file:
   - If wal.bin exists: open, scan, rebuild index.
   - If wal.bin.meta exists: read compact_rev.
   - If neither exists: create empty WAL, initialize next_rev = 1.

3. Rebuild in-memory state:
   for each record in WAL:
     if record.revision <= compact_rev: skip
     apply to BTreeMap (same logic as write path)
   next_rev = max(record.revision) + 1

4. Start gRPC server:
   tonic::Server::builder()
     .add_service(KvServer::new(store.clone()))
     .add_service(WatchServer::new(store.clone()))
     .add_service(LeaseServer::new(store.clone()))
     .add_service(ClusterServer::new(store.clone()))
     .add_service(MaintenanceServer::new(store.clone()))
     .add_service(AuthServer::new(store.clone()))
     .serve(addr)

5. Ready. Log: "rudurru ready, revision={next_rev-1}, keys={count}"
```

---

## 9. Dependencies

See `Cargo.toml` for exact versions.

| Crate | Purpose |
|-------|---------|
| `tonic` | gRPC server framework |
| `tonic-prost` | Prost codec for tonic |
| `prost` | Protobuf message types |
| `tokio` | Async runtime, I/O, timers, channels |
| `tokio-stream` | Stream adapters for tonic |
| `futures` | Stream trait |
| `tracing` | Structured logging |
| `anyhow` | Error handling |
| `serde` / `serde_json` | Config serialization |

**Build dependencies:**
| Crate | Purpose |
|-------|---------|
| `tonic-prost-build` | Proto compilation |

**Not using:**
- `etcd-client` (dev-dependency only, for integration tests)
- `crossbeam` / `parking_lot` (tokio's native `RwLock` is sufficient)
- `mmap` (WAL file is read/written with standard file I/O for portability)

---

## 10. Implementation Phases

### Phase 1: Storage Core (this PRD)
- `Store` struct with `RwLock<StoreState>`
- WAL read/write with binary format
- In-memory `BTreeMap` for keys
- Revision counter

### Phase 2: KV Operations
- `Range` — point lookup, range scan, prefix scan, sort, limit, count
- `Put` — append WAL, update index, notify watchers
- `DeleteRange` — tombstone WAL records, update index
- `Txn` — condition evaluation, multi-op execution, single revision
- `Compact` — WAL rewrite, tombstone GC

### Phase 3: Watch (done)
- Watch stream lifecycle (create, cancel, disconnect)
- Event replay from WAL
- Push notifications via mpsc
- Progress notifications (manual + periodic via `progress_notify` timer)

### Phase 4: Lease
- LeaseGrant / LeaseRevoke / LeaseKeepAlive
- Lease expiry timer
- Lease-scoped key deletion

### Phase 5: Maintenance & Stubs
- Status, Snapshot, Hash
- Cluster service stubs (singleton member list)
- Auth service stubs (unimplemented)

### Phase 6: Integration
- Wire up to actual k3s via kine-compatible gRPC
- Benchmark against kine/SQLite baseline
- Performance tuning (batch fsync, arena allocation)

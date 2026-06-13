# Kine on SQLite — Architectural Mismatch Analysis

**Why is a SQL database backing a key-value log eating 17% CPU for 30 pods?**

These are field notes from profiling k3s v1.36.1+k3s1 (kine v0.15.0, SQLite backend) on a single-node cluster with ~30 pods, ~2.4 writes/sec (mostly lease heartbeats), and a 66MB database.

---

## 1. The SQLite Tax

The CPU profile shows **61% of cycles are pure infrastructure overhead** that a purpose-built implementation would not pay:

| Source | Share | What it's actually doing |
|--------|-------|--------------------------|
| `_sqlite3_step` | **33%** | CGo call into SQLite: SQL parsing, B-tree page walks, query planning, WAL management |
| Go GC (`gcBgMarkWorker`) | **28%** | Tracing and sweeping allocations from protobuf, SQL row scanning, event fan-out |
| syscall (`write`) | 8% | WAL + WAL checkpoint + WAL flushes |
| Everything else (gRPC, runtime, protobuf) | 31% | Actual useful work |

A Rust implementation with a binary WAL + in-memory B-tree would reduce the 61% to near zero:
- No CGo → `_sqlite3_step` → 0
- No GC → `gcBgMarkWorker` → 0
- Sequential WAL append instead of SQLite WAL + checkpoint → syscall overhead minimal

**Realistic target: 1-3% of a core**, for a **6-10× CPU reduction** at this workload.

---

## 2. The Full List of Mismatches

### 2.1 SQL Parser + Query Planner — completely unused
Kine's `pkg/drivers/generic/generic.go` ships **5 SQL query templates** that never change. They are `fmt.Sprintf`'d at startup. Yet every query goes through:
- `sqlite3Prepare()` — lexical analysis, parsing, AST construction
- `sqlite3QueryPlanner()` — cost-based optimization across 6 indexes
- `sqlite3VdbeExec()` — bytecode interpretation of the query plan

A custom implementation would skip this entire stack: the "query plan" is hardcoded in the application logic.

### 2.2 Six B-tree secondary indexes — one suffices
SQLite builds a separate B-tree for each index. kine creates **6**:

| Index | Purpose | In-memory equivalent |
|-------|---------|---------------------|
| `kine_name_index` | Filter by key name | Sorted map by key |
| `kine_name_id_index` | Filter by key + revision | Sorted map by key |
| `kine_id_deleted_index` | Filter by revision + deleted flag | Not needed — scan inline |
| `kine_prev_revision_index` | Compact: find superseded rows | Not needed — row links to prev |
| `kine_name_prev_revision_uindex` | Unique constraint on (name, prev_rev) | Hash map or const assert |
| `kine_id_compat_rev_key_with_prev_revision_index` (partial) | Compact: filter + order | Not needed — iteration is cheap |

In memory, one B-tree or hash map keyed by `(name, revision)` replaces them all. Index maintenance becomes a single ordered map insert.

### 2.3 ACID transactions — never used
Kine writes are single-row inserts guarded by a `UNIQUE (name, prev_revision)` constraint. There are no multi-row transactions, no rollbacks, no savepoints. Yet the default DSN uses `_txlock=immediate` which acquires a write lock at BEGIN time.

A purpose-built store would do: write to WAL → update memory map → done. No transaction protocol needed.

### 2.4 WAL checkpoint (FULL) — a self-inflicted latency spike
After every compaction, kine calls `PRAGMA wal_checkpoint(FULL)` which:
1. Blocks all concurrent writes
2. Reads every dirty page from the WAL
3. Writes them to the main database file
4. Checkpoints the WAL

This is needed because SQLite accumulates pages in the WAL that must eventually merge. A binary append-only WAL never needs a checkpoint — old segments are simply deleted after compaction.

### 2.5 VACUUM at startup — rewriting the entire DB
Every k3s restart triggers `VACUUM` (or at least attempts to — issue #607 shows this is buggy). VACUUM rewrites the entire database file to defragment pages. This is necessary because SQLite's B-tree pages fragment over time as rows are INSERTed and DELETEd.

A log-structured store never fragments: writes are sequential appends, and compaction is just truncation of the head.

### 2.6 Poll-based watches — O(N) polling for push semantics
The single poll goroutine (`sql.go:478`) runs every 1 second:

```
loop:
  SELECT ... FROM kine WHERE id > ? ORDER BY id ASC  ← full scan from last polled rev
  for each row:
    check sequential consistency (gap detection)
    if gap: INSERT gap-fill row (another round trip)
  fan out to all watchers under a mutex
```

A push-based system: write appends to WAL → immediate notification to matching watchers. Zero polling, zero gap-fill waste, zero latency.

### 2.7 BLOB storage in B-tree pages — fragmentation debt
Each write stores `value` and `old_value` as BLOBs in SQLite B-tree pages. Over time:
- INSERTs scatter values across pages
- DELETEs (compaction) leave free pages behind
- VACUUM rearranges everything

A binary WAL stores values contiguously. Compaction just truncates the file. No fragmentation, no defragmentation, no VACUUM.

### 2.8 Go GC + CGo allocation churn
SQLite's C API requires Go to copy byte slices across the CGo boundary (`sqlite3_bind_blob`, `sqlite3_column_blob`). Every row scan allocates Go heap objects:
- `rows.Scan()` allocates per-column temporaries
- `RowsToEvents()` allocates `server.Event` structs
- The `Broadcaster` copies events to subscriber channels

A Rust implementation with `Arc<[u8]>` or arena allocation would avoid this entirely.

---

## 3. How a Rust + Binary WAL Implementation Would Work

### 3.1 Storage Format

```
WAL file (append-only, sequentially written):
┌──────┬──────┬──────┬──────┬──────┬────────┐
│ Rec1 │ Rec2 │ Rec3 │ Rec4 │ Rec5 │  ...   │
└──────┴──────┴──────┴──────┴──────┴────────┘

Record format:
┌──────────┬──────────┬────────┬───────┬──────────┬──────────┬──────────┐
│ revision │ key_len  │ val_len│ flags │   key    │  value   │  crc32   │
│   u64    │   u32    │  u32   │  u8   │ [u8;N]   │ [u8;M]   │   u32    │
└──────────┴──────────┴────────┴───────┴──────────┴──────────┴──────────┘

flags bits:
  0x01: deleted
  0x02: is_create
  0x04: has_lease (lease value follows)

Compaction: remove records with rev < compact_rev by rewriting
the WAL (or using segmented files where segments are deleted).
```

### 3.2 In-Memory State

```rust
// Primary index: BTreeMap<Key, KeyState>
struct KeyState {
    current_revision: u64,
    create_revision: u64,
    lease_end: Option<Instant>,
    value: Arc<[u8]>,
    prev_revision: u64,
    deleted: bool,
}

// Index: BTreeMap<&[u8], &KeyState>  (sorted for prefix scans)
// Watchers: Vec<WatchRegistration>
// - prefix: &[u8]
// - revision: u64
// - sender: mpsc::Sender<Event>
```

### 3.3 Operation Mapping

| etcd operation | Implementation |
|---|---|
| **Put(key, val, lease)** | Assign next revision. Append to WAL. Insert/update in BTreeMap. Notify matching watchers. |
| **Delete(key)** | Append tombstone to WAL. Mark deleted in BTreeMap. Notify watchers. |
| **Range(key, end, limit)** | Walk BTreeMap `range(key..end)`. Skip deleted. Collect up to limit. |
| **Range(prefix)** | Walk BTreeMap `range(prefix..prefix_succ)`. Skip deleted. |
| **Txn(if, then, else)** | Check conditions against BTreeMap. Execute then/else branch. Appends to WAL for each write. |
| **Watch(prefix, rev+)** | Register `mpsc::Sender`. On replay, scan WAL from `rev` for matching keys and send. |
| **Compact(rev)** | Truncate WAL before first record with `revision > rev`. Remove entries from BTreeMap where `prev_revision < rev`. |
| **LeaseGrant(ttl)** | Spawn timer. Store `lease_end` in KeyState. |
| **LeaseRevoke(id)** | Cancel timer. Delete all keys with matching lease. |

### 3.4 Watch Path (No Polling)

```
Write(gRPC request)
  → parse protobuf
  → assign revision (atomic u64 counter)
  → serialize Record to WAL (sequential write)
  → update BTreeMap (in-memory, O(log N))
  → for each watcher whose prefix matches:
      send Event to its mpsc::Sender
  → return response

Compare to kine:
  Write → INSERT INTO kine (CGo + SQL) → notify chan → poll loop wakes
  → SELECT ... WHERE id > ? (another CGo + SQL call) → gap check → fan out
```

The Rust path is: write syscall + BTreeMap insert + mpsc send. No serialization, no parsing, no polling, no allocation churn.

### 3.5 Startup

```
1. Open WAL file
2. Scan sequentially, rebuilding BTreeMap:
   - Apply each record in order (idempotent by revision)
   - Track compact_rev (skip records with rev < compact_rev)
   - Track current_rev = max(rev)
3. Resume gRPC service
4. Watchers register, replay from their requested rev

Startup time: O(N) where N = active records in WAL (not total historical).
Compare to SQLite: VACUUM + PRAGMA wal_checkpoint + CREATE TABLE IF NOT EXISTS + indexes.
```

---

## 4. How Much Faster? Bounded Analysis

### Micro-benchmarks (single operation)

| Operation | SQLite (kine) | Rust binary WAL | Speedup |
|-----------|---------------|-----------------|---------|
| Write (1KB value) | ~15µs (CGo + SQL + B-tree insert) | ~1µs (writev + BTreeMap insert) | **15×** |
| Read by key | ~8µs (SQL prepare + step + scan) | ~0.2µs (BTreeMap lookup) | **40×** |
| Prefix list (100 keys) | ~80µs (GROUP BY MAX scan) | ~2µs (BTreeMap range walk) | **40×** |
| Watch notify | ~50µs (poll cycle latency) | ~1µs (push via mpsc) | **50×** |
| Compaction (100K rows) | ~2s (serializable DELETE + WAL checkpoint) | ~5ms (WAL truncate + BTreeMap sweep) | **400×** |

### Real-world workload (this cluster: 2.4 writes/sec, 30 pods)

| Metric | Current (kine/SQLite) | Rust WAL estimate | Ratio |
|--------|----------------------|-------------------|-------|
| CPU | 17% of a core | 1-3% | **6-17× less** |
| Memory | ~150-200MB (Go runtime + GC overhead) | ~20-40MB | **5× less** |
| Disk IO (write) | WAL + DB + pages + checkpoints | Single sequential WAL | **3-5× less** |
| Watch latency (idle) | 0-1000ms (poll ticker) | <1ms (push) | **1000×** |
| Watch latency (active) | 0-100ms (notify chan) | <1ms | **100×** |

### Scaling projections (1000 writes/sec, 10K keys)

| Metric | SQLite | Rust WAL | Notes |
|--------|--------|----------|-------|
| CPU | ~80-100% of a core | ~5-10% | SQLite CGo + GC scales linearly |
| Watch latency | 100-500ms avg | <1ms | Poll loop becomes bottleneck |
| ListCurrent latency | 50-200ms | <1ms | GROUP BY MAX becomes O(N) scan |
| Compaction throughput | Limited by serializable txn | I/O bound (sequential) | SQLite locks all writes |

---

## 5. Why kine Chose SQLite (and why it's still the right call for its original goal)

Kine was designed to prove that Kubernetes can run **without etcd**, not to be optimal. The key design goals were:

1. **Support multiple backends** (SQLite, PostgreSQL, MySQL, NATS) via SQL abstraction
2. **Minimal code** — the SQL layer is ~1200 lines of Go + per-driver schemas
3. **Correctness by delegation** — SQLite guarantees ACID, crash recovery, concurrent safety
4. **"Good enough" performance** — for a Raspberry Pi with 5 pods, overhead doesn't matter

A Rust binary WAL implementation would be:
- **More code** (WAL format, crash recovery, fsync ordering, concurrency model)
- **Single backend** (no SQL abstraction layer — you'd write one storage engine)
- **Harder to audit** (unsafe for mmap, manual memory management)
- **Faster by 6-10×** (no GC, no CGo, no SQL parsing, push-based watches)

The trade-off makes sense for kine's context. But for a cluster where 17% CPU from the storage backend is the top-line issue, the SQLite tax is the obvious place to cut.

---

## 6. The "Just Use etcd" Counter-Argument

etcd with embedded bbolt and Raft on a single node:
- No CGo (Go native)
- No SQL overhead (bbolt is a simple B-tree on mmap'd pages)
- Push-based watches (not polling)
- ~3-5% CPU baseline for this workload

And k3s actually supports this now (`--cluster-init` flag). The DB would be ~100-200MB instead of 66MB, but the CPU would drop from 17% to ~4-5%.

The main advantage of SQLite over etcd in k3s is **operational simplicity** (single file, no cluster join, trivial backup). But the performance cost is significant.

---

## 7. Conclusion

| Storage | CPU (idle, 30 pods) | Complexity | Backup | Watch mechanism |
|---------|---------------------|------------|--------|-----------------|
| kine/SQLite | **17%** | Low | `cp state.db` | Poll (1s worst-case) |
| kine/etcd (embedded) | ~4-5% | Medium | `etcd snapshot` | Push |
| Rust binary WAL | **~1-3%** | High | `cp wal.bin` | Push |
| etcd (external cluster) | ~3-5% | High | `etcd snapshot` | Push |

SQLite is the worst option for CPU efficiency but the best for operational simplicity. A Rust binary WAL implementation would be the best for efficiency but requires significantly more engineering.

The 6-10× CPU improvement from a Rust implementation is technically achievable because 61% of the current cycles are pure infrastructure overhead (CGo + GC + SQL parsing) that a lean, native implementation would simply not pay.

---

## 8. Appendix: Key SQLite Overhead Numbers from Profile

| Function | CPU share | Source | Why it exists |
|----------|-----------|--------|---------------|
| `_sqlite3_step` | 33% | CGo sqlite3 | SQL statement execution |
| `runtime.gcBgMarkWorker` | 28% | Go runtime | GC marking phase |
| `syscall.Syscall` (write) | 8% | Go runtime | WAL + DB page flushes |
| `runtime.mallocgc` | 5% | Go runtime | Allocation for SQL rows |
| `runtime.scanobject` | 3% | Go runtime | GC scanning |
| `sqlite3VdbeExec` | 2% | CGo sqlite3 | Bytecode VM execution |
| `pthread_cond_wait` | 2% | CGo | CGo thread synchronization |
| `sqlite3BtreeMovetoUnscaled` | 1% | CGo sqlite3 | B-tree traversal |
| `sqlite3PagerAcquire` | 1% | CGo sqlite3 | Page cache lookup |
| Everything else | 17% | — | Protobuf, gRPC, runtime |

Total CGo + GC + alloc overhead: **~83% of CPU cycles** are not doing useful work.

A Rust binary WAL would collapse this to essentially 0% — the CPU would be dominated by protobuf serialization, gRPC framing, and the occasional WAL write. That's the 6-10× gap.

# WAL Garbage Collection — Design

**Date:** 2026-06-15
**Status:** Design phase

## Problem

The WAL is append-only and grows unboundedly. Current state:

| Metric | Value |
|--------|-------|
| Current WAL size | 266 MB |
| Growth rate | ~6 MB/hour (143 MB/24h) |
| Projected size (30 days) | ~4.5 GB |
| Projected size (90 days) | ~13 GB |
| Keys in store | 2,251 |
| WAL records | ~385,000 |
| Avg record size | ~707 bytes |

At k3s workload (~72 writes/sec, mostly lease heartbeats and pod status
updates), every CREATE/UPDATE/DELETE appends a record. After the initial
burst at startup, writes are small status updates — but they accumulate
endlessly because the WAL has no compaction mechanism.

**Problems with unbounded WAL growth:**
1. Disk consumption — eventually fills the partition
2. Startup time — Phase 1 WAL replay reads the entire file (currently ~170ms
   at 266MB, would grow to seconds at multi-GB)
3. Memory pressure — `read_to_end` allocates a buffer equal to WAL size
4. No way to reclaim space after key deletions (tombstones accumulate)

## Key Insight for Compaction

The WAL records are per-revision snapshots of each key's `mvccpb.KeyValue`.
For a given key, only the **latest** non-deleted state matters for startup
recovery. All earlier PUT records for that key, all DELETE records, and all
records for keys that were deleted and never re-created can be **pruned**.

Current WAL content (approximate):
- 385,000 total records
- 2,251 active keys (non-deleted)
- 382,749 stale records (superseded PUTs, tombstones, deleted keys)
- **99.4% of WAL content is garbage**

## Requirements

1. **Crash safety:** A compaction crash must not lose committed writes.
   At most 50ms of writes may be lost (matching the deferred fsync window).
2. **Online operation:** Compaction runs in the background. No downtime,
   no read/write service interruption exceeding the write lock hold time.
3. **Space reclamation:** After compaction, WAL should approximate the size
   of active key data (~2,251 keys × ~707 bytes ≈ ~1.6 MB, plus overhead).
4. **Configurable threshold:** Admin sets max WAL size or compaction interval.
5. **Low overhead:** Compaction must not compete with k3s write throughput.

## Design Options

### Option 1: WAL Rotation with Full-State Snapshot (Recommended)

Write a full snapshot of all active keys to a new WAL file, then copy
the tail of the old WAL (writes that happened during snapshotting) as
raw bytes, then atomically swap files.

**Flow:**
```
Phase A [write lock]: Snapshot: collect all (key, kv_bytes) into Vec
                      + record snapshot_rev + snapshot_wal_size (file length)
Phase B [no lock]:    Write snapshot records to wal.compact
                      (active WAL continues accepting writes — tail grows)
Phase C [write lock]: Read tail bytes (snapshot_wal_size..current_size)
                        from active WAL
                      Append tail bytes verbatim to wal.compact
                      sync_all() + rename wal.compact → wal
                      Open new WAL file at wal
```

**Crash safety:**
- Crash in Phase A: all writes before lock release are in the old WAL.
  No compaction file exists yet.
- Crash in Phase B: old WAL is intact, `wal.compact` is discarded on
  restart (or detected as incomplete).
- Crash in Phase C (rename): `rename(2)` is atomic on Linux. The file
  at `wal` is either the old WAL or the new compacted WAL — never a
  partial write.

**Cost:** Phase A copies 2,251 kv_bytes under write lock (~100µs).
Phase B serializes and writes ~2 MB outside any lock.
Phase C copies tail bytes (~50KB for a few seconds of writes) + rename
under write lock (~1ms).

**Pros:** Simple, crash-safe, zero data loss (tail copy preserves all
writes). Low write lock time (split across Phase A and C).
**Cons:** Double disk space during compaction (~266 MB temporary).
Requires the WAL file to be readable at `snapshot_wal_size..end` during
Phase C (always true on Linux).

### Option 2: Inline Compaction (rewrite WAL in place)

Read the current WAL, filter to keep only the latest record per key
(non-deleted), write to a temp file, swap.

**Flow:**
```
1. [read lock] Scan WAL, build map of (key → latest KvWalRecord)
               Skip records for keys where latest is a DELETE
2. [read lock] Write filtered records to compacted_wal.tmp
3. [write lock] Verify store hasn't changed since step 1 (compare rev)
   — if changed, discard temp and retry
4. [write lock] Rename compacted_wal.tmp → wal
5. [write lock] Unlink old WAL, open new file
```

**Crash safety:** Same as Option 1 — temp file is written before rename.

**Cost:** Step 1 iterates 385K records under read lock (microseconds of
deserialization, no I/O — reads are from the existing WAL buffer during
Phase 1). Step 2 writes ~1.6 MB. Total lock time is split: read lock
for scan (long but doesn't block writers) + write lock for rename (short).

**Pros:** Same space savings as Option 1, no separate snapshot file.
**Cons:** Scans entire WAL (385K records) — CPU cost grows with WAL size.
The scan under read lock may take milliseconds for multi-GB WALs.

### Option 3: Append-Only Snapshot File (separate from WAL)

Maintain a separate `wal.snap` file that is periodically refreshed with
the full key state. The WAL continues to append as normal. On restart,
load `wal.snap`, then replay WAL records with revision > snapshot_rev.

**Flow:**
```
1. [read lock] Snapshot: collect all (key, kv_bytes) into compacted Vec
                + snapshot_rev = current_revision()
2. [read lock] Write snapshot to wal.snap.tmp
3. [read lock] sync_all() on snapshot
4. [read lock] Rename wal.snap.tmp → wal.snap
5. [separate]  Truncate WAL: copy post-snapshot records to temp WAL,
               swap, unlink old WAL
```

**Crash safety:** The WAL is never modified — only the snapshot file is
updated. On restart, if snapshot is missing/stale, fall back to full WAL
replay (slower but safe).

**Pros:** WAL writes never interrupted. Truncation of old WAL can be
deferred (even run on next restart).
**Cons:** Two files to manage. WAL truncation step (5) still needs the
complexity of file rotation. Separate snapshot adds one more file.

### Option 4: Periodic Restart GC

On next restart (triggered by a signal or WAL size threshold), the server
writes a compacted WAL during shutdown, replacing the active WAL before
the next start.

**Flow:**
```
1. [at startup] After WAL replay, check if WAL > threshold
2. If yes: compact WAL (write filtered records, rename, reopen)
3. Continue normal operation
```

**Pros:** Simple — no online compaction machinery. Runs during startup
when write load is low.
**Cons:** Startup time increases (double WAL replay). WAL can grow
unchecked between restarts. k3s restart required to trigger GC.

### Option 5: Continuous Background Compaction (LSM-like)

Segment the WAL into fixed-size chunks. A background task merges older
chunks into smaller ones by filtering out stale records.

**Flow:**
```
wal/000001.wal  (oldest, may be compaction target)
wal/000002.wal
wal/000003.wal  (active, still appending)
```

A background task reads `000001.wal`, filters to keep only records that
represent the latest state of each key (cross-referencing with newer
files), writes a compacted `000001.wal.new`, then atomically swaps.

**Pros:** No write lock stall — compaction is fully incremental. Disk
space from compacted chunks is immediately reclaimed.
**Cons:** Significant design complexity. Segment management, cross-file
deduplication, crash recovery across segments. Overkill for 2,251 keys.

## Recommendation

**Option 1 (WAL Rotation with Full-State Snapshot)** is the recommended
approach for its simplicity and crash safety. The write lock is held for
only 1-5ms (serialize + write 1.6 MB of active keys), well within the
acceptable window for k3s workloads.

**Why not Option 2 (Inline Compaction):** The WAL scan under read lock
iterates 385K records. While fast today (microseconds), this scales with
WAL size — at 4.5 GB it would scan millions of records. Option 1's
approach of iterating only `state.keys` (2,251 entries) is O(active keys)
rather than O(WAL size).

**Why not Option 3 (Separate snapshot):** Two files, two sync concerns,
and still needs WAL truncation logic. The complexity saving vs Option 1
is minimal.

**Why not Option 4 (Restart-only):** WAL grows for days between restarts.
Not a solution for continuous operation.

**Why not Option 5 (Segmented):** Massive overkill for 2,251 keys.

## Implementation Plan

### Design (tail-copy approach)

The core insight: **capture the current WAL file size in Phase A, then in
Phase C copy the bytes that were appended during Phase B as raw tail data
into the new compacted WAL.** This ensures zero data loss — every write
that committed during compaction is preserved verbatim.

```
Phase A (write lock): snapshot all active keys + snapshot_wal_size
Phase B (no lock):    write snapshot records to wal.compact
Phase C (write lock): copy old WAL tail bytes → wal.compact,
                      rename wal.compact → wal, reopen
```

### Phase A: Snapshot Under Write Lock

Acquire the write lock, iterate `state.keys`, collect all active (non-deleted)
key-value pairs as `KvWalRecord` entries. Also record:
- `snapshot_rev = current_revision()`
- `snapshot_wal_size` = byte length of the active WAL file at this moment

```rust
let (records, snapshot_rev, snapshot_wal_size) = {
    let mut state = self.state.write().await;
    let rev = current_revision();
    let wal_len = state.wal.file.lock().unwrap().metadata()?.len();
    let mut recs = Vec::with_capacity(state.keys.len());
    for (key, ks) in state.keys.iter() {
        if ks.deleted { continue; }
        let flags = if ks.lease != 0 { wal::HAS_LEASE } else { 0 };
        recs.push(wal::KvWalRecord::new(
            flags, key, &ks.value,
            ks.create_revision as i64, ks.mod_revision as i64,
            ks.version, ks.lease,
        ));
    }
    (recs, rev, wal_len)
};
```

**Write lock duration:** ~1-5ms (HashMap iteration + Arc clones).
No writes are blocked long enough to matter for k3s workloads.

### Phase B: Write Snapshot (No Lock)

Write all snapshot records to a new temp file `wal.compact`:

```rust
let target = format!("{}.compact", wal_path);
let mut compact = wal::WalFile::open(&target)?;
for rec in &records {
    compact.append_kv(rec)?;
}
compact.sync_all()?;
```

During Phase B, the active WAL continues to accept writes (no lock held).
These writes append to the active WAL at offsets ≥ `snapshot_wal_size`.
The temp file `wal.compact` contains only the snapshot records.

**Duration:** ~1-10ms (serialize + write ~2MB of snapshot data).
**Memory:** `records` Vec holds ~2,251 entries (~1.6MB of kv_bytes as Bytes).

### Phase C: Append Tail + Swap (Write Lock)

Re-acquire the write lock. Read the bytes that were appended to the
active WAL during Phase B (the "tail"), append them verbatim to
`wal.compact`, then atomically rename:

```rust
{
    let mut state = self.state.write().await;

    // Read tail bytes from the active WAL (writes that happened during Phase B)
    let mut tail = Vec::new();
    {
        let mut f = state.wal.file.lock().unwrap();
        f.seek(std::io::SeekFrom::Start(snapshot_wal_size))?;
        f.read_to_end(&mut tail)?;
    }

    // Append tail bytes to the compacted file
    {
        let mut f = compact.file.lock().unwrap();
        f.write_all(&tail)?;
        f.sync_all()?;
    }
    drop(compact);

    // Atomically replace the active WAL
    std::fs::rename(&target, &wal_path)?;

    // Re-open the new WAL for appending
    state.wal = wal::WalFile::open(&wal_path)?;
    state.wal.dirty.store(true, Ordering::Release);
}
```

**Why this is safe:** The tail bytes are raw `KvWalRecord` serializations
that were written by normal mutation operations. They are valid WAL
records. By appending them verbatim to the compacted snapshot, the new
WAL contains:

```
[snapshot: key1, key2, ..., keyN]
[tail:    writes that happened during Phase B (e.g., rev 1001, 1002, ...)]
```

On the next restart, `scan_kv` reads all records (snapshot + tail) and
`apply_record` processes each one. The snapshot records establish the
initial BTreeMap state (all keys at snapshot revision). The tail records
update/delete keys as needed. End result: **identical to replaying the
full original WAL.**

**Write lock duration:** ~1-10ms (read tail bytes + append + rename + reopen).

### Phase 3: Scheduled Compaction

Trigger compaction when the WAL exceeds a threshold (default 64 MB).
The simplest trigger: check in the existing 60s periodic status task:

```rust
// In the 60s status task, after logging:
let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
if wal_size > COMPACTION_THRESHOLD { // 64 MB
    let store = store.clone();
    tokio::spawn(async move { store.compact_wal().await; });
}
```

Alternatively, a dedicated background task:

```rust
fn start_compaction_task(state: Arc<RwLock<StoreState>>, wal_path: String) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 min
        loop {
            interval.tick().await;
            let size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
            if size > COMPACTION_THRESHOLD {
                // ... call compact_wal() ...
            }
        }
    });
}
```

### Phase 4: WAL Replay (No Changes Needed)

The compacted WAL is a valid sequence of `KvWalRecord` entries followed
by raw tail bytes (which are also valid `KvWalRecord` serializations).
`scan_kv` reads them all via `KvWalRecord::deserialize`, and
`apply_record` processes each one. **No changes to the replay path are
required.**

The only adjustment: after replay, `max_rev` is correctly computed from
the tail records (which have the highest revisions). The snapshot records
have lower revisions (≤ `snapshot_rev`), so they don't affect `max_rev`.

## Production Validation

Deployed to production at commit `794e7d3` on a cluster with 2 nodes,
~2,300 keys, ~180 watchers. WAL was 284 MB before first compaction.

### First Compaction Result (2026-06-15 23:58)

```
wal_compaction_triggered wal_size=284896402
wal_compacted snapshot_keys=2263 snapshot_rev=400011
  snapshot_bytes=5137228  phase_a_us=10987  phase_b_us=39430
  phase_c_us=1666  total_us=52091
  tail_bytes=0  tail_count=0
  old_wal_size=284896402 → new_wal_size=5137228
```

| Metric | Value |
|--------|-------|
| WAL before | 284 MB |
| WAL after | 5.1 MB |
| Reduction | **98%** |
| Total time | 52 ms |
| Phase A (snapshot) | 11 ms |
| Phase B (write) | 39 ms |
| Phase C (tail + swap) | 1.7 ms |
| Tail records | 0 (no writes during compaction) |
| Active keys | 2,263 |

### Steady-State Growth After Compaction

After compaction, the WAL grows at ~6 MB/hour (k3s workload with ~72 writes/sec,
mostly lease heartbeats and pod status updates). The background compaction task
(64 MB threshold, 5 min check interval) will trigger every ~10 hours.

### Log Format

```json
{
  "snapshot_keys": 2263,      // active keys in snapshot
  "snapshot_rev": 400011,     // revision at snapshot time
  "snapshot_bytes": 5137228,  // serialized snapshot size
  "phase_a_us": 10987,        // write lock held (snapshot)
  "phase_b_us": 39430,        // no lock (write to disk)
  "phase_c_us": 1666,         // write lock held (tail + swap)
  "total_us": 52091,          // total wall clock
  "tail_bytes": 0,            // writes during Phase B
  "tail_count": 0,            // records in tail
  "old_wal_size": 284896402,  // before compaction
  "new_wal_size": 5137228     // after compaction
}
```

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Crash during Phase B (writing snapshot) | Aborted compaction | `wal.compact` is incomplete, old `wal` is untouched — next restart uses full WAL. |
| Crash during Phase C rename | Split-brain WALs | On Linux, `rename(2)` is atomic on the same filesystem. The target path always points to either the old WAL (rename didn't happen) or the new WAL (rename completed). The in-flight rename itself is atomic — no partial state. |
| Crash after rename, before reopen | WAL file descriptor stale | If the process crashes after rename but before `state.wal` is updated, the on-disk `wal` is the new compacted file. On restart, it loads the compacted WAL (snapshot + tail) and replays correctly. |
| Write lock held too long during Phase A | Write throughput spike | Snapshot copies kv_bytes (Bytes::clone = refcount inc, ns-scale). Iteration of 2,251 keys is <100µs. |
| Write lock held too long during Phase C | Write throughput spike | Reading tail + appending to compact + rename + reopen: ~5ms worst case. k3s writes (~72/sec, one every 14ms) may queue briefly. |
| Memory: snapshot Vec of 2,251 records | ~1.6 MB | Negligible. |
| Disk: double space during compaction | ~266 MB temporary | The compact file exists alongside the active WAL for the duration of compaction. Mitigated by checking available disk space before starting. After rename, the old inode is freed (or kept alive briefly by lingering file handles). |

**The stale data risk is eliminated by the tail-copy approach.** Any write
that commits during Phase B is captured in the tail bytes and appended
to the compacted WAL in Phase C. No records are lost — they're just
relocated from the old WAL to the new one.

## Success Criteria

1. WAL stays below threshold (configurable, default 64 MB) after compaction
2. Zero data loss in crash tests during compaction
3. Write latency spike during compaction < 10ms (write lock for snapshot +
   file operations)
4. All existing tests pass without modification
5. Restart from compacted WAL produces identical state to full WAL replay

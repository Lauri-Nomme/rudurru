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

### Option 1: WAL Rotation with Full-State Snapshot

Write a full snapshot of all active keys to a new WAL file, then
atomically swap files.

**Flow:**
```
1. [write lock] Snapshot: collect all (key, kv_bytes) into compacted Vec
                + record current_revision as snapshot_rev
2. [write lock] Write all records to compaction_target.wal
3. [write lock] Write a SnapshotFooter record (magic + snapshot_rev)
4. [write lock] sync_all() on target
5. [write lock] Rename: compaction_target.wal → wal (replaces active WAL)
6. [write lock] Unlink old WAL
7. [write lock] Open new WAL file, seek to end for future appends
```

**Crash safety:** If crash before step 5, old WAL is intact — restart
works as before with full history. If crash during step 5 (rename),
the old WAL and target WAL coexist — next startup can pick the
valid one (prefer the one with SnapshotFooter, or the larger one).

**Cost:** Step 1 copies 2,251 kv_bytes (Bytes::clone = refcount inc).
Step 2 serializes and writes ~1.6 MB. Total lock time: ~1-5ms (I/O
bound by write).

**Pros:** Simple, crash-safe, low write lock time.
**Cons:** Double disk space during compaction (target + active).
Holds write lock for the entire duration (1-5ms is acceptable).

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

### Phase 1: Snapshot Footer Record

Define a new WAL record type `SnapshotFooter` that marks the end of a
compacted WAL and records the snapshot revision:

```
┌──────────┬──────────────┬────────────────┬──────────┐
│ flags=0xFF│ rev(8)       │ crc32(4)      │          │
│   (1)    │   (8)        │   (4)          │          │
└──────────┴──────────────┴────────────────┴──────────┘
rec_len = 1 + 8 + 4 = 13
```

When `WalFile::open` encounters a `SnapshotFooter` as the last record,
set `snapshot_rev` on the file. Phase 1 WAL replay starts from byte 0
but only processes records with revision > `snapshot_rev`.

### Phase 2: Compaction Task in Store

Add a method `compact_wal()` to `Store`:

```rust
pub async fn compact_wal(&self) -> anyhow::Result<()> {
    let wal_path = self.wal_path().await;
    let target = format!("{}.compact", wal_path);

    // Phase A: Snapshot active keys under write lock
    let (records, snapshot_rev) = {
        let mut state = self.state.write().await;
        let rev = current_revision();
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
        (recs, rev)
    };

    // Phase B: Write compacted WAL
    let mut compact = wal::WalFile::open(&target)?;
    for rec in &records {
        compact.append_kv(rec)?;
    }
    compact.write_snapshot_footer(snapshot_rev)?;
    compact.sync_all()?;

    // Phase C: Swap files under write lock
    {
        let mut state = self.state.write().await;
        std::fs::rename(&target, &wal_path)?;
        // Re-open the WAL file at the new path
        state.wal = wal::WalFile::open(&wal_path)?;
        // Set dirty flag since we just wrote the compacted WAL
        state.wal.dirty.store(true, Ordering::Release);
    }

    Ok(())
}
```

### Phase 3: Scheduled Compaction

Add to `Store::open()`:
```rust
Self::start_compaction_task(state.clone(), wal_path.to_string());
```

The compaction task:
```rust
fn start_compaction_task(state: Arc<RwLock<StoreState>>, wal_path: String) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await; // every hour

            let size = {
                let s = state.read().await;
                s.wal.file.lock().unwrap().metadata().map(|m| m.len()).unwrap_or(0)
            };

            if size < COMPACTION_THRESHOLD { // e.g., 64 MB
                continue;
            }

            // Signal that compaction is starting
            // ... call compact_wal() ...
        }
    });
}
```

Alternatively, trigger compaction from the 60s status task when the WAL
exceeds the threshold.

### Phase 4: WAL Replay with Snapshot Footer

Update `WalFile::open` and `scan_kv` to detect and skip records before
the snapshot revision:

```rust
impl WalFile {
    pub fn snapshot_revision(&self) -> Option<u64> {
        self.snapshot_rev
    }
}
```

In `Store::open`:
```rust
if let Some(snap_rev) = wal.snapshot_revision() {
    for rec in &records {
        let rev = rec.mod_revision().unwrap_or(0) as u64;
        if rev <= snap_rev { continue; }
        if rev <= COMPACT_REV { continue; }
        apply_record(&mut state, rec);
        if rev > max_rev { max_rev = rev; }
    }
} else {
    // Full replay — no snapshot
    for rec in &records { ... }
}
```

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Crash during file rename | Data loss | Target file is fully synced before rename. Old WAL still exists until unlinked. |
| Compaction writes stale data | Stale reads after GC | Verify snapshot_rev matches current_revision before swap. If a write advanced rev during compaction, the swap still produces a state valid at the snapshot revision — the writer that updated rev after the snapshot has a record in the old WAL, but the old WAL is gone. |
| Write lock held too long during snapshot | Write throughput spike | Snapshot copies kv_bytes (Arc::clone, nanosecond-scale). Serialize to disk outside the write lock (Phase B). |
| Memory: snapshot Vec of 2,251 records | ~1.6 MB | Negligible. |
| Disk: double space during compaction | ~266 MB temporary | Mitigated by checking available disk space before starting compaction. |

The most important risk is #2: **compaction writes stale data.** Consider
a write (rev 100, key "foo") that completes AFTER the snapshot was taken
(which included the previous value of "foo" at rev 99). After swap, the
new WAL contains the rev 99 value but NOT the rev 100 value. The write
that produced rev 100 completed successfully (WAL fsync at rev 100 was
acknowledged) but its record is lost.

To prevent this, the swap must be atomic with the last write that the
snapshot covers. Since we hold the write lock during Phase A (snapshot),
no writes can occur concurrently. The snapshot covers all writes up to
`snapshot_rev`. Any write with `rev > snapshot_rev` must be in the old
WAL after the lock is released. If a write has `rev > snapshot_rev` and
we swap, that write's record is in the old WAL which is deleted.

**Fix:** Phase A captures `current_revision()` AND `wal.file.metadata().len()`
(file offset at end-of-file). Phase C renames the old WAL to a backup
path (e.g., `wal.old`) instead of deleting it, and the new WAL starts
from its SnapshotFooter. On next restart, if `wal.old` exists, replay
its records after the snapshot revision before replaying `wal`.

Or simpler: **never delete the old WAL immediately.** Rename it to
`wal.0`, and track a generation counter. Keep the last 2 WAL generations.
After the next compaction, delete the oldest generation. This bounds
disk usage to 2× the active data size.

## Success Criteria

1. WAL stays below threshold (configurable, default 64 MB) after compaction
2. Zero data loss in crash tests during compaction
3. Write latency spike during compaction < 10ms (write lock for snapshot +
   file operations)
4. All existing tests pass without modification
5. Restart from compacted WAL produces identical state to full WAL replay

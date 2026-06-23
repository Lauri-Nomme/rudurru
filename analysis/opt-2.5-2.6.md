# Optimization 2.5: `scan_wal_range` starts from byte 0

**Cost:** Each call to `scan_wal_range` opens the WAL segment and scans from byte 0,
discarding all records before `from_revision`. When a watcher connects at revision R,
every WAL record with revision < R is read, decoded, then skipped.

**Production impact:** Measured 0.025% CPU. Zero user reports.
**Occurrence:** Triggers only on watch stream creation (not per-event).

**Options:**
1. **Won't fix** — not a bottleneck at current scale. If watchers grow to 1000+
   concurrent streams per server with high churn, revisit.
2. **WAL index** — maintain a sparse byte-offset index keyed by revision. Adds ~8 bytes
   per WAL record (in-memory), requires recovery on restart.
   **Risk:** Memory grows with WAL length before compaction. Complexity: moderate.
3. **First-record bookmark** — store the first revision's byte offset after each
   compaction. Only helps if `from_revision` is near the compaction watermark.
   Partial fix.

**Recommendation:** Won't fix. Option 2 only if metrics show >1% CPU.

# Optimization 2.6: Watcher list scan is O(W)

**Cost:** `flush_global_batch_at` iterates the entire watcher list (W watchers) for
every WAL record produced during Phase 1. O(W × records). With 10 watchers and 1000
WAL records, that's 10,000 match checks at ~50ns each = 0.5ms — negligible.

**Production impact:** Measured 0.011% CPU. Zero user reports.
**Occurrence:** Every `flush_global_batch_at` invocation (every write batch).

**Options:**
1. **Won't fix** — not a bottleneck. O(W) scan is fine up to thousands of watchers.
2. **Per-key watcher index** — maintain a map from key prefix → list of watchers.
   **Risk:** Doubles watcher registration cost and memory. Adds complexity.
3. **Partitioned watcher list** — shard by key range hash. Reduces scan per batch to
   O(W/S). **Risk:** Moderate complexity, unclear benefit.

**Recommendation:** Won't fix. Revisit if watchers exceed 1000.

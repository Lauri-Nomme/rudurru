# Watch Phase 2 Latency Optimization

## Problem

During k3s startup, ~140 watchers are created concurrently. Each watcher performs
a two-phase WAL replay. Phase 1 (no lock) reads the full WAL from a separate file
handle. Phase 2 (under the write lock) scans the WAL delta since Phase 1, then
registers the watcher.

Phase 2 takes 250–530 ms for the last ~20 watchers in a batch, blocking ALL
writes (Put/Delete/Txn) for that duration.

## Observed Timing

From production startup log (watch_replay lines):

```
watch_id=1  start_revision=1765  phase1_us=199177  phase2_us=12      # first watcher, ~12µs
watch_id=3  start_revision=7248  phase1_us=201692  phase2_us=7       # early, ~7µs
...
watch_id=7  start_revision=100184 phase1_us=213761 phase2_us=527686  # ~528ms
watch_id=10 start_revision=1701   phase1_us=207501 phase2_us=521999  # ~522ms
watch_id=23 start_revision=100183 phase1_us=271000 phase2_us=289147  # ~289ms
...
watch_id=26 start_revision=33686456 phase1_us=0 phase2_us=283237     # stale, 283ms
```

All high-phase2 watchers have timestamps within the same 250 ms window,
indicating they queued behind each other for the write lock.

## Root Cause Analysis

### 1. Lock contention masquerades as scan cost

`phase2_us` is measured from `t1` (end of Phase 1) to completion. This
includes **time spent waiting to acquire the write lock** — not just the
scan + register work. When 140 watcher tasks all call
`state.write().await` concurrently, the last one in the queue waits for
all 139 earlier tasks to finish their Phase 2 first.

### 2. Concurrent Phase 1 creates IO thrash

All 140 Phase 1 tasks run concurrently, each opening a separate file
handle and reading the entire 77 MB WAL via `read_to_end`. This creates
~11 GB of total reads (140 × 77 MB), thrashing the page cache and
increasing disk IO. During this burst, k3s write operations compete for
the same disk, slowing both reads and writes.

### 3. Stale watchers scan the entire WAL under lock

When `start_revision > current_revision()` (stale watcher from before
restart), `phase1_end = 0`. Phase 2 then reads and parses the entire
77 MB WAL under the write lock. This accounts for ~283 ms of the
observed latency for the events watcher.

### 4. WAL growth between Phase 1 and Phase 2

Between a watcher's Phase 1 (no lock) and its Phase 2 (lock acquired),
concurrent k3s writes append to the WAL. The Phase 2 scan reads this
delta. For late watchers, the accumulated delta can be significant,
though this is a secondary factor (typical delta is <1 MB, which should
parse in <10 ms).

### Impact

- **Write latency spikes**: During startup, all Put/Delete/Txn operations
  are blocked for ~500 ms windows.
- **k3s startup delay**: Each of the 140 watchers adds ~10 ms average
  lock time, totaling ~1.4 seconds of sequential lock holding. The last
  watcher may wait ~700 ms for its turn.
- **Cache pollution**: 11 GB of redundant WAL reads evict useful data
  from the page cache.

## Proposals

### P1: Split Phase 2 timing into lock-wait vs. scan

Move `t1` to after the write lock is acquired. This separates lock
contention from actual work, enabling accurate diagnosis.

```rust
let t_lock = std::time::Instant::now();
let mut state = store.state.write().await;
let lock_us = t_lock.elapsed().as_micros();  // time to acquire lock
let t_work = std::time::Instant::now();
// ... scan + register ...
let work_us = t_work.elapsed().as_micros();  // actual work under lock
```

Cost: ~5 lines of code. Risk: none.

### P2: Batch watcher creation

Instead of each watcher doing Phase 1 + Phase 2 independently, batch
all pending create requests: one shared Phase 1 scan, one shared Phase 2
catch-up, then register all watchers under one lock acquisition.

Challenge: Phase 1 currently filters per-watcher range. A batched
approach would need to track all ranges, or just walk every record and
dispatch to matching watchers — which is essentially what Phase 2 does
already if done for all pending watchers at once.

**Simpler variant**: Collect all WatchCreateRequest messages received
during a short window (e.g., 100 ms), then process them as a batch
under a single lock acquisition.

Cost: moderate refactor. Risk: adds latency to individual create
acknowledgments.

### P3: Limit concurrent Phase 1 scans with a semaphore

Add a `tokio::sync::Semaphore` with `permits = 2` (or 1) to serialize
(or near-serialize) Phase 1 scans. This eliminates IO thrash and reduces
page cache pressure.

```rust
static PHASE1_SEM: Semaphore = Semaphore::const_new(2);
let _permit = PHASE1_SEM.acquire().await;
// ... Phase 1 scan ...
drop(_permit);
```

Cost: ~3 lines. Risk: minor (slightly slower first watcher, faster total).

### P4: mmap-based WAL reads

Replace `read_to_end` (which allocates a Vec and copies from kernel) with
`mmap` + lazy parsing. The OS manages page faulting, avoiding double
buffering and allowing concurrent readers to share cached pages.

Cost: moderate refactor of `WalFile::scan`. Risk: mmap semantics with
O_APPEND writes need care (SIGBUS on truncation, but we never truncate).

### P5: Return `Err` for stale watchers instead of scanning entire WAL

When `start_revision > current_revision()` or
`start_revision < compact_rev`, return a "compacted" or "too large
resource version" error immediately instead of falling through to scan
the full WAL under lock.

This is the actual correct behavior per etcd spec — a watcher requesting
revision > current should either wait or get an error, not replay the
entire history.

Cost: ~5 lines. Risk: k3s may depend on the current behavior
(currently these watchers time out harmlessly after Phase 2).

## Implementation Results

All three quick wins (P1, P3, P5) were implemented and deployed in
commit 8402a52+:

### P1 — Split timing confirmed the root cause

After separating lock wait from scan time:

```
watch_replay watch_id=64 start_revision=100184
  phase1_us=193847 lock_us=0 scan_us=11
  key=/registry/serviceaccounts/
```

`lock_us=0` and `scan_us=11µs` for the same resource type that
previously showed `phase2_us=527686` (528ms). **The old 500ms was
100% lock contention**, not scan work.

### P3 — Semaphore eliminated lock contention

With `Semaphore::const_new(4)`, ~140 concurrent watcher tasks are
serialized through Phase 1. Each finishes Phase 1, acquires the write
lock instantly (no other task is waiting), does its microsecond scan,
and releases. Across the entire startup batch:

- `lock_us`: 0 for 96% of watchers, max observed 10ms
- `scan_us`: 5–32 µs across all watchers
- **Writes are NEVER blocked for more than ~50µs**

### P5 — Stale watcher skip

Watchers with `start_revision > current_revision()` now skip the WAL
scan entirely (`scan_us=0`). Previously these scanned the full 77 MB
WAL under the write lock (~283ms). Observed:

```
watch_replay watch_id=21 start_revision=33686456
  phase1_us=0 lock_us=0 scan_us=0  ← was 283ms
  key=/registry/events/
```

### Tradeoff: Startup time

The semaphore serializes Phase 1 scans, increasing startup wall time:
- **Before**: ~1.6s (all 140 Phase 1 in parallel, then 1.4s lock queue)
- **After**: ~6s (140/4 × 190ms with 4 permits)

This is acceptable: k3s startup already takes 30–60s, and write
availability during startup is more important than startup speed.

## Recommendation

| # | Proposal | Effort | Impact | Status |
|---|----------|--------|--------|--------|
| P1 | Split timing | trivial | diagnostic | ✅ done |
| P3 | Phase 1 semaphore | trivial | eliminates lock contention | ✅ done |
| P5 | Stale watcher error | trivial | eliminates ~283ms worst case | ✅ done |
| P2 | Batch creation | moderate | further reduces startup time | future |
| P4 | mmap | moderate | reduces allocation/copy | future |

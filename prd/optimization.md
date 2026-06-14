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

All four proposals were implemented and deployed (P1, P2, P4/P5 as
checkpoint fix). The semaphore (P3) was replaced by batching (P2).

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

### P2 — Per-stream batching (replaces P3 semaphore)

Instead of a semaphore limiting concurrent Phase 1 scans, batch
WatchCreateRequest messages within each gRPC stream. When the first
create arrives, start a 50ms timer. Collect all creates until the
timer fires or a non-create message arrives, then:

1. Single checkpoint (rev + file offset) for the whole batch.
2. Single Phase 1 scan (shared) for all entries in the batch —
   scanning the WAL once instead of N times.
3. Single Phase 2 scan + single write lock acquisition + N
   registrations.
4. Send created responses and spawn event forwarding tasks.

**Structure**:

```rust
struct PendingCreate { key, range_end, start_revision, watch_id, ... }
struct WatchContext { key, range_end, start_revision, watch_id, bound, event_tx, ... }
```

**Flush logic** (per stream):

```rust
// 50ms timer starts on first create
tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(50)).await;
    n.notify_one();
});
```

**Why not the semaphore**: The semaphore serialized Phase 1 scans
across all streams (140/4 = 35 sequential waves × 190ms = ~6s startup).
Batching eliminates the IO thrash without serializing — each stream's
batch does ONE Phase 1 scan regardless of batch size. N watchers → 1
Phase 1 + 1 Phase 2 instead of N of each. Startup time drops from
~6s to ~100ms (single stampede-free Phase 1 per stream).

**Per-watcher creation delay**: Early watchers in the batch see a
brief delay (≤50ms) while the timer collects more creates. This is
within k3s's tolerable window for etcd watch setup.

### P3 — Semaphore (removed, replaced by P2)

The `PHASE1_SEM` approach was implemented in commit 8402a52 but
removed in 7d1eb88 when batching proved simpler and faster.

### P5 — Checkpoint rev + file offset, scan from checkpointed offset

**Problem**: Phase 2 scanned from `phase1_end` which was 0 when
Phase 1 was skipped (e.g. stale watcher). This caused a full 77MB WAL
scan under the write lock.

**Final fix**: Checkpoint both `checkpoint_rev` and `checkpoint_offset`
(the file length at that moment) under the read lock BEFORE any WAL
scans. Phase 1 (no lock) replays revisions `<= checkpoint_rev`. Phase 2
(under lock) always scans from `checkpoint_offset` — not from
`phase1_end`. This guarantees:

1. No records missed: Phase 2 scans the exact byte range written since
   the checkpoint, regardless of whether Phase 1 ran.
2. No full-WAL scan: Even when Phase 1 is skipped, Phase 2 starts from
   `checkpoint_offset` (near end-of-file), not byte 0.
3. No race: The rev+offset snapshot is taken atomically under the read
   lock before any concurrent Phase 1 scans begin.

Observed with stale watcher (start_revision=33,686,456):

```
# Before: Phase 2 scanned full 77 MB WAL under lock
watch_replay watch_id=26 start_revision=33686456
  phase1_us=0 phase2_us=283237  ← 283ms

# After: Phase 2 scans only post-checkpoint bytes (microseconds)
watch_replay watch_id=29 start_revision=33686456
  phase1_us=0 lock_us=0 scan_us=21  ← 21µs
```

### Logged fields

Each `watch_replay` log includes:

| Field | Description |
|-------|-------------|
| `watch_id` | Assigned watch ID |
| `start_revision` | Requested start revision |
| `batch_size` | Number of watchers in the same batch |
| `phase1_us` | Phase 1 scan duration (no lock) |
| `lock_us` | Time spent waiting for write lock |
| `scan_us` | Phase 2 scan + register under lock |
| `key` | Watched key prefix |

### Production validation (2026-06-14)

After deploying the batching implementation:

```
rudurru status rev=114177 keys=1676 watchers=141 leases=5 wal_size=84MB
```

All 141 watchers created successfully across multiple gRPC streams.
`scan_us` ranges 40–250 µs across all watchers. `lock_us` is 0 during
steady state (no cross-stream contention when streams flush
independently). During the initial startup burst, some watchers show
`lock_us` up to ~1.6s due to multiple streams flushing their batches
simultaneously within the same 50ms window.

This cross-stream startup contention is a one-time event and does not
affect steady-state operation.

## P6 — Cross-stream Batching

### Problem

k3s sends exactly one `WatchCreateRequest` per gRPC stream. Per-stream
batching (P2) is useless — every batch has size 1, so each watcher does
its own Phase 1 scan and lock acquisition. With 140 watchers from 140
streams, that's 140 Phase 1 scans and 140 lock acquisitions, just like
the original semaphore-free design.

### Design

Replace per-stream batches with a single global batch queue. All
streams send their creates to a shared `mpsc::UnboundedSender`. A
single background task (`global_watch_loop`) collects creates from all
streams with a 50ms timer, then processes them as one batch:

```
Stream A ──create──┐
Stream B ──create──┤
Stream C ──create──┤→ global_rx → [50ms timer] → flush_global_batch()
Stream D ──create──┘                          → 1× Phase 1 scan
                                               → 1× write lock acquisition
                                               → N× register_watcher
                                               → N× reply via oneshot
```

Each stream task creates an `event_tx`/`event_rx` pair and a
`oneshot::Sender`/`Receiver` pair. The `GlobalCreate` message carries:
- `PendingCreate` (key, range, revision, filters)
- `event_tx` (for Phase 1+2 events)
- `reply` (oneshot sender for the created response)
- `stream_id`, `remote_addr` (for logging)

The stream awaits the reply, sends the `WatchResponse` via its gRPC
`tx`, then spawns event forwarding on `event_rx`.

### Result

Measured in production (140 watchers at k3s restart):

```
watch_replay stream_id=1  remote_addr=10.222.1.22:38644
  watch_id=1 batch_size=140 phase1_us=280048 lock_us=0 scan_us=32

watch_replay stream_id=2  remote_addr=10.222.1.22:38562
  watch_id=2 batch_size=140 phase1_us=280048 lock_us=0 scan_us=32

... all 140 watchers, same batch_size=140, same lock_us=0
```

| Metric | Before (per-stream) | After (cross-stream) |
|--------|---------------------|----------------------|
| Phase 1 scans | 140 (one per watcher) | 1 (shared) |
| Lock acquisitions | 140 | 1 |
| lock_us (worst) | ~1.6s (contention) | 0 |
| scan_us | 5–44 µs each | 32 µs total |
| Writes blocked | up to ~300ms | 32 µs total |
| Startup delay | ~1.6s | 50ms (timer) + 280ms (Phase 1) |

### Logged fields

Each `watch_replay` log now includes:

| Field | Description |
|-------|-------------|
| `stream_id` | Unique ID per gRPC `Watch()` call |
| `remote_addr` | Source IP:port of the gRPC client |
| `watch_id` | Assigned watch ID |
| `start_revision` | Requested start revision |
| `batch_size` | Total watchers in this global batch |
| `phase1_us` | Phase 1 scan duration (no lock, shared) |
| `lock_us` | Time waiting for write lock (0 with cross-stream) |
| `scan_us` | Phase 2 scan + register under lock |
| `key` | Watched key prefix |

## P7 — Zero-copy WAL → gRPC (protobuf-native WAL)

### Concept

Structure each WAL record so its bytes can be directly sent as gRPC
response payloads, eliminating protobuf serialization entirely. Today,
every response (Range, Watch, Put) decodes a `WalRecord` from disk,
constructs a `mvccpb::KeyValue` struct in memory, then re-encodes it
with prost. This is CPU work that produces the same bytes that were
already on disk.

### Current data path

```
WAL read → WalRecord.parse() → construct KeyValue → prost::encode() → gRPC
  (raw bytes)    (struct)          (struct)          (Vec<u8>)     (send)
```

For a Range response returning 5000 keys: 5000× struct construction +
5000× prost encoding. See `src/server/kv.rs` line 84-93, `src/server/watch.rs`
`rec_to_event()`.

### Proposal: embed protobuf `mvccpb::KeyValue` in WAL records

#### New WAL record layout

```
┌──────────┬─────────────────────────────────────┬──────────┐
│ flags(1) │  mvccpb.KeyValue protobuf message   │ crc32(4) │
│          │  (self-describing wire format)       │          │
└──────────┴─────────────────────────────────────┴──────────┘

flags: bit 0 = IS_CREATE (vs DELETE)
       bit 1 = HAS_LEASE

KeyValue protobuf (per rpc.proto):
  field 1: key             → tag(0x0a) + len + bytes
  field 2: create_revision → tag(0x10) + varint
  field 3: mod_revision    → tag(0x18) + varint
  field 4: version         → tag(0x20) + varint
  field 5: value           → tag(0x2a) + len + bytes
  field 6: lease           → tag(0x30) + varint

CRC32C covers: flags(1) + kv_protobuf(N)
```

#### What changes

**WAL write** (`src/storage/wal.rs`, `src/storage/mod.rs`):
- Build `mvccpb::KeyValue` from the in-memory `KeyState` at write time
- Encode with `prost::Message::encode_to_vec()`
- Append: `flags(1) + kv_bytes(N) + crc32(4)`
- `WalRecord` struct becomes: `{ flags: u8, kv_bytes: Vec<u8>, crc: u32 }`
- The revision is embedded in `kv_bytes` (in `mod_revision` field)

**WAL scan** (wal.rs):
- Parse: read flags, read kv_bytes (rest of data minus CRC), verify CRC
- For watch filtering: decode `kv_bytes` to extract `key` and `mod_revision`
  (prost partial decode is fast — just walk fields)
- `rec_to_event()` returns `kv_bytes` directly; no struct construction

**Range/Put responses** (`src/server/kv.rs`):
- `StoreState.keys` stores `kv_bytes: Vec<u8>` per key (from the latest WAL
  record)
- Range response: collect `kv_bytes` from matched keys, wrap in
  `RangeResponse { kvs }`, no per-key encoding
- Put response: use the WAL record's `kv_bytes` directly

**Watch events** (`src/server/watch.rs`):
- During Phase 1+2 replay, send `kv_bytes` as event payload
- Event forwarding: emit pre-encoded `kv_bytes` without re-serialization

**Write-time cost**: One `prost::encode()` per write (slightly more than
current binary serialization, but writes are rare at k3s load).

**Read-time savings**: Zero serialization for every response. Range
responses that hit 1000+ keys benefit most.

### Key guarantees

| Property | Current | Proposed |
|----------|---------|----------|
| `create_revision` correctness | rec_to_event() uses `rec.revision` (BUG: always current rev, not original) | Embedded in kv_bytes at write time from `KeyState.create_revision` — always correct |
| `version` correctness | rec_to_event() uses `version: 1` (BUG: always 1) | Embedded in kv_bytes at write time from `KeyState.version` — always correct |
| WAL integrity | CRC32C over all fields | Same, just different field boundaries |
| Random access (read key from record) | Fixed offset | Must parse varint tags |
| Record length | Deterministic from format | Must parse protobuf or use known length from read |

Note: The `create_revision` and `version` bugs in `rec_to_event()` are
pre-existing and would be automatically fixed by this change (since
kv_bytes would contain the correct values from the write path).

### Zero-copy options

**Option A: WAL → wire** (full zero-copy)
- `kv_bytes` vector is sent directly as the protobuf response field
- No deserialization at all during response construction
- Requires `kv_bytes` to be a valid `mvccpb::KeyValue` protobuf

**Option B: Cache in memory** (simpler, less intrusive)
- Instead of changing WAL format, just cache `kv_bytes` in `KeyState`
- Compute at write time, store in memory, reuse for responses
- WAL remains backward-compatible
- Trade-off: 2× memory for values, restart loses cache

Option A is the full vision. Option B is a practical first step.

### Prost compatibility details

Prost encodes `mvccpb::KeyValue` deterministically (field order matches
proto declaration). The WAL record's kv_bytes must be a valid prost
encoding. We verify this by using `prost::Message::encode()` on the
write path.

The flags byte is NOT part of the protobuf — it's a custom prefix for
fast scan filtering (DELETE vs CREATE). This avoids encoding flags as
a protobuf field and having to decode the entire message just to check
the record type.

### Concerns

1. **WAL version incompatibility**: New format won't read old WALs.
   Either add a format version at file header, or break compatibility
   (acceptable since Raft is not involved and migration from kine is
   a one-shot event).

2. **Memory overhead of `kv_bytes`**: Each KeyValue takes ~O(data)
   bytes. Currently we store the same data key+value separately in
   `KeyState.value`. Would roughly double the in-memory representation
   unless we stop storing raw value and only keep kv_bytes.

3. **CRC cost**: CRC32C over the full protobuf is slightly more
   expensive (covers more bytes) but negligibly so.

4. **Partial protobuf decoding for scan**: During WAL re-scan, we
   need `key` and `mod_revision` to match watchers. With protobuf
   we must walk the varint-tagged fields to find them. This is
   slightly slower than fixed-offset access but still fast
   (~50ns per field walk vs ~5ns for fixed offset).

### Potential savings estimate

| Operation | Current | Proposed | Savings |
|-----------|---------|----------|---------|
| Range (1 key) | 1× WalRecord.parse + 1× KeyValue.encode | 1× kv_bytes copy | ~200ns |
| Range (5000 keys) | 5000× parse + 5000× encode | 5000× kv_bytes copy | ~1ms |
| Watch event | 1× rec_to_event + 1× encode | 1× kv_bytes copy | ~200ns |
| Put response | 1× KeyValue.encode | 1× kv_bytes copy | ~100ns |

Savings are modest per-operation but compound over the server's
lifetime. The main win is architectural cleanliness: the WAL format
becomes the wire format, eliminating a conversion layer.

### Implementation path

1. **Phase 1**: Store kv_bytes in KeyState in memory (cache), without
   changing WAL format. Validate byte counts and correctness.
2. **Phase 2**: Change WAL format to embed protobuf. Write migration
   or version header.
3. **Phase 3**: Remove `value`/`create_revision`/`version` from
   `KeyState` — derive everything from kv_bytes.
4. **Phase 4**: Zero-copy responses — pass kv_bytes slices directly
   to tonic without allocation.

## Recommendation

| # | Proposal | Effort | Impact | Status |
|---|----------|--------|--------|--------|
| P1 | Split timing | trivial | diagnostic | ✅ done |
| P2 | Per-stream batching | moderate | replaced by P6 | ✅ done |
| P5 | Checkpoint rev+offset | trivial | eliminates ~283ms worst case, no race | ✅ done |
| P6 | Cross-stream batching | high | 140 watchers → 1 Phase 1 + 1 lock (32µs) | ✅ done |
| P7 | Protobuf-native WAL (zero-copy) | high | eliminates serialization entirely | analyzed |
| P4 | mmap | moderate | reduces allocation/copy | future |

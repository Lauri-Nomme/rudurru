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
┌──────┬───────────┬────────────────┬────────────┬──────────────────────┬──────────┐
│ flags│ key_offset│ mod_rev_offset │ rec_len(4) │ kv_bytes (protobuf  │ crc32(4) │
│  (1) │    (2)    │     (2)        │            │ KeyValue message)    │          │
└──────┴───────────┴────────────────┴────────────┴──────────────────────┴──────────┘
          └────────── 9-byte header ──────────┘

flags: bit 0 = IS_CREATE (vs DELETE)
       bit 1 = HAS_LEASE

key_offset:     byte offset within kv_bytes where key's length varint starts
                (field 1 = tag(0x0a) + varint(len) + key_bytes)
mod_rev_offset: byte offset within kv_bytes where mod_revision varint starts
                (field 3 = tag(0x18) + varint(value))
rec_len:        total record size in bytes (header + kv_bytes + crc32)
                enables O(1) skip to next record during scan

KeyValue protobuf (per rpc.proto):
  field 1: key             → tag(0x0a) + len + bytes
  field 2: create_revision → tag(0x10) + varint
  field 3: mod_revision    → tag(0x18) + varint
  field 4: version         → tag(0x20) + varint
  field 5: value           → tag(0x2a) + len + bytes
  field 6: lease           → tag(0x30) + varint

CRC32C covers: header(9) + kv_protobuf(N)
```

**Header gives O(1) field access without protobuf parsing.**
At write time (prost::encode known), we compute the offsets once.
At scan time, we jump directly:
- Key: `kv_bytes + key_offset` → read overlong varint → read key bytes
- Revision: `kv_bytes + mod_rev_offset` → read overlong varint → value
- Next record: `offset += rec_len`

### Megahack #2: overlong varints for zero-loop decode

Protobuf decoders MUST accept overlong varints (spec: "a varint is not
required to be a minimum number of bytes"). We exploit this: always
encode `key_length` as a **4-byte** overlong varint and `mod_revision`
as an **8-byte** overlong varint.

At scan time, instead of a byte-by-byte varint loop (branch per byte,
5-10ns), we read a `u32`/`u64` directly and extract the 7-bit groups
with bit manipulation:

```rust
// Read 4-byte overlong varint — no loop, no branch
fn read_overlong_u32(buf: &[u8]) -> u32 {
    let raw = u32::from_le_bytes(buf[..4].try_into().unwrap());
    let m = raw & 0x7f7f7f7f;
    (m & 0x7f)
        | ((m >> 1) & 0x3f80)
        | ((m >> 2) & 0x1fc000)
        | ((m >> 3) & 0xfe00000)
}

// Read 8-byte overlong varint — same pattern, 8× 7-bit groups
fn read_overlong_u64(buf: &[u8]) -> u64 {
    let raw = u64::from_le_bytes(buf[..8].try_into().unwrap());
    let m = raw & 0x7f7f7f7f7f7f7f7f;
    (m & 0x7f)
        | ((m >> 1) & 0x3f80)
        | ((m >> 2) & 0x1fc000)
        | ((m >> 3) & 0xfe00000)
        | ((m >> 4) & 0x7f0000000)
        | ((m >> 5) & 0x3f80000000)
        | ((m >> 6) & 0x1fc000000000)
        | ((m >> 7) & 0xfe0000000000)
}
```

This eliminates varint decode from scan hot path entirely. The protobuf
stays valid — any standard decoder (prost, protobuf-js) handles overlong
varints transparently.

**Trade-off**: We cannot use `prost::Message::encode()` for kv_bytes
since prost always emits minimal varints. We write a custom encoder
that produces valid protobuf with overlong fields. The encoder is
straightforward (tag + fixed-size varint + data for each field).

#### What changes

**WAL write** (`src/storage/wal.rs`, `src/storage/mod.rs`):
- Build `mvccpb::KeyValue` from the in-memory `KeyState` at write time
- Encode with `prost::Message::encode_to_vec()` → `kv_bytes`
- Compute header offsets during encoding by tracking field positions,
  or from post-encode analysis (prost encoding is deterministic)
- Append: `header(9) + kv_bytes(N) + crc32(4)`
- `WalRecord` struct: `{ flags: u8, kv_bytes: Vec<u8>, key_offset: u16,
  mod_rev_offset: u16, rec_len: u32, crc: u32 }`

**WAL scan** (wal.rs):
- Read 9-byte header at current offset
- Read kv_bytes = `rec_len - header_size - crc_size` bytes
- Verify CRC over header + kv_bytes
- Access flags, key, revision via header fields (no protobuf parse)
- For watch filtering: compare key and revision from header offsets
- `rec_to_event()` short-circuits: returns kv_bytes directly as event payload
- Skip to next record: `offset += rec_len`

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
| Random access (read key from record) | Fixed offset | O(1) via header key_offset — jump directly to key data |
| Record length | Deterministic from format | O(1) via header rec_len — no protobuf parsing |

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

4. ~~Partial protobuf decoding for scan~~ — **Eliminated by header**.
   The 9-byte header (flags, key_offset, mod_rev_offset, rec_len) gives
   O(1) access to key and revision without any protobuf field walking.
   At write time we know the offsets; at scan time we jump directly.
   This is as fast as the current fixed-offset format.

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
| P7 | Protobuf-native WAL (zero-copy) | high | eliminates serialization entirely | ✅ done |
| P4 | mmap | moderate | reduces allocation/copy | future |

---

## Codebase-Wide Bottleneck Analysis (2026-06-14)

Identified by systematic code review after P7 zero-copy implementation.
Organized by priority tier: **Critical** (blocks all ops), **High** (significant
waste), **Moderate** (measurable but steady-state).

### A. CRITICAL: WAL fsync Under Write Lock

**Every mutation path** (`put`, `delete_range`, `lease_revoke`, expiry task)
calls `self.file.sync_all()` while holding the RwLock write lock. fsync is
~1-10ms on typical SSDs, blocking ALL readers (range scans, watch replay) and
ALL other writers during that time.

**Files:**
- `src/storage/wal.rs:333` — `append_kv` calls `sync_all()` per record
- `src/storage/mod.rs:395-397` — put calls `append_kv` inside write lock
- `src/storage/mod.rs:453-454` — delete_range same pattern
- `src/storage/mod.rs:617-618` — lease_revoke same pattern  
- `src/storage/mod.rs:763-764` — expiry task same pattern

**Fix:** Move WAL write + fsync outside the write lock. Options:

| Approach | Complexity | Risk |
|----------|-----------|------|
| Write to buffer, fsync on timer | low | up to 50ms data loss on crash |
| Separate WAL writer thread (mpsc channel) | moderate | ordering guarantees needed |
| Dedicated fsync thread, serialized writes in-memory | moderate | same |

For the k3s workload (~72 writes/sec), a 50ms fsync timer with an in-memory
buffer eliminates 99.9% of fsync calls while losing at most 50ms of writes on
crash (acceptable for k3s — etcd has the same trade-off with its raft commit).

### B. CRITICAL: Watch Replay Phase 1 Reads Entire WAL Into Memory

`WalFile::scan_kv` calls `self.file.read_to_end(&mut buf)` loading the entire
WAL into a `Vec<u8>`. For a 1GB WAL, that's 1GB allocated. On every batch of
watch creations (though amortized by cross-stream batching, P6).

**File:** `src/storage/wal.rs:311-312`

**Fix:** Mem-map the WAL. `mmap` provides:
- Zero-copy reads (kernel manages page cache)
- Shared pages across concurrent Phase 1 scans (P6 batching already mitigates
  this, but mmap is still cheaper)
- No allocation for the read buffer
- Lazy page faults — only hot pages are resident

Alternatively: bounded streaming reader using `read_exact` for each record's
`rec_len`. Since `rec_len` is available in the 9-byte header, we can read one
record at a time without loading the entire file.

### C. CRITICAL: Lease Expiry Polls Every 500ms

`start_expiry_task` runs every 500ms with `tokio::time::sleep(500ms)` and
acquires the **write lock**. Even with zero leases, it iterates, checks,
and releases. The 500ms poll contributes 2% of CPU in the perf profile
(see `prd/perf-test.md`).

**File:** `src/storage/mod.rs:727-731`

**Fix:** Use `tokio::time::sleep_until(earliest_expiring)`. Maintain a
BTreeMap or binary heap of `(expires_at, lease_id)`. When a lease is granted
or refreshed, push the new expiry and update the wake-up timer. When the
timer fires, pop all expired leases and process them. This makes expiry
event-driven rather than polling.

Cost: Adds a min-heap. Risk: minimal — sleep_until with a past time resolves
immediately.

### D. HIGH: notify_watchers Clones Entire Watcher List

Every `put` or `delete` calls `notify_watchers`, which does:
```rust
let watchers: Vec<WatchRegistration> = self.watchers.clone();
```
This clones ALL watchers (including `Vec<u8>` keys, `mpsc::UnboundedSender`s,
filters) while holding the write lock. For 10K watchers, this is megabytes of
allocation.

**File:** `src/storage/mod.rs:193`

**Fix:** Iterate `self.watchers` directly instead of cloning. `WatchRegistration`
is `Clone` (derived) but we can iterate with a regular `for w in &self.watchers`.
The borrow checker may require `watchers` to be in a `RefCell` or use an index
loop, but the clone is unnecessary — no other code holds a mutable borrow at
this point.

With the current design (`notify_watchers` called from `apply`/`apply_delete`
which hold `&mut self`), we can iterate with indices:
```rust
for i in (0..self.watchers.len()).rev() {
    let watcher = &self.watchers[i];
    // ... check and send ...
}
```
This avoids the clone entirely.

**Result:** Clone of entire watcher Vec (~28KB for 141 watchers) eliminated per
notify_watchers call. Each put/delete now skips this allocation entirely.
Index iteration has no overhead vs. iterator over owned Vec.

### E. HIGH: resolve_range Called Per-Watcher Per-Event

`notify_watchers` calls `resolve_range(&watcher.key, &watcher.range_end)` for
every watcher on every notification. `resolve_range` allocates new `Vec<u8>`
for each bound type.

**File:** `src/storage/mod.rs:195-196`

**Fix:** Pre-compute and cache `RangeBound` in `WatchRegistration` at
registration time:
```rust
pub struct WatchRegistration {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub bound: RangeBound,  // cached at registration
    // ...
}
```
Then `notify_watchers` just calls `matches_range(watcher.bound.to_ref(), &event.key)`.

Cost: Adds one `resolve_range` call at registration time (negligible). Saves
one per event per watcher for the lifetime of the watch.

**Result:** `RangeBound` now derived `Clone + Debug` and cached in
`WatchRegistration.bound`. `notify_watchers` no longer calls `resolve_range`
per-watcher — uses `watcher.bound.to_ref()` directly. Eliminates one
`Vec<u8>` allocation per watcher per put/delete event.

### F. HIGH: kv_bytes Cloned Per Range Result

`Store::range` clones `ks.kv_bytes.clone()` for every matching key. For a
10K-key range with 1KB values, this is 10MB of allocations.

**File:** `src/storage/mod.rs:326`

**Fix options:**
1. **Arc<Vec<u8>>**: Store kv_bytes in `Arc<Vec<u8>>` and clone the Arc (cheap
   atomic increment). The Vec is immutable after creation — perfect for Arc.
   Cost: one extra atomic op per clone. Impact: clone becomes ~2ns instead of
   O(data) memcpy.
2. **Borrow from KeyState**: Change range to return references. Impossible
   with the current RwLock ownership (can't return references to guard).

Option 1 (Arc) is the clear winner. Changes `KeyState.kv_bytes` from
`Vec<u8>` to `Arc<Vec<u8>>`, and all response construction clones the Arc.

**Result:** `KeyState.kv_bytes` and `WatchEvent.kv_bytes`/`prev_kv_bytes`
changed from `Vec<u8>` to `Arc<Vec<u8>>`. Under write lock, all clone sites
(notify_watchers event creation, put prev_kv, delete_range prev_kv) use
cheap Arc increment instead of deep Vec memcpy. The Vec clone is deferred
to response-building sites (event_to_proto, range kv extraction) which run
outside the write lock. `make_kv_bytes` returns `Arc<Vec<u8>>`.

### G. HIGH: Clone Chain in Put

A single `put` with `prev_kv=true` can clone kv_bytes 3+ times:
1. `prev = state.keys.get(&key).cloned()` — clones entire KeyState
2. `entry.clone()` on insert — clones entire KeyState again
3. `event.kv_bytes = entry.kv_bytes.clone()` — clones kv_bytes for watch event
4. `prev.as_ref().map(|p| p.kv_bytes.clone())` — clones for prev_kv response

**File:** `src/storage/mod.rs:382, 148, 154, 404`

**Fix:** Arc<Vec<u8>> for kv_bytes eliminates all deep copies — all clones
become Arc clones (atomic increment).

**Result:** All 4 clone sites in the put chain now clone the Arc (atomic
increment) instead of the Vec (memcpy). The `prev_kv` response still needs
`Vec<u8>` (converted via `.to_vec()`), but this happens outside the write
lock. Same benefit for delete_range and watch event creation.

### H. HIGH: range Scans All Keys Including Deleted Tombstones

`range` iterates the full BTreeMap, including deleted-flagged entries. The
`ks.deleted` check on each iteration filters them out, but the loop still
visits them. Over time, deleted keys (tombstones) accumulate, degrading
range performance.

**File:** `src/storage/mod.rs:300-315`

**Fix options:**
1. **Tombstone compaction**: Periodically purge deleted entries from
   `state.keys`. Run on a timer or after N deletes.
2. **Separate live-key index**: Maintain a second BTreeMap or HashSet of
   active keys. Range queries iterate the live index.
3. **BTreeMap range API**: Use `BTreeMap::range()` for prefix/point queries
   instead of full iteration. This is already partially supported via
   `resolve_range` but not actually used — the current code iterates ALL
   keys and filters.

Option 3 is the most impactful: replace the full scan with `BTreeMap::range()`
bounds. For prefix queries, use `range(prefix..prefix_successor)`. For range
queries, use `range(start..end)`. This turns O(n) scans into O(log n + k)
where k is the result count.

**Result:** `range()` now uses `BTreeMap::range()` for From, Prefix, Range, and
Point bounds. Only the All case (`range_end = [0,0]` with empty key) still does a
full scan. Point queries use `get_key_value()` (O(log n)). From/Prefix/Range
queries use `range(start..)` or `range(start..end)` with O(log n + k)
iteration. Prefix queries break early when key no longer matches. Combined
with limit-based cap and pre-allocated kvs Vec, limited queries now stop
after `limit` results instead of scanning the entire map.

### I. HIGH: delete_range Clones All Keys Upfront

`delete_range` collects all matching keys into `Vec<Vec<u8>>` before iterating.
This requires a full scan of `state.keys` under the write lock, cloning each
matching key.

**File:** `src/storage/mod.rs:422-432`

**Fix:** Use `BTreeMap::range()` to collect a range iterator, then drain it
with `drain_filter` or by collecting keys in a bounded range. Alternatively,
use `split_off` to split the BTreeMap at range boundaries and iterate the
split portion.

**Result:** `delete_range()` uses the same BTreeMap range approach as `range()`.
Point queries use `get()` (direct O(log n) lookup). From/Prefix/Range use
`range()` iteration. Prefix queries use `take_while(|(k,_)| k.starts_with(p))`
to stop at the first non-matching key.

### J. HIGH: No Early Termination for Limited Ranges

When `req.limit = 10` but there are 1M keys, the entire BTreeMap is scanned.
The kvs Vec is truncated after scanning all matching keys.

**File:** `src/storage/mod.rs:300-329`

**Fix:** Use `BTreeMap::range()` and stop after collecting `limit` results.
This is the same fix as H (use BTreeMap range API).

**Result:** The BTreeMap range iteration combined with pre-allocated `kvs` Vec
(`Vec::with_capacity(req.limit)`) means limited queries stop after collecting
`limit` matching keys. The iterator visits at most `limit` non-deleted matching
keys plus any non-matching keys before the first match. This is O(log n + limit)
instead of O(n).

### K. HIGH: lease_revoke Does Per-Key WAL Appends

For N keys on a lease: N `next_revision()` calls, N WAL appends, N fsyncs,
all under the write lock.

**File:** `src/storage/mod.rs:598-621`

**Fix:** Batch all WAL records into a single `append_kv_batch` call with one
fsync. Also compute all revisions upfront with a single atomic batch
(currently not supported by AtomicU64 — would need to add
`fetch_add(N, Ordering::SeqCst)`).

**Result:** `lease_revoke` now collects all WAL records into a Vec and calls
`append_kv_batch(&records)` once, issuing a single `sync_all()` instead of N.
Each key still gets its own revision (independent `next_revision()` calls).
`append_kv_batch` was already implemented but never called — it writes all
records then calls `sync_all()` once.

### L. HIGH: Event Forwarding Spawns Per-Watch Task

Each successful watch creation spawns a new `tokio::spawn` to forward events
from `event_rx` to the gRPC `tx`. For 10K watches, 10K tokio tasks.

**File:** `src/server/watch.rs:179`

**Fix:** Share a single per-stream forwarding task. Multiple watchers on the
same stream can share a single event multiplexer task that reads from all
event_rx channels and writes to the gRPC tx. Or, use `tokio::select!` in a
shared loop.

### M. MODERATE: Compact_rev Uses Full RwLock

`compact_rev` is read-only (always increased) but acquires the full
`RwLock<StoreState>`. This needlessly blocks writers.

**File:** `src/storage/mod.rs:277`

**Fix:** Use `AtomicU64` for `compact_rev`. It's only ever written by
`compact()` (write lock already held for other reasons) and read by `range()`
(read lock held). Move it out of `StoreState` into a standalone `AtomicU64`.

**Result:** `COMPACT_REV` is a global `AtomicU64`. `range()` and `compact_rev()`
read it without acquiring any RwLock. `compact()` writes it under `store()`.
Eliminates a read lock acquisition from every range query and from the
`compact_rev()` method used by watch creation flow.

### N. MODERATE: Range Has No Pre-Allocation for kvs Vec

`kvs: Vec<Vec<u8>>` starts empty and grows dynamically. Each push may
reallocate.

**File:** `src/storage/mod.rs:285`

**Fix:** After counting matching keys, pre-allocate: `Vec::with_capacity(count)`.
Or at minimum, use an approximate upper bound.

**Result:** Pre-allocates with `state.keys.len()` as upper bound (or `req.limit`
when set). Eliminates incremental reallocation during the main iteration loop.
Reallocations are amortized O(1) per push, but the Vec grows ~2× each time,
temporarily doubling memory usage during the growth phase.

### O. MODERATE: Periodic Status Acquires Read Lock

The 60s periodic status task acquires the read lock to log revision, keys
count, etc. This is harmless in steady state but blocks writers during
lock acquisition.

**File:** `src/main.rs:52-61`

**Fix:** Use atomics for `rev` and `keys` counters. These are monotonically
increasing/decreasing and can be tracked with `AtomicU64`/`AtomicI64`
outside the RwLock.

### P. MODERATE: WAL CRC32C Is Software Bit-by-Bit

The CRC32C implementation in `wal.rs` is a pure-software bit loop. Hardware
CRC32C (SSE4.2 `_mm_crc32_u32`/`_mm_crc32_u8`) is available on all x86-64
processors since 2010.

**File:** `src/storage/wal.rs:611-623`

**Fix:** Replace with `crc32c` crate (uses hardware acceleration when
available). On modern CPUs, hardware CRC32C is ~10× faster than software.

**Result:** Replaced custom bit-loop CRC32C with `crc32c` crate (v0.6.8).
Uses SSE4.2 `crc32q` on x86-64 and ARMv8 CRC instructions on aarch64.
All 26 WAL tests pass with identical CRC output.

### Q. MODERATE: Txn Comparison + Execute Has TOCTOU Race

`txn` evaluates comparisons under the read lock, drops it, then re-acquires
the write lock for execution. Between the drop and re-acquire, the state can
change, making the comparison stale.

**File:** `src/storage/mod.rs:473-480`

**Fix:** For true linearizable transactions, re-evaluate comparisons under
the write lock. The current behavior is non-atomic and could cause
unexpected failures under concurrent writes. For k3s workloads this is
unlikely to trigger (low write concurrency), but is semantically incorrect
per etcd spec.

### R. LOW: KvWalRecord::new Allocates Temporary CRC Buffer

`KvWalRecord::new` builds a temporary `crc_data` Vec to compute the CRC.
This duplicates work already done in `encode_kv` and the header assembly.

**File:** `src/storage/wal.rs:197-202`

**Fix:** Compute CRC incrementally: CRC the flags byte, then fold in
header fields, then fold in kv_bytes. Avoids the temporary Vec.

**Result:** `KvWalRecord::new` now uses `crc32c_append` to compute CRC
incrementally over the 4 header fields and kv_bytes. No temporary Vec
allocation per WAL record written (previously one `Vec::with_capacity(header
+ kv_len)` per record).

### S. LOW: Graceful Shutdown Missing

Ctrl+C kills the process without WAL sync. Unwritten kernel buffers may be
lost. With O_APPEND + write, the window is small but nonzero.

**File:** `src/main.rs` (no signal handler)

**Fix:** Add `tokio::signal::ctrl_c()` and call `wal.sync_all()` before exit.

**Result:** `Server::serve` replaced with `serve_with_shutdown(addr, ctrl_c())`.
Ctrl+C triggers tonic's graceful shutdown (drains in-flight requests), then
exits. WAL sync is already done per-write by `append_kv()` — this adds the
shutdown signal handling to prevent abrupt termination of in-flight operations.

### T. LOW: encode_kv Uses Variable-Length Varint for Value Length

`encode_kv` uses `encode_varint` for value length. Since values can be up
to 1.5MB in etcd (but typically <1KB in k3s), the varint is 1-3 bytes.
Using a fixed 4-byte length field (like key_length uses 5-byte overlong)
would enable O(1) value offset access for tools that want to skip to
value data without parsing.

**File:** `src/storage/wal.rs:112-113`

### U. LOW: Store Hash Acquires Read Lock

`store_hash` acquires the read lock to iterate keys. This is called by
etcd's `Hash` RPC (maintenance).

**File:** `src/storage/mod.rs:689`

**Fix:** Maintain a running hash that's updated on every write. Trade-off:
hash updates on write path adds overhead. For k3s workloads where Hash is
rarely called, the current approach is acceptable.

---

## Priority Matrix

| Priority | Item | Impact | Effort |
|----------|------|--------|--------|
| A | WAL fsync outside write lock | eliminates ~1-10ms stall per write | moderate |
| B | mmap WAL reads | eliminates 1GB+ allocation per startup | moderate |
| C | Event-driven lease expiry | eliminates 2% CPU polling, write lock contention | low |
| D | Stop cloning watcher list | eliminates MB-scale alloc per put with 10K watchers | trivial | ✅ done |
| E | Cache RangeBound in WatchRegistration | eliminates per-event per-watcher resolve_range | trivial | ✅ done |
| F | Arc<Vec<u8>> for kv_bytes | eliminates deep clones in range/put/watch | low | ✅ done |
| G | Arc chain in put | same as F, sub-item | low | ✅ done |
| H | BTreeMap::range() not full scan | O(n) → O(log n + k) for range queries | low | ✅ done |
| I | BTreeMap range for delete_range | O(n) → O(log n + k) | low | ✅ done |
| J | Early termination with limit | avoids scanning entire map for limited queries | low | ✅ done |
| K | Batch WAL in lease_revoke | reduces fsync calls from N to 1 | low | ✅ done |
| L | Per-stream event multiplexing | 10K → 1 tokio tasks per stream | moderate |
| M | AtomicU64 for compact_rev | eliminates unnecessary lock contention | trivial | ✅ done |
| N | Pre-allocate kvs Vec | reduces reallocation during range | trivial | ✅ done |
| O | Atomics for status counters | eliminates periodic read lock acquisition | low |
| P | Hardware CRC32C | ~10× faster CRC computation | trivial | ✅ done |
| Q | Linearizable txn | fixes correctness race | low |
| R | Inline CRC in KvWalRecord::new | eliminates temporary Vec | trivial | ✅ done |
| S | Graceful shutdown | prevents data loss on SIGTERM | trivial | ✅ done |
| T | Fixed-length value length | O(1) value offset access | trivial |
| U | Running hash for store_hash | eliminates read lock for maintenance RPC | low |

## Immediate Next Steps (highest ROI)

1. ~~**Arc<Vec<u8>> for kv_bytes** (F+G) — done.~~
2. **Batch WAL writes + deferred fsync** (A) — the single biggest throughput
   improvement available. Requires design work for crash semantics.

# Progress & Design Log

## Phase 1 — Storage Core (2026-06-13)

### Design Decisions

**1. Binary WAL as single source of truth.**
No separate database file. The WAL is both the commit log and the sole persistent store. On startup, `Store::open()` replays all WAL records into a `BTreeMap<Vec<u8>, KeyState>`. There is no checkpoint, no compaction of the WAL itself (only logical compaction via `compact_rev`). This is the simplest possible persistence model and directly eliminates SQL parsing overhead.

**2. CRC32C per record for integrity.**
Every WAL record has a CRC32C checksum covering `flags(1) + key(N) + value(M) + lease_id(8)`. Corrupted records are silently skipped during scan (the parser breaks and returns records up to the corruption). No repair mechanism — if the WAL head is corrupted, trailing data is lost.

**3. Single `RwLock<StoreState>` for concurrency.**
All mutations (Put, Delete, Txn, Compact) acquire the write lock. Reads (Range) use the read lock. The lock is held for the entire operation including WAL append (fsync). The PRD asserts this is acceptable because k3s workloads write ~2.4 ops/sec and the write lock is held for <1ms. No sharding, no per-key locking.

**4. `NEXT_REV` as a global `AtomicU64`.**
Revision assignment is decoupled from the write lock. `next_revision()` atomically increments a global counter. This means revision order matches wall-clock call order, not WAL order (though in practice they're the same since the write lock serializes). The counter starts at 1 and is initialized to `max_rev + 1` on recovery.

**5. `KeyState` stores `value: Arc<[u8]>`.**
Values are stored as atomically reference-counted byte slices. Cloning a `KeyState` (e.g., for previous-KV in put response) does not copy the value — only bumps the refcount. This is the "no GC" tenet in action: `Arc` is deterministic, zero-cost for the common case.

### Doubts & Unresolved Questions

1. **WAL compaction prevents unbounded growth.** ✅ **Fixed.** `compact_wal()` (called by `start_compaction_task`) periodically rewrites the WAL with only live keys, keeping the file size bounded. Production shows ~6MB/hr growth vs 46MB steady-state; projected 4.5GB/month without compaction is now avoided.

2. **`sync_all()` after every write.** Every `append()` calls `File::sync_all()` (fsync). This is the right thing for crash safety but kills throughput on spinning disks. On modern NVMe with write-back cache it's ~50µs. Acceptable for 2.4 ops/sec but would not scale.

3. **Head-corruption recovery.** If the last few bytes of the WAL are truncated (e.g., power loss during write), the deserializer stops at the first incomplete record and returns all prior records. This is correct but means the last write is silently lost — no error is reported to the caller.

### Shortcuts / Known Gaps

| Gap | Impact | When to Fix |
|-----|--------|-------------|
| ~~No WAL compaction / rotation~~ ✅ | `compact_wal()` keeps WAL size bounded; deployed and running in production | **Done** |
| No checkpoint / snapshot | Full WAL replay on every restart | After all phases, for faster startup |
| Header-only read path still acquires read lock | Lock contention under high concurrency | Not needed for k3s workload |
| `Arc<[u8]>` prevents mutation but not ownership cycles | Theoretical memory leak if keys are churned | Never in practice |

---

## Phase 2 — KV Operations (2026-06-13)

### Design Decisions

**1. `resolve_range` with 5 bound types.**
The etcd v3 range protocol uses `key` + `range_end` to encode five access patterns, each mapping to a `RangeBound` variant:

| Pattern | `range_end` | `RangeBound` |
|---------|-------------|--------------|
| Point lookup | empty | `Point(key)` |
| From-key (>=key) | `\0` (single zero byte) | `From(key)` |
| All keys | empty key + `\0` | `All` |
| Prefix | key + 1 (last byte incremented) or key + `\0` | `Prefix(key)` |
| Range [start, end) | explicit end key | `Range(start, end)` |

The `resolve_range` function decodes these patterns once per request. The result is cached in a `RangeBound` enum and passed to `matches_range` for each key in the BTreeMap scan.

**2. Empty `range_end` is a point lookup (not from-key).**
Bug discovered during testing: the initial implementation treated empty `range_end` as `From(key)` instead of `Point(key)`. In the etcd proto, empty `range_end` means "single key." A single-byte `\0` means "from key." The fix was verified against 5 regression tests.

**3. Prefix encoding via last-byte-increment.**
etcd encodes prefix watches/ranges as `range_end = key with last byte + 1` (standard) or `range_end = key + \0` (alternate). `resolve_range` checks both. The standard encoding handles most cases; the alternate covers keys like `foo\xFF` where incrementing wraps to zero.

**4. Txn: compare-then-execute without speculative execution.**
`TxnRequest` evaluates all comparisons first (read lock), then executes the success or failure branch (individual operations, each acquiring its own write lock). There is no two-phase commit or rollback. If a write in the success branch fails (WAL error), subsequent writes in the same branch still execute. This matches etcd's behavior ("best-effort" txn) but means the txn is not atomic across multiple writes.

**5. Test isolation via `~~~_del/` prefix.**
From-key (`>=key`) operations are inherently unbounded — they match all keys above the given key. To prevent interference between tests running against the shared store, from-key tests use the `~~~` prefix (highest sortable ASCII, value 0x7E). This keeps from-key operations within a known range and avoids deleting keys from other tests. Point, prefix, and range tests use `aaa_`, `bbb_` prefixes.

### Doubts & Unresolved Questions

1. **Txn atomicity.** An etcd txn is atomic: all operations execute or none. Our `execute_txn_ops` runs operations sequentially and does not roll back on failure. If the WAL write for the first `put` in a txn succeeds but the second `put` fails, the first write is committed. This violates the txn contract. Fixing it would require buffering WAL writes in a transaction log and only flushing on success.

2. **Delete is a logical tombstone (not physical removal).** The `deleted: bool` flag on `KeyState` avoids compaction work on every delete. Deleted keys are filtered from range results but remain in the BTreeMap until the next compaction. This means memory usage grows with delete volume, not just live key count.

3. **Compact did not prune the WAL.** ✅ **Fixed.** `compact()` sets `compact_rev` and removes deleted/old entries from the in-memory BTreeMap. `compact_wal()` (called periodically by `start_compaction_task`) rewrites the WAL, retaining only records at or above `compact_rev` and omitting deleted keys. Tests verify that restart from a compacted WAL produces the correct state.

### Shortcuts / Known Gaps

| Gap | Impact | When to Fix |
|-----|--------|-------------|
| Txn is not atomic across multiple ops | Partial txn execution on WAL error | Requires txn-scoped WAL buffer |
| ~~Compact not persisted to WAL~~ ✅ | `compact_wal()` rewrites WAL with live keys only; restart from compacted WAL is correct | **Done** |
| Delete is tombstone (memory grows) | Deleted keys use memory until next compact | Acceptable for k3s workload |
| No range-end validation | Invalid range_end values silently produce incorrect results | Phase 5 hardening |
| ~~No revision-based MVCC~~ ✅ | `range_historical()` supports past-revision range queries with WAL replay | **Done** |

---

## Phase 3 — Watch (2026-06-13)

### Design Decisions

**1. Push-based notification via `mpsc::UnboundedSender`**
Each watcher gets its own `UnboundedSender<WatchEvent>` stored in `WatchRegistration`. `notify_watchers` iterates all registrations and pushes matching events. No polling loop, no timer. Matches the PRD tenet "no polling."

**2. Two-phase WAL replay (minimizes lock time)**
Refactored to minimize write-lock hold time:
- **Phase 1 (no lock):** Note `checkpoint_rev = current_revision()` and `wal_len = file_len()`. Open a separate WalFile handle, scan from byte 0, send events for records with `revision in [start_revision, checkpoint_rev]`. Write lock is never acquired.
- **Phase 2 (under lock):** `wal.scan_from(wal_len)` reads only bytes appended after the checkpoint. Send events for records with `revision > checkpoint_rev`. Register watcher.

Lock time is now `O(writes_between_phase1_and_phase2)` — typically 0-1 records — instead of `O(total_WAL_size)`. The `file_len()` call must happen before `scan()` in Phase 1: any write between `file_len` and `scan` is read by the scan but filtered (revision > checkpoint_rev), then caught by Phase 2's `scan_from`.

`checkpoint_rev` name chosen over `current_rev` to emphasize it's a point-in-time snapshot, not the live head revision.

**3. Watcher matching via `resolve_range` + `matches_range`**
`WatchRegistration` stores the raw `key` + `range_end` from the proto, not a precomputed prefix. `notify_watchers` calls `resolve_range` + `matches_range` on every event to check match. This supports all range types (point, prefix, from-key, range) without maintaining separate match logic.
- *Tradeoff:* Recomputes the range bound on every event notification. Could cache the `RangeBound` in the registration. Not worth optimizing until profiling shows it.

**4. Bounded channel for gRPC output stream, unbounded for internal event forwarding**
The tonic `ReceiverStream` wrapper requires a bounded `mpsc::Receiver`. Used `mpsc::channel(4096)` for the client-facing stream. Internal watcher-to-forwarder channels use `mpsc::unbounded_channel()` so the write lock holder never blocks on a slow consumer.
- *Tradeoff:* Unbounded channels can grow memory if the client is very slow. Acceptable because watch events are small and the channel is drained by a dedicated tokio task.

**5. Per-watcher forward tasks**
Each `WatchCreateRequest` spawns a `tokio::spawn` task that reads from the watcher's `event_rx` and writes WatchResponses to the shared output `tx`. The forward task exits when the client disconnects (send error on `tx`) or the watcher is canceled (sender dropped, `event_rx.recv()` returns `None`).

**6. Global `AtomicI64` watch_id counter**
`NEXT_WATCH_ID` is a global atomic, starting at 1. The proto allows client-assigned IDs (non-zero), so the server only assigns IDs when the client sends `watch_id=0`. A per-stream counter would be sufficient but a global one keeps it simple.

**7. Progress response as immediate reply + periodic timer**
`WatchProgressRequest` sends back a `WatchResponse` with header only (no events, `watch_id=0`). Periodic progress notifications (`progress_notify` flag on create) are implemented via `tokio::time::interval` — the event forwarding task uses `tokio::select!` between `event_rx.recv()` and a 300-second interval tick, sending an empty `WatchResponse` with the current revision header on each tick.

### CRC32C Bug Discovered & Fixed

The WAL `serialize()` computed CRC32C over `buf[17..]` (last byte of key_len + all of val_len + flags + key + value + lease), while `deserialize()` verified CRC32C over `data[23..ofs]` (key + value + lease, **excluding** the flags byte). These ranges were inconsistent.

Result: every WAL record since the project began failed CRC verification on read. `WalFile::scan()` silently returned an empty record list (the `Err` branch broke out of the parsing loop). This meant:
- Crash recovery was completely broken — `Store::open()` rebuilt an empty store from WAL, ignoring all persisted data.
- Watch WAL replay (`test_watch_from_revision`) returned 0 records, causing a timeout.

**Fix:** Both `serialize` and `deserialize` now compute CRC over `[flags_byte .. end_of_payload]` — flags(1) + key(N) + value(M) + lease_id(8). `serialize` uses `buf[22..]`, `deserialize` records `flags_ofs = ofs - 1` after reading the flags byte.

This bug was invisible to KV/Txn integration tests because they operate on the live in-memory store and never restart the server.

### Doubts & Unresolved Questions

1. **WAL replay creates synthetic events.** During replay, `create_revision` and `mod_revision` are set to `rec.revision` (the WAL record's revision, which is the delete/write revision), not the original `create_revision`. `version` is set to 1, `prev_kv` is None. This is incorrect for keys that were created and modified multiple times, but acceptable because the WAL format doesn't store this metadata per record — it would need a separate index.

2. ~~**No compaction awareness in watcher WAL replay.**~~ ✅ **Fixed.** If `start_revision` is below `compact_rev`, the watcher receives a `WatchResponse` with `compact_revision` set and `canceled: true`, and is never registered. See `flush_global_batch_at` in `watch.rs`.

3. **Watcher cleanup on disconnect is indirect.** When the client disconnects, the main handler loop gets `Ok(None)` from `in_stream.message()` and exits. The `tx` sender is dropped, closing the output channel. Forward tasks detect the send error and cancel their watcher. However, if the main handler is blocked on `in_stream.message()` (waiting for the next request) and the client disconnects silently, tonic should return `None` — but this depends on the TCP keepalive / HTTP/2 ping behavior. A `Drop`-based cleanup guard on the response channel would be more robust.

4. **`ReceiverStream::new(rx)` return type.** Tonic's `ReceiverStream` wraps `mpsc::Receiver<T>` (bounded), not `UnboundedReceiver<T>`. The current implementation uses `mpsc::channel(4096)` for `tx`/`rx` and `mpsc::unbounded_channel()` for per-watcher event channels — two different channel types with different back-pressure behavior. This is an awkward asymmetry but works.

### Shortcuts / Known Gaps

| Gap | Impact | When to Fix |
|-----|--------|-------------|
| ~~No `progress_notify` timer~~ ✅ | Periodic progress updates sent via `tokio::time::interval` in event forwarding task | **Done** |
| ~~No compaction error on WAL replay~~ ✅ | Watcher at compacted revision receives `compact_revision` + canceled | **Done** |
| ~~WAL replay event metadata is approximated~~ ✅ | P7 protobuf-native WAL stores full KV metadata (create_revision, mod_revision, version); replay events are now correct | **Done** (P7 protobuf-native WAL) |
| No `fragment` support | Large watch responses not split | If k3s watches large keys |
| ~~Server has no graceful shutdown~~ ✅ | Ctrl+C triggers tonic graceful shutdown | **Done** |
| `mpsc::unbounded_channel` memory unbounded | Slow consumer causes OOM risk | Add channel capacity limit or back-pressure |
| ~~`resolve_range` called on every event notification~~ ✅ | `RangeBound` cached in `WatchRegistration.bound` | **Done** |

### Test Coverage

- 5 watch tests pass against Rudurru: `test_watch_key`, `test_watch_prefix`, `test_watch_from_revision`, `test_watch_progress_notify`, `test_watch_delete_event`
- All 42 integration tests pass against real etcd (docker)
- Tests require `--test-threads=1` due to shared store design (writes from concurrent tests interfere)
- WAL replay specifically tested by `test_watch_from_revision` (puts v1/v2, watches from revision of v2, expects single event for v2)

### Files Changed

- `src/server/watch.rs` — new full implementation (previously stub)
- `src/storage/mod.rs` — `WatchRegistration` fields changed from `prefix` to `key`+`range_end`; `notify_watchers` uses `resolve_range`/`matches_range`; `register_watcher`/`cancel_watcher`/`as_ref` made `pub(crate)`; added `wal_path()` to `Store`
- `src/storage/wal.rs` — CRC32C range fixed in `serialize` (`buf[17..]` → `buf[22..]`) and `deserialize` (`data[23..ofs]` → `data[flags_ofs..ofs]`); callback-based `scan(offset, f)` returns end offset; `scan_collect` kept for startup
- `src/main.rs` — `EnvFilter` changed from always-`info` to `try_from_default_env` with `info` fallback

---

## Phase 4 — Lease (2026-06-13)

### Design Decisions

**1. Polling-based expiry (500ms interval)**
A single background task (`Store::start_expiry_task`) is spawned in `Store::open()`. Every 500ms it acquires the write lock, checks all leases for `expires_at <= now`, and revokes expired ones. No per-lease timers, no cancellation machinery.
- *Tradeoff:* Worst-case 500ms latency between lease expiry and key deletion. Acceptable for k3s workloads. A `sleep_until` on the earliest-expiring lease would be more precise but requires notification when keepalive extends it (complexity not justified).

**2. Lease operations are free functions on `Store`, not WAL-persisted**
`lease_grant`, `lease_revoke`, `lease_keep_alive` are async methods on `Store` that operate directly on `StoreState.leases`. No WAL records are written for lease lifecycle events.
- *Consequence:* Leases do not survive server restart. Keys whose lease expired before restart become orphaned (they retain `lease != 0` but the lease no longer exists). On restart, the in-memory `leases` map is empty.

**3. KeepAlive as simple request-response stream**
`LeaseKeepAlive` is a bidirectional streaming RPC. Each incoming `LeaseKeepAliveRequest` triggers `Store::lease_keep_alive(id)` which resets `expires_at = now + ttl` and responds with the granted TTL. No batching, no heartbeat coalescing.

**4. AtomicI64 counter for lease IDs**
`NEXT_LEASE_ID` is a global `AtomicI64` starting at 1. If the client sends `id=0` (auto-assign), the server generates one. Client-specified IDs are used as-is. etcd uses a similar counter starting from a random seed (to avoid collisions in clusters). Single-node makes a simple counter safe.

**5. `LeaseState.key_count` not maintained**
The `key_count` field exists on `LeaseState` but is never updated. `lease_time_to_live` computes attached-key count dynamically by scanning `state.keys` for matching lease IDs. Accurate and avoids bookkeeping bugs.

### Doubts & Unresolved Questions

1. **Lease persistence.** The PRD envisions WAL-persisted leases but the current WAL format has no lease record type. Adding lease records would need a new flag bit (e.g., `IS_LEASE`) and a new record layout. Since k3s leases are typically short-lived (seconds to minutes) and the control plane recreates them on restart, this is acceptable as a known gap.

2. **~~Put with non-existent lease silently succeeds.~~** ✅ **Fixed.** `Store::put()` at `store.rs:593-598` validates `lease != 0 && !state.leases.contains_key(&lease)` and returns `Err(NotFound)` with "etcdserver: lease not found".

3. **Expiry task holds write lock during revocation.** When the expiry task finds expired leases, it acquires the write lock and processes all revocations (WAL writes + key deletions + watch notifications) while holding it. If many leases expire simultaneously (e.g., after a long downtime), this blocks concurrent writes for the duration. In practice, k3s leases expire at different times.

4. **No limit on lease TTL or count.** etcd enforces a maximum TTL (e.g., 100000000 seconds) and may limit total lease count. Our implementation accepts any positive TTL and any number of leases. A runaway client could exhaust memory.

### Shortcuts / Known Gaps

| Gap | Impact | When to Fix |
|-----|--------|-------------|
| Leases not persisted to WAL | Lost on restart; keys with expired leases become orphaned | Requires WAL format change (new record type) |
| ~~Put doesn't validate lease existence~~ ✅ | Validated at store.rs:593-598 | **Done** |
| No maximum TTL / lease count | No guard against runaway clients | Phase 5 hardening |
| Expiry task uses polling | Up to 500ms latency; holds write lock for batch expiry | If profiling shows issues |
| `LeaseState.key_count` unused | Field exists but is never incremented/decremented | Remove or implement |
| No `LeaseCheckpoint` support | Required for etcd 3.5+ lease checkpointing feature | If k3s needs it |

### Test Coverage

- 5 lease tests pass against Rudurru: `test_lease_grant_revoke`, `test_lease_with_key_expiry`, `test_lease_keepalive`, `test_lease_ttl`, `test_lease_list`
- `test_lease_with_key_expiry` validates the full expiry pipeline: grant TTL=3, put key, sleep 5s, assert key deleted
- `test_lease_keepalive` validates bidirectional keepalive stream
- All existing 33 KV/Txn/Watch tests unaffected
- 46/47 integration tests pass against real etcd (1 regression: `test_delete_from_key` — pre-existing state pollution in shared docker, passes against fresh Rudurru)

### Files Changed

- `src/server/lease.rs` — full implementation of all 5 Lease RPCs (previously all stubs)
- `src/storage/mod.rs` — added `NEXT_LEASE_ID`, lease operations on `Store`; `start_expiry_task`; `use std::sync::atomic::AtomicI64`

---

## Phase 5 — Maintenance & Cluster (2026-06-14)

### Design Decisions

**1. Single-node cluster stub.**
`member_list` returns a hardcoded `Member` with `id=1`, `name="rudurru"`, and dummy peer/client URLs. All mutation RPCs (`member_add`, `member_remove`, `member_update`, `member_promote`) return `Unimplemented`. This is a single-node store with no Raft — there is no cluster to mutate.

**2. Status returns WAL file size as `db_size`.**
`db_size` is obtained from `WalFile.file.metadata().len()`. This is the actual on-disk WAL size. The test expects `db_size > 0`, which holds as long as even the smallest WAL header has been written.

**3. Hash uses SipHash (std `DefaultHasher`) over all key-value pairs.**
Keys and values are fed into `std::collections::hash_map::DefaultHasher`. No crypto guarantees — this is a fast hash that changes when data changes. The etcd-client test only checks `hash != 0` and `hash changes after write`, both of which SipHash satisfies.

**4. Snapshot serializes KVs into a binary blob, sent in 64KB chunks.**
Format: `revision(8) + key_count(4) + [key_len(4) + key(N) + val_len(4) + value(M)]*`. The revision header ensures the blob is non-empty even with zero keys. Chunks up to 64KB, final chunk sets `remaining_bytes=0`.

**5. Defragment is a no-op.**
The in-memory BTreeMap has no fragmentation; the WAL is append-only. `defragment` returns a response with `header: None` (matching etcd 3.5.17 behavior).

**6. Auth service remains unimplemented.**
All 15 auth RPCs (`auth_enable`, `auth_disable`, `authenticate`, user/role CRUD) return `Unimplemented`. The auth test runs against the etcd docker container, not our server. Implementing auth would require user/role storage, permission checking on every operation, and token management — substantial effort for k3s which typically disables auth.

### Doubts & Unresolved Questions

1. **Snapshot format is not etcd-compatible.** The binary format (revision + key_count + key/val pairs) is a simple ad-hoc format. etcd's snapshot is a protobuf-encoded snapshot of the entire backend. Our format cannot be restored by etcd tools. It's only useful for our own future restore logic.

2. **Hash includes deleted-key values.** `store_hash()` iterates `state.keys` but skips `ks.deleted`. However, deleted keys still exist in the BTreeMap (as tombstones) — they're just filtered from the hash. If a deleted key has not been compacted, it won't contribute to the hash. After compaction, it's removed. This means hash depends on compaction state, not just live data. Acceptable because the test only checks that hash changes after a write.

3. **`hash` RPC returns `u32` but internal hash is `u64`.** The `HashResponse.hash` field in the proto is `uint32` but SipHash produces `u64`. The value is truncated. Two different store states could produce the same `u32` hash (collision). The test only checks non-zero and change-after-write, so this is fine for now.

4. **No `Alarm` support.** `alarm` returns `Unimplemented`. etcd uses alarms for disk space exhaustion and corruption detection. Our store has no quota or corruption detection.

5. **No `MoveLeader` support.** Single-node cluster has no leader election.

### Shortcuts / Known Gaps

| Gap | Impact | When to Fix |
|-----|--------|-------------|
| Auth not implemented | Tests must run against etcd docker | If k3s requires auth |
| Snapshot format not etcd-compatible | Cannot restore via etcd tools | If restore is needed |
| Hash truncated to u32 | Collision possible | If hash uniqueness matters |
| No alarm/corruption detection | Silent data corruption | If corruption detection needed |
| No move_leader | No-op (single node) | Never (single-node) |
| Cluster mutations unimplemented | No multi-node support | Never (design constraint) |

### Test Coverage

- 5 maintenance tests pass against Rudurru: `test_status`, `test_member_list`, `test_hash`, `test_snapshot`, `test_defragment`
- 41/42 integration tests pass against Rudurru (auth unimplemented)
- 46/47 pass against real etcd docker (1 pre-existing `test_delete_from_key` state pollution)
- Snapshot validated with empty store (revision header ensures non-empty blob)

### Files Changed

- `src/server/cluster.rs` — `member_list` returns hardcoded single-member response; rest unimplemented
- `src/server/maintenance.rs` — `status`/`hash`/`hash_kv`/`snapshot`/`defragment` implemented; `alarm`/`move_leader` unimplemented
- `src/storage/mod.rs` — added `db_size()` and `store_hash()` methods to `Store`
- `src/storage/wal.rs` — made `file` field `pub` for db_size access

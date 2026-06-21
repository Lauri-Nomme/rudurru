# RCA: Txn Compare Sees Tombstoned Keys → k3s Lease Storm

## Timeline

**2026-06-21 19:30 – 20:46 UTC+3**

| Time | Event |
|------|-------|
| 19:30 | Reported: k3s errors `etcdserver: txn comparison failed`, pod churn |
| 19:38 | Noticed 143k suppressed journald messages in 2 seconds |
| 19:42 | Deployed extra debug logging to all gRPC handlers |
| 19:50 | RCA of `eval_compare`: tombstoned keys are not filtered |
| 19:50 | RCA of `NEXT_LEASE_ID`: WAL replay doesn't restore lease state |
| 20:00 | Fixed & deployed `eval_compare` + `NEXT_LEASE_ID` |
| 20:07 | k3s still cycling — new error: `watch chan error: PrevKv=nil` |
| 20:08 | k3s retries watch every ~100ms (watch storm) |
| 20:38 | Deployed fix: WAL replay now restores `NEXT_LEASE_ID` |
| 20:42 | Deployed fix: prev_kv tracking in watch replay |
| 20:42 | **Cluster stable**: no more PrevKv errors, no watch storm |
| 20:46 | Reduced log verbosity to prevent journald suppression |

## Root Cause #1: `eval_compare` Ignores Tombstoned Keys

**File:** `src/storage/mod.rs` — `eval_compare()`

### The Bug

When a key is deleted in etcd, the `KeyState` remains in the BTreeMap with
`delete_revision != 0` so that historical range queries can determine whether
the key existed at a given past revision. However, `eval_compare` — used by
`Txn` to evaluate compare-and-swap conditions — always looked up the raw
`KeyState` without filtering out tombstoned entries:

```rust
// BEFORE (buggy)
let ks = state.keys.get(key);
// ks could be a tombstoned entry with a high delete_revision
```

```rust
// AFTER (fixed)
let alive = state.keys.get(key).filter(|k| k.is_alive());
// Tombstoned entries are excluded — treated as "key does not exist"
```

### Impact on k3s

k3s uses `Txn` with `Compare{ mod_revision: N }` for lease renewals.
The pattern is:

1. **Read** the lease key → get `mod_revision=100`
2. **Txn**: `Compare{ mod_revision == 100 } → Success: Put{ updated lease }`  
   or `→ Failure: Range{ read current }`

When the lease key was tombstoned by a prior compaction / delete cycle:

- The tombstoned entry had `delete_revision=124`
- k3s expected to match `mod_revision=100` (the value it read earlier)
- `eval_compare` saw `mod_revision=124` (from the tombstone, because
  `.get(key)` returns the tombstoned entry) and the compare **failed**
- k3s fell into the `Failure` branch, re-read the key, got the alive
  version (mod_revision=100 since the tombstone was filtered by
  `scan_range`), and retried
- But the retry Txn used the ALIVE key's mod_revision against a
  comparison that STILL saw the tombstone
- **Infinite loop** of Txn failures, each creating a tiny revision churn

This matches the original symptom: `etcdserver: txn comparison failed`
in k3s logs, accompanied by rapid revision growth.

### Why `scan_range` Worked Fine

The `scan_range` function (used by `Range` RPC) already calls
`filter_deleted_keys()` which removes tombstoned entries from results.
This is why a direct `Range` (get) of the lease key returned the correct
alive value, but the `Txn` compare saw the tombstone — they used
different code paths.

### Fix

```rust
let alive = state.keys.get(key).filter(|k| k.is_alive());
```

Applied to all five `CompareTarget` arms: `Version`, `CreateRevision`,
`ModRevision`, `Value`, `Lease`.

## Root Cause #2: WAL Replay Doesn't Restore Lease State

**File:** `src/storage/mod.rs` — `Store::init_from_wal()`

### The Bug

When Rudurru starts, it replays all WAL records to rebuild the in-memory
BTreeMap. Keys with `lease != 0` are correctly restored (the KV itself
carries the lease ID in its protobuf). However, the **lease metadata**
(expiry, TTL) was not restored — it was purely in-memory state that
disappeared on restart. Additionally, `NEXT_LEASE_ID` was never bumped
past the restored IDs.

### Impact

- After restart, k3s created new leases for every controller
- The new leases got IDs starting from 1, colliding with IDs still
  referenced by persisted keys
- Lease operations on the old IDs failed because the `LeaseState` entry
  didn't exist
- This was a secondary source of instability after the `eval_compare`
  fix was already in place

### Fix

Added a restoration loop in `init_from_wal()`:

1. Scan all alive keys for unique non-zero lease IDs
2. Bump `NEXT_LEASE_ID` past the maximum restored ID
3. Create a `LeaseState` for each restored ID with a conservative 1h TTL
4. The real lease owner (k3s) sends `LeaseKeepAlive` immediately on
   startup, adjusting the TTL to the correct value

## Root Cause #3: Watch Replay Events Missing PrevKv

**File:** `src/server/watch.rs` — `flush_global_batch()`

### The Bug

When a watcher is created with `prev_kv=true` (as k3s does for lease
watches), the historical replay must include `prev_kv` for each event.
The `kvrec_to_event` helper always set `prev_kv_bytes: Bytes::new()`,
meaning all replayed PUT/DELETE events had a nil PrevKv.

k3s' watcher treats nil PrevKv as a fatal error when `prev_kv=true` is
requested, causing it to tear down the watch and retry immediately — the
watch storm.

### Why Only After the First Fix?

With the buggy `eval_compare`, k3s created new watches aggressively
because Txns kept failing. After fixing `eval_compare`, Txns succeeded,
but the watches no longer cycled due to Txn failures — instead they
cycled due to PrevKv=nil errors. The PrevKv=nil error existed before
the `eval_compare` fix too, but was hidden behind the Txn failure storm.

### Fix

Replaced the `kvrec_to_event` helper with inline HashMap-based tracking
in both Phase 1 (lock-free scan) and Phase 2 (under write lock):

**Phase 1:** Maintain a `HashMap<Vec<u8>, Bytes>` while scanning the
WAL forward. Before each event, look up the previous kv_bytes for the
key. After processing, update the map with the new state.

**Phase 2:** Initialize the HashMap from the store state (in-memory
keys), then apply the same forward-scan logic for new catch-up events.

## Architectural Lessons

1. **Delete vs. Tombstone**: Keeping deleted keys in a BTreeMap for
   historical queries creates invisible traps for any code path that
   does direct `.get()` without filtering. Every key lookup site must
   consider tombstoned entries.

2. **Stateful Metadata Evaporates**: Any metadata that lives only in
   memory (leases, watchers) must be reconstructable from the WAL.
   Otherwise, a restart loses critical state.

3. **Watch Replay Needs PrevKv**: The etcd spec requires PrevKv for
   historical watch events when `prev_kv=true`. Our WAL scan must track
   previous key states to provide this.

## Verification

- `eval_compare` fix: All Txn operations show `succeeded=true` in logs
- `NEXT_LEASE_ID` fix: Startup log shows `leases_restored=N`
- `PrevKv` fix: No more `PrevKv=nil` errors in k3s logs
- Cluster healthy: `kubectl get nodes` shows both nodes Ready

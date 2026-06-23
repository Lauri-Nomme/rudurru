# Known Limitation: `PrevKv=nil` on Non-Create Watch Events After WAL Compaction

**Date:** 2026-06-23
**Reported by:** k3s log `watcher.go:336` — observed twice during overnight monitoring
**Rudurru version:** `8926a23` (module split)
**k3s version:** v1.36.1+k3s1
**Cluster:** changwang (control-plane), precision (worker)

---

## Symptom

```
E0623 03:47:19.336  watcher.go:336] watch chan error:
  etcd event received with PrevKv=nil
  (key="/registry/events/mastodon/mastodon-streaming.18bb2f3ac7acf2d6",
   modRevision=4670127, type=PUT)
```

Two occurrences, same key, same modRevision, 2 minutes apart. Harmless — k3s terminates the watch and re-establishes it transparently. No user-facing impact.

---

## Root Cause

### Upstream: Kubernetes `parseEvent()` validation

Source: `kubernetes/kubernetes/staging/src/k8s.io/apiserver/pkg/storage/etcd3/event.go:44` (not k3s-specific; k3s embeds the same k8s.io/apiserver code).

```go
func parseEvent(e *clientv3.Event) (*event, error) {
    if !e.IsCreate() && e.PrevKv == nil {
        return nil, fmt.Errorf("etcd event received with PrevKv=nil ...")
    }
    // ...
}
```

- `e.IsCreate()` = `e.Kv.CreateRevision == e.Kv.ModRevision`
- The error fires when **a non-create event** (PUT update or DELETE) has `PrevKv == nil`

Per the etcd protocol, `PrevKv` **can legitimately be nil** when the previous KV has been compacted. The etcd proto comment states: *"If prev_kv is set, created watcher gets the previous KV before the event happens. If the previous KV is already compacted, nothing will be returned."* (etcd-io/etcd#10681)

Kubernetes is actively removing this dependency. PR #131862 (merged May 2025) adds the `WatchFromStorageWithoutPrevKV` feature gate to stop requesting PrevKV when the watch cache is enabled.

### Rudurru-specific trigger: WAL compaction + Phase 1 scan

**Two-stage problem:**

#### 1. WAL compaction drops historical records

Rudurru's `compact_wal()` (`src/storage/store.rs:998-1131`) snapshots the current key state into a compacted WAL file. The snapshot records are created with:

```rust
// store.rs:1020-1029 — compact_wal Phase A snapshot
let flags = if ks.lease != 0 { wal::HAS_LEASE } else { 0 };
recs.push(wal::KvWalRecord::new(
    flags,
    key,
    &ks.value,
    ks.create_revision as i64,   // original creation rev
    ks.mod_revision as i64,      // latest modification rev
    ks.version,
    ks.lease,
));
```

Key observations:
- `IS_CREATE` flag (`0x02`) is **not set** on snapshot records (only `HAS_LEASE` may be set)
- `create_revision` and `mod_revision` in the encoded `kv_bytes` retain their **original values** — if the key was created at rev 10 and last updated at rev 20, the snapshot record has `create_revision=10, mod_revision=20`
- The snapshot is **not a real event** — it's a state dump

#### 2. Phase 1 scan treats snapshot records as real events

When a watcher connects, `flush_global_batch_at()` (`src/server/watch.rs:324-569`) runs:

- **Phase 1** (no lock): scans WAL from offset 0, builds `prev_kv_map`, and **sends events** to the watcher for any record with `rev >= start_revision`
- **Phase 2** (under lock): continues from checkpoint, finishes the map and event replay

The Phase 1 scan code:

```rust
// watch.rs:460-497 — simplified
reader.scan_kv(0, |rec| {
    // ...
    let prev_kv = prev_kv_map.get(key).cloned().unwrap_or(Bytes::new());
    // insert/remove from map based on event type
    prev_kv_map.insert(key.to_vec(), Bytes::from(rec.kv_bytes.clone()));
    // send event to watchers with prev_kv as computed above
});
```

For a **snapshot record** (first appearance of the key after compaction):
- `prev_kv_map.get(key)` returns `None` → `prev_kv = Bytes::new()` (nil)
- The event's `kv_bytes` has `create_revision=10, mod_revision=20`
- k8s decodes this: `IsCreate = (10 != 20) = false`, `PrevKv = nil` → **ERROR**

**Before compaction, the WAL would contain the creation record** (with `create_revision=10, mod_revision=10`, making `IsCreate=true`), so the first appearance would be accepted. Compaction removes this record, leaving only the snapshot record with mismatched create/mod revisions.

### Why only 2 occurrences

The key was a Kubernetes Event (`mastodon-streaming.18bb2f3ac7acf2d6`). Events are:
1. Created (first PUT — works fine, `IsCreate=true`)
2. Heartbeat-updated periodically (subsequent PUTs — would hit the bug if a new watcher connects after compaction)

The two errors are the same watcher reconnecting after the first error and hitting the same compacted snapshot record again. After both watchers connect successfully and move past the snapshot record, subsequent events flow through `notify_watchers()` which computes `prev_kv_bytes` from the in-memory state (which is always correct).

---

## Root Cause Summary

| Layer | Issue |
|-------|-------|
| **k8s apiserver** | `parseEvent()` requires `PrevKv` for non-create events, but etcd protocol allows nil after compaction |
| **Rudurru compaction** | Snapshot records omit `IS_CREATE` flag and preserve `create_revision < mod_revision` in kv_bytes |
| **Rudurru Phase 1 scan** | Treats compacted snapshot records as real events, sending them with `prev_kv = nil` despite `IsCreate = false` |
| **etcd protocol** | Backfills `PrevKv` via a Range lookup at `ModRevision-1`; if compacted, leaves it nil. Rudurru doesn't do this backfill — it relies on `prev_kv_map` which has no entry for first-seen keys |

---

## Upstream Status

| Item | Status |
|------|--------|
| k8s `parseEvent()` check | PR #76675 (2019, merged) — added check for DELETE events with nil PrevKv |
| etcd compaction + nil PrevKv | etcd-io/etcd#10681 (2019) — acknowledged as surprising but intentional |
| k8s remove PrevKV dependency | PR #131862 (May 2025, merged) — `WatchFromStorageWithoutPrevKV` feature gate. K8s will no longer set `WithPrevKV()` on watches when watch cache is enabled. |
| Rudurru Phase 1 + compaction | **Untracked** — this is a Rudurru-specific manifestation |

Once k3s ships the k8s version with `WatchFromStorageWithoutPrevKV` enabled by default, this error will disappear regardless of the compaction behavior.

---

## Mitigation Options

### Option 1: Set `IS_CREATE` flag on compaction snapshot records (band-aid)

Set `IS_CREATE` on all snapshot records during compaction. Phase 1 currently ignores this flag, so we'd need to also teach Phase 1 to check it.

**Problem:** Even with the flag, the kv_bytes still has `create_revision < mod_revision`, so k8s would still see `IsCreate=false` and error. The flag is a Rudurru-internal marker and doesn't affect the protobuf-encoded watch event.

**Doesn't fix the issue.**

### Option 2: Don't send events for first-seen keys during Phase 1

When Phase 1 encounters a key not in `prev_kv_map`, suppress the event. The watcher would miss the state at that revision, but the next event (or a range sync) would provide it.

**Problem:** This changes the watch semantics — watchers expect to receive all events from their start_revision. A missing event could cause the watcher to miss a state transition.

### Option 3: Decode and rewrite kv_bytes for first-seen keys in Phase 1

When a key appears for the first time in Phase 1, decode its `kv_bytes`, set `create_revision = mod_revision`, re-encode, and use the modified bytes for the event. This makes `IsCreate = true` in k8s, which accepts `PrevKv = nil`.

```rust
// Pseudo-code for Phase 1 first-seen handling
if !prev_kv_map.contains_key(key) && !deleted {
    // First appearance: promote to "create" for event purposes
    let mut kv = mvccpb::KeyValue::decode(&rec.kv_bytes[..])?;
    kv.create_revision = kv.mod_revision;
    event.kv_bytes = Bytes::from(kv.encode_to_vec());
}
```

**Trade-off:** The watcher receives a synthetic `create_revision` that doesn't match the value stored in etcd. For most consumers this is invisible (k8s uses `create_revision` only for the `IsCreate()` gate; the actual resource version in the object metadata is independent). However, any client explicitly comparing `create_revision` across watch events and range responses would see a mismatch.

### Option 4: Backfill `PrevKv` via range lookup in Phase 1

When `prev_kv_map` doesn't have a key, perform a range query against the in-memory state (or WAL) to find the previous value. This matches what real etcd does (`watch.go:415-424`):

```go
if needPrevKV && !IsCreateEvent(evs[i]) {
    opt := mvcc.RangeOptions{Rev: evs[i].Kv.ModRevision - 1}
    r, err := sws.watchable.Range(context.TODO(), evs[i].Kv.Key, nil, opt)
    if err == nil && len(r.KVs) != 0 {
        events[i].PrevKv = &(r.KVs[0])
    }
}
```

**Trade-off:** Additional range lookup per event (latency + complexity). The in-memory state has the information, but Phase 1 runs without the store lock, so a concurrent modification could produce stale results.

### Option 5: Wait for upstream k8s fix (recommended)

PR #131862 (`WatchFromStorageWithoutPrevKV`) eliminates the dependency entirely. Once k8s/k3s ships this with the feature gate enabled by default, the error goes away regardless of Rudurru's compaction behavior.

**Current status:** PR merged May 2025. Feature gate `WatchFromStorageWithoutPrevKV` likely to graduate to beta/GA in a future k8s release. The error is harmless (watch reconnects transparently) and rare (only 2 occurrences in 14h on a live cluster).

### Recommendation

**Do nothing now.** The error is benign, rare, and will be resolved by upstream k8s evolution. If it becomes frequent, implement **Option 3** (promote first-seen keys to creates in Phase 1).

---

## Related Files

| File | Relevance |
|------|-----------|
| `src/storage/store.rs:998-1131` | `compact_wal()` — snapshot records omit `IS_CREATE` flag |
| `src/server/watch.rs:455-548` | Phase 1 & Phase 2 WAL scan — sends events with `prev_kv = prev_kv_map.get(key).unwrap_or_default()` |
| `src/storage/apply.rs:18-47` | `apply()` — computes `prev_kv_bytes` from in-memory state (always correct) |
| `src/storage/wal.rs:6-9` | WAL flags: `DELETED`, `IS_CREATE`, `HAS_LEASE` |
| `src/storage/state.rs:41-56` | `to_key_value()` — decodes `kv_bytes` for range responses |
| `staging/src/k8s.io/apiserver/pkg/storage/etcd3/event.go` (upstream) | `parseEvent()` — the `PrevKv=nil` check |
| `staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go` (upstream) | `startWatching()` — calls `parseEvent()`, terminates watch on error |
| `server/etcdserver/api/v3rpc/watch.go` (etcd upstream) | Etcd's backfill of `PrevKv` via Range at `ModRevision-1` |

## References

- [kubernetes/kubernetes#76624](https://github.com/kubernetes/kubernetes/issues/76624) — Original report: pod informers stuck due to nil PrevKv (2019)
- [kubernetes/kubernetes#76675](https://github.com/kubernetes/kubernetes/pull/76675) — Added the `parseEvent` check for DELETE events (2019)
- [etcd-io/etcd#10681](https://github.com/etcd-io/etcd/issues/10681) — "prevKV not being returned if the previous KV was compacted is surprising behavior" (2019)
- [kubernetes/kubernetes#115376](https://github.com/kubernetes/kubernetes/issues/115376) — Proposal to remove PrevKV for most objects (2022)
- [kubernetes/kubernetes#130939](https://github.com/kubernetes/kubernetes/issues/130939) — Removing dependence on PrevKV when watch cache is enabled (2024)
- [kubernetes/kubernetes#131862](https://github.com/kubernetes/kubernetes/pull/131862) — Apiserver watch from storage without PrevKV option (merged May 2025)
- [kubernetes/kubernetes#123072](https://github.com/kubernetes/kubernetes/issues/123072) — APIServer watchcache lost events correlated to PrevKv=nil

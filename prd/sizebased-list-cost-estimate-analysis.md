# `SizeBasedListCostEstimate` — Root Cause Analysis

## The Bug

`--feature-gates=SizeBasedListCostEstimate=true` (Beta, default-on since v1.34)
causes a perpetual stream of errors in every cacher's periodic cleanup:

```
E0706 01:56:57.123 123 stats.go:119] "Error getting keys"
  err="Too large resource version: xxx, current: yyy"
```

Rate: ~103/minute across all resource cachers on an idle cluster (2183 keys).
Upstream issue [#134343](https://github.com/kubernetes/kubernetes/issues/134343)
— closed "not planned".

---

## How `GetCurrentResourceVersion` Works

The error chain is:

```
cleanKeysIfNeeded (stats.go:112)
  → cacher.getKeys (cacher.go:1356)
    → storage.GetCurrentResourceVersion (store.go:704)
      → s.GetList(ctx, s.resourcePrefix, storage.ListOptions{Predicate: {Limit:1}}, emptyList)
        → etcd Range RPC (prefix=.../pods/, keys_only, limit=1)
        → response header.revision → **global key-value store revision**
    → returns global revision
  → watchCache.WaitUntilFreshAndGetKeys(ctx, globalRev)
    → waitUntilFreshAndBlock(ctx, globalRev)
      → for w.resourceVersion < globalRev { ... timeout 3s }
```

### `GetCurrentResourceVersion` (store.go:704-733)

```go
func (s *store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
    emptyList := s.newListFunc()
    pred := storage.SelectionPredicate{
        Label: labels.Everything(),
        Field: fields.Everything(),
        Limit: 1,
    }
    err := s.GetList(ctx, s.resourcePrefix, storage.ListOptions{Predicate: pred}, emptyList)
    // reads listObj.metadata.resourceVersion from the response
    // response header.revision → ResourceVersion → return
}
```

This issues a fresh `List(Range, limit=1)` against the **storage backend** (etcd/rudurru).
The response header's `revision` is the **global key-value store revision** — always.

### The etcd Protocol Specification

From `rudurru/proto/rpc.proto:372-384`:

```protobuf
message ResponseHeader {
  // revision is the key-value store revision when the request was applied.
  int64 revision = 3;
}
```

The comment says **"key-value store revision"** — not "prefix-scoped revision".
The revision is a single, global, monotonically increasing counter over the **entire**
store. There is no per-prefix revision in the etcd v3 protocol.

Both real etcd and rudurru implement it identically:

| Backend | Implementation |
|---------|---------------|
| **rudurru** | `state.header()` → `current_revision()` → `NEXT_REV.load() - 1` (state.rs:124-131) |
| **real etcd** | `s.currentRev` — store-level counter (mvcc/kvstore.go) |

---

## The Watch Cache's `resourceVersion`

`w.resourceVersion` is set from **three** sources:

### 1. Normal watch events — `processEvent()` (watch_cache.go:327)

```
reflector receives watch event from etcd/rudurru
  → event.Object.metadata.resourceVersion = kv.ModRevision (PER-OBJECT revision)
  → watchCache.Add(obj) → objectToVersionedRuntimeObject(obj)
    → versioner.ObjectResourceVersion(object) → kv.ModRevision
  → processEvent(event, resourceVersion, ...)
    → w.resourceVersion = resourceVersion  ← PER-OBJECT mod_revision
```

Each per-object `ModRevision` is the MVCC version of that **specific key** in the
store. Because etcd increments the global revision on every write, a per-object
`ModRevision` is also a valid point in the global timeline — but it's sparse.

### 2. Bookmark/progress events — `UpdateResourceVersion()` (watch_cache.go:413)

```
reflector receives Bookmark watch event
  → bookmark.revision = watch response header.revision (GLOBAL)
  → setLastSyncResourceVersion(rv, ...) → rvu.UpdateResourceVersion(rv)
    → w.resourceVersion = rv  ← GLOBAL revision from watch header
```

### 3. Initial list / re-list — `Replace()` (watch_cache.go:786)

```
reflector performs initial List to populate cache
  → list metadata.resourceVersion = etcd header.revision (GLOBAL)
  → store.Replace(toReplace, version)
    → w.resourceVersion = version  ← GLOBAL revision from list header
```

---

## The Mismatch

The watch cache's `w.resourceVersion` is set from either:
- **Per-object `ModRevision`** (from prefix-filtered watch events) — sparse, only
  advances on writes to keys under this prefix
- **Bookmark/list header revisions** (global) — advances on every write to any key

`GetCurrentResourceVersion()` returns the **fresh current global revision** —
the total number of mutations across ALL keys at the instant of the List call.

Since other resources (services, endpoints, configmaps, leases, etc.) are
constantly being written, the global revision races ahead of any single
prefix's watch cache:

| Time | Global Store Rev | Pod Watch Cache Rev | What Happens |
|------|-----------------|-------------------|--------------|
| T0 | 1000 | 1000 | Initial list, `Replace` sets both to 1000 |
| T1 | 1001 | 1001 | Pod write → `processEvent` sets cache to 1001 |
| T2 | 1002 | — | Service write → global advances, cache stays at 1001 |
| T3 | 1005 | — | Endpoint + ConfigMap writes → cache stays at 1001 |
| **T4** | **1005** | **1001** | `cacher.getKeys`: `GetCurrentResourceVersion()` → **1005**, then `WaitUntilFreshAndGetKeys(ctx, 1005)` → `w.resourceVersion (1001) < 1005`, waits 3s |

---

## Bookmarks — Do They Sync the Revision?

### Yes — the full chain works end-to-end

The cacher's `listerWatcher.Watch()` sets `ProgressNotify: true` (lister_watcher.go:115),
which causes:

1. **Kubernetes** creates the etcd watch with `clientv3.WithProgressNotify()` (watcher.go:386)
2. **Rudurru** stores `progress_notify: true` in the WatchRegistration (state.rs:87)
   and the per-watch goroutine sends periodic empty responses every **300 seconds**
   (watch.rs:202-222)
3. **Kubernetes `progressRequester`** (cacher.go:437) also sends explicit
   `WatchProgressRequest` messages every **100ms** while any goroutine is waiting
   (progress/watch_progress.go:36)
4. **Rudurru** handles `WatchProgressRequest` immediately (watch.rs:249-263):
   ```rust
   ProgressRequest(_) => {
       let resp = etcdserverpb::WatchResponse {
           header: Some(make_header(current_revision() as i64)),
           watch_id: 0, events: vec![], fragment: false, ...
       };
   }
   ```
5. **etcd3 watcher** receives it (watcher.go:403-406):
   ```go
   if wres.IsProgressNotify() {
       wc.queueEvent(progressNotifyEvent(wres.Header.GetRevision()))
   }
   ```
6. **reflector** (reflector.go:1058-1075):
   ```go
   case watch.Bookmark:
       setLastSyncResourceVersion(resourceVersion, true)
       // → rvu.UpdateResourceVersion(rv) → w.resourceVersion = rv
   ```
7. `w.cond.Broadcast()` wakes `waitUntilFreshAndBlock`, which checks
   `w.resourceVersion >= requestedRev` → succeeds.

### Why It Still Fails

The chain works in theory. The error rate of ~103/min on a near-idle cluster
means it's failing in practice. Likely causes:

1. **Race: global revision advances between List and progress response**
   - `GetCurrentResourceVersion()` returns revision R1
   - Between List response and progress response arrival, more writes
     advance global to R2 > R1
   - This is fine — bookmark carries R2, and R2 >= R1

2. **Watch stream not yet established**
   - If the etcd client's watch stream is being re-established
     (connection drop, leader change), progress requests fail silently
     or go to the wrong stream

3. **etcd v3 Go client dispatch issues**
   - `RequestProgress` sends on the shared watch stream; responses
     are dispatched to ALL watchers. If the dispatch misses the
     intended watcher (e.g., race in watcher registration), the
     bookmark is lost

4. **Multiple cachers, each with its own error**
   - With ~35 cachers (one per resource type), each timing out every
     ~60s cleanup cycle, that's 35/min expected. The observed 103/min
     with 2183 keys matches this scale

5. **Rudurru per-watch timer (300s) is irrelevant**
   - The explicit `WatchProgressRequest` path (100ms interval) should
     cover the gap. The 300s timer is a fallback that never fires in
     the relevant window

---

## The `cleanKeysIfNeeded` Code Path (stats.go:112-125)

```go
func (sc *resourceSizeEstimator) cleanKeysIfNeeded(ctx context.Context) {
    lastKeyCleanup := sc.lastKeyCleanup.Load()
    if lastKeyCleanup != nil && time.Since(*lastKeyCleanup) < sizerRefreshInterval {
        return  // only runs every ~60s
    }
    keys, err := sc.getKeys(ctx)            // ← triggers the error
    if err != nil {
        klog.InfoS("Error getting keys", "err", err)
        return
    }
    sc.keysLock.Lock()
    defer sc.keysLock.Unlock()
    sc.cleanKeys(keys)
}
```

Each cacher has its own `resourceSizeEstimator`. Every ~60s (with jitter),
`cleanKeysIfNeeded` calls `cacher.getKeys()`, which:
1. Calls `GetCurrentResourceVersion()` → fresh List → returns **current global rev**
2. Calls `WaitUntilFreshAndGetKeys(ctx, currentGlobalRev)` → waits 3s
3. If the progress notification doesn't arrive in time → `TooLargeResourceVersionError`
4. Logs "Error getting keys" and returns

---

## The Core Issue

`GetCurrentResourceVersion()` is the **wrong tool** for this job:

- It's designed to get the **current global store revision** — which is correct
  per the etcd protocol spec
- But it's used to synchronize a **prefix-scoped watch cache** — which has no
  concept of a global revision
- The watch cache's revision is tied to per-object `ModRevision` values from
  its prefix-filtered watch stream
- These two revisions are fundamentally incomparable

The correct fix would be to use the watch cache's own `resourceVersion` directly
instead of fetching a fresh global revision. Or, equivalently, to use a
storage-backend-agnostic mechanism that doesn't require the watch cache to catch
up to the global counter.

---

## The Missing Link: Report Version Blacklist

Even though rudurru implements `RequestWatchProgress` correctly (immediate
response via `ProgressRequest` handler in watch.rs:249-263, plus periodic
progress notifications every 300s), **Kubernetes never sends progress
requests**. The root cause: rudurru reports etcd version `"3.5.0"` in its
`StatusResponse` (maintenance.rs:49).

Kubernetes' `endpointSupportsRequestWatchProgress` (feature_support_checker.go:157-172)
explicitly blacklists versions in the range `[3.5.0, 3.5.13)` due to
known etcd bugs:

```go
v3_5_0  = version.MustParseSemantic("3.5.0")
v3_5_13 = version.MustParseSemantic("3.5.13")

if ver.AtLeast(v3_5_0) && ver.LessThan(v3_5_13) {
    return false, nil  // RequestWatchProgress NOT supported!
}
```

`"3.5.0"` matches this blacklist, so `Supports(RequestWatchProgress)` returns
`false`, which makes `ConsistentReadSupported()` (delegator/interface.go:109-112)
return `false`, which means:

```go
// watch_cache.go:541
if delegator.ConsistentReadSupported() && w.notFresh(resourceVersion) {
    w.waitingUntilFresh.Add()  // NEVER REACHED
    // ...
}
```

Without `waitingUntilFresh.Add()`, the `ConditionalProgressRequester` is never
activated, no `WatchProgressRequest` is ever sent, and the only bookmark events
come from the 300s periodic timer — far too slow for the 3s `blockTimeout`.

**Fix:** Change rudurru's reported version to `"3.5.14"` (or anything ≥ 3.5.13)
in `maintenance.rs:49`. This is safe because rudurru implements
`RequestWatchProgress` correctly without the bugs that plagued real etcd in
those versions.

---

## The Second Bug: Broadcast vs Unicast

Even after fixing the version check (so `RequestWatchProgress` is enabled),
progress request responses still weren't delivered. The etcd client library's
`dispatchEvent` (watch.go:714-736) distinguishes two modes:

```go
// InvalidWatchID = -1
if wr.IsProgressNotify() && pbresp.WatchId == InvalidWatchID {
    return w.broadcastResponse(wr)   // send to ALL watchers
}
return w.unicastResponse(wr, pbresp.WatchId)  // send to ONE watcher
```

`broadcastResponse` (watch.go:739-747) sends the response to every active
watcher's channel. `unicastResponse` (watch.go:750-761) looks up a specific
watcher by ID; if not found, it returns `false` and the response is dropped.

Rudurru's `ProgressRequest` handler sent `watch_id: 0` (`AutoWatchID` in etcd
terminology — value 0, meaning "auto-assign"). But no watcher has ID 0
(rudurru assigns IDs starting from 1 at watch.rs:14). So `unicastResponse`
always returned `false`, and every progress notification was silently dropped.

The periodic 300s timer (watch.rs:202-225) worked because it used the correct
per-watcher `watch_id`, so unicast succeeded — but 300s is far too slow for the
3s `blockTimeout`.

**Fix: `watch_id: -1`** (InvalidWatchID) so the client library broadcasts the
response to all watchers:

```rust
// watch.rs:257
watch_id: -1,
```

This matches the real etcd behavior: a `WatchProgressRequest` (no target
watch_id) should notify every active watcher on the stream.

---

## What We Changed (Summary)

### Quick fix (reversible via config):
```yaml
kube-apiserver-arg:
  - "--feature-gates=SizeBasedListCostEstimate=false"
```
Disables the `resourceSizeEstimator` entirely — no estimate, no error.

### Proper fix (rudurru side — two changes):
```rust
// maintenance.rs:49  — pass the etcd version blacklist check
version: "3.5.14".into(),

// watch.rs:257  — broadcast progress responses to all watchers
watch_id: -1,
```

Upstream status: the bug [#134343](https://github.com/kubernetes/kubernetes/issues/134343)
was closed as "not planned". The feature gate is still configurable in K3s v1.36
(the Beta → GA transition hasn't locked it in this fork).

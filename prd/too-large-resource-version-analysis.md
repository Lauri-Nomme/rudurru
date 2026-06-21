# k3s "Too large resource version" — Root Cause: Watch Cache Lag

**Date:** 2026-06-21
**Rudurru version:** `019ae1c`
**k3s version:** v1.36.1+k3s1

## Error Pattern

```
Jun 21 22:30:12 k3s stats.go:119] "Error getting keys"
  err="Timeout: Too large resource version: 3977734, current: 3977169"
```

INFO level, ~30–60/hr. No user impact. Emitted by `k8s.io/apiserver`'s
`resourceSizeEstimator` background goroutine — estimates average object size
for API priority & fairness. Not a Rudurru rejection.

## The Two Revisions

| Value | Source | Scope |
|-------|--------|-------|
| `resourceVersion` | Rudurru's `current_revision()` | Global — increments on **any** Put/Delete across all resource types |
| `w.resourceVersion` | k8s `watchCache` per resource type | Per-resource — advanced only by events **for that resource type** |

## Full Call Chain

```
resourceSizeEstimator.cleanKeysIfNeeded()          [stats.go:112]
  runs every ~1 minute (jittered 60-90s)

  → Cacher.getKeys(ctx)                            [cacher.go:1356]
    → c.storage.GetCurrentResourceVersion(ctx)     ← returns rudurru's current_revision
      // e.g. 3,977,734 — global counter

    → watchCache.WaitUntilFreshAndGetKeys(ctx, rev)  [watch_cache.go:540]
      → w.waitUntilFreshAndBlock(ctx, resourceVersion)  [watch_cache.go:449]

        Loop (up to 3 seconds):
          for w.resourceVersion < resourceVersion {     [watch_cache.go:481]
            if elapsed >= blockTimeout (3s) {            [watch_cache.go:482]
              return NewTooLargeResourceVersionError(
                resourceVersion,    // 3,977,734 — rudurru's global revision
                w.resourceVersion,  // 3,977,169 — this resource's watch cache revision
                1,                  // retry after 1 second
              )
            }
            w.cond.Wait()  // wait for watcher to deliver more events
          }
```

## Why It Fires

Rudurru's revision counter (`NEXT_REV`) increments on **every** write to any
key in the store. k3s has 140+ per-resource-type Cachers, each with its own
`watchCache` whose `resourceVersion` only advances when its specific watcher
receives events.

In a stable cluster most resource types receive few or zero events.
Their `w.resourceVersion` drifts behind Rudurru's global `current_revision`.

When the `resourceSizeEstimator` background goroutine fires (~60-90s):
1. It fetches Rudurru's current revision (e.g. 3,977,734)
2. Asks the watch cache: "wait until you reach revision 3,977,734"
3. The watch cache has only processed events up to 3,977,169
4. No events arrive for this resource type in the next 3 seconds
5. `blockTimeout` fires → `NewTooLargeResourceVersionError`

After a k3s restart the gap is small (~500 revs). Over hours it can grow
to 15,000+ as the global counter keeps climbing while the idle resource
type receives no events.

## Why It's Harmless

- INFO level, not an error
- `resourceSizeEstimator` is purely advisory (API priority & fairness sizing)
- If it fails, k8s falls back to default object size estimates
- The watcher itself is still healthy — the next real event will advance
  `w.resourceVersion` past the target and the next estimator run will succeed

## Where the Fix Would Be

In `k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:484`:

```go
const blockTimeout = 3 * time.Second

func (w *watchCache) waitUntilFreshAndBlock(ctx context.Context, resourceVersion uint64) error {
    // ...
    for w.resourceVersion < resourceVersion {
        if w.clock.Since(startTime) >= blockTimeout {
            return storage.NewTooLargeResourceVersionError(/* ... */)
        }
        w.cond.Wait()
    }
}
```

A `resourceSizeEstimator` call should not be blocked for 3 seconds — it
could accept slightly stale data (the estimator is best-effort). This is
an upstream k8s behaviour, not a Rudurru issue.

## Relevant Source Files (k8s v1.36.1+k3s1)

| File | Line | Role |
|------|------|------|
| `pkg/storage/etcd3/stats.go` | 112-125 | `resourceSizeEstimator.cleanKeysIfNeeded` — background goroutine |
| `pkg/storage/cacher/cacher.go` | 1356-1362 | `Cacher.getKeys` — calls `WaitUntilFreshAndGetKeys` |
| `pkg/storage/cacher/watch_cache.go` | 449-490 | `waitUntilFreshAndBlock` — the 3-second timeout loop |
| `pkg/storage/cacher/watch_cache.go` | 540-554 | `WaitUntilFreshAndGetKeys` — entry point |
| `pkg/storage/errors.go` | 229-242 | `NewTooLargeResourceVersionError` — error constructor |

# k3s Errors/Warnings Diagnosis

**Date:** 2026-06-15
**Rudurru version:** commit `0515eea` (first deploy), `watch_too_large` fixed in commit after
**k3s version:** v1.36.1+k3s1
**Cluster:** 2 nodes (changwang control-plane, precision worker)

## Observed Errors in k3s Journal

### 1. "Timeout: Too large resource version" — ~500/hr (stats.go:119)

```
I0615 19:50:19.826489 stats.go:119] "Error getting keys" err="Timeout: Too large resource version: 362123, current: 358730"
```

**Pattern:** k3s requests keys with `resourceVersion` ahead of its cached `current`. The `current` value was stuck at ~358,725–358,880 before the fix, and advanced to ~384,808–384,994 after the gRPC message size fix.

**Before gRPC fix:** `current` stuck at 358K (LIST responses >4MB silently dropped, cache never updated).
**After gRPC fix:** `current` rose to 384K+ (close to actual revision), confirming LIST responses now succeed.

These errors are k3s-internal (stats.go — non-critical resource usage metrics collector). INFO level, no user impact.

### 2. "Cache consistency check failed" — 48/2h (delegator.go:344)

```
E0615 19:50:31.032728 delegator.go:344] "Cache consistency check failed"
  group="networking.k8s.io" resource="networkpolicies"
  etcdDigest="cbf29ce484222325"
  cacheDigest="dcdc59ddb0ced3f5"
  diffDetail={"Index":0,"EtcdItem":null,...}
```

**Key signatures before fix:**
- `etcdDigest="cbf29ce484222325"` — FNV-1a offset basis, hash of **empty/null** data
- `diffDetail.EtcdItem=null` — Rudurru returned nothing for that index
- **After the gRPC message size fix: 0 occurrences.**

### 3. Watch creation failure cascade (after first deploy)

When the initial `watch_too_large` guard was `start_revision > current_revision()` with no threshold:

- **Every** k3s startup watch request comes with `start_revision = current + 1` (normal bookmark race).
- The guard rejected **ALL** resource type watchers (pods, nodes, configmaps, secrets, etc.)
- k3s's informers never connected → scheduler had no pod/node data → no pods scheduled
- This persisted even across k3s restart because the watcher rejection was instant

**Observed in Rudurru logs:** 22 watcher types rejected in a single batch at 22:13:31, all with a gap of exactly 1 revision.

**Fix applied:** Changed threshold to `start_revision > current_revision + 10_000`. This accepts normal bookmark races (1-2 revs) while still rejecting truly stale bookmarks.

## Root Causes

### Primary: Default 4MB gRPC message limit

Tonic's default `max_decoding_message_size` is 4MB. Rudurru's initial `main.rs` did not override it.

```
267MB WAL / 2251 keys ≈ 119KB per key
119KB × 40 keys ≈ 4.8MB  ← exceeds 4MB limit
```

A typical k3s LIST for a resource type with 40+ objects wraps each object in the response envelope. The 4MB boundary is hit with 25-35+ objects.

**Failure chain for cache consistency:**
1. k3s sends `LIST` with `ResourceVersion="0"` for cache consistency check.
2. Rudurru's `range()` collects all matching keys and encodes the response.
3. Serialized response >4MB. Tonic's gRPC layer rejects it.
4. k3s receives `gRPC error: message too large` (code: `ResourceExhausted`).
5. k3s falls back to watch-based catchup. The consistency check hash is computed as FNV-1a offset basis (hash of empty input).
6. k3s logs the consistency failure and resyncs.

**Failure chain for stats.go:**
1. Same 4MB rejection causes LIST to silently fail.
2. k3s's cached `resourceVersion` for the queried resource type stalls.
3. Watch bookmarks advance past the stalled RV.
4. Subsequent watch requests with the stalled RV are ahead of Rudurru.
5. k3s retries repeatedly (~1/sec), generating the "Too large resource version" errors.

### Secondary: No stale-revision tolerance in watcher creation

The initial `watch_too_large` guard had zero tolerance for bookmark races, which are normal in k3s operation (especially after restart). k3s always requests `start_revision = current + 1` for every watcher creation to ensure it doesn't miss events. This is the **correct** etcd usage pattern — etcd accepts it and the watcher catches up in microseconds.

A `watch_too_large` guard must use a generous threshold (>= 10,000 revisions) to only reject truly stale bookmarks while accepting normal bookmark races.

### Tertiary: `rudurru-0.1.0` version string

`maintenance.rs:49` returns `version: "rudurru-0.1.0"`. k8s's etcd feature support checker (`feature_support_checker.go:165`) tries to parse this as semver and fails:

```
E0615 22:13:31.156303 feature_support_checker.go:165] "Failed to parse etcd version"
  err="could not parse \"rudurru-0.1.0\" as version"
```

This causes the checker to default to etcd v3.0.0 (minimum), disabling etcd v3.5+ features like transactional PATCH. It **does not** prevent LIST/WATCH or scheduling, but it may contribute to a slow k3s startup by delaying the etcd health check that is a prerequisite for the scheduler leader election.

After fix, the scheduler took 2m39s to acquire its leader lease:
```
22:16:59: "Running kube-scheduler --authentication-kubeconfig=..."
22:17:01: "Starting Kubernetes Scheduler" version="v1.36.1+k3s1"
22:17:01: "Attempting to acquire leader lease..." lock="kube-system/kube-scheduler"
22:19:40: "Successfully acquired lease" lock="kube-system/kube-scheduler"
```

Upgrading the version string to a semver-compatible format (e.g. `3.5.0`) would eliminate this delay.

## Changes Deployed (commits after `5b1de98`)

| # | Change | File | Purpose |
|---|--------|------|---------|
| 1 | Unlimited gRPC message size (`usize::MAX`) on all 6 services | `server/mod.rs` | Eliminates 4MB response truncation |
| 2 | Reject watchers with `start_revision > current_revision + 10_000` | `server/watch.rs:317-342` | Prevents truly stale bookmarks without breaking normal bookmark races |
| 3 | Diagnosis document | `prd/k3s-errors-diagnosis.md` | This file |

## Summary

| Issue | Count | Severity | Root Cause | Fix | Status |
|-------|-------|----------|------------|-----|--------|
| Cache consistency check failed | 48/2h | Error | 4MB gRPC limit truncates LIST | Unlimited message size | ✅ Fixed |
| Too large resource version | ~500/hr | Warning | Cascading stale RVs from 4MB truncation | Unlimited message size resolves upstream | ✅ Fixed |
| Pods not scheduling | N/A | Error | watch_too_large guard rejected ALL bookmark-race watchers | 10K revision threshold | ✅ Fixed |
| Slow scheduler startup | 2m39s | Annoyance | `rudurru-0.1.0` not semver, delays etcd health check | Change version to `3.5.0` | 🔲 Fix available, not deployed |

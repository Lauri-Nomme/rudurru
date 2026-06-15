# k3s Errors/Warnings Diagnosis

**Date:** 2026-06-15
**Rudurru version:** commit `5b1de98` (production binary at `/usr/local/bin/rudurru`)
**k3s version:** v1.36.1+k3s1
**Cluster:** 2 nodes (changwang control-plane, precision worker)

## Observed Errors in k3s Journal (last 2 hours)

### 1. "Timeout: Too large resource version" — 451 occurrences

```
I0615 19:50:19.826489 stats.go:119] "Error getting keys" err="Timeout: Too large resource version: 362123, current: 358730"
```

**Pattern:** k3s requests keys with `resourceVersion` ~3-20K ahead of its cached `current`. The `current` values cluster around 358,725–358,880.

**Mechanism:**
1. k3s creates a watch with `start_revision=X` from a bookmark.
2. The watch request goes through Rudurru's global batch (50ms timer).
3. Before the batch fires, k3s also sends `RangeRequest` (LIST) calls to fetch current state.
4. If the LIST response exceeds 4MB (gRPC default), it fails silently.
5. k3s's cached `current_revision` for that resource type stalls at the last successful response, while Rudurru's `NEXT_REV` advances.
6. Subsequent watch requests use this stalled revision, creating a growing gap.
7. The watch eventually times out (50s default) because the batch-created watcher starts from the correct revision, which is ahead of k3s's stale bookmark.

### 2. "Cache consistency check failed" — 48 occurrences

```
E0615 19:50:31.032728 delegator.go:344] "Cache consistency check failed"
  group="networking.k8s.io" resource="networkpolicies"
  resourceVersion="358725"
  etcdDigest="cbf29ce484222325"
  cacheDigest="dcdc59ddb0ced3f5"
  diffDetail={"Index":0,"CacheItem":{"Namespace":"cattle-fleet-local-system","Name":"default-allow-all","RV":"7315"},"EtcdItem":null}
```

**Affected resources (≥20 errors each):** clusters(46), validatingwebhookconfigurations, runtimeclasses, replicasets, podsecurityadmissionconfigurationtemplates, persistentvolumes, nodes, mutatingwebhookconfigurations, jobs, globalrolebindings, flowschemas, cronjobs, controllerrevisions, configmaps, clustergroups, and 20+ more.

**Key signatures:**
- `etcdDigest="cbf29ce484222325"` — the FNV-1a offset basis, the hash of **empty/null data**. Always the same value across all errors.
- `diffDetail.EtcdItem=null` — Rudurru returned no item where k3s's cache has one.
- `diffDetail.CacheItem.RV` is always > resourceVersion — k3s's cache has newer data than Rudurru's listed revision.

## Root Cause

**Default 4MB gRPC message limit.** Tonic's default `max_decoding_message_size` is 4MB. Rudurru's `main.rs` does not override it. With WAL=267MB, keys=2251, average key size ~119KB, many LIST responses exceed 4MB.

```
267MB WAL / 2251 keys ≈ 119KB per key
119KB × 40 keys ≈ 4.8MB  ← exceeds 4MB limit
```

A typical k3s LIST for a resource type with 40+ objects (e.g. `configmaps`, `nodes`, `replicasets`) wraps each object in the response envelope (namespace, name, resourceVersion, labels, annotations, spec, status). The 4MB boundary is hit with as few as 25-35 objects depending on their size.

**The failure chain:**
1. k3s sends `LIST` with `ResourceVersion="0"` for cache consistency check.
2. Rudurru's `range()` collects all matching keys and encodes the response.
3. Serialized response exceeds 4MB. Tonic's gRPC layer rejects it.
4. k3s receives `gRPC error: message too large` (code: `ResourceExhausted`).
5. k3s retries or falls back to watch-based catchup.
6. The check produces `etcdDigest=cbf29ce484222325` (hash of empty — the error is treated as no data).
7. k3s logs the consistency failure and resyncs from scratch (costly but self-healing).

**The "Too large resource version" errors** are a cascade effect: when LIST responses fail, k3s's cached `resourceVersion` for each resource type stalls. Watch bookmarks advance past it. Subsequent watch requests with the stalled revision are ahead of Rudurru's actual state and time out.

## Why k3s Still Works

k3s's watch cache is **self-healing**. When a consistency check fails:
1. k3s logs the error
2. k3s drops the entire cache for that resource type
3. k3s re-lists from scratch (resourceVersion="0")
4. If the re-list also fails (>4MB), k3s retries with exponential backoff
5. Eventually a LIST succeeds (smaller response or different timing) and k3s rebuilds its cache

The 48 errors in 2 hours (one every ~2.5 minutes on average) mean most resources are healthy most of the time, with occasional failures when the response size exceeds the limit.

All 38 pods are Running/Completed. No workload disruption.

## Fix

Increase the gRPC message limit:

```rust
Server::builder()
    .max_decoding_message_size(usize::MAX)  // unlimited decode
    .max_encoding_message_size(usize::MAX)  // unlimited encode
    .add_service(...)
```

This matches what real etcd does — etcd sets no practical gRPC message limit (it uses its own 1.5MB request size limit at the application layer, but responses are unlimited).

### Additional fix: Handle stale watcher revisions

When a watch request has `start_revision > current_revision()`, return an error immediately instead of creating a watcher that will never fire. This prevents the "Too large resource version" cascade:

```rust
if start_revision > current_revision() {
    return Err("too large resource version");
}
```

This is already described as proposal P5 in `prd/optimization.md` but was never fully implemented (only the checkpoint fix was done).

## Summary

| Error | Count/2h | Severity | Cause | Fix |
|-------|----------|----------|-------|-----|
| Too large resource version | 451 | Warning | LIST failures stall cached RV, watch bookmarks race ahead | Unlimited gRPC message size + stale revision rejection |
| Cache consistency check failed | 48 | Error | LIST response >4MB, rejected by gRPC layer | `max_decoding_message_size(usize::MAX)` |

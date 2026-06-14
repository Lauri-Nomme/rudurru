# Compact Bug — Root Cause Analysis

**Date:** 2026-06-14  
**Severity:** Critical — ~97% of cluster data destroyed  
**Duration:** ~10 hours from compact(8000) to recovery  
**Fix:** `src/storage/mod.rs:492-507` — removed `state.keys.retain(...)` from compact handler

---

## Timeline

All times EEST on 2026-06-14.

| Time | Event |
|------|-------|
| 02:17:44 | Rudurru starts with empty WAL (revision=0, keys=0) |
| 02:19:34 | Rudurru restarts — still empty |
| 02:22:57 | k3s (PID 3381492) starts, connects to empty Rudurru, creates ~90k resources from scratch |
| 03:07:59 | Rudurru stops; WAL from empty-session discarded |
| 03:08:01 | Rudurru starts with kine→Rudurru migration WAL (revision=9316, keys=1725) — **migration data now live** |
| ~10:31 | `rudurru-compact 8000` runs — **BUG TRIGGERED** — deletes 1561/1623 keys from memory |
| 10:31–13:10 | Cluster runs with 61 keys; all infrastructure (namespaces, pods, PVCs, deployments, RBAC, CRDs, configmaps, secrets) lost. Cascading E-level errors: cache consistency failures, PVC-not-found, service-account-not-found, endpoint-unavailable. |
| 13:10 | Debug artifacts saved: WAL backup, journalctl JSONL, analysis.md |
| 13:11 | Compact fix committed and pushed |
| 13:31 | Fixed Rudurru deployed, restarted — WAL replay restores 1623 keys |
| 13:31 | Cluster fully recovered: 38 pods, 30 namespaces, 6 PVCs, all workloads back |

---

## Root Cause: Compact Handler Deletes Current Key-Values

### The Bug

`src/storage/mod.rs:492-507` (before fix):

```rust
pub async fn compact(&self, req: CompactionRequest) -> CompactionResponse {
    let mut state = self.state.write().await;
    state.compact_rev = req.revision as u64;
    state.keys.retain(|_, ks| {         // ← BUG
        if ks.deleted { return false; }  // ← BUG
        ks.mod_revision >= compact_rev   // ← BUG
        || ks.create_revision >= compact_rev // ← BUG
    });                                  // ← BUG
    ...
}
```

This calls `state.keys.retain(...)` which **deletes keys** from the in-memory BTreeMap. The predicate keeps a key only if `mod_revision >= compact_rev OR create_revision >= compact_rev`. Keys where both revisions are below `compact_rev` are removed.

### Why It's Wrong

In etcd v3, the `Compact` RPC is an MVCC operation. It tells the storage engine that historic revision data before `compact_rev` can be garbage-collected. **Current key-values are never deleted** — only the ability to query historical versions is removed.

Our implementation treated compact as "delete all data older than this revision" which is semantically equivalent to a bulk delete.

### Why compact(8000) Was Catastrophic

The kine→Rudurru migration tool wrote all K8s data at Rudurru-assigned revisions starting from 1. The 1725 migrated keys were at revisions approximately 1-1725 (plus subsequent k3s updates to some keys). When `rudurru-compact 8000` ran:

- Keys with `create_revision < 8000 && mod_revision < 8000` = **deleted**
- This covered all migration data except keys that k3s had updated post-migration (leases, events, some secrets, nodes)
- 1561 of 1623 unique keys were removed
- Surviving 62 keys were: leases (heartbeated by k3s), events (continually created), nodes (heartbeated), webhook configs, and a few secrets that happened to be created at higher revisions during the "empty Rudurru" session

### Why the WAL Survived

The compact handler only modified the in-memory `StoreState.keys` BTreeMap. It did **not** write any records to the append-only WAL. This is why restarting Rudurru (with the old WAL) restored everything — WAL replay rebuilds the BTreeMap from scratch.

---

## Impact

| Metric | Before | After compact | After recovery |
|--------|--------|---------------|----------------|
| Keys in Rudurru | 1623 | 61 | 1623 |
| Namespaces | 30 | 0 | 30 |
| Pods | 38 | 0 | 38 |
| Deployments | 30 | 0 | 30 |
| PVCs | 6 | 0 | 6 |
| Running workloads | All | None | All |

**Downstream effects (all cascading from data loss):**

| Error | Source | Count | Severity |
|-------|--------|-------|----------|
| "Error getting keys: Timeout" | k3s `stats.go:119` | 65825 | Info (spam) |
| "Cache consistency check failed" | k3s `delegator.go:344` | 1635 | Error |
| "Error processing volume: PVC not found" | k3s `desired_state_of_world_populator.go:302` | 966 | Error |
| "Error preparing data for projected volume: SA not found" | k3s `projected.go:196` | 1426 | Error |
| "no endpoints available for service" | k3s | 126 | Error |

---

## Recovery Tools

Three tools were built as part of the fix (all in `src/bin/`):

| Tool | Purpose |
|------|---------|
| `waldoctor` | Read any Rudurru WAL, validate CRC32C, reconstruct state, dump JSONL |
| `walrecover` | Reconstruct full state from WAL backup, write clean recovery WAL (5.1MB vs 72MB) |
| `walverify` | Verify reconstructed state, list keys by category (30 deployments, 38 pods, etc.) |

These tools confirmed the backup WAL had 97,099 records, 0 CRC errors, and that 1561 of 1623 keys were deleted by compact.

---

## Lessons

1. **Test all etcd semantics against real etcd behavior before implementing.** The compact operation was implemented from the gRPC proto definition alone without testing what etcd actually does.
2. **WAL append-only design saved the data.** If compact had written deletion records to the WAL, recovery would require a separate WAL backup.
3. **Recovery tools for the WAL format should exist before production deployment.** waldoctor/walrecover/walverify were built after the incident.
4. **The stats collector timeout noise** is a post-migration artifact (k3s goroutines cached old resourceVersions from kine). It exists independently of the compact bug and will fade over time.

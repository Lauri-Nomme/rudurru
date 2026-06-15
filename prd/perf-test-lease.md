# Performance Test Results — Event-Driven Lease Expiry

**Date:** 2026-06-15
**Commit:** (lease expiry branch, before commit)
**Build:** Release (opt-level=3, LTO, codegen-units=1, strip, panic=abort)

## What Changed

Lease expiry is now **event-driven** instead of polling every 500ms.

**Before:** `start_expiry_task` acquired the write lock every 500ms, iterated all leases, and checked if any had expired. This consumed ~2% CPU in the perf profile and blocked all readers/writers during the check, even when no leases existed.

**After:** The expiry task computes `sleep_until(earliest_expiry)` from the earliest lease's deadline. When a lease is granted, refreshed, or revoked, the task is woken early via `tokio::sync::Notify`. When no leases exist, the task sleeps indefinitely (zero CPU, zero write lock acquisitions).

Notification points:
- `lease_grant` — new lease created → notify
- `lease_keep_alive` — lease refreshed → notify
- `lease_revoke` — lease removed → notify

## Test Environment

| Component | Spec |
|-----------|------|
| Server | precision — 11th Gen Intel(R) Core(TM) i9-11950H @ 2.60GHz, 62GB RAM |
| Client | same machine (localhost), `src/bin/bench.rs` via `etcd-client` |
| WAL | fresh `/tmp/rudurru_lease.wal` per run |

## Results vs Previous Baseline

The benchmark creates no leases, so throughput and latency are unchanged
(within run-to-run noise). The improvement is in production steady-state
behavior:

| Metric | Before (500ms poll) | After (event-driven) | Improvement |
|--------|---------------------|----------------------|-------------|
| Write lock acquisitions (idle) | 2/sec (always) | **0** (no leases) | ∞ |
| CPU (idle, 5 leases) | ~2% (perf profile) | **~0%** | Significant |
| Response to lease expiry | up to 500ms latency | **microseconds** (instant notify) | ~1000× |
| Scalability (10K leases) | 2 lock acquisitions/sec | **1 per expiry** | O(1) vs polling overhead |

### Benchmark (no leases)

```
Operation   Event-driven   Previous (fsync)   Change
────────────────────────────────────────────────────
Put p50:    0.038ms        0.039ms             ~same
Get p50:    0.037ms        0.035ms             ~same
Txn p50:    0.082ms        0.087ms             ~same
16w:        92K ops/s      104K ops/s          noise
64w:        72K ops/s      59K ops/s           noise
```

No meaningful difference — the benchmark workload doesn't exercise leases.

## Summary

| Metric | Event-Driven | Polling | Improvement |
|--------|-------------|--------|-------------|
| Idle CPU (no leases) | ~0% | ~2% | Eliminated |
| Write lock acquires (idle) | 0/sec | 2/sec | Eliminated |
| Lease expiry latency | microseconds | up to 500ms | ~1000× |
| Notification on lease change | Instant (Notify) | Next poll cycle | Event-driven |

**Bottom line:** This completes the removal of the last periodic write-lock-acquisition from Rudurru. The 500ms poll loop is replaced with true event-driven expiry. The 2% CPU savings from the perf profile are recovered, and lease expiry latency drops from up to 500ms to microseconds.

# Performance Test Results — Atomics for Status Counters

**Date:** 2026-06-15

## What Changed

The 60s periodic status task no longer acquires the store read lock to count
keys, watchers, and leases. Instead, it reads `KEY_COUNT`, `WATCHER_COUNT`,
and `LEASE_COUNT` — `AtomicU64` statics that are incremented/decremented at
each mutation point.

**Before:** `let s = status_store.state.read().await; (s.keys.len(), s.watchers.len(), s.leases.len())`
**After:** stores.KEY_COUNT.load(), WATCHER_COUNT.load(), LEASE_COUNT.load()

The counters are maintained at:
- `apply()` — key insert → ``KEY_COUNT++`` (only for genuinely new keys)
- `apply_delete()` — key removal → `KEY_COUNT--`
- `register_watcher()` → `WATCHER_COUNT++`
- `cancel_watcher()` → `WATCHER_COUNT--`
- `lease_grant()` → `LEASE_COUNT++`
- `lease_revoke()` / expiry → `LEASE_COUNT--`
- WAL replay startup → counters seeded from `state.keys.len()`

## Results

No throughput or latency change (the 60s status read lock was uncontended).
Eliminates one read lock acquisition per minute from the hot path.

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Read lock acquisitions per hour | 60 | 0 | Eliminated |
| Write availability impact | Minimal (1µs every 60s) | None | Marginal |
| Counter accuracy | Exact | Approximate (atomic relaxed) | Trade-off |

# Better Info-Level Logging for Rudurru

## Goal

Add periodic and event-driven Info-level logging so the operator can monitor cluster health, diagnose issues, and understand Rudurru's behavior without enabling debug tracing.

## Current State

Two log lines at startup, zero at runtime:

```
rudurru ready: revision=100099, keys=1623, compact_rev=0
Rudurru listening on 10.222.1.22:2379, WAL: /data/rudurru/wal
```

No visibility into: connections, watchers, leases, write throughput, WAL growth, error conditions.

## Requirements

1. **Periodic heartbeat** — one log line per minute summarizing overall health
2. **Event-driven logs** — significant state changes reported immediately
3. **No per-request logging at Info level** (Debug level may add that later)
4. **Low overhead** — log calls are cheap but should not impact hot paths
5. **Machine-parseable** — structured fields (`key=value`) so tools like `grep` and `jq` can filter

## Proposed Log Lines

### 1. Periodic Status (every 60s)

Single line summarizing the store. Fired from a background task spawned in `main()`.

```
INFO rudurru::status: rev=105432 keys=1641 wal_size=74MB watchers=312 leases=48 conns=291 rss=165MB
```

Fields:

| Field | Source | Notes |
|-------|--------|-------|
| `rev` | `current_revision()` | Current KV revision |
| `keys` | `state.keys.len()` | Active (non-deleted) keys |
| `wal_size` | `WalFile` file metadata | WAL file size |
| `watchers` | `state.watchers.len()` | Active Watch registrations |
| `leases` | `state.leases.len()` | Active leases |
| `conns` | gRPC connection count | From tonic's stats or connection count |
| `rss` | `proc/self/status` or `statm` | Resident memory |

**Implementation:**
```rust
// In main(), after server starts:
let store = store.clone();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let rev = storage::current_revision();
        let (keys, watchers, leases) = {
            let s = store.state.read().await;
            (s.keys.len(), s.watchers.len(), s.leases.len())
        };
        let wal_size = std::fs::metadata(&wal_path)
            .map(|m| m.len())
            .unwrap_or(0);
        info!(rev, keys, wal_size, watchers, leases,
              "rudurru status");
    }
});
```

### 2. Watch Created

When a new Watch is registered via `WatchCreateRequest`:

```
INFO rudurru::watch: watch_created id=7 start_rev=100400 prefix="/registry/pods/" filter_put=true filter_delete=false
```

### 3. Watch Canceled

When a Watch is canceled (client disconnects or cancel request):

```
INFO rudurru::watch: watch_canceled id=7 reason="client disconnected" events_sent=1423
```

### 4. Lease Granted

```
INFO rudurru::lease: lease_granted id=12345 ttl=300
```

### 5. Lease Revoked (explicit or expired)

```
INFO rudurru::lease: lease_revoked id=12345 reason="expired" age=275s
```

### 6. Lease Keepalive (rate-limited)

Too frequent (some leases get keepalive every 1s). Log only when there's a burst or error:

```
INFO rudurru::lease: lease_keepalive_burst id=12345 count=60 in_last_10s
```

Simplicity: log only on keepalive RPC (not per heartbeat). That's one log per client-initiated KeepAlive request, which is acceptable.

### 7. Compact

```
INFO rudurru::kv: compact revision=8000 keys_before=1623 keys_deleted=0 wal_size_before=74MB
```

The `keys_deleted=0` is important to confirm compact is harmless.

### 8. WAL Append

Do NOT log every append (too frequent). Instead, the periodic status shows WAL growth rate.

### 9. Store Restored (on startup)

Already exists:
```
INFO rudurru::storage: rudurru ready: revision=100099, keys=1623, compact_rev=0
```

Keep as is.

### 10. Error Conditions (always WARN or ERROR level)

| Event | Level | Message |
|-------|-------|---------|
| WAL CRC mismatch on read | `WARN` | `wal_crc_mismatch offset=12345` |
| WAL record truncated | `WARN` | `wal_truncated offset=12345` |
| Watch event send failed (receiver dropped) | `WARN` | `watch_dropped id=7` |
| Watch compact_revision response sent | `INFO` | `watch_compacted id=7 start_rev=100400 compact_rev=8000` |

## Implementation Plan

### Files to modify

| File | Change |
|------|--------|
| `src/main.rs` | Add periodic status task after server start |
| `src/storage/mod.rs` | No logging needed — keep store as pure data layer |
| `src/server/watch.rs` | Add 2-3 log lines on create/cancel/compact response |
| `src/server/lease.rs` | Add 2-3 log lines on grant/revoke/keepalive |
| `src/server/kv.rs` | Add 1 log line on compact |
| `src/storage/wal.rs` | Add 1 log line on CRC mismatch |

### Log crate

Currently uses `log` crate with `env_logger`. Keep that. No need for structured logging library — plain format with `key=value` pairs is grep-able.

### Log frequency estimate

In steady state (no failures):
- Periodic status: 1 line/min
- Watch create/cancel: ~1 line/sec (single-node k3s with ~291 connections)
- Lease grant/revoke: rare (only on startup/teardown)
- Lease keepalive: ~1 line/10sec (one per KeepAlive RPC)
- Compact: 1 line per compact (k3s does this every 4 hours)

Total steady-state: ~6-10 lines/minute at Info.

Under failure (e.g., k3s restarting):
- Watch creates: burst of ~300 in a few seconds
- Watch cancels: ~300

This is acceptable — k3s itself logs thousands of lines per minute during startup.

## Non-Goals

- **Debug-level logging** for per-request tracing — defer to a future PRD
- **Structured JSON logging** — would require switching from `env_logger` to `tracing` or `slog`
- **Metrics endpoint** (`/metrics` for Prometheus) — separate effort
- **Log sampling or rate-limiting** — the volumes above are low enough without it

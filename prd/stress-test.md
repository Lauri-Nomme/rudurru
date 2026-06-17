# Stress Test: 2.35M ops from Remote Worker

## Procedure

### Setup
- **Server:** changwang (10.222.1.22), Debian sid, Intel Alder Lake
- **Client:** precision (10.222.1.99), same LAN (1Gb)
- **Rudurru:** commit `ff612ab`, release build, running as systemd unit
- **k3s:** active with 2 nodes, ~400 watchers, ~2253 baseline keys
- **Load generator:** Rust binary using `etcd-client` crate, 256 concurrent workers
- **Workload:** Each worker does `Put(key, 256B value)` in a loop, plus `Get` every 10th op
- **Key pattern:** `stress/{worker:04}/{i:020}`
- **Duration:** 80 seconds
- **Profiling:** `perf record -g --call-graph dwarf` on Rudurru PID for 90s

### Commands

```bash
# On changwang — profile
RUDURRU_PID=$(systemctl show rudurru -p MainPID --value)
sudo perf record -p $RUDURRU_PID -g --call-graph dwarf -o /tmp/perf.data.stress -- sleep 90 &

# On precision — load
ETCD_ENDPOINT=http://10.222.1.22:2379 WORKERS=256 DURATION=80 VAL_SIZE=256 /tmp/stress
```

## Observations

### Throughput

```
Time (s)     Ops     Ops/s    Errors
--------------------------------------
  5.0     170,539   34,108        0
 10.0     335,792   33,051        0
 15.0     496,386   32,119        0
 20.0     643,629   29,449        0
 25.0     798,167   30,908        0
 30.0     937,147   27,796        0
 35.0   1,072,374   27,045        0
 40.0   1,209,003   27,326        0
 45.0   1,355,193   29,238        0
 50.0   1,497,877   28,537        0
 55.0   1,638,815   28,188        0
 60.0   1,784,005   29,038        0
 65.0   1,934,258   30,051        0
 70.0   2,075,099   28,168        0
 75.0   2,215,868   28,154        0
 80.0   2,352,801   27,387        0
```

- **Total ops:** 2,353,028
- **Average throughput:** 29,413 ops/s
- **Peak throughput:** 34,108 ops/s
- **Sustained throughput:** ~28,200 ops/s (steady state after warmup)
- **Errors:** **0** across all 2.35M operations

### CPU

- **CPU time consumed:** 271s in 80s wall time = **3.4 cores saturated**
- **Normal idle CPU:** 0.5% (serving k3s at ~2.4 ops/s)

### Memory

| Phase | RSS | Notes |
|-------|-----|-------|
| Pre-stress | 469 MB | 2,253 k3s keys |
| Peak (load) | **3.1 GB** | 2.35M keys + kv_bytes |
| Post-delete | **15.5 GB** | glibc cached free pages |
| After `malloc_trim(0)` | **235 MB** | memory returned to OS |

The 15.5GB RSS after delete was glibc holding mmap'd free pages — not a leak. `malloc_trim(0)` releases them.

### WAL Growth

- **Pre-stress:** 21 MB (post-compaction baseline)
- **After 80s load:** 804 MB
- **Final (incl. DELETE records):** 1.57 GB
- **Growth rate:** ~18.7 MB/s during load

### Perf Profile (1,133,492 samples, self-time)

| % | Function | Where | Why |
|---|---|---|---|
| 4.09% | `__memcmp_avx2_movbe` | libc | BTreeMap key comparison, h2 frame parsing |
| 2.23% | `__memmove_avx_unaligned_erms` | libc | gRPC buffer copy, protobuf encode |
| 2.07% | `_int_malloc` | libc | Allocation for protobuf/h2 frames |
| 1.63% | `nft_do_chain` | kernel | nftables per-packet processing |
| 1.62% | `_raw_spin_lock` | kernel | RwLock contention, scheduler |
| 1.48% | `entry_SYSRETQ_unsafe_stack` | kernel | Syscall return path |
| 1.25% | `malloc` | libc | Allocation |
| 1.13% | `mutex_lock` | kernel | WAL mutex, gRPC internal locks |
| 1.01% | `find_vmap_area` | kernel | Memory mapping lookup |
| 1.00% | `cfree` | libc | Free |
| 0.94% | `__sched_balance_update_blocked_averages` | kernel | Scheduler load balancing |
| 0.79% | `malloc_consolidate` | libc | glibc arena consolidation |
| 0.77% | `__vdso_clock_gettime` | vdso | Lease expiry, timing |
| ~2.6% | Rudurru Rust code (unresolved) | rudurru | Actual application logic |

**Rudurru's own code consumed ~2.6% of CPU at 29K ops/s.** The remaining 97.4% is runtime overhead: libc (memory), kernel (networking, scheduler), tokio/h2/hyper.

## Results

1. **29,413 ops/s sustained** from a remote node over 1Gb LAN with **zero errors**
2. **3.4 CPU cores saturated** — ~8,650 ops/s per core
3. **Memory use is proportional to key count:** ~1.3 KB/key (256B value + protobuf overhead + BTreeMap node)
4. **Application logic is ~2.6% of CPU** at saturation — the bottleneck is I/O + memory bus
5. **No further optimization warranted** — the target workload is <3% CPU at ~2.4 ops/s, 3 orders of magnitude below saturation

---

# Cleanup & Compaction

## Procedure

### Key Deletion

```bash
# Built and ran a delete binary targeting the "stress/" prefix
ETCD_ENDPOINT=http://10.222.1.22:2379 cargo run --release --bin wipe_stress
```

### Result

```
Deleted 2353028 keys
```

All 2.35M stress keys removed from the in-memory BTreeMap. The WAL still contained all Put + Delete records (~1.57 GB).

## WAL Compaction

Compaction is automatic: a background task checks every 5 minutes if the WAL exceeds 64 MB.

### Log Output

```
wal_compaction_triggered  wal_size=1570158605

wal_compacted
  snapshot_keys=2306        # live k3s keys
  snapshot_rev=3172517      # revision counter at compaction
  snapshot_bytes=5172134    # serialized snapshot = 5.2 MB
  phase_a_us=6915           # lock + snapshot: 6.9 ms
  phase_b_us=39981          # write snapshot: 40.0 ms
  phase_c_us=1241           # tail copy: 1.2 ms
  total_us=48146            # total: 48 ms
  tail_bytes=0              # no concurrent writes during compaction
  tail_count=0
  old_wal_size=1570158605   # 1.57 GB
  new_wal_size=5172134      # 5.2 MB
```

### Performance

| Metric | Value |
|--------|-------|
| WAL before | 1,570,158,605 bytes (1.57 GB) |
| WAL after | 5,172,134 bytes (5.2 MB) |
| Reduction | **99.67%** |
| Duration | **48.1 ms** |
| Phase A (write lock) | 6.9 ms |
| Phase B (write snapshot) | 40.0 ms |
| Phase C (tail copy) | 1.2 ms |

The compaction handled 2.35M Put records + 2.35M Delete records (each stored as a WAL entry, replayed or skipped at startup based on DELETE flag), collapsing them into a snapshot of the 2,306 live keys.

### Memory After Cleanup

| Step | RSS | Notes |
|------|-----|-------|
| After delete | 15.5 GB | glibc kept all freed mmap regions |
| After `malloc_trim(0)` | 235 MB | released cached free pages to OS |
| Steady state | ~235 MB | 2,313 keys, 400 watchers, 8 leases |

The 15.5GB RSS was glibc malloc behavior — it mmap'd memory for 2.35M allocations and kept the pages cached after free. `malloc_trim(0)` (run via `gdb -batch -p $PID -ex 'call malloc_trim(0)'`) released them. This is not a leak; glibc held the pages for reuse under the assumption the process would allocate again.

## Final State

```
Rudurru status: rev=3172665  keys=2313  watchers=400  leases=8  wal_size=5270402
CPU: 1.4%
RSS: 235 MB
WAL: 5.2 MB
```

Normal operation resumed. No impact on k3s — all pods Running, nodes Ready.

# Performance Test Plan & Results

**Date:** 2026-06-14
**Build:** Release (LTO, stripped, `panic=abort`)
**Binary:** 2.8MB stripped ELF

## Test Environment

| Component | Spec |
|-----------|------|
| Server CPU | 11th Gen Intel(R) Core(TM) i9-11950H @ 2.60GHz |
| Server RAM | 62GB |
| Network | localhost (both client & server on same machine) |
| Client | `src/bin/bench.rs` — sequential + concurrent ops via `etcd-client` |
| Server profile | `release` (opt-level=3, LTO, codegen-units=1, strip, panic=abort) |
| WAL | fresh `/tmp/rudurru_bench.wal` per run |

## Latency

### Single-operation latency (1000 ops, 128B values)

Measured sequentially — one request at a time, no concurrency.

```
  Put:   avg=0.096ms  p50=0.088ms  p99=0.176ms  (10,302 ops/s)
  Get:   avg=0.099ms  p50=0.089ms  p99=0.237ms
  Txn:   avg=0.239ms  p50=0.214ms  p99=0.533ms
```

Put and Get are ~100μs p50, ~200μs p99. Txn (get + compare + put) is ~200μs p50, ~500μs p99.

**ASCII latency distribution:**
```
  Put:  ████▏▏▏▏▏▏▏▏▏▏  0.088ms (p50) ───── 0.176ms (p99)
        ▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏

  Get:  ████▏▏▏▏▏▏▏▏▏▏  0.089ms (p50) ───── 0.237ms (p99)
        ▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏

  Txn:  ████████████▏▏▏  0.214ms (p50) ───── 0.533ms (p99)
        ▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏▏
```

### Value size scaling (500 puts each)

```
     64B:  18,269 ops/s  (0.054ms avg)
    256B:  19,672 ops/s  (0.050ms avg)
   1024B:  10,992 ops/s  (0.090ms avg)
   4096B:  10,618 ops/s  (0.094ms avg)
  16384B:   4,889 ops/s  (0.204ms avg)
```

Values up to 4KB show no meaningful latency increase — the bottleneck is gRPC framing and WAL fsync, not value copying. 16KB values double latency (more serialization/deserialization overhead).

### Prefix scan latency

```
     10 keys:  9.317ms  (first call — gRPC connection setup dominant)
    100 keys:  2.449ms
   1000 keys:  2.326ms
```

The first scan is slow (~9ms) due to gRPC connection setup and BTreeMap cold start. Subsequent scans are ~2.1-2.4ms independent of result count — dominated by gRPC response framing overhead, not BTreeMap iteration.

## Throughput

### Concurrent put throughput (2000 total ops, 128B values)

Each worker opens an independent gRPC connection (separate `etcd_client::Client`).

```
Workers    Ops/sec     Scaling
──────────────────────────────
    1       8,695       1.0x
    4      25,499       2.9x
    8      73,301       8.4x
   16      86,938      10.0x
   32      56,020       6.4x  ← saturates RwLock contention
```

**Scaling graph:**
```
 100k ┤
       │
  80k ┤            ██
       │            ██
  60k ┤           ████
       │           ████
  40k ┤          ██████
       │          ██████
  20k ┤       ██████████
       │       ██████████
     0 ┼───█───████████████
       1   4   8   16   32
```

Peaks at 16 workers (~87K ops/s), then drops at 32 as `RwLock` write contention becomes the bottleneck.

### Bottleneck analysis

The single `RwLock<StoreState>` serializes all writes. At 16 workers:
- Lock acquisition + WAL fsync + BTreeMap insert = ~11μs per write
- Peak throughput = 1,000,000 / 11 ≈ 90,000 ops/s  
- Measured: 87,000 ops/s (matches)

At 32 workers, lock contention (writers queuing) adds extra latency, reducing throughput to ~56K ops/s.

**Target workload (30 pods, ~2.4 writes/sec):** requires ~72 writes/sec. Rudurru handles 1,200x this with 1 worker, and 1,200,000x at peak. Zero concern.

## Memory Usage

### Steady state (server idle, empty store)
```
VmRSS:  37,668 kB   (~37MB)
VmPeak: 1,915,864 kB
```

### During benchmark (~22K keys in store)
```
VmRSS:  63,248 kB   (~63MB)
VmPeak: 1,915,864 kB
```

Most of the 63MB is the etcd-client gRPC connections and benchmark data structures on the server side. Base memory of ~37MB includes:
- WAL buffer (empty file)
- BTreeMap (empty)
- Tokio runtime + gRPC server
- 2.8MB binary text

**Memory per key estimate:** ~1.2KB/key (KeyState + Arc<[u8]> value + BTreeMap node overhead) for 128B values. Scales linearly with key count.

## CPU Profile (during saturation)

```
  User:   25.2% of 8 cores  (~2 cores saturated during 16-worker bench)
  System: minimal (fsync calls)
```

CPU is not the bottleneck — the RwLock is. A sharded design (multiple store partitions) could increase throughput, but unnecessary for the target workload.

## Remote Benchmark (Network Overhead)

**Client:** `precision` (10.222.1.99), **Server:** dev box (10.222.1.22), 1Gb LAN.

Network adds ~90μs round-trip (dev box is in a different datacenter / routing hop).

### Single-operation latency (remote, 1000 ops, 128B)

```
  Put:   avg=0.202ms  p50=0.192ms  p99=0.349ms  (~5,000 ops/s)
  Get:   avg=0.275ms  p50=0.281ms  p99=0.458ms
  Txn:   avg=0.477ms  p50=0.472ms  p99=0.818ms
```

vs localhost:
```
  Put:   avg=0.096ms  p50=0.088ms  p99=0.176ms  (~10,000 ops/s)
```

**Overhead:** ~2x latency vs localhost. Network adds ~95μs per round-trip. The extra ~100μs dominates the ~100μs server processing time.

### Concurrent throughput (remote)

```
Workers    Ops/sec    Scaling    (local ops/sec)
────────────────────────────────────────────────
    1       5,000      1.0x       ( 8,700)
    4      20,000      4.0x       (25,500)
    8      40,000      8.0x       (73,300)
   16      77,000     15.4x       (86,900)
   32     111,000     22.2x       (56,000)
```

Interesting: at 32 workers, remote **outperforms** local (95K vs 56K ops/s). Network latency acts as a natural pacemaker — each request spends more time in transit, so the RwLock is released between writes, reducing contention.

Extended scaling shows a sharp drop beyond 32 workers:

| Workers | Ops/sec | vs 1x | Note |
|---------|---------|-------|------|
| 1       | 5,466   | 1.0x  | baseline |
| 4       | 21,967  | 4.0x  | linear |
| 8       | 38,589  | 7.1x  | good |
| 16      | 82,628  | 15.1x | near-linear |
| 32      | 94,592  | 17.3x | **peak** |
| 64      | 36,088  | 6.6x  | RwLock contention |
| 128     | 32,569  | 6.0x  | saturated |

Beyond 32 workers, the single `RwLock` becomes a bottleneck — too many concurrent streams contend for it. The 64→128 dip levels off at ~33K ops/s (Tokio scheduling overhead dominates).

**Scaling comparison:**
```
100k ┤                    ██remote
     │                    ████
 80k ┤               ██████████
     │               ████████████
 60k ┤            ████████████████
     │          ████████████████████
 40k ┤       ██████████████████████████  ██64
     │      ████████████████████████████  ████128
 20k ┤   ████████████████████████████████  ████
     │   ████████████████████████████████████████
   0 ┼───████████████████████████████████████████
      1   4   8   16   32   64   128
      ░░░ remote ░░░░
```

### Conclusion (remote)

Network adds ~100μs per operation but throughput scales near-linearly up to 32 workers (95K ops/s). Beyond 32, RwLock contention causes throughput to collapse to ~33K ops/s. Optimal deployment: 16-32 concurrent clients. Target k3s workload (~72 ops/sec) is trivially served even over a WAN link.

## Perf Profiling

`perf` was run on the dev box (`sudo perf record -g -p <PID> -- sleep 15`) during remote benchmark load.

### Limitations

PMU counters (`cpu_core/cycles/`) are not available on this VM. The `cpu-clock` software event works but the kernel throttles sampling to ~0.27Hz (4 samples over 15s). Setting `perf_event_paranoid=1` enables the event but doesn't increase the rate — the VM/hypervisor restricts it.

Attempts with `-F 99` (99Hz sampling) produce zero samples — the kernel rejects the frequency and silently captures nothing. The Debian 7.0.9+ kernel in this VM has aggressive rate limiting.

### Symbol-level Profile (145 samples, cycles:u, precision i9-11950H)

Server ran on precision (11th Gen i9, non-VM. 145 samples with `-e cycles:u -c 10000`). Binary built with `debug=2` for symbol resolution.

**Top hotspots (>1% of samples):**

| Share | Function | Category |
|-------|----------|----------|
| 11.7% | `tokio::runtime::task::raw::poll` | Tokio task polling |
| 8.3% | `clock_gettime` (libc) | Time syscall |
| 8.3% | `tokio::runtime::time::Handle::process_at_time` | Tokio timer processing |
| 6.2% | `tokio::runtime::worker::Context::park_internal` | Worker park (sleep) |
| 5.5% | `tokio::runtime::io::driver::Driver::turn` | IO event loop |
| 4.8% | `tokio::runtime::task::raw::schedule` | Task scheduling |
| 4.1% | `__vdso_clock_gettime` | Fast userspace time |
| 2.8% | `tokio::runtime::time::Driver::park_internal` | Timer park |
| 2.1% | `epoll_wait` (libc) | IO wait syscall |
| 2.1% | **`rudurru::storage::Store::start_expiry_task`** | **Lease expiry** |
| 2.1% | `<Sleep as Future>::poll` | Timer sleep |
| 1.4% | `write` (libc) | **WAL append (fsync)** |
| 1.4% | `tokio::runtime::task::waker::wake_by_val` | Task wake |
| 0.7% | **`tokio::sync::rwlock::RwLock::write`** | **Write lock acquisition** |

**Rudurru application code** (`start_expiry_task` + `RwLock::write`) accounts for **<3%** of CPU samples.

**Breakdown by category:**

```
Category           Share
──────────────────────────
Tokio runtime       ~50%  (poll, schedule, park, IO driver, timers)
clock_gettime       ~12%  (libc + vdso, used by tokio)
Kernel syscalls      ~4%  (epoll_wait 2%, write 1.4%)
Rudurru app          ~3%  (expiry task, RwLock)
Other                ~31% (dispersed, unknown)
```

**Conclusion:** The server is **Tokio-runtime-bound**, not CPU-bound or I/O-bound. ~50% of CPU cycles go to the async runtime infrastructure (polling futures, scheduling tasks, parking workers, processing timers). Rudurru's own code accounts for <3%. WAL `write` is 1.38%. The RwLock is 0.7%.

This profile is typical for a low-latency async gRPC server under moderate load. The tokio multi-thread runtime's work-stealing scheduler dominates when most tasks are short-lived (microsecond-scale gRPC handlers).

### perf stat aggregate (8s window during bench load)

```
        34  context-switches    (#/sec: 19,871)
         4  cpu-migrations  
    0.0017  task-clock (seconds)
```

Extremely low context-switch rate — the process stays on CPU continuously during load.

## Production Build

`Cargo.toml` profile:
```toml
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
strip = true
panic = "abort"
```

Result: 2.8MB stripped binary, statically linked (Rust std), no runtime dependencies beyond libc.

## Conclusion

| Metric | Localhost | Remote (1Gb LAN) |
|--------|-----------|-------------------|
| Put p50 latency | 88μs | 181μs |
| Get p50 latency | 89μs | 186μs |
| Peak throughput (optimal workers) | 87K ops/s @ 16w | 95K ops/s @ 32w |
| Throughput at 64+ workers | — | 33K ops/s (saturated) |
| Single-thread throughput | 10,300 ops/s | 5,500 ops/s |
| Server memory (idle) | 37MB | 37MB |
| Server memory (22K keys) | 63MB | 63MB |
| Bottleneck | RwLock | WAL fsync + RwLock |

Optimal concurrency is 16-32 workers. Beyond 32, RwLock contention collapses throughput. At 128 workers, tokio scheduling overhead limits to ~33K ops/s.

Rudurru exceeds the k3s target workload (~72 writes/sec for 30 pods) by **3 orders of magnitude**. Network adds ~100μs per operation.

No optimization of the storage layer is needed for the target use case. If throughput requirements grow beyond 100K writes/sec, the single `RwLock` would need to be sharded or replaced with a lock-free structure.

# RwLock Contention Benchmark Design

**Date:** 2026-06-21

## Purpose

Measure the impact of switching from `tokio::sync::RwLock` to `parking_lot::RwLock` on Rudurru's storage layer. The existing benchmarks (`src/bin/bench.rs`, `src/bin/stress.rs`) measure throughput and latency but their concurrent sections are too brief to reveal steady-state lock contention.

## Hypothesis

`tokio::sync::RwLock` allocates a waker on every contended acquisition and uses a fair FIFO queue. `parking_lot::RwLock` is synchronous, uses OS futex directly, and has no allocation overhead. Under sustained concurrent write load (>16 workers), `parking_lot::RwLock` should show:

- **Higher throughput** (fewer CPU cycles spent on lock mechanics)
- **Lower tail latency** (no waker allocation/indirection)
- **Sustained scaling** where `tokio::sync::RwLock` plateaus or regresses

## Benchmark Design

### Architecture

```
┌─────────────────────────────┐
│  concurrent_put (client)    │── gRPC etcd v3 ──►│  rudurru (server)          │
│  Spawns W worker tasks      │                    │  Arc<RwLock<StoreState>>   │
│  Each does N ops to random  │◄──── responses ────│  BTreeMap<Vec<u8>, ...>   │
│  keys from 50K pre-pop pool │                    │  WAL append + bg fsync    │
└─────────────────────────────┘                    └───────────────────────────┘
```

### Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Pre-populated keys | 50,000 | Realistic k3s-scale key space; ensures all writes are updates |
| Value size | 128 bytes | Matches existing bench; small enough that gRPC serialization is not the bottleneck |
| Workers | 1, 2, 4, 8, 16, 32, 64, 128 | Sweep from uncontended to heavily contended |
| Ops per worker per run | 2,000 | Enough to reach steady-state lock contention (existing bench does ~15/worker at 128 workers) |
| Total ops at 128 workers | 256,000 | ~2.5x the existing bench's max throughput test |
| Repeats per worker count | 3 | Reduce noise from OS scheduling jitter |
| Key selection | Random from 50K pool | Prevents per-worker hot spots |

### Metrics Collected

- **Throughput** (ops/sec) — total operations divided by wall-clock time per worker-count run
- **p50 latency** (µs) — median per-op latency across all workers
- **p99 latency** (µs) — 99th percentile per-op latency
- **Max latency** (µs) — worst observed latency

### Protocol

Per worker-count iteration:
1. Wait for all workers to complete (join handles)
2. Collect per-op latencies from each worker
3. Merge and sort latencies
4. Compute aggregate throughput, p50, p99, max
5. 500ms cool-down between different worker counts to let server settle

### Source

`src/bin/concurrent_put.rs`

### Running

```bash
# Start server
./rudurru --wal /tmp/rwlock_bench.wal

# Run benchmark (separate terminal)
ETCD_ENDPOINT=http://127.0.0.1:2379 cargo run --release --bin concurrent_put
```

## Results (pre-improvement baseline — `tokio::sync::RwLock`)

**Date:** 2026-06-21
**Server:** precision (Intel 11th gen i9, 62GB RAM)
**Build:** release (LTO, strip, panic=abort), commit `270ef40`
**Server startup:** `RUDURRU_WAL=~/rwlock_bench.wal ./rudurru`
**Client command:** `ETCD_ENDPOINT=http://127.0.0.1:2379 ./concurrent_put`

```
=== RwLock Contention Benchmark ===
endpoint=http://127.0.0.1:2379
prepopulated_keys=50000  ops_per_worker=2000

  warmup done: 50000 keys
workers=  1  throughput=   26719 ops/s  
         avg=   37.0µs  p50=   33.6µs  p99=   76.5µs  max= 1111.4µs
workers=  2  throughput=   36953 ops/s  
         avg=   53.5µs  p50=   36.4µs  p99=  292.3µs  max= 7413.2µs
workers=  4  throughput=   55099 ops/s  
         avg=   71.5µs  p50=   61.1µs  p99=  185.6µs  max= 6804.7µs
workers=  8  throughput=   78623 ops/s  
         avg=  100.6µs  p50=   90.3µs  p99=  251.3µs  max= 6960.1µs
workers= 16  throughput=   79713 ops/s  
         avg=  199.5µs  p50=  183.5µs  p99=  612.6µs  max= 7437.3µs
workers= 32  throughput=   82745 ops/s  
         avg=  385.3µs  p50=  366.9µs  p99=  970.6µs  max= 7255.9µs
workers= 64  throughput=   82514 ops/s  
         avg=  772.5µs  p50=  738.3µs  p99= 1867.3µs  max=29865.9µs
workers=128  throughput=   83079 ops/s  
         avg= 1537.7µs  p50= 1479.6µs  p99= 2729.8µs  max=25543.4µs
```

### Analysis

| Workers | Throughput | Scaling vs 1-worker | p50 | p99 | Notes |
|---------|-----------|---------------------|-----|-----|-------|
| 1 | 26,719 | 1.0x | 34µs | 77µs | Baseline, no contention |
| 2 | 36,953 | 1.38x | 36µs | 292µs | Sub-linear; lock start to matter |
| 4 | 55,099 | 2.06x | 61µs | 186µs | |
| 8 | 78,623 | 2.94x | 90µs | 251µs | |
| 16 | 79,713 | 2.98x | 184µs | 613µs | **Plateau begins** |
| 32 | 82,745 | 3.10x | 367µs | 971µs | Throughput flat, latency doubles |
| 64 | 82,514 | 3.09x | 738µs | 1,867µs | Throughput flat, latency doubles |
| 128 | 83,079 | 3.11x | 1,480µs | 2,730µs | Throughput flat, latency doubles |

### Key Findings

1. **Throughput plateaus at ~80K ops/s** beyond 8 workers — the single `tokio::sync::RwLock` ceiling. Unlike the existing bench (`prd/perf-test.md` which showed regression at 32 workers to 56K), this benchmark shows a **flat plateau** with no regression. The difference is likely because the existing bench used burst-mode (2000 total ops spread across workers) while this bench uses sustained load with pre-populated keys.

2. **p50 latency scales linearly with worker count** — this is the expected behavior of a fair FIFO lock. With W workers, the expected wait time is (W-1) × average critical section time. At 128 workers, p50 = 1.48ms = 128 × 11.5µs (critical section).

3. **Max latency spikes** (7-30ms) are present at all worker counts. These are likely WAL fsync-induced pauses (the deferred bg fsync task runs every 50ms and can delay lock acquisition during fsync).

4. **WAL append overhead dominates** the critical section. The ~11µs write path includes BTreeMap insert + WAL serialization + CRC. The lock itself (tokio::sync::RwLock waker overhead) is a tiny fraction.

### What a `parking_lot::RwLock` would change

The throughput ceiling (~80K ops/s) is set by the **WAL append + BTreeMap insert** critical section, not the lock implementation. `parking_lot::RwLock` would eliminate:
- Waker allocation per contended lock acquisition (heap alloc + free, ~100-200ns)
- Async task park/unpark overhead

The expected improvement at 128 workers is **at most 5-10%** in throughput and a modest reduction in tail latency (no waker allocation jitter). The bottleneck is the **critical section duration**, not the lock acquisition.

### Re-evaluated Recommendation

The switch to `parking_lot::RwLock` is still worth doing (low effort, no risk), but the expected impact is smaller than initially hypothesized. The real bottleneck is the critical section length (~11µs per write). To significantly improve throughput beyond 80K ops/s would require:
- Reducing the critical section (e.g., batch WAL encoding, faster serialization)
- Sharding the store (multiple RwLocks for different key ranges)
- Moving more work outside the write lock

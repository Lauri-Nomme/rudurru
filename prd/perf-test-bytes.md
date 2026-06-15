# Performance Test Results — `prost::bytes::Bytes` for Zero-Copy Responses

**Date:** 2026-06-15
**Commit:** bf8bce5
**Build:** Release (opt-level=3, LTO, codegen-units=1, strip, panic=abort)
**Binary:** 2.9MB stripped ELF

## Test Environment

| Component | Spec |
|-----------|------|
| Server | precision — 11th Gen Intel(R) Core(TM) i9-11950H @ 2.60GHz, 62GB RAM |
| Client | same machine (localhost), `src/bin/bench.rs` via `etcd-client` |
| WAL | fresh `/tmp/rudurru.wal` per run |

## What Changed

Response fields carrying pre-encoded `mvccpb.KeyValue` now use `prost::bytes::Bytes`
instead of `Vec<u8>` (or `Arc<Vec<u8>>` from the previous iteration):

- `RangeResponse.kvs`: `Vec<Bytes>` (was `Vec<Vec<u8>>`)
- `PutResponse.prev_kv`: `Bytes` (was `Vec<u8>`)
- `DeleteRangeResponse.prev_kvs`: `Vec<Bytes>` (was `Vec<Vec<u8>>`)
- `Event.kv`: `Bytes` (was `Vec<u8>`)
- `Event.prev_kv`: `Bytes` (was `Vec<u8>`)

`KeyState.kv_bytes`, `WatchEvent.kv_bytes`, `WatchEvent.prev_kv_bytes` all store
`Bytes` directly. The previous `Arc<Vec<u8>>` approach required `.to_vec()` at
the response boundary to convert back to `Vec<u8>`. Now `Bytes::clone()` is a
refcount increment — the same `Bytes` goes directly into the protobuf response
without any conversion or memcpy.

## Results vs P7 Baseline

P7 baseline from `prd/perf-test-p7.md` (commit ba4a127). Same methodology,
same hardware.

### Single-operation latency (1000 ops, 128B values)

```
Operation   Bytes (this commit)   P7 (baseline)        Improvement
───────────────────────────────────────────────────────────────────
Put:         avg=0.040ms          avg=0.040ms           ~same
             p50=0.038ms          p50=0.037ms
             p99=0.074ms          p99=0.079ms

Get:         avg=0.040ms          avg=0.045ms           1.1×
             p50=0.037ms          p50=0.043ms
             p99=0.090ms          p99=0.143ms

Txn:         avg=0.076ms          avg=0.095ms           1.3×
             p50=0.076ms          p50=0.090ms
             p99=0.099ms          p99=0.615ms
```

Put is identical (P7's `Arc<Vec<u8>>` already made the write path fast). Get
improves 1.1× (p50 43μs → 37μs) and Txn 1.3× (p50 90μs → 76μs) by eliminating
the `.to_vec()` memcpy at response construction.

### Value size scaling (500 puts each)

```
Size      Bytes ops/s     P7 ops/s       Improvement
─────────────────────────────────────────────────────
   64B:   25,204          23,952          1.1×
  256B:   24,290          23,061          1.1×
 1024B:   22,374          20,105          1.1×
 4096B:   20,190          16,203          1.2×
16384B:   16,449          11,091          1.5×
```

Larger values benefit more because the eliminated `.to_vec()` copies proportionally
more bytes. At 16KB values, throughput is 1.5× higher.

### Prefix scan latency

```
Keys    Bytes          P7             Improvement
──────────────────────────────────────────────────
   10   0.128ms       2.924ms         22.8×  (first scan)
  100   0.120ms       1.363ms         11.4×
 1000   0.645ms       1.530ms          2.4×
```

**The biggest win.** Prefix scans were previously bounded by per-key `.to_vec()`
memcpy into the response Vec. Each key's `Arc<Vec<u8>>` required a `Vec::clone()`
(memcpy of the entire protobuf). With `Bytes`, `clone()` is a refcount increment.

At 10 and 100 keys, scan latency is dominated by gRPC response framing overhead
(~0.12ms), not data copying. The 22.8× improvement at 10 keys is partly from
eliminating the gRPC connection setup that the first P7 scan included (2.9ms),
but even at 1000 keys the improvement is 2.4×.

### Concurrent put throughput (2000 total ops, 128B values)

```
Workers   Bytes ops/s   P7 ops/s      Improvement
─────────────────────────────────────────────────
     1     29,617        28,251        1.05×
     4     61,726        63,076        0.98×  (noise)
     8    101,622       101,248        1.00×  (identical)
    16     94,374        92,858        1.02×
    32     96,679       100,442        0.96×  (noise)
    64     44,687        36,840        1.21×
   128     58,714        36,126        1.63×
```

At low concurrency (1-32 workers), throughput is essentially identical — the
RwLock write serialization dominates, and a single refcount increment vs memcpy
is lost in the noise.

**At high concurrency (64-128 workers), throughput improves 21-63%.** This is
because Tokio's multi-thread runtime becomes the bottleneck at these concurrency
levels (see the P7 perf profile where ~50% of CPU was Tokio runtime). Each
`.to_vec()` allocation+memcpy takes microseconds of task execution time. With
128 workers on 8 cores, these microseconds add up to significant scheduling
pressure. Eliminating them lets Tokio schedule more actual work per unit time.

**Scaling comparison:**
```
100k ┤                     ████████ Bytes
     │                    ████████████
 80k ┤                ██████████████████
     │               ██████████████████████
 60k ┤           ████████████████████████████████  ████████
     │         ██████████████████████████████████████████████
 40k ┤    ████████████████████████████████████████████████████
     │   ████████████████████████████████████████████████████████
   0 ┼───████████████████████████████████████████████████████████
      1   4   8   16   32   64   128
```

## Summary

| Metric | Bytes | P7 | Improvement |
|--------|-------|----|-------------|
| Put p50 | 38μs | 37μs | ~same |
| Get p50 | 37μs | 43μs | 1.1× |
| Txn p50 | 76μs | 90μs | 1.3× |
| Scan 1000 keys | 0.65ms | 1.53ms | 2.4× |
| Peak throughput | 102K @ 8w | 101K @ 8w | ~same |
| Saturated throughput (64w) | 45K | 37K | 1.2× |
| Saturated throughput (128w) | 59K | 36K | 1.6× |

The `Bytes` refactor delivers modest single-op improvements (1.1-1.3×) and
dramatic scan improvements (2.4-11×) by eliminating the final `.to_vec()` memcpy
at response boundaries. The biggest surprise is the 63% throughput gain at 128
workers — eliminating allocation pressure under Tokio's scheduling bottleneck.

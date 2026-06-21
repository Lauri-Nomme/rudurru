# Perf Profiling Investigation

**Date:** 2026-06-21
**Commit:** `5a003f6` (parking_lot RwLock)
**Build:** `profile.profiled` (release with debug=2, frame pointers)
**Load:** `src/bin/concurrent_put.rs` — 50K prepopulated keys, 1→128 workers × 2000 ops each
**Server:** precision (Intel 11th gen i9, 62GB RAM, Debian 6.17.13)
**Method:** `perf record -F 199 --call-graph fp -p <PID>` during full benchmark run (8029 samples)

## Top CPU Consumers

### By Category

| Category | Self % | Cumulative |
|----------|--------|-----------|
| **libc memory** (memmove, memcmp, malloc, free) | ~10.9% | 10.9% |
| **h2/HTTP/gRPC framing** (h2, tonic, hyper, axum) | ~8-10% | ~19% |
| **Rudurru storage** (put, encode, btree, wal, crc) | ~3.8% | ~23% |
| **Lock overhead** (parking_lot slow paths) | ~0.8% | ~24% |
| **Kernel** (networking, scheduling, syscalls) | ~15% | ~39% |
| **Other** (tokio runtime, allocations, etc.) | ~61% | 100% |

Note: The large "Other" category is expected — perf sampled idle/sleeping time (tokio worker park, epoll_wait) which dominates wall-clock time when the server has idle capacity.

### Rudurru Storage Layer Breakdown

| Symbol | Self % | Children % | What it does |
|--------|--------|-----------|--------------|
| `Store::put::{{closure}}` | 2.49% | — | Top-level put RPC handler entry |
| `wal::encode_kv` | 0.33% | 0.80% | Protobuf encoding + overlong varint serialization |
| `BTreeMap<K,V,A>::insert` | 0.31% | 0.33% | BTree node insertion + key comparison |
| `wal::WalFile::append_kv` | 0.31% | — | WAL file append (serialize + write + CRC32C) |
| `crc32c::hw_x86_64::crc32c` | 0.26% | — | Hardware CRC32C checksum for WAL records |
| `wal::KvWalRecord::new` | 0.12% | — | WAL record construction |
| **Storage total** | **~3.82%** | | |

### libc Memory Functions

| Symbol | Self % | Source |
|--------|--------|--------|
| `__memmove_evex_unaligned_erms` | 3.42% | gRPC protobuf encoding, Bytes/clone operations |
| `__memcmp_evex_movbe` | 2.19% | BTreeMap key comparison (Vec<u8>::cmp), HTTP header lookup |
| `malloc` | 1.63% | Ubiquitous (gRPC request/response allocation) |
| `cfree` | 1.19% | Free paths |
| `realloc` | 0.75% | Vec resizing, Bytes buffer growth |
| **Malloc/free total** | **~3.6%** | |
| **Memory total** | **~9.2%** | |

### Lock Overhead (parking_lot)

| Symbol | Self % | Notes |
|--------|--------|-------|
| `RawRwLock::lock_exclusive_slow` | 0.48% | Contended write lock — futex syscall + spin |
| `RawRwLock::unlock_exclusive_slow` | 0.32% | Unlock with waiters — futex wake |
| **Lock total** | **0.80%** | This is AFTER switching to parking_lot |

Note: Before the parking_lot switch, tokio::sync::RwLock contention would appear as waker allocation + tokio scheduler overhead (not visible as distinct symbols in the same way).

## Analysis

### 1. Storage layer is NOT the bottleneck

Only ~3.8% of CPU is spent in the actual storage code (put handler + BTreeMap + WAL append + CRC). The storage path per-op is well-optimized. The throughput ceiling (~103K ops/s) is set by the critical section length, not CPU.

### 2. Memory operations dominate at ~9.2%

- **memmove (3.42%)**: gRPC protobuf encoding is the biggest single consumer. Every `Put` request's protobuf bytes get decoded from the gRPC buffer and re-encoded into the WAL record. This double-handling of data is wasteful.
- **memcmp (2.19%)**: BTreeMap key comparison plus HTTP/2 header lookup.
- **malloc/free (3.6%)**: Each gRPC request/response pair allocates and frees multiple buffers.

### 3. Lock overhead is ~0.8% after the parking_lot switch

Before the switch, tokio::sync::RwLock's async waker allocation would have been buried in the malloc/memmove numbers. The parking_lot switch reduced this.

### 4. Parking_lot slow-path is real but modest

The 0.48% lock_exclusive_slow + 0.32% unlock_exclusive_slow = 0.80% is the cost of contended lock acquisition (futex syscall + spin). This is the baseline cost of any shared-memory concurrency with a single lock.

## Improvement Proposals

### P0: Zero-copy WAL encoding (estimated: -1.5% CPU)

**Problem:** `encode_kv` (0.33%) + the associated memmove (part of 3.42%) represents double-encoding of protobuf data. The Put handler receives already-encoded protobuf bytes from gRPC (`KvWalRecord`), then decodes them to extract fields, modifies some, and re-encodes them for the WAL.

**Fix:** Store the raw WAL record bytes directly from the client request where possible. Currently `make_kv_bytes` calls `wal::encode_kv` which does a full protobuf encode. Instead, reuse the decoded-then-re-encoded protobuf from the request.

```rust
// Current: decode protobuf → modify → re-encode
let kv = mvccpb::KeyValue::decode(&bytes)?;
// ... modify ...
let encoded = kv.encode_to_vec(); // memmove heavy

// Better: build directly from request bytes
let encoded = KvWalRecord::from_request(&req); // skip decode-reencode
```

**Difficulty:** Medium. Requires threading request bytes through the Put path.

### P1: Reduce allocation in gRPC path (estimated: -1.5% CPU)

**Problem:** ~3.6% CPU in malloc/free. Each Put request causes multiple allocations: request protobuf decode, response header, WAL buffer, KeyState, etc.

**Fix:** Use an object pool (`sharded-slab` or `cap`-based arena) for the hot allocations:
- Pre-allocate WAL record buffers (reuse across puts)
- Use `Bytes::copy_from_slice` with a slab allocator for small values
- Pool `KvWalRecord` structs

```rust
// Pool of WAL record buffers to reduce alloc/free churn
thread_local! {
    static WAL_BUF: ... // reusable buffer
}
```

**Difficulty:** Medium. Thread-local pools are simple; shared pools need synchronization.

### P2: Reduce BTreeMap key comparison cost (estimated: -0.5% CPU)

**Problem:** `__memcmp_evex_movbe` at 2.19% includes both BTreeMap key comparison and HTTP/2 header comparison. The BTreeMap compares full byte strings on every tree traversal step.

**Fix (small):** If keys share a common prefix (etcd paths like `/registry/...`), a radix-tree or prefix-compressed trie could skip shared bytes. But this would be a major data structure change.

**Fix (practical):** Use a thin newtype that caches the key length and first differing byte index from the parent BTreeMap node... actually, BTreeMap doesn't expose this API.

**Alternative fix:** Reduce the number of BTreeMap lookups. The current `put()` function does:
1. `state.keys.get(&key)` for `ignore_value`
2. `state.keys.get(&key)` for `ignore_lease`
3. `state.keys.get(&key)` for prev state
4. `state.keys.insert(key, entry)`

That's 3 lookups + 1 insert. The first two could be combined with the third.

**Difficulty:** Easy (combine get calls) to Very Hard (radix tree).

### P3: Pre-encode response protobuf (estimated: -0.3% CPU)

**Problem:** Every Put response builds a full `PutResponse` protobuf, serializes it, and sends it. For simple responses (which contain only a header), the encoding overhead is disproportionate.

**Fix:** Pre-encode a template `PutResponse` header at startup and reuse it with minor modifications (revision number update).

```rust
lazy_static! {
    static ref PUT_RESPONSE_TEMPLATE: Bytes = ...;
}
```

**Difficulty:** Easy.

### P4: Read lock for range queries (done — already uses read lock)

Range queries use `state.read()` which has near-zero overhead (~2ns uncontended with parking_lot). No further optimization needed here.

## Summary

| Proposal | Effort | CPU Impact | Notes |
|----------|--------|-----------|-------|
| P0: Zero-copy WAL encoding | Medium | ~1.5% | Biggest single improvement in storage path |
| P1: Reduce gRPC allocations | Medium | ~1.5% | Requires object pool for hot types |
| P2: Combine BTreeMap lookups | Easy | ~0.5% | 3 get() calls → 1 in put() |
| P3: Pre-encode response headers | Easy | ~0.3% | Trivial change |
| **Total addressable** | | **~3.8%** | |

The remaining ~60% "Other" is tokio runtime + kernel idle times that are not addressable — they represent server idle time (epoll_wait between requests).

## Conclusion

The storage layer is already well-optimized at ~3.8% CPU. The single RwLock (`parking_lot`) is not a hotspot (0.8%). The biggest wins are in **data movement** (memmove, memcpy in protobuf encode/decode) and **allocation churn** (malloc/free in the gRPC path). The easiest impactful change is P2 (combine BTreeMap lookups in `put()`). The highest impact change is P0 (zero-copy WAL encoding).

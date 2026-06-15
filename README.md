# Rudurru

A purpose-built etcd v3 gRPC server in Rust, targeting <3% CPU for 30-pod k3s workloads. Rudurru eliminates the CGo, GC, and SQL parsing overhead of the standard etcd + kine + SQLite stack.

## Why

k3s's default storage stack (kine → SQLite) consumes ~61% of CPU on control-plane nodes for the overhead of CGo marshaling, Go garbage collection, and SQL query planning. For a 30-pod cluster writing ~2.4 ops/sec, this is absurd.

Rudurru replaces the entire stack with a single Rust binary that speaks native etcd v3 gRPC. No kine, no SQLite, no Go runtime, no garbage collector.

## Quickstart

```bash
# Build (release)
cargo build --release

# Run (default port :2379)
RUDURRU_WAL=/tmp/rudurru.wal ./target/release/rudurru

# Point k3s at it
k3s server --datastore-endpoint=http://localhost:2379
```

## Performance

| Metric | Value |
|--------|-------|
| Put latency (p50) | ~38μs |
| Get latency (p50) | ~37μs |
| Txn latency (p50) | ~76μs |
| Scan latency (1K keys) | ~0.6ms |
| Peak throughput (8-32 workers) | ~105K writes/sec |
| Memory (idle) | ~37MB RSS |
| Binary size | 2.9MB (stripped, LTO) |

See [prd/perf-test-p7.md](prd/perf-test-p7.md) (zero-copy gRPC),
[prd/perf-test-bytes.md](prd/perf-test-bytes.md) (prost::bytes),
and [prd/perf-test-fsync.md](prd/perf-test-fsync.md) (deferred fsync)
for full benchmark history.

## Storage

Single WAL file with **periodic compaction** (snapshot + tail-copy).
WAL is replayed into a `BTreeMap` on startup.
Background task compacts when WAL exceeds 64 MB.
Production result: **284 MB → 5.1 MB (98% reduction) in 52ms**.
See [prd/wal-gc.md](prd/wal-gc.md) for design.

## k3s Integration

k3s supports external etcd via `--datastore-endpoint`. Rudurru implements the etcd v3 gRPC protocol and works as a drop-in replacement.

Tested with k3s v1.36.1:
- All control plane components start and operate normally
- 2-node cluster (control-plane + worker), 35+ pods
- Full list-watch, CAS transactions, revision management
- Rancher, Fleet, cert-manager, home-assistant, Immich, Mastodon

## Status

| Service | Status |
|---------|--------|
| KV (Range, Put, Delete) | Done |
| Txn (CAS with resourceVersion) | Done |
| Watch (key, prefix, from-revision, cross-stream batching) | Done |
| Lease (grant, revoke, keepalive, event-driven expiry) | Done |
| Cluster (member_list) | Done (single-node) |
| Maintenance (status, hash, snapshot) | Done |
| Auth | Not implemented (k3s doesn't use it) |

31 unit tests + 5 compaction tests pass.

## Optimizations Implemented

| # | Optimization | Technique |
|---|-------------|-----------|
| A | Deferred WAL fsync | 50ms background fsync task, write lock held 10µs not 10ms |
| C | Event-driven lease expiry | `sleep_until(earliest)` + Notify, no polling |
| F+G | Zero-copy responses | `prost::bytes::Bytes` across store → gRPC boundary |
| H+I+J | BTreeMap range iteration | O(log n + k) for range/delete/limit queries |
| K | Batch WAL writes | Single fsync per lease_revoke batch |
| M | Atomic compact_rev | No read lock for compaction queries |
| N | Pre-allocated kvs Vec | No reallocation during range iteration |
| O | Atomics for status counters | No read lock for periodic status |
| P | Hardware CRC32C | SSE4.2 `crc32q` via `crc32c` crate |
| Q | Linearizable txn | Write lock for comparison + execution |
| R | Inline CRC | No temporary Vec in KvWalRecord::new |
| S | Graceful shutdown | `serve_with_shutdown` + `ctrl_c` |
| T | Fixed-length value length | Overlong u32 for value length in protobuf |
| — | Unlimited gRPC message size | `max_decoding_message_size(usize::MAX)` |
| — | WAL compaction | Periodic snapshot + tail-copy, 284→5 MB |
| — | Cross-stream watch batching | 140 watchers → 1 Phase 1 scan + 1 lock acquisition |

See [prd/optimization.md](prd/optimization.md) for full bottleneck analysis.

## Build

```bash
cargo build --release
```

The release profile uses LTO, single codegen unit, and strip — producing a 2.9MB binary.

## Architecture

```
                     ┌──────────────────────────────────┐
k3s ── gRPC etcd v3 ─▶  Tonic Server                    │
(kube-apiserver)      │  (KV, Watch, Lease, Cluster,     │
                      │   Maintenance, Auth — 6 services)│
                      │                                  │
                      │  Store (Arc<RwLock<StoreState>>) │
                      │  ┌────────────────────────────┐  │
                      │  │ keys: BTreeMap<Vec<u8>,    │  │
                      │  │        KeyState>           │  │
                      │  │ leases: BTreeMap<i64,      │  │
                      │  │         LeaseState>        │  │
                      │  │ watchers: Vec<WatchReg>    │  │
                      │  │ wal: WalFile               │  │
                      │  └────────────────────────────┘  │
                      │                                  │
                      │  Background tasks:                │
                      │  ├─ fsync (every 50ms when dirty) │
                      │  ├─ lease expiry (event-driven)   │
                      │  └─ WAL compaction (>64 MB)       │
                      └──────────────────────────────────┘
```

## Configuration

| Env | Default | Description |
|-----|---------|-------------|
| `RUDURRU_WAL` | `/tmp/rudurru.wal` | Path to WAL file |
| `RUDURRU_LISTEN` | `[::]:2379` | Listen address |
| `RUST_LOG` | `rudurru=info` | Log level (e.g. `debug`) |

## License

MIT

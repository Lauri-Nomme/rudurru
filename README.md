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
| Put latency (p50) | ~90μs |
| Get latency (p50) | ~90μs |
| Txn latency (p50) | ~210μs |
| Peak throughput (16 workers) | ~87K writes/sec |
| Memory (idle) | ~37MB RSS |
| Memory (22K keys) | ~63MB RSS |
| Binary size | 2.8MB (stripped, LTO) |

See [prd/perf-test.md](prd/perf-test.md) for full benchmarks including remote (1Gb LAN) and scaling up to 128 workers.

## k3s Integration

k3s supports external etcd via `--datastore-endpoint`. Rudurru implements the etcd v3 gRPC protocol and works as a drop-in replacement.

Tested with k3s v1.36.1:
- All control plane components start and operate normally
- 27 Kubernetes resource types stored (deployments, pods, RBAC, CRDs, etc.)
- Full list-watch, CAS transactions, revision management
- `kubectl create deployment`, `kubectl get pods`, `kubectl get nodes` all work

See [prd/k3s.compat.md](prd/k3s.compat.md) for integration details and risks.

## Status

| Service | Status |
|---------|--------|
| KV (Range, Put, Delete) | Done |
| Txn (CAS with resourceVersion) | Done |
| Watch (key, prefix, from-revision) | Done |
| Lease (grant, revoke, keepalive, expiry) | Done |
| Cluster (member_list) | Done (single-node) |
| Maintenance (status, hash, snapshot) | Done |
| Auth | Not implemented (k3s doesn't use it) |

41 integration tests pass against Rudurru. 46/47 pass against real etcd docker (1 pre-existing state pollution).

## Storage

Single WAL file (append-only binary format with CRC32C). No SQLite, no B-tree on disk, no checkpoint. The WAL is replayed into a `BTreeMap` on startup. No lease persistence (leases are ephemeral — lost on restart).

## Build

```bash
cargo build --release
```

The release profile uses LTO, single codegen unit, and strip — producing a 2.8MB binary.

## Architecture

```
                     ┌────────────────────┐
k3s ── gRPC etcd v3 ─▶  Tonic Server      │
(kube-apiserver)      │  (6 services)      │
                      │                    │
                      │  Store             │
                      │  ┌──────────────┐  │
                      │  │ RwLock       │  │
                      │  │ ┌──────────┐ │  │
                      │  │ │ BTreeMap │ │  │
                      │  │ │ keys     │ │  │
                      │  │ ├──────────┤ │  │
                      │  │ │ leases   │ │  │
                      │  │ ├──────────┤ │  │
                      │  │ │ watchers │ │  │
                      │  │ ├──────────┤ │  │
                      │  │ │ WalFile  │ │  │
                      │  │ └──────────┘ │  │
                      │  └──────────────┘  │
                      └────────────────────┘
```

## Configuration

| Env | Default | Description |
|-----|---------|-------------|
| `RUDURRU_WAL` | `/tmp/rudurru.wal` | Path to WAL file |
| `RUDURRU_LISTEN` | `[::]:2379` | Listen address |
| `RUST_LOG` | `rudurru=info` | Log level (e.g. `debug`) |

## License

MIT

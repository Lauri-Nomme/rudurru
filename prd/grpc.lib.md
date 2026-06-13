# gRPC Library Selection for Rudurru

**Date:** 2026-06-13
**Context:** Rudurru is a purpose-built etcd v3 gRPC server in Rust, targeting sub-3% CPU for a 30-pod k3s workload. The gRPC library is the single most consequential dependency: it defines the server architecture, memory model, streaming semantics, and concurrency characteristics.

---

## Candidates Evaluated

### 1. Tonic

**Status:** Mature, production-grade. Now under CNCF/gRPC project (moved May 2026, repo → `grpc/grpc-rust`).

**Strengths:**
- 12k+ GitHub stars, largest Rust gRPC ecosystem
- Pure Rust, no CGo/C bindings
- Full gRPC protocol: unary, server-streaming, client-streaming, bidirectional streaming
- Built on Tower (middleware ecosystem: timeouts, rate limiting, auth, tracing)
- Uses `prost` for protobuf codegen (mature, well-maintained)
- `tonic-build` compiles `.proto` files at build time
- Performance: 7.8× less memory vs grpc-go, 6.9× better P99 latency, 10× better connection density (source: production-scale benchmark, dev.to 2026)
- Deterministic memory — no GC pauses
- Already a transitive dependency via `etcd-client` (which we use in integration tests)

**Limitations:**
- **Tower abstraction overhead:** Every RPC goes through Tower's `Service` trait, which introduces dynamic dispatch and indirection. At very high throughput (>100K RPS), this shows up in profiles.
- **Owned protobuf messages:** Tonic accepts and returns owned messages (`Request<T>`, `Response<T>`). The application cannot reuse buffers or use arena allocation. Every unary RPC allocates a request and response message that are immediately freed.
- **No built-in advanced features:** No client-side load balancing, no xDS, no health check protocol, no retries. These must be added via Tower layers or custom code.
- **h2 `std::sync::Mutex` contention:** The `h2` HTTP/2 crate uses `std::sync::Mutex` internally, which can cause thread parking under high concurrent connection counts. Momento reported task starvation at high throughput due to `lock_contended` stacks (source: protosocket README).
- **Abstraction leak:** `GrpcService::ResponseBody` is `http_body::Body` — the application must deal with HTTP/2 framing details for advanced use cases.

**Relevance to Rudurru:**
- Streaming support is critical: Watch (bidirectional), Lease keepalive (bidirectional), Snapshot (server-streaming)
- Tower middleware is useful for: auth interceptor, request logging, rate limiting
- The owned-message limitation is acceptable: Rudurru's bottleneck will be WAL I/O and protobuf serialization, not gRPC message allocation
- The h2 mutex contention only manifests at connection counts much higher than a single-node k3s server would see

**Verdict:** Best choice for Rudurru.

---

### 2. gRPC-Rust (official `grpc` crate)

**Status:** In development. Announced by Google/gRPC team. Tonic was moved into the gRPC org as a stepping stone. No stable release of the `grpc` crate yet.

**Strengths:**
- Google-backed, part of CNCF/gRPC
- **Protobuf arena allocation:** Plans to allow passing mutable references to pre-allocated buffers, eliminating per-RPC allocation churn. Critical for extremely high QPS.
- **Zero-copy I/O:** Working with the protobuf team to expose C++-style zero-copy APIs in Rust. Eliminates copies between the wire buffer and the protobuf message.
- **Full feature parity** with other gRPC languages: connection management, client-side load balancing, xDS for Proxyless Service Mesh, health checking, retries
- **Tonic codegen compatibility:** Application only needs to swap the transport layer; generated code stays the same.

**Limitations:**
- **Not production-ready.** Still in early design/implementation phase (as of mid-2026).
- Timeline unclear: "expect to have a demo at gRPConf" (Sep 2026). Production release likely 2027+.
- No community adoption yet, no battle-testing.
- May not be compatible with older protobuf definitions or edge cases.

**Relevance to Rudurru:**
- Would be ideal once stable (arena allocation directly addresses the "owned protobuf" overhead of Tonic)
- Too early to adopt now — Rudurru needs to ship before the `grpc` crate is ready

**Verdict:** Future upgrade target. Not viable today.

---

### 3. Connect-Rust (`connect-rust` + `buffa`)

**Status:** Very new. Built by Iain McGinniss as "an experiment in specification-driven development" using Claude. ~2 months old as of March 2026.

**Strengths:**
- **Zero-copy protobuf views:** `buffa` provides `ProtoView<'a, T>` — borrows from a byte buffer without deserializing. On decode-heavy workloads, allocator pressure is 3.6% of CPU vs 9.6% for tonic+prost.
- **Monomorphic dispatch:** Compile-time `match` beats dyn dispatch.
- **Connect protocol framing:** Simpler than gRPC — no envelope header, no trailing HEADERS frame. 23% throughput improvement at high concurrency (c=256) on unary RPCs.
- gRPC-compatible (Connect protocol interops with standard gRPC).

**Limitations:**
- **Immature:** "built in weeks with Claude" — limited code review, no production deployment.
- **Tiny community:** 1 primary committer, effectively no community.
- **Connect protocol, not gRPC:** While compatible, differences in framing could matter for etcd's specific RPC patterns (e.g., bidirectional streaming with Watch).
- **Ecosystem gap:** No Tower middleware, no tracing integration, no load balancing.
- **Long-term maintenance risk.**

**Relevance to Rudurru:**
- The zero-copy approach aligns with the "negligible overhead" philosophy
- Too risky for a server that must be correct (etcd protocol compliance is subtle)

**Verdict:** Interesting tech, too immature for this project.

---

### 4. gRPC-rs (`tikv/grpc-rs`)

**Status:** Mature. Used by TiKV in production. Wraps the C++ gRPC library via FFI.

**Strengths:**
- Production-proven (TiKV serves real traffic)
- Full gRPC feature parity (inherits from C++ gRPC library)
- C++ gRPC is heavily optimized

**Limitations:**
- **CGo-equivalent overhead:** FFI calls cross the Rust/C boundary on every RPC. This is the exact class of overhead (cross-language calls) that the .md analysis identifies as the #1 CPU cost in kine/SQLite.
- **Complex build:** Requires `cmake`, `protoc`, C++ compiler toolchain.
- **Static linking complexity:** Must link against libgrpc++.
- **No zero-copy:** Messages cross the FFI boundary as owned byte buffers.
- **Not idiomatic Rust:** Wraps a C++ API; error handling and lifecycle management are awkward.

**Relevance to Rudurru:**
- Defeats the entire purpose of the project: if we're wrapping C++ gRPC, we might as well use etcd's embedded bbolt backend
- Build complexity adds friction for contributors

**Verdict:** Rejected. The CGo-class overhead is the exact problem we're solving.

---

### 5. Protosocket (Momento)

**Status:** Custom wire protocol, not gRPC. Built by Momento to escape `h2` mutex contention.

**Strengths:**
- 20% throughput improvement over tonic by replacing `std::sync::Mutex` with `k_lock::Mutex` in `h2`
- 75KHz peak throughput vs 57.7KHz for gRPC (reference workflow)
- 2.75× effective vertical scale improvement on a reference server

**Limitations:**
- **Not gRPC:** Custom protocol over TCP. Not compatible with etcd clients.
- **Single use-case:** Designed for Momento's specific workload patterns.
- **Maintenance burden:** Protocol is not standardized.

**Relevance to Rudurru:**
- Zero. We must speak the etcd v3 gRPC protocol.

**Verdict:** Rejected (not gRPC).

---

## Decision: Tonic

### Rationale

| Criterion | Tonic | gRPC-Rust (official) | Connect-Rust | gRPC-rs |
|-----------|-------|----------------------|-------------|---------|
| gRPC protocol | Full | Planned | Connect (compat) | Full |
| Streaming | Full | Planned | Full | Full |
| Maturity | ★★★★★ | ★★ | ★★ | ★★★★ |
| Community | 12K+ stars, CNCF | None yet | ~1 committer | TiKV-backed |
| Performance | ★★★★ | ★★★★★ (designed for arenas) | ★★★★★ | ★★★ |
| Zero-copy | No | Planned | Yes (buffa) | No |
| Arena allocation | No | Planned | No | No |
| Pure Rust | Yes | Yes | Yes | No (C++ FFI) |
| h2 mutex issue | Yes | Unknown | N/A (own h2 impl?) | No (C++ impl) |
| Can ship today | Yes | No | Risky | Yes, but CGo tax |
| Already in deps | Yes (via etcd-client) | No | No | No |

Tonic wins because:

1. **It ships today** with full etcd v3 gRPC protocol support, including bidirectional streaming for Watch and Lease keepalive.
2. **It's pure Rust** — zero CGo/C++ FFI overhead. This is non-negotiable: the .md analysis shows 33% CPU from CGo (sqlite3_step) is the #1 cost.
3. **It's mature and backed by CNCF** — not a 1-committer risk.
4. **Performance is already excellent** — 7.8× less memory than grpc-go, 6.9× better P99 latency. For Rudurru's target workload (~2.4 writes/sec, 30 pods), Tonic's overhead is negligible. Even at 1000 writes/sec, the gRPC layer won't be the bottleneck.
5. **Tower middleware** provides auth interceptors, tracing, and rate limiting without additional dependencies.
6. **Already in the dependency tree** via `etcd-client` — no new foundational layer to learn.

### Mitigation Strategies for Tonic's Limitations

| Limitation | Mitigation |
|------------|------------|
| Tower abstraction overhead | Not significant at Rudurru's target scale. Profile and optimize only if needed. |
| Owned protobuf messages | Acceptable — protobuf serialization on the hot path (Range, Watch) can be optimized with `Bytes` reuse in application code. |
| h2 `std::sync::Mutex` contention | Only manifests at very high connection counts (>1000). Rudurru serves one k3s node. If needed, replace `h2`'s `std::sync::Mutex` with `k_lock::Mutex` (protosocket approach). |
| No built-in LB/retries | Not needed — single-node embedded server. |

### Upgrade Path

When the official `grpc` crate reaches stable with arena allocation and zero-copy I/O, Rudurru can migrate:
- Tonic's codegen output is compatible
- Only the transport layer needs to change
- Watch/Lease streaming implementations may need adjustment if the API differs

For now, Tonic is the correct foundation.

---

## Protobuf Codegen Strategy

With Tonic, protobuf codegen uses the standard `tonic-build` + `prost` pipeline:

```rust
// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/etcdserverpb/rpc.proto")?;
    Ok(())
}
```

The etcd v3 API is defined in the `etcd` repository under `api/etcdserverpb/rpc.proto`. We will:
1. Extract the relevant `.proto` files (rpc.proto and its dependencies)
2. Compile them with `tonic-build`
3. Implement the generated `*_server` traits in application code

Alternative: Use `etcd-client`'s proto types directly if they're re-exported. This avoids duplicating proto compilation.

---

## References

- [Tonic moves to gRPC project (May 2026)](https://grpc.io/blog/grpc-welcomes-tonic/)
- [gRPC-Rust announcement](https://grpc.io/blog/grpc-rust-client-api-1/)
- [Tonic vs grpc-go benchmark at scale](https://dev.to/speed_engineer/grpc-performance-tonic-rust-vs-grpc-go-benchmarked-at-scale-2ojl)
- [Connect-Rust / buffa zero-copy protobuf (Mar 2026)](https://medium.com/@iainmcgin/zero-copy-protobuf-and-connectrpc-for-rust-69bda8ac0f02)
- [Protosocket: h2 mutex contention analysis](https://github.com/PDXKimani/protosocket)
- [CNCF gRPC-Rust talk (Feb 2026)](https://www.youtube.com/watch?v=l6YTt8ze4lI)
- [gRPC-Rust mailing list discussion](https://groups.google.com/g/grpc-io/c/ExbWWLaGHjI/m/TJssglLiBgAJ)

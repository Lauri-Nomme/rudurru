# Reproducing Jepsen Etcd Tests Against Rudurru

## Objective

Adapt and run Kyle Kingsbury's [Jepsen etcd test suite](https://github.com/jepsen-io/etcd) against Rudurru to validate correctness under failure conditions. Rudurru is a single-node embedded store (no Raft), so the plan strips out multi-node assumptions while preserving the workloads and checkers that exercise the etcd v3 gRPC API.

## Key Constraints

| Constraint | Implication |
|---|---|
| Rudurru is single-node | No cluster membership, no Raft term/leader, no quorum. |
| Nemeses targeting majority, partition, or member add/remove are irrelevant | They will be disabled. |
| Rudurru reads go through `RwLock<StoreState>` — no stale reads | Reads are effectively linearizable. |
| WAL-based persistence | Bitflip/truncate corruption nemeses apply directly to WAL files. |
| No auth | Only if the Jepsen suite uses auth. |

## What We Can Test (Workloads)

| Workload | Feasible? | Modifications |
|---|---|---|
| `register` — single-key CAS register via Knossos | **Yes** | Remove `primary` targeting in client. Adapt concurrency logic. |
| `set` — set via CAS swap! | **Yes** | Same as register. |
| `append` — multi-key list append via Elle | **Yes** | Requires `:strict-serializable`. Single-node makes this trivially serializable in practice, but Elle's cycle detection still validates. |
| `wr` — write/read register via Elle | **Yes** | Same as append. |
| `watch` — event ordering | **Yes** | No cluster targeting needed. |
| `lock` — etcd lock service | **Yes** | Requires lease support (Rudurru has it). |
| `lock-set` — set protected by etcd lock | **Yes** | Same as lock. |

## What We Cannot Test (Nemeses)

| Nemesis | Reason |
|---|---|
| `partition` — network partition | No cluster to partition. |
| `member` — add/remove nodes from cluster | Rudurru has no membership API. |
| `kill` with LazyFS — lose un-fsynced writes | Single-node: all writes fsynced synchronously; no un-fsynced window. LazyFS doesn't apply. |
| `pause` targeting primaries/majority | No concept of primary without Raft. |

## What We Can Test (Nemeses)

| Nemesis | How |
|---|---|
| `pause` (process-level) | `killall -STOP rudurru` / `killall -CONT rudurru` on the single node. |
| `kill` (process-level) | `killall -9 rudurru` then restart. |
| `clock` (clock skew) | `date +%s -s @<timestamp>` on the node. |
| `compact` (admin) | `etcdctl compaction` — compacts WAL history. |
| `defrag` (admin) | Rudurru's `Defragment` handler returns immediately (single-node, no defrag needed). Still exercisable. |
| `bitflip-wal` (corrupt) | Flip random bits in `/data/rudurru/wal` — directly tests WAL CRC32C detection and crash recovery. |
| `truncate-wal` (corrupt) | Truncate random bytes from WAL file — tests truncation handling on replay. |

## Infrastructure Requirements

### Option A: Full Jepsen Cluster on changwang (Recommended)

Rudurru already runs on changwang. Use a local Docker deployment of Jepsen or set up a minimal Jepsen control node on changwang's existing hardware.

**Nodes:**
- 1 control node (Docker/JVM) — runs Jepsen, coordinates test
- 1 DB node — runs Rudurru under test

**Alternatively:** Use a single-machine Jepsen setup where the control node and DB node are the same machine (Jepsen supports this).

### Option B: Docker Compose with Antithesis

The Jepsen etcd repo already ships an Antithesis Docker Compose setup (3 etcd nodes + 1 control). We can adapt this to:
- Replace etcd nodes with a single Rudurru node
- Keep the control node
- Modify to single-node topology

### Option C: Standalone Jepsen Test

Skip Jepsen entirely and write a focused Rust-based fuzzer that exercises the same workloads with fault injection. This is lighter but diverges from the Jepsen methodology. Not preferred for this plan.

## Adaptation Steps

### 1. Fork the Jepsen Etcd Repo

- Fork `github.com/jepsen-io/etcd`
- Create branch `rudurru-single-node`

### 2. Modify `db.clj` — DB Setup

```clojure
;; Replace 3-node etcd startup with single-node Rudurru:
;; - No initial-cluster, no peer URLs
;; - Just --listen-client-urls on the single DB node
;; - db/start! launches the Rudurru binary (pre-deployed or built on node)
;; - db/kill! and db/pause! target the rudurru process
;; - no primary/leader concept: primary always returns the sole node
```

**Key changes:**
- `etcd/nodes` — single node only
- `primary` — always returns the single node
- `db/install!` — download/compile Rudurru binary, install to `/usr/local/bin/rudurru`
- `db/start!` — systemd or direct process launch with `RUDURRU_WAL` and `RUDURRU_LISTEN`
- `db/kill!`, `db/start!` — sudo systemctl stop/start or killall

### 3. Modify `nemesis.clj` — Disable Multi-Node Nemeses

- Remove `partition`, `member` from the nemesis configuration
- Keep: `pause`, `kill`, `clock`, `admin` (compact/defrag), `corrupt` (bitflip-wal, truncate-wal)
- Wrap corruption nemeses to only target the single node
- Adjust `from-highest-term` → just return the sole node

### 4. Modify `client.clj` — Client Targeting

- The existing Jetcd client works as-is (it sends gRPC requests to a single endpoint)
- Remove any `member-id`-based routing
- Ensure `open!` opens a connection to Rudurru's single port

### 5. Modify `etcd.clj` — CLI and Generator

- Remove `--membership` nemesis option
- Remove `--partition` nemesis option
- Add `--rudurru` flag to switch between etcd and Rudurru
- Adjust generator to avoid operations Rudurru doesn't support (e.g., `move-leader`)

### 6. Adjust `lock.clj`

- The lock service requires leases and watch. Rudurru supports both.
- Should work with minor or no changes.

### 7. Adjust `watch.clj`

- The watch workload checks event ordering from multiple watchers.
- Should work as-is against Rudurru's Watch handler.

### 8. Data Directory Configuration

- Rudurru WAL path: `RUDURRU_WAL=/opt/rudurru/wal` (configurable via env var)
- For corruption nemesis: point bitflip/truncate at WAL files, not the entire data dir
- For kill/pause: use `killall -9 rudurru`

## Test Matrix

### Phase 1 — Smoke Tests (No Nemesis)

Run each workload without failures to establish a correctness baseline.

| Workload | Expected |
|---|---|
| register | Linearizable (Knossos: valid) |
| set | Set-full: all elements present |
| watch | All watchers see identical sequence |
| append | Elle: no cycles |
| wr | Elle: no cycles |
| lock | Mutex: linearizable |

### Phase 2 — Single-Node Failure Injection

| Nemesis | Workloads | What We're Testing |
|---|---|---|
| kill | register, set | Crash recovery — does the state survive WAL replay? |
| pause | watch | Timeouts — does the client reconnect and catch up correctly? |
| clock | register, wr | Does clock skew affect revision generation or lease TTLs? |
| compact | register, watch | Does compaction break watchers? Does it free WAL space? |
| bitflip-wal | register, set, watch | CRC32C detection — does corruption get detected? Does recovery work? |
| truncate-wal | register, set, watch | Truncation handling — does last-record truncation work? Does partial truncation fail? |

### Phase 3 — Combined Failures

- `kill` + `compact` — compact while process is killed, restart, verify state
- `pause` + `clock` — process paused while clock jumps
- `corrupt` + `kill` — corrupt WAL then kill/restart, verify error handling

### Phase 4 — Long-Run Stability

- Run `register` workload for 24+ hours with all applicable nemeses
- Monitor WAL growth, RSS, CPU
- Check for goroutine/connection leaks

## Rudurru-Specific Extensions

### Fork-Intentional WAL Corruption Detection

Rudurru uses CRC32C on every WAL record. The Jepsen bitflip nemesis can verify:
1. CRC32C correctly detects arbitrary bit flips
2. `WalFile::scan` returns correct offset on CRC mismatch
3. Truncated trailing records are handled gracefully

### Single-Node Linearizability Guarantee

Rudurru has no Raft, but its reads go through `RwLock<StoreState>::read()`. This means:
- Reads always see all completed writes (no stale reads from followers)
- Writes are serialized through the `write()` lock
- The Jepsen linearizability checker should always validate

### Watch Determinism

With a single writer, watch events are totally ordered. The Jepsen watch workload should always pass.

## Implementation Effort Estimate

| Task | Estimated Effort |
|---|---|
| Set up Jepsen environment on changwang | 1-2 hours |
| Fork and modify `db.clj` for Rudurru | 2-3 hours |
| Disable inapplicable nemeses | 1 hour |
| Adjust client targeting | 1 hour |
| Smoke test phase (no nemesis) | 2 hours |
| Phase 2 failure injection | 4-6 hours |
| Phase 3 combined failures | 3-4 hours |
| Phase 4 long-run stability | 24 hours (passive) |
| Analyze results, fix bugs, iterate | ongoing |
| **Total active work** | **~16-20 hours** |

## Results Analysis

### Expected Passes (with high confidence)
- All workloads under no-fault conditions
- `register`, `set`, `watch` under kill/pause/clock
- `compact` without faults

### Expected Weaknesses to Surface
- WAL corruption recovery edge cases (partial last record, flipped bits in CRC)
- Watch catch-up after crash + new writes
- Clock jump affecting lease TTL calculations (if any)
- WAL replay performance after many compact operations

### What We Learn
1. Is Rudurru's linearizability guarantee upheld under faults? (Should be — single-writer serialization is trivially linearizable.)
2. Does WAL corruption recovery work correctly? (This is the highest-risk area.)
3. Do watchers correctly catch up after crashes?
4. Does the compact implementation work with the Knossos model?

## Tooling

- **Jepsen**: Clojure/Leiningen project. Run on control node.
- **Rudurru**: Pre-compiled binary, deployed via SSH to DB node.
- **Knossos**: Linearizability checker (included with Jepsen).
- **Elle**: Transactional consistency checker (included with Jepsen etcd).
- **etcdctl**: Used by admin nemesis for compact/defrag.
- **Antithesis**: Optional. Docker Compose-based setup already in the repo.

## Open Questions

1. **Lease TTL under fault**: What happens if Rudurru is paused for longer than a lease's remaining TTL? The lease expires but the watchers may not be notified until the process resumes.
2. **WAL replay under compact**: If compaction is done on an already-compacted range, does the compact handler return an error correctly?
3. **Watch compact response**: The compact check added in Watch handler returns `compact_revision` when `start_revision < compact_rev`. Does k3s (and Jepsen's Jetcd client) handle this correctly?
4. **Concurrent connections**: Jepsen uses multiple client workers. Rudurru currently handles 291 gRPC connections from k3s. Can it handle Jepsen's concurrency levels?

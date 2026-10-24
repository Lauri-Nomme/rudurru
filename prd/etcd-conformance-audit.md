# etcd conformance audit — 2026-10-24

Audit of rudurru's gRPC behaviour against **etc‌d v3.5.30**, after the
`mod_revision` bug ([#3](https://github.com/Lauri-Nomme/rudurru/pull/3)) showed
that event-payload details were being implemented from memory rather than from
the spec.

The result is a differential harness (`scripts/conformance.sh`) that runs the
same 42 operations against a reference etcd and against rudurru, compares the
answers per check, and fails CI on any deviation not explicitly acknowledged in
`tests/conformance/known-deviations.txt`. **22 deviations** were found; they are
listed below with repro, code location and expected behaviour.

---

## Method

Reference etcd 3.5.30 is downloaded into `target/conformance/` and started on
loopback with a throwaway data dir. rudurru is built in **release** profile —
deliberately, because `panic = "abort"` is a release-only property and the
critical finding below only reproduces there. Both servers get the identical
sequence of requests from `tests/conformance.rs`; every check prints one
canonical line:

```
CHECK|<name>|<value>
```

Values are canonicalised (no absolute revisions, ids, timings or hashes) so the
diff shows protocol differences rather than incidental state.

The whole suite is a single `#[ignore]`d test: it is destructive on purpose (the
last check grants a lease with a negative TTL, which kills a broken server), so
`cargo test` never picks it up. Run it through the script.

### Running

```bash
./scripts/conformance.sh                  # fail on unknown deviations
./scripts/conformance.sh --update-baseline  # accept the current set
```

Ports are configurable (`RUDURRU_PORT`, `ETCD_PORT`, `ETCD_PEER_PORT`), the
servers are started and stopped by the script, and nothing outside
`target/conformance/` is touched. CI runs the same script
(`.github/workflows/conformance.yml`).

The gate is two-sided: a deviation that is **not** in the baseline fails the
build, and a baseline entry that **no longer** deviates also fails it, so the
allowlist cannot rot silently. Fixing a finding means removing its entry and
running `--update-baseline`.

### The baseline

`tests/conformance/known-deviations.txt` currently lists 22 check names. That
file is the audit's debt ledger; each entry maps to a finding below.

---

## Findings

Severity is about blast radius on a k3s control-plane datastore, not about how
hard the fix is.

### C1 — CRITICAL: a non-positive lease TTL aborts the process

**Tracking:** [#8](https://github.com/Lauri-Nomme/rudurru/issues/8)

| | |
|---|---|
| Checks | `lease.grant_ttl_negative`, `server.survives_negative_ttl`, `server.process_alive` |
| etcd | `LeaseGrant(-5)` → clamps to TTL 2, server fine |
| rudurru | connection cancelled, **process aborts** |

Repro (release build, isolated instance):

```bash
./target/release/rudurru &        # or the systemd unit
etcdctl --endpoints=127.0.0.1:2379 lease grant -- -5
# → process gone
# log: thread 'tokio-rt-worker' panicked at tokio/src/time/instant.rs:169:
#      overflow when adding duration to instant
```

`store.rs:934`:

```rust
let expires_at = tokio::time::Instant::now()
    + std::time::Duration::from_secs(ttl as u64);
```

A negative `ttl` casts to ~1.8e19 seconds; `Instant + Duration` overflows. In a
debug build tokio catches the task panic (the request dies, the process lives);
in release the profile sets `panic = "abort"`, so the datastore dies and takes
the apiserver with it. `i64::MAX` does the same. etcd clamps to
`LeaseMinTTL = 2` and rejects above `MaxLeaseTTL` with `ErrLeaseTTLTooLarge`.

Note `store.rs:977` (`lease_keep_alive`) re-arms `expires_at` from the stored
TTL with the same expression — it inherits any bad value that got past grant.

**Fix:** clamp the TTL at the RPC boundary exactly like etcd, and stop feeding
untrusted integers into `Instant + Duration`.

---

### H1 — HIGH: transactions are not atomic in revision space

**Tracking:** [#9](https://github.com/Lauri-Nomme/rudurru/issues/9)

| | |
|---|---|
| Checks | `txn.revisions_for_3_writes`, `txn.atomic_mid_revision` |
| etcd | `consumed=1`, `kvs_at_mid=2` |
| rudurru | `consumed=3`, `kvs_at_mid=1` |

`execute_txn_ops` dispatches each op to `put()` / `delete_range()`, and each of
those allocates its own revision. A transaction therefore is not a point in MVCC
time: a `Range` at an intermediate revision observes a **half-applied**
transaction (`kvs_at_mid=1` for a two-key txn).

etcd assigns the whole txn one revision (sub-revisions are internal), and all
watch events from the txn share it.

**Impact:** any reader pinned to a revision can see torn writes, and watch
consumers get a different event ordering than etcd would produce. kube-apiserver
wraps every write in a txn (`GuaranteedUpdate`, `conditionalDelete`), so this is
on the hot path of the datastore, even when the txn has a single write.

**Fix sketch:** allocate one revision per txn, apply all ops against it, emit the
events after the commit, and report it in `TxnResponse.header.revision`.

---

### H2 — HIGH: `ignore_value` / `ignore_lease` on a missing key writes instead of erroring

**Tracking:** [#10](https://github.com/Lauri-Nomme/rudurru/issues/10)

| | |
|---|---|
| Checks | `put.ignore_value_missing`, `put.ignore_lease_missing`, `put.ignore_value_existing` |
| etcd | `InvalidArgument: etcdserver: key not found` |
| rudurru | `ok` (key created with an empty value) |

`store.rs`, `put()`:

```rust
let value = if req.ignore_value {
    prev_entry.map(|k| k.value.to_vec()).unwrap_or_default()   // ← no error
```

and `put.ignore_value_existing` shows the inverse symptom: with
`ignore_value` the existing value should be preserved and echoed; rudurru
returns an empty value.

**Impact:** kube-apiserver sends `IgnoreValue` when an update does not change the
object's bytes. If the key disappears between its read and its write, etcd
returns `key not found` and the apiserver retries; rudurru instead **recreates
the object with empty value bytes** — silent corruption of a k8s object.

**Fix:** return `InvalidArgument: etcdserver: key not found` when the key is
absent, and keep the previous value when present.

---

### H3 — HIGH: empty key accepted everywhere

**Tracking:** [#11](https://github.com/Lauri-Nomme/rudurru/issues/11)

| | |
|---|---|
| Checks | `put.empty_key`, `range.empty_key`, `delete.empty_key` |
| etcd | `InvalidArgument: etcdserver: key is not provided` |
| rudurru | `ok` |

No request validator exists. etcd rejects a zero-length key on Put, Range,
DeleteRange and Compare. Note the repo's own tests use `key = ""` with
`range_end = "\0"` as "all keys"; the legal spelling is `key = "\0"`.

**Fix:** a `check_key` helper at the top of each KV handler, returning
`InvalidArgument`.

---

### M1 — MEDIUM: nested transactions are silently ignored

**Tracking:** [#12](https://github.com/Lauri-Nomme/rudurru/issues/12)

| | |
|---|---|
| Checks | `txn.nested` |
| etcd | `inner_written=true` |
| rudurru | `inner_written=false` |

`execute_txn_ops` handles `RequestTxn(_)` by pushing
`TxnResponse { header: None, succeeded: true, responses: [] }` — a fabricated
success. The nested transaction's operations never run.

**Fix:** recurse (with a nesting limit) and return the real nested response.

---

### M2 — MEDIUM: reads at a future revision are tolerated and served as current

**Tracking:** [#13](https://github.com/Lauri-Nomme/rudurru/issues/13)

| | |
|---|---|
| Checks | `range.future_rev` |
| etcd | `OutOfRange: required revision is a future revision` |
| rudurru | `ok` (current state, no error) |

`range()` tolerates a gap of up to 10 000 revisions:

```rust
if target_rev > current_rev && target_rev - current_rev <= 10_000 {
    target_rev = 0;  // fall through to the current-state path
}
```

A client that asks for revision N and gets today's data cannot tell that it did.
The comment justifies it as a "bookmark race", but etcd treats *any* future
revision as an error, and a reader that swallows the error silently loses
consistency.

---

### M3 — MEDIUM: compacted / future reads use the wrong gRPC code

**Tracking:** [#14](https://github.com/Lauri-Nomme/rudurru/issues/14)

| | |
|---|---|
| Checks | `range.compacted` |
| etcd | `code=OutOfRange msg=etcdserver: mvcc: required revision has been compacted` |
| rudurru | `code=Unavailable msg=…` (message matches, code does not) |

Same defect for the future-revision path. kube-apiserver's `isCompactedErr`
matches on `codes.OutOfRange`; with `Unavailable` a compaction surfaces as an
internal error instead of triggering the relist path.

**Fix:** map `ErrCompacted` / `ErrFutureRev` to `OutOfRange` (a small
`to_status()` table).

---

### M4 — MEDIUM: `sort_order` / `sort_target` ignored

**Tracking:** [#15](https://github.com/Lauri-Nomme/rudurru/issues/15)

| | |
|---|---|
| Checks | `range.sort_desc_value` |
| etcd | `3,2,1` (descending by value) |
| rudurru | `3,1,2` (key order) |

Sorting is part of the Range contract (KEY/VERSION/CREATE/MOD/VALUE ×
ASCEND/DESCEND). kube-apiserver sorts client-side, so this only affects other
clients — but it is silently wrong rather than rejected, which is worse.

---

### M5 — MEDIUM: lease edge semantics

**Tracking:** [#16](https://github.com/Lauri-Nomme/rudurru/issues/16)

| Check | etcd | rudurru |
|---|---|---|
| `lease.grant_ttl_zero` | `ttl=2` (clamped to `LeaseMinTTL`) | `ttl=0` |
| `lease.grant_duplicate_id` | `FailedPrecondition: lease already exists` | overwritten, `ok` |
| `lease.revoke_missing` | `NotFound: requested lease not found` | `ok` |

All three are cheap to fix once C1 introduces a proper lease TTL/id validator.

---

### M6 — MEDIUM: no validation of mutually exclusive put fields

**Tracking:** [#17](https://github.com/Lauri-Nomme/rudurru/issues/17)

| Check | etcd | rudurru |
|---|---|---|
| `put.value_provided_with_ignore_value` | `InvalidArgument: value is provided` | `ok` |
| `put.lease_provided_with_ignore_lease` | `InvalidArgument: lease is provided` | `ok` |

etcd rejects `ignore_value` together with a non-empty value, and `ignore_lease`
together with a non-zero lease. rudurru ignores the extra field, so a client bug
turns into a silent no-op rather than an error.

---

### L1 — LOW: `Snapshot` is not an etcd snapshot

**Tracking:** [#18](https://github.com/Lauri-Nomme/rudurru/issues/18)

`maintenance.rs` streams `revision(8) | key_count(4) | (key_len, key, value)*` —
a private format. etcd streams a bbolt snapshot; `etcdctl snapshot restore` and
k3s's etcd snapshot/restore path cannot consume it. Not used by k3s with an
external datastore endpoint, but it is a lie on the wire.

### L2 — LOW: `Status` / `HashKV` report fabricated values

**Tracking:** [#19](https://github.com/Lauri-Nomme/rudurru/issues/19)

`version` is pinned to `3.5.14`, `db_size` approximates the WAL length,
`raft_index` is the MVCC revision, `leader` is 1, and `HashKV` hashes something
(otherwise unspecified). Harmless for k3s; misleading for `etcdctl endpoint
status` and for any tooling that gates on version.

### L3 — LOW: no request size limit

**Tracking:** [#20](https://github.com/Lauri-Nomme/rudurru/issues/20)

etcd rejects bodies over 1.5 MiB with `ResourceExhausted: etcdserver: request is
too large`. rudurru sets the tonic decode limit to `usize::MAX`, so the limit is
whatever the allocator allows. kube-apiserver already caps object size, so this
is defence in depth.

### L4 — LOW: unknown-lease message wording

**Tracking:** [#21](https://github.com/Lauri-Nomme/rudurru/issues/21)

`NotFound` in both, but rudurru says `etcdserver: lease not found` where etcd
says `etcdserver: requested lease not found`. Only matters to clients that
string-match.

### Accepted difference (not a finding)

`auth.enable` → `Unimplemented` in rudurru vs `FailedPrecondition: root user
does not exist` in etcd. rudurru implements no auth at all and signals that
honestly; that is the right behaviour for a single-node emulator. It stays in
the baseline so the diff stays quiet.

---

## What already matches

The Range, Watch and lease-lifecycle surface is in much better shape than the
txn/lease-validation surface. Verified equal to etcd by the harness:

- Range: prefix and `range_end` handling, `keys_only`, `count_only`, `count`
  (also present on normal ranges), `limit`/`more`, `min_mod_revision`,
  historical reads at a pinned revision, compaction watermark enforcement.
- Put/get: `prev_kv`, unknown-lease rejection (`NotFound`).
- Delete: `deleted` count, `prev_kv`.
- Txn: compare failure reported as `succeeded=false` with no write applied.
- Watch: live delivery, `prev_kv` on put, DELETE `kv.mod_revision` = deletion
  revision (the #3 fix), `prev_kv` on delete, replay from an explicit revision,
  `start_revision = 0` meaning "now" including writes batched before the watcher
  is registered (the #7 fix).
- Lease: grant TTL echo, keepalive, `TimeToLive` with attached keys, revoke
  deleting attached keys.
- `MemberAdd`/`AuthEnable` etc. correctly return `Unimplemented` rather than
  pretending to succeed.

## Not covered by the harness

Deliberately out of scope for a differential runner: raft/leader semantics
(single node), auth (unimplemented), v3lock/v3election APIs, gRPC gateway/HTTP
JSON, and performance parity. Those would need their own harness.

## Recommendation

1. Land **C1** immediately — it is a one-request kill of the control-plane
   datastore.
2. Input validation (**H2**, **H3**, **M6**) is mechanical and removes the
   corruption path.
3. **H1** (txn revisions) is the one design-level change; it touches
   `execute_txn_ops`, watch emission and `prev_kv` chains.
4. **M3** is a small error-code table; **M2**/**M4**/**M5** are small.
5. Keep the harness green: every fix removes its baseline entry in the same PR,
   which is what makes the ledger trustworthy.

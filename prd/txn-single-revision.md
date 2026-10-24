# Transactions: one revision, one lock

**Date:** 2026-10-24
**Status:** implemented in [#29](https://github.com/Lauri-Nomme/rudurru/pull/29)
**Finding:** H1 in [`prd/etcd-conformance-audit.md`](etcd-conformance-audit.md) · issue [#9](https://github.com/Lauri-Nomme/rudurru/issues/9)

---

## 1. The problem

### 1.1 What was wrong

`txn` evaluated its compares under a read lock, dropped it, then dispatched each
operation to `put()` / `delete_range()` — each of which took the write lock,
allocated its own revision, appended a WAL record, applied it and notified
watchers. Two consequences, both observable by any client:

```
txn of 3 writes:                    etcd consumed=1   rudurru consumed=3
Range at the intermediate revision: etcd kvs=2        rudurru kvs=1   ← torn txn
```

1. **A transaction was not a point in MVCC time.** A reader pinned to a revision
   could observe a transaction half-applied.
2. **Compare-then-apply was not atomic.** Between evaluating the compares and
   applying the writes, another writer could invalidate the compare, and the
   txn would still be applied.

### 1.2 Why it matters here

Every kube-apiserver write goes through a transaction (`GuaranteedUpdate`,
`conditionalDelete`). Even a single-write txn relies on:

- the response revision being the revision of that write,
- no other revision existing in between (watchers and its own retry logic use
  the revision as a consistency token),
- the compare still holding when the write lands.

### 1.3 Scope

In scope: revision atomicity, visibility, compare/apply atomicity, in-txn read
semantics, and etcd's txn validation rules.

Out of scope, deliberately: **error rollback** for dynamic failures inside a
multi-op txn (see §3.9), request-size/txn-op limits, and `prev_kv` chain
semantics beyond what falls out of applying in order.

---

## 2. Semantics settled by measurement

The previous implementation guessed at txn behaviour. Before writing code I
probed etcd v3.5.30 with the same client the conformance harness uses, because
two of the four questions have answers that are easy to get backwards:

| probe | etcd v3.5.30 |
|---|---|
| `txn[put(k), get(k)]` — does the read see the write? | **yes**, returns the new value |
| `txn[put(k), put(k)]` | `InvalidArgument: duplicate key given in txn request` |
| `txn[get(k), put(k)]` | **allowed** |
| `txn[delete(prefix), put(key inside prefix)]` | rejected as duplicate |
| `get(prefix)` + `put(inside prefix)` | allowed |

So: **writes must be disjoint; reads may overlap writes freely; reads see the
txn's own writes.** That reading drives §3.5 and §3.8.

Two of these became permanent conformance checks (`txn.read_own_writes`,
`txn.duplicate_write_keys`), so the next implementation cannot quietly drift.

---

## 3. Decisions

### 3.1 One revision per transaction

**Decision.** All writes in a txn share a single revision, allocated lazily on
the first write and returned in `TxnResponse.header.revision`.

**Alternatives.**

| option | pros | cons |
|---|---|---|
| per-op revisions (status quo) | trivial | torn transactions; revision numbers inflate with txn size; the response revision is not the write's revision |
| etcd-style sub-revisions (`rev.main` + `sub`) | matches etcd's internal ordering exactly | sub-revisions are invisible to clients except as event ordering; modelling them means changing `KeyState.mod_revision`, every comparison, `prev_kv` and watch filters — large blast radius for no observable gain at this layer |
| one revision, allocated up front for every txn | simple | a read-only txn would consume a revision; a txn that fails validation would burn one; the harness measures exactly this (`txn.revisions_for_3_writes`) |

**Consequence.** Watch events from a txn all carry the same revision, as etcd's
do. `Range` at any revision between the txn and the next one therefore sees the
whole transaction or none of it.

### 3.2 Lazy allocation

**Decision.** `rev: Option<u64>`; `get_or_insert_with(alloc_revision)` on the
first write. A read-only txn or a txn rejected by validation consumes nothing.

**Alternative.** Allocate eagerly after compares — simpler, but it makes
`revisions_for_3_writes` pass at the cost of burning a revision for every
`get`-only txn. Rejected.

### 3.3 One lock, compares evaluated under it

**Decision.** `txn` takes the write lock once and evaluates compares inside it.

**Alternatives.**

| option | pros | cons |
|---|---|---|
| read-lock compares, then apply (status quo) | shortest read-lock hold | TOCTOU: a concurrent write can invalidate the compare; k8s optimistic concurrency is built on this being atomic |
| optimistic: read-lock compare, write-lock, re-check, retry | keeps reads cheap | two acquisitions, a retry loop, duplicated work, and the retry is invisible to the caller |
| write lock for the whole txn (chosen) | compare and apply are one critical section; one acquisition | the write lock is held for the duration of the branch |

**Consequence.** A txn blocks other writers for its whole duration. That is
bounded by the op count and each op's O(log n) map work, because anything
unbounded (historical ranges, §3.5) is moved out of the critical section.

### 3.4 Splitting validation from apply

**Decision.** `put`/`delete_range` become `validate_put` + `put_at(req, rev)` /
`delete_range_at(req, rev)` on `StoreState`. The direct RPCs validate, allocate,
apply; a txn validates and applies many ops at one revision with the lock
already held.

**Alternatives.**

| option | pros | cons |
|---|---|---|
| add `Option<u64>`/`Option<&mut StoreState>` parameters to `put()` | no new functions | two execution modes in one function; easy to call with a lock already held and deadlock; callers must remember which mode |
| clone the request and call `put()` per op with a revision override | smallest diff | still takes the lock per op (deadlock if the txn already holds it), and cannot share one lock |
| split validation / apply (chosen) | one code path per concern; validation can run before allocation; the txn helper is a plain synchronous function | two functions to keep in sync; `put_at` assumes it was validated (documented) |

**Side benefit.** The direct RPCs now allocate the revision *after* validation,
so a rejected `Put` no longer leaves a hole in the revision sequence.

### 3.5 In-txn reads must see the txn's own writes

**Decision.** The current-state body of `range()` was extracted into
`Store::range_current(&StoreState, &RangeRequest)`. In-txn ranges call it with
the write guard dereferenced, so they read the already-mutated state.

**Alternatives.**

| option | pros | cons |
|---|---|---|
| run all ranges *before* the writes (pre-txn snapshot) | easy | contradicts measured etcd behaviour (read-your-writes); a `put`-then-`get` txn would return stale data |
| call `self.range()` per read, releasing the lock | no refactor | another writer can interleave and observe the half-applied txn — exactly the bug being fixed |
| take a read lock while holding the write lock | no refactor | self-deadlock: `parking_lot` is not re-entrant |
| extract the current-state range (chosen) | no second lock, no duplication, correct semantics | the range body had to move; historical reads cannot use it (§3.6) |

**Note.** `range_current` is an associated function rather than a free function
purely for locality with `range()`; it takes no `self`.

### 3.6 Historical ranges inside a txn are deferred

A `Range` with `revision > 0` needs the WAL *and* takes a read lock. Under the
txn's write lock both are impossible (deadlock), and running it in place after
temporarily dropping the lock would expose the txn's partial writes.

**Decision.** Defer these ops, record the response path (`Vec<usize>` through
nested txns), finish the txn, release the lock, then execute them through the
normal `self.range()` and patch the response tree.

**Why it is correct:** the requested revision is at or below the pre-txn
current revision, so the txn's own writes (at a later revision) cannot appear in
that view.

**Alternatives.**

| option | pros | cons |
|---|---|---|
| reject with `InvalidArgument` | trivial | invents a deviation; etcd supports it |
| run in place under the write lock (make `range_historical` take `&StoreState`) | no bookkeeping | a WAL scan while holding the write lock: up to ~100 ms measured on a 40 MB WAL, and it grows with the WAL — it would stall every writer |
| defer + patch (chosen) | no writer stall, correct result | two-phase response assembly; a path vector per deferred op |

Covered by `txn_historical_range_is_deferred_and_unaffected`, which checks both
that the deferred range is filled in and that it returns the *historical* value,
not the txn's.

### 3.7 Nested transactions

**Decision.** A nested txn runs under the same lock and shares the same
revision; nesting is limited to one level, as etcd does.

**Alternatives.** Flattening nested ops changes the response shape. A separate
revision per nesting level is as wrong as per-op revisions. Removing the depth
limit invites recursion on attacker-controlled input.

Deferred historical ranges inside a nested txn are recorded with a path like
`[outer_index, inner_index]` and patched by walking `ResponseTxn` nodes.

### 3.8 Duplicate-key validation

**Decision.** Before anything is applied, both branches are validated: writes
(puts and delete ranges) must be pairwise disjoint. Puts are point intervals
`[k, k\0)`; delete bounds are normalised to `[start, end)` with `None` meaning
unbounded (`From`, `All`, or a prefix of all `0xFF`). Overlap is the standard
interval test, `O(n²)` over at most a handful of ops.

**Alternatives.**

| option | pros | cons |
|---|---|---|
| only compare put keys | simple | misses `delete(a) + put(a)` and `delete(prefix) + put(inside)`, both of which etcd rejects |
| include range *reads* in the overlap set | fewer special cases | rejects `get(k) + put(k)`, which etcd **allows** — measured, and used by optimistic-concurrency patterns |
| sort + sweep | O(n log n) | pointless for ≤ 128 ops; more code |

**Edge cases handled:** empty intervals (`Range` with `start >= end`) overlap
nothing; a prefix ending in `0xFF` has no upper bound; a point key is `[k, k\0)`
so `k` does not collide with `k2`.

### 3.9 Error atomicity: knowingly left as-is

A **dynamic** validation error in the middle of a multi-op txn — the only such
case is `ignore_value`/`ignore_lease` on a key that does not exist at that point
in the txn — leaves the earlier ops applied at the txn revision. etcd rolls the
whole txn back.

**Options considered.**

| option | pros | cons |
|---|---|---|
| static validation only (chosen) | simple; catches every client-side mistake (empty key, `value is provided`, `lease is provided`, duplicate keys) before any write | the dynamic case can still partially apply |
| simulate an overlay of touched keys/ranges and pre-check `ignore_*` | fully atomic without touching the WAL path | ~40 lines of shadow bookkeeping that must mirror `put`/`delete_range` exactly; easy to drift and hard to be confident about |
| stage WAL records + watch events, commit at the end, undo log on failure | the real design: full rollback | WAL buffering, event buffering, undo of `keys`/`key_count`/revision, and new failure paths — a much larger change than the finding requires |

**Why this is acceptable now:** every txn kube-apiserver issues has a single
write, where the failure happens before any write. The gap only exists for
multi-op txns that mix a write with `ignore_*` on a key that the same txn does
not create. It is recorded here and in the PR rather than left implicit.

**It is also measured, not just documented.** `txn.dynamic_error_atomicity` runs
a two-op txn whose second op fails this way and reports whether the first write
survived:

```
etcd    : code=InvalidArgument msg=etcdserver: key not found first_write_survived=false
rudurru : code=InvalidArgument msg=etcdserver: key not found first_write_survived=true
```

That entry sits in `tests/conformance/known-deviations.txt` (annotated with a
pointer to this section), so the gap is visible in every CI run and disappears
from the ledger the day error rollback is implemented — the same mechanism that
keeps the rest of the audit honest.

### 3.11 Alternative considered: defer *every* read (rejected)

**The proposal.** Use one path for all in-transaction reads: defer them past the
unlock like historical ones, pinning a non-historical read to the transaction's
own revision (or to the pre-transaction revision when no write has happened
yet). Claimed benefits: one read path instead of two, and no read work inside
the write lock.

**Why it is not equivalent.** etcd's rule is *writes so far*, not *all writes in
the transaction*:

| transaction | etcd | defer + pin |
|---|---|---|
| `get(k)`, then `put(k)` | pre-txn value | pre-txn value (pin to the pre-txn revision) ✅ |
| `put(k)`, then `get(k)` | new value | new value (pin to the txn revision) ✅ |
| `put(a)`, `get(b)`, `put(b)` | **old `b`** | **new `b`** ❌ |

Measured against etcd v3.5.30: `get(b)` between `put(a)` and `put(b)` returns
`"old"`. etcd's `TxnWrite.Range` reads at `beginRev+1` once the transaction has
made changes, and the backend only contains the writes issued so far — so a read
observes exactly the writes that precede it.

With one revision per transaction (§3.1) "writes so far" is not expressible: all
of the transaction's records share `mod_revision = R`, so a revision-pinned read
sees every one of them, including later ones. Being exact requires either
sub-revisions or reading at the operation's position inside the critical
section — which is the current design.

**What the performance claim is worth.** The cost of an inline read is exactly
its occupancy of the write lock. Measured in release with 50 000 keys:

| read shape | time |
|---|---|
| point `get` (the only shape kube-apiserver issues in a txn) | **84 ns** |
| prefix scan over all 50 000 keys | 2.18 ms |

So the change would recover ~84 ns per read for the target workload, and 2.18 ms
only for a range that no Kubernetes transaction issues.

**And it would not be simpler.** Making the deferral *correct* needs the
pre-txn/txn revision distinction, and reads that precede a later write still
require the inline path — two paths, plus a pre-scan to classify each read. The
version that is genuinely one path is the version that is wrong in the table's
third row.

**Decision.** Keep the inline reads and the historical deferral. If read work
inside transactions ever becomes significant (large ranges in txns — not
something the control plane does), the exact optimisation is: defer reads with
no later write in the same transaction (pinning to the pre-txn revision when
nothing precedes them, the txn revision otherwise) and keep the inline path only
for reads with writes on both sides.

`txn.read_between_writes` is now a conformance check, so this property is pinned
regardless of which design wins later.

### 3.10 Response assembly

The txn's `header` is taken from the state *after* applying, so it equals the
txn revision (or the current revision for a read-only txn). Nested responses get
their own header at the same revision.

---

## 4. Performance

No hot path outside `txn` changed; `put`/`delete_range` are the same code moved
behind a function call.

| aspect | before | after |
|---|---|---|
| lock acquisitions for a 3-write txn | 4 (1 read for compares + 3 writes) | **1** |
| revisions consumed by a rejected put | 1 | 0 |
| revisions consumed by a read-only txn | 0 | 0 |
| in-txn read | full `range()` with its own lock acquisition | `range_current` on the held guard: no lock, no WAL |
| historical range in a txn | inline | deferred (one small `Vec`), executed after unlock |

The conformance bench (`sort_bench`) and the `range.*` checks are unaffected, so
the range watch paths keep their measured numbers.

Cost accepted: the write lock is held for the whole transaction instead of per
op. For k8s traffic that is the same wall time (single op) with fewer
acquisitions; for a large multi-op txn it serialises other writers for the txn's
duration, which the design bounds by deferring the only unbounded work.

---

## 5. Testing

- `txn_writes_share_one_revision` — three writes, one revision; nothing visible
  before it; no revision exists after it yet (future-revision error).
- `txn_read_own_writes_and_reject_duplicate_keys` — read-your-writes and the
  duplicate-key error.
- `txn_historical_range_is_deferred_and_unaffected` — the deferred path returns
  the historical value, not the txn's.
- `txn.revisions_for_3_writes` / `txn.atomic_mid_revision` in the differential
  harness went from deviations to matches.
- `txn.read_own_writes`, `txn.duplicate_write_keys`, `txn.read_between_writes`
  and `txn.dynamic_error_atomicity` were added as permanent checks (the last one
  pins the §3.9 limitation, the third pins the property that rules out §3.11).
- The ledger is **2 entries**: `auth.enable` (accepted) and
  `txn.dynamic_error_atomicity` (the documented limitation above). Before this
  PR it was 3, two of which were the torn-transaction checks.
- `tests/txn_test.rs::test_txn_nested` previously asserted the *stub* behaviour
  ("inner txn ops are not executed") — updated in the nested-txn PR; this PR
  keeps it green.

## 6. Risks

| risk | mitigation |
|---|---|
| write lock held longer | only unbounded work (historical ranges) was moved out; everything else is O(log n) per op |
| deferred-response patching misaligns a nested response | path is recorded during construction and walked structurally; covered by the historical-range test |
| duplicate-key rule too strict / too loose | both rules measured against etcd before implementing, and two checks are now permanent in the harness |
| `put_at` called without validation | only two call sites, both validated; documented on the function |

## 7. Follow-ups

1. **Error rollback** for multi-op txns (§3.9) — staged commit + undo log.
2. **`maxTxnOps` / request size limits** (issue #20) — etcd rejects oversized
   txns with `ResourceExhausted`; we have no limit at all.
3. **Nested historical ranges** are implemented but only unit-tested indirectly;
   a conformance check would be cheap.
4. `Snapshot` format and `Status`/`HashKV` accuracy (issues #18, #19).

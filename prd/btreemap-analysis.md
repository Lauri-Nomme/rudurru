# BTreeMap Data Structure Analysis

**Date:** 2026-06-21

## Access Pattern Summary

| Pattern | Operations | Frequency | BTreeMap complexity |
|---------|-----------|-----------|-------------------|
| Range/prefix scan | Get, DeleteRange | **Dominant** (k3s does prefix scans) | O(log n + k) |
| Point lookup | Put (prev state), Txn compare, point Delete | Per-write (~2-3x) | O(log n) |
| Full iteration | Compaction, store hash, lease expiry | Rare (periodic) | O(n) |
| Insert | Put | Per-write (1x) | O(log n) |
| Delete from map | **None** (tombstoned, not removed) | Never under normal op | N/A |

## Why BTreeMap is the Right Choice

1. **Range/prefix queries are the dominant pattern** — k3s/etcd storage is built on prefix scans (`/registry/pods/...`, `/registry/services/...`). BTreeMap's `range()` delivers O(log n + k), which is asymptotically optimal for sorted ordered data. HashMap would be unusable here (no ordering at all).

2. **No deletions = no rebalancing penalty** — Keys are tombstoned in-place (`delete_revision != 0`), never removed from the BTreeMap. The rebalancing cost of deletions is never incurred.

3. **Sorted order is a protocol requirement** — etcd returns results sorted by key. BTreeMap provides this for free; any unordered structure (HashMap) would require O(k log k) sorting on every range response.

4. **Cache-efficient node layout** — BTreeMap stores multiple keys per node (contiguous arrays), unlike binary search trees with pointer chasing. This matters at scale (2.35M keys in stress test).

## Why Alternatives Would Be Worse

| Alternative | Problem |
|-------------|---------|
| `HashMap<Vec<u8>, _>` | No range/prefix queries; needs a second ordered index |
| `Vec<(Vec<u8>, _)>` | O(n) inserts — catastrophic at 105K writes/sec |
| Skip list | Worse cache locality, higher memory overhead (forward pointers), no clear win |
| Radix tree | Interesting (shared key prefixes) but unproven for this workload; would need custom implementation |

## Bottleneck Reality Check

From profiling (`prd/stress-test.md`): `__memcmp_avx2_movbe` (BTreeMap key comparison) is only **4.09% of CPU**. The actual write path is dominated by lock acquisition + WAL fsync + serialization (~11us total, of which BTreeMap insert is ~0.5us). **BTreeMap is not the bottleneck.**

## Verdict

**BTreeMap is the optimal choice.** The combination of dominant range-scan access patterns, no-deletion semantics, and the protocol requirement for sorted output makes it strictly superior to HashMap, Vec, or skip lists.

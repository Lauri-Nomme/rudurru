# Custom Key Comparator Analysis

**Date:** 2026-06-21

## Question

Can a custom BTreeMap key comparator improve performance over the default `Vec<u8>::Ord`?

## Current State

The store uses `BTreeMap<Vec<u8>, KeyState>`. The comparator is `Vec<u8>`'s default `Ord` implementation, which delegates to `memcmp` — a lexicographic byte-by-byte comparison. This accounts for **4.09% of CPU** as `__memcmp_avx2_movbe` (`prd/stress-test.md:85`).

## Why the Default Comparator is Already Optimal

### 1. memcmp is SIMD-accelerated

`__memcmp_avx2_movbe` uses AVX2 vector instructions (256-bit registers), comparing 32 bytes per cycle on modern x86-64 hardware. There is no faster general-purpose byte sequence comparison.

### 2. Rust BTreeMap doesn't support custom comparators

Unlike C++ `std::map` (which accepts a comparator template parameter), Rust's `BTreeMap` uses the `Ord` trait on the key type. To change comparison behavior, you must wrap the key in a newtype:

```rust
struct KeyBuf(Vec<u8>);
impl Ord for KeyBuf { fn cmp(&self, other: &Self) -> Ordering { self.0.cmp(&other.0) } }
```

This adds wrapper allocations and `KeyBuf::new(key)` calls at every insertion/lookup site (dozens of locations) for **zero performance gain** — the inner comparison is still `memcmp`.

### 3. No algorithmic shortcut exists

etcd keys are paths (`/registry/pods/namespace/name`). Lexicographic ordering is required for correct prefix scan behavior. Consider what a custom comparator could try:

| Idea | Problem |
|------|---------|
| Skip common prefix | Unknown at comparison time — BTreeMap compares arbitrary keys, not sequential ones. Precomputed prefix would be stale on every write. |
| Compare only suffix | Breaks lexicographic ordering — `/registry/pods/a` vs `/registry/services/b` must sort by the full path. |
| Integer encoding of path segments | Adds encode/decode cost; no faster than memcmp for the comparison itself. |
| Use `&[u8]` instead of `Vec<u8>` for keys | Would require keys to live elsewhere (e.g., arena) — complex memory management, no comparator speedup. |

### 4. Common prefix is a memcmp strength, not weakness

For prefix scans (`/registry/pods/...`), memcmp compares from byte 0 forward, finding the first difference byte. This is the **same work** any comparator must do. A custom comparator cannot skip the common prefix without knowing it at compile time.

## Verdict

**Do not change.** The default `Vec<u8>::Ord` → `memcmp` → `__memcmp_avx2_movbe` is the optimal implementation for lexicographic byte comparison. A wrapper newtype would add complexity at dozens of call sites for zero performance gain. The 4.09% CPU cost is an unavoidable baseline of any ordered KV store.

# P7 WAL Format Migration: Old Format → Protobuf-Native

**Date:** 2026-06-15
**Cluster:** changwang (control-plane) + precision (worker), 215d uptime
**Migration:** Binary WAL format upgrade — old `WalRecord` (magic `0x5255`, 23-byte header) → protobuf-native `KvWalRecord` (9-byte header, zero-copy field access)
**Workloads:** Rancher, cert-manager, fleet, HomeAssistant, Immich, Mastodon, Odoo, ntfy, registry, xrdp

## Timeline

### 2026-06-14: P7 Phase 2 Implementation

- Implemented protobuf-native WAL format with overlong varints for O(1) key/mod_revision access
- Proto patching in `build.rs`: changed `mvccpb.KeyValue` → `bytes` in all response fields before codegen
- Stripped old-format WAL backward compat (`AnyRecord`, `scan_any`) from main code, kept `WalRecord` for migrator
- Wrote `wal_migrate.rs` — converts old-format to new-format WAL with state tracking (`create_revision`, `version`)
- Wrote `walverify_new.rs` — new-format verifier with CRC validation and state reconstruction

### 2026-06-14/15: Performance Tests & Design Docs

- Benchmarked P7: 2.4× latency improvement, 3.2× throughput
- Peak 101K ops/s @ 32 workers
- Documented in `prd/perf-test-p7.md`
- Design doc: `prd/p7-zero-copy-design.md` — 5 approaches considered, proto patching selected

### 2026-06-15: Code Review & Quality

- CI green on `p7-protobuf-wal` (formatting, clippy, build + integration tests)
- Bottleneck analysis documented in `prd/optimization.md` — 21 findings (A-U)
- PR #1 merged to `master`
- Copilot PR review: 5 real bugs fixed, 2 skipped (correct behavior), 1 minor optimization deferred
  - `rec_len` underflow check added to `KvWalRecord::deserialize`
  - `IS_CREATE` flag now only set on creates, not updates
  - Delete WAL records use delete-operation revision instead of pre-delete `mod_revision`
  - `prev_kv` filtered per-watcher flag in `notify_watchers`

### 2026-06-15 19:28:58 — Stop & Backup

```bash
sudo systemctl stop rudurru
sudo cp /vokk/rudurru/wal /vokk/rudurru/wal.bak.20260615-192858
```

Old WAL: 247,374,078 bytes (old format, magic `0x5255`)
Backup preserved at `/vokk/rudurru/wal.bak.20260615-192858`

### 2026-06-15 19:29:02 — Migration

```bash
sudo wal_migrate /vokk/rudurru/wal /tmp/rudurru-migrated-prod.wal
```

| Metric | Value |
|--------|-------|
| Old-format records read | 358,735 |
| Progress interval | every 10,000 records |
| Migration time | ~4 seconds |
| New-format file size | 253,895,688 bytes (+2.6% overhead for protobuf encoding + header) |

The migrator reads each old-format `WalRecord`, tracks per-key state (`create_revision`, `version`, `lease`), and writes new-format `KvWalRecord` with correct `IS_CREATE`/`DELETED`/`HAS_LEASE` flags.

### 2026-06-15 19:29:07 — Verification

```bash
sudo walverify_new /tmp/rudurru-migrated-prod.wal
```

| Check | Result |
|-------|--------|
| Records | 358,735 |
| CRC errors | **0** |
| Key/mod_revision accessor errors | **0** |
| Reconstructed live keys | 1,711 |
| Max revision | 358,725 |
| Key categories | 26 |
| Infrastructure keys | all 8 present |

### 2026-06-15 19:29:33 — Deploy & Restart

```bash
sudo cp /tmp/rudurru-migrated-prod.wal /vokk/rudurru/wal
sudo cp target/release/rudurru /usr/local/bin/rudurru
sudo systemctl start rudurru
```

### 2026-06-15 19:30:34 — Stabilized

First status log after k3s reconnection:

```
rudurru status rev=358924 keys=1781 watchers=141 leases=6 wal_size=254151213
```

**Zero errors** during monitoring. k3s reconnected all watchers (141), leases (6), and revision advancing normally (358725 → 358924 in 5 minutes).

## Migration Details

### Old Format (`WalRecord`)

```
[magic(2) | revision(8) | crc32(4) | key_len(4) | val_len(4) | flags(1) | key(N) | value(N) | lease(8)]
```

- Header: 23 bytes fixed
- Key/value stored as raw byte arrays
- No protobuf encoding — data must be decoded and re-encoded on every read
- Magic `0x5255` at start of every record

### New Format (`KvWalRecord`)

```
[flags(1) | key_offset(2) | mod_rev_offset(2) | rec_len(4) | kv_bytes(N) | crc32(4)]
```

- Header: 9 bytes fixed
- `kv_bytes` is a valid `mvccpb.KeyValue` protobuf with overlong varints
- Key accessed via offset in O(1) — no protobuf decode needed
- Mod_revision accessed via offset in O(1) — no protobuf decode needed
- No magic — records are self-describing via `rec_len`

### State Tracking During Migration

The migrator maintains a `BTreeMap<Vec<u8>, (create_revision, version, value, lease)>` across all records to correctly compute:

| Field | Source |
|-------|--------|
| `create_revision` | First occurrence of key (IS_CREATE=0x02 in old format implies no create -- use current revision) |
| `version` | Incremented on each put after deletion or first create |
| `mod_revision` | Record's revision number |
| `lease` | From old record's lease_id (if HAS_LEASE flag set) |

### Bug Fix for Delete Records (copilot review item 3-5)

During the migration and in live code, delete WAL records now store the **delete operation's revision** as `mod_revision`, not the pre-delete value's `mod_revision`. This ensures:

- `max_rev` correctly advances past delete operations during WAL replay
- No revision reuse after restart
- The embedded `mvccpb.KeyValue` represents the key state with the delete timestamp, not the last update timestamp

## Before/After

| Aspect | Before | After |
|--------|--------|-------|
| WAL format | Old `WalRecord` (magic 0x5255) | Protobuf-native `KvWalRecord` |
| WAL size | 247 MB | 254 MB (+2.6%) |
| Records | 358,735 | 358,735 (identical) |
| Live keys | 1,711 | 1,711 (identical) |
| Max revision | 358,725 | 358,725 (identical) |
| Binary version | 3a5bfd0-dirty (pre-P7) | ed04ec5 (P7 + fixes) |
| Rust runtime % | 0.00% (idle) | 0.00% (idle — same) |

## Files Created

| File | Purpose |
|------|---------|
| `src/bin/wal_migrate.rs` | Old→new WAL converter |
| `src/bin/walverify_new.rs` | New-format WAL verifier |
| `prd/p7-zero-copy-design.md` | Design doc for zero-copy approach |
| `prd/perf-test-p7.md` | P7 benchmark results |
| `prd/wal-migration.md` | This document |

## Files Modified

| File | Change |
|------|--------|
| `src/storage/wal.rs` | Added `KvWalRecord`, overlong varints, removed old `WalRecord::append`/`scan` |
| `src/storage/mod.rs` | Switched to kv_bytes pipeline, fixed IS_CREATE flag, fixed delete mod_revision |
| `src/server/watch.rs` | Raw bytes event pipeline, offset-based key extraction |
| `build.rs` | Proto patching for zero-copy gRPC |
| `prd/optimization.md` | Added codebase-wide bottleneck analysis (21 items) |

## Rollback

To revert to old format:

```bash
sudo systemctl stop rudurru
sudo cp /vokk/rudurru/wal.bak.20260615-192858 /vokk/rudurru/wal
# Install pre-P7 binary (commit 3a5bfd0-dirty or earlier)
sudo systemctl start rudurru
```

The old-format binary cannot read the new-format WAL (no backward compat in main code). The backup WAL is preserved.

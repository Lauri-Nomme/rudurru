# Progress & Design Log

## Phase 3 — Watch (2026-06-13)

### Design Decisions

**1. Push-based notification via `mpsc::UnboundedSender`**
Each watcher gets its own `UnboundedSender<WatchEvent>` stored in `WatchRegistration`. `notify_watchers` iterates all registrations and pushes matching events. No polling loop, no timer. Matches the PRD tenet "no polling."

**2. WAL replay inside the write lock**
When a watcher specifies `start_revision > 0`, the handler acquires the store's write lock, scans the WAL, sends matching historical events to the watcher's channel, and registers the watcher — all atomically. This prevents the race where a concurrent write between replay and registration would be missed.
- *Tradeoff:* The write lock is held during WAL scan. WAL scan reads the entire file and deserializes every record. For a large WAL this could block writes for milliseconds. Acceptable because k3s workloads write ~2.4 ops/sec.

**3. Watcher matching via `resolve_range` + `matches_range`**
`WatchRegistration` stores the raw `key` + `range_end` from the proto, not a precomputed prefix. `notify_watchers` calls `resolve_range` + `matches_range` on every event to check match. This supports all range types (point, prefix, from-key, range) without maintaining separate match logic.
- *Tradeoff:* Recomputes the range bound on every event notification. Could cache the `RangeBound` in the registration. Not worth optimizing until profiling shows it.

**4. Bounded channel for gRPC output stream, unbounded for internal event forwarding**
The tonic `ReceiverStream` wrapper requires a bounded `mpsc::Receiver`. Used `mpsc::channel(4096)` for the client-facing stream. Internal watcher-to-forwarder channels use `mpsc::unbounded_channel()` so the write lock holder never blocks on a slow consumer.
- *Tradeoff:* Unbounded channels can grow memory if the client is very slow. Acceptable because watch events are small and the channel is drained by a dedicated tokio task.

**5. Per-watcher forward tasks**
Each `WatchCreateRequest` spawns a `tokio::spawn` task that reads from the watcher's `event_rx` and writes WatchResponses to the shared output `tx`. The forward task exits when the client disconnects (send error on `tx`) or the watcher is canceled (sender dropped, `event_rx.recv()` returns `None`).

**6. Global `AtomicI64` watch_id counter**
`NEXT_WATCH_ID` is a global atomic, starting at 1. The proto allows client-assigned IDs (non-zero), so the server only assigns IDs when the client sends `watch_id=0`. A per-stream counter would be sufficient but a global one keeps it simple.

**7. Progress response as immediate reply**
`WatchProgressRequest` sends back a `WatchResponse` with header only (no events, `watch_id=0`). Periodic progress notifications (`progress_notify` flag on create) are **not implemented** — the server sends no unsolicited progress responses.

### CRC32C Bug Discovered & Fixed

The WAL `serialize()` computed CRC32C over `buf[17..]` (last byte of key_len + all of val_len + flags + key + value + lease), while `deserialize()` verified CRC32C over `data[23..ofs]` (key + value + lease, **excluding** the flags byte). These ranges were inconsistent.

Result: every WAL record since the project began failed CRC verification on read. `WalFile::scan()` silently returned an empty record list (the `Err` branch broke out of the parsing loop). This meant:
- Crash recovery was completely broken — `Store::open()` rebuilt an empty store from WAL, ignoring all persisted data.
- Watch WAL replay (`test_watch_from_revision`) returned 0 records, causing a timeout.

**Fix:** Both `serialize` and `deserialize` now compute CRC over `[flags_byte .. end_of_payload]` — flags(1) + key(N) + value(M) + lease_id(8). `serialize` uses `buf[22..]`, `deserialize` records `flags_ofs = ofs - 1` after reading the flags byte.

This bug was invisible to KV/Txn integration tests because they operate on the live in-memory store and never restart the server.

### Doubts & Unresolved Questions

1. **WAL replay creates synthetic events.** During replay, `create_revision` and `mod_revision` are set to `rec.revision` (the WAL record's revision, which is the delete/write revision), not the original `create_revision`. `version` is set to 1, `prev_kv` is None. This is incorrect for keys that were created and modified multiple times, but acceptable because the WAL format doesn't store this metadata per record — it would need a separate index.

2. **No compaction awareness in watcher WAL replay.** If `start_revision` is below `compact_rev`, the watcher should receive a `WatchResponse` with `compact_revision` set and `canceled: true`. Currently, replay returns nothing for compacted revisions and the watcher stays registered, waiting for events that will never come. This would manifest as a hung watcher if a client reconnects after compaction.

3. **Watcher cleanup on disconnect is indirect.** When the client disconnects, the main handler loop gets `Ok(None)` from `in_stream.message()` and exits. The `tx` sender is dropped, closing the output channel. Forward tasks detect the send error and cancel their watcher. However, if the main handler is blocked on `in_stream.message()` (waiting for the next request) and the client disconnects silently, tonic should return `None` — but this depends on the TCP keepalive / HTTP/2 ping behavior. A `Drop`-based cleanup guard on the response channel would be more robust.

4. **`ReceiverStream::new(rx)` return type.** Tonic's `ReceiverStream` wraps `mpsc::Receiver<T>` (bounded), not `UnboundedReceiver<T>`. The current implementation uses `mpsc::channel(4096)` for `tx`/`rx` and `mpsc::unbounded_channel()` for per-watcher event channels — two different channel types with different back-pressure behavior. This is an awkward asymmetry but works.

### Shortcuts / Known Gaps

| Gap | Impact | When to Fix |
|-----|--------|-------------|
| No `progress_notify` timer | Clients using `progress_notify` flag won't receive periodic progress updates | Phase 6 or if a client needs it |
| No compaction error on WAL replay | Watcher at compacted revision silently returns no events and stays registered | Phase 5 (Lease + compaction hardening) |
| WAL replay event metadata is approximated | `create_revision`, `version`, `prev_kv` in replayed events may be wrong | Requires storing KV metadata in WAL (breaking format change) |
| No `fragment` support | Large watch responses not split | If k3s watches large keys |
| Server has no graceful shutdown | Active watch streams are dropped on server exit | Phase 5 |
| `mpsc::unbounded_channel` memory unbounded | Slow consumer causes OOM risk | Add channel capacity limit or back-pressure |
| `resolve_range` called on every event notification | ~5 branches + BTreeMap iteration per event per watcher | Cache `RangeBound` in `WatchRegistration` |

### Test Coverage

- 5 watch tests pass against Rudurru: `test_watch_key`, `test_watch_prefix`, `test_watch_from_revision`, `test_watch_progress_notify`, `test_watch_delete_event`
- All 42 integration tests pass against real etcd (docker)
- Tests require `--test-threads=1` due to shared store design (writes from concurrent tests interfere)
- WAL replay specifically tested by `test_watch_from_revision` (puts v1/v2, watches from revision of v2, expects single event for v2)

### Files Changed

- `src/server/watch.rs` — new full implementation (previously stub)
- `src/storage/mod.rs` — `WatchRegistration` fields changed from `prefix` to `key`+`range_end`; `notify_watchers` uses `resolve_range`/`matches_range`; `register_watcher`/`cancel_watcher`/`as_ref` made `pub(crate)`; added `wal_path()` to `Store`
- `src/storage/wal.rs` — CRC32C range fixed in `serialize` (`buf[17..]` → `buf[22..]`) and `deserialize` (`data[23..ofs]` → `data[flags_ofs..ofs]`)
- `src/main.rs` — `EnvFilter` changed from always-`info` to `try_from_default_env` with `info` fallback

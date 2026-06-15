# P7: Zero-Copy gRPC Responses via Proto Patching

## Problem

We store `kv_bytes` (a valid `mvccpb.KeyValue` protobuf with overlong varints) in
`KeyState` and in the WAL (`KvWalRecord`). When serving gRPC responses, these
bytes need to reach the client. The naive path:

```
kv_bytes (Vec<u8>) → mvccpb::KeyValue (struct, decode) → prost re-encode → gRPC wire
```

This is double work: decode the protobuf we just encoded, then re-encode it for
the wire. We want:

```
kv_bytes (Vec<u8>) → gRPC wire   (zero copies of KeyValue data)
```

## Chosen Approach: Proto Patching in build.rs

**Core idea:** Before code generation, patch the `.proto` files to change all
`mvccpb.KeyValue` occurrences in response messages to `bytes`.

| Original | Patched | Generated Rust type change |
|----------|---------|---------------------------|
| `repeated mvccpb.KeyValue kvs = 2` | `repeated bytes kvs = 2` | `Vec<mvccpb.KeyValue>` → `Vec<Vec<u8>>` |
| `mvccpb.KeyValue prev_kv = 2` | `bytes prev_kv = 2` | `Option<Box<mvccpb.KeyValue>>` → `Vec<u8>` |
| `mvccpb.KeyValue kv = 2` (in Event) | `bytes kv = 2` | `Option<Box<mvccpb.KeyValue>>` → `Vec<u8>` |
| `mvccpb.KeyValue prev_kv = 3` (in Event) | `bytes prev_kv = 3` | `Option<Box<mvccpb.KeyValue>>` → `Vec<u8>` |

### Why This Works

Protobuf wire type 2 (length-delimited) is used for **both** `bytes` and
embedded messages (`message`). The wire format for a field is identical:

```
tag(varint) + length(varint) + payload(bytes)
```

The proto schema tells the *parser* how to interpret `payload` — as raw bytes or
as another protobuf message. On the wire, there is no difference.

When we send `repeated bytes kvs`, each entry is:
```
0x12 (tag for field 2, wire type 2) + varint(len(kv_bytes)) + kv_bytes
```

The client (k3s, etcdctl) uses the *original* proto schema
(`repeated mvccpb.KeyValue kvs`), so it parses each entry as an
`mvccpb.KeyValue` message. Since `kv_bytes` **is** a valid `mvccpb.KeyValue`
protobuf, the client decodes it correctly.

### Proto3 Default Value Semantics

For proto3, `bytes` fields have a default value of empty `Vec<u8>`. An empty
`Vec<u8>` is not encoded on the wire (proto3 omits default-valued fields). This
maps cleanly to the semantics of optional KeyValue fields:

- `PutResponse.prev_kv`: empty bytes = no previous value (same as `None`)
- `Event.prev_kv`: empty bytes = no previous KV (same as `None`)

### Implementation

In `build.rs`:

1. Read each `.proto` file
2. Apply string replacements to change the field types
3. Write patched files to `proto/patched/`
4. Compile from `proto/patched/rpc.proto` (which imports `patched/kv.proto`)
5. Clean up `proto/patched/`

The replacements are:
- `kv.proto`: `KeyValue kv = 2` → `bytes kv = 2`, `KeyValue prev_kv = 3` → `bytes prev_kv = 3`
- `rpc.proto`: import paths updated, `repeated mvccpb.KeyValue kvs = 2` → `repeated bytes kvs = 2`,
  `mvccpb.KeyValue prev_kv = 2` → `bytes prev_kv = 2`,
  `repeated mvccpb.KeyValue prev_kvs = 3` → `repeated bytes prev_kvs = 3`

The `mvccpb.KeyValue` message definition itself is **not** changed — it remains
in the generated code for internal use (e.g., `apply_record` still decodes
kv_bytes to populate `KeyState` fields).

### Data Flow (Range Response)

```
KeyState.kv_bytes (Vec<u8>, overlong KeyValue protobuf)
  → cloned into RangeResponse.kvs (Vec<Vec<u8>>)
    → prost encodes tag(0x12) + varint(len) + raw kv_bytes
      → client parses each entry as mvccpb.KeyValue
```

One `Vec::clone()` from KeyState to response. No protobuf decode of kv_bytes
until the client reads it.

---

## Alternatives Considered

### A. Custom prost::Message impl via UnknownFields / retain_unknown_fields

**Approach:** Enable `prost-build`'s `retain_unknown_fields` (or
`include_unknown_fields` from PR #1340) to add an `UnknownFields` field to
generated messages. Clear `kvs` and inject raw wire-format field entries into
unknown fields. Prost would encode known fields first, then append unknown
fields — producing the correct wire format.

**Pros:**
- No proto patching needed
- Generated types stay semantically correct
- Client code is unaffected

**Cons:**
- **`retain_unknown_fields` / `include_unknown_fields` do not exist in any
  released prost version** — they're from an unmerged PR (#1340) against
  `tokio-rs/prost`. Not available in prost 0.14.4.
- Even if available, unknown fields are encoded *after* all known fields, which
  breaks proto field ordering expectations. Protobuf guarantees that repeated
  fields from multiple appearances are concatenated, so the client would merge
  empty `kvs` from known fields + real data from unknown fields. This works,
  but is fragile.
- Requires the extra `UnknownFieldList` field on every generated struct (unless
  scoped per-type).

**Verdict:** Dead end — the feature doesn't exist in released prost.

### B. Custom prost::Message impl with manual encode_raw

**Approach:** Create a newtype wrapper around `RangeResponse` that implements
`prost::Message` manually, encoding kv_bytes directly.

**Pros:**
- Full control over encoding
- No proto changes

**Cons:**
- **Cannot change return type of the tonic service trait** —
  `tonic::Response<etcdserverpb::RangeResponse>` is fixed by the generated
  `Kv` trait. A newtype implements a different type, so it can't be returned
  from the trait method.
- Could use `unsafe` transmute, but the layouts differ (different fields).
- Requires modifying generated code or using raw tower services.

**Verdict:** Incompatible with tonic's type system without modifying generated
code.

### C. Raw tower::Service for range (bypass tonic encoding)

**Approach:** Implement a custom `tower::Service` that handles
`/etcdserverpb.KV/Range` by decoding the request with prost, calling the store,
building raw response protobuf bytes, framing them for gRPC (5-byte header +
payload), and returning them as an `http::Response<Body>`. Register this
service alongside the generated tonic `KvServer` for other RPCs.

**Pros:**
- Absolute control over encoding
- True zero-copy from store to wire
- Works with any prost version

**Cons:**
- **Tonic's Router doesn't support splitting a service by path** — the
  generated `KvServer` handles all paths under `/etcdserverpb.KV/`. You can't
  register a separate service for `/etcdserverpb.KV/Range` without the Router
  treating them as separate services with potential conflicts.
- Must duplicate tonic's gRPC framing (5-byte header, HTTP/2 headers, status
  codes, trailers).
- Must implement request decoding, error handling, compression — all things
  tonic provides for free.
- Fragile — tonic's internal framing could change.

**Verdict:** Works but is a significant amount of work for diminishing returns
over the proto patching approach.

### D. Pre-encode responses at the Store layer

**Approach:** Have `Store::range()` return a struct containing the pre-encoded
response bytes (built manually with prost encoding helpers). The gRPC handler
sends these bytes directly.

**Pros:**
- No proto changes
- Clear separation of concerns

**Cons:**
- **Same type system problem as B** — can't change the return type of the
  tonic trait method.
- Must still go through prost's `Message::encode` for the framing.
- Pre-encoding means the response is serialized twice if any post-processing
  is needed.

**Verdict:** Same fundamental issue as B — incompatible with tonic's trait.

### E. Keep existing approach (kv_bytes in KeyState, decode for responses)

**Approach:** Store kv_bytes in KeyState but decode them back to
`mvccpb::KeyValue` structs for responses (what we had before this patch).

**Pros:**
- Simplest code
- No build.rs hacks
- No compatibility concerns

**Cons:**
- **Every range response does a full protobuf decode + re-encode** of every
  KV entry. On a 10K-key range, that's 10K protobuf decode+re-encode cycles.
- The decode allocates Vec<u8> for key and value, the struct fields, then prost
  re-encodes everything — defeating the purpose of storing kv_bytes.

**Verdict:** Works but leaves performance on the table.

---

## Viability Analysis

### Wire Compatibility

This approach is **fully wire-compatible**. The protobuf wire format for
`bytes` and `message` is identical at the field level. Any protobuf client
(etcdctl, k3s, java etcd client) will decode each `kvs` entry as a
`mvccpb.KeyValue` from the raw bytes.

### Server-Side Decode

The server **never receives** response messages — it only sends them. So the
patched code generation affects only the encode path. Request messages are
unaffected.

### Txn Response Op

`ResponseOp` contains nested `RangeResponse`, `PutResponse`,
`DeleteRangeResponse`. Since we patch the leaf message types, the `oneof`
`ResponseOp.response` automatically picks up the new field types. No additional
changes needed.

### Overlong Varints

kv_bytes uses overlong varints for key_length (5 bytes) and mod_revision (10
bytes). Protobuf spec §2.2 requires decoders to accept non-minimal varint
encodings. All major protobuf implementations (C++, Go, Java, Rust) handle
this correctly. **No compatibility risk.**

### Proto Regeneration

The build.rs patching is idempotent and deterministic. It runs on every
rebuild, patches from the canonical proto files, and cleans up after itself.
If the proto files are updated (e.g., new etcd version), the patching adapts
automatically as long as the field names/tags remain the same.

### Robustness

The string replacements are fragile if proto field names change. Mitigations:
- The replacements target specific patterns like `mvccpb.KeyValue prev_kv = 2;`
  — the tag number serves as a checksum.
- If a replacement fails to match (proto upstream changes), the build will fail
  with a type error (expected `Vec<u8>`, found `Option<Box<KeyValue>>`),
  alerting the developer immediately.

---

## Summary

| Criterion | Assessment |
|-----------|------------|
| Zero-copy | ✅ One Vec clone from KeyState to response |
| Wire compatible | ✅ Identical wire format (both wire type 2) |
| Client compatibility | ✅ Client sees valid mvccpb.KeyValue entries |
| Server decode path | ✅ Unchanged (we never decode responses) |
| Txn support | ✅ Automatic (leaf types change, oneof propagates) |
| Overlong varints | ✅ Protobuf spec requires decoder acceptance |
| Maintenance | ⚠️ String replacements fragile but fail loudly |
| Complexity | Low: ~20 lines of build.rs, mechanical code changes |

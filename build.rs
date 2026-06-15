fn git_version() -> String {
    let describe = std::process::Command::new("git")
        .args(["describe", "--always", "--dirty", "--long"])
        .output();
    match describe {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => "unknown".to_string(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-env=GIT_REVISION={}", git_version());

    let proto_root = "proto";
    println!("cargo:rerun-if-changed={proto_root}");

    // ── Patch kv.proto: Event.kv and Event.prev_kv → bytes ──────────
    let kv_raw = std::fs::read_to_string("proto/kv.proto")?;
    let kv_patched = kv_raw
        .replace("KeyValue kv = 2;", "bytes kv = 2;")
        .replace("KeyValue prev_kv = 3;", "bytes prev_kv = 3;");
    std::fs::create_dir_all("proto/patched")?;
    std::fs::write("proto/patched/kv.proto", &kv_patched)?;

    // ── Patch rpc.proto: response fields mvccpb.KeyValue → bytes ────
    let rpc_raw = std::fs::read_to_string("proto/rpc.proto")?;
    let rpc_patched = rpc_raw
        .replace(r#"import "kv.proto""#, r#"import "patched/kv.proto""#)
        .replace(r#"import "auth.proto""#, r#"import "patched/auth.proto""#)
        .replace(
            "repeated mvccpb.KeyValue kvs = 2;",
            "repeated bytes kvs = 2;",
        )
        .replace("mvccpb.KeyValue prev_kv = 2;", "bytes prev_kv = 2;")
        .replace(
            "repeated mvccpb.KeyValue prev_kvs = 3;",
            "repeated bytes prev_kvs = 3;",
        );
    std::fs::write("proto/patched/rpc.proto", &rpc_patched)?;

    std::fs::copy("proto/auth.proto", "proto/patched/auth.proto")?;

    tonic_prost_build::configure()
        .build_server(true)
        // Response fields carrying pre-encoded mvccpb.KeyValue: use Bytes
        // (zero-copy sharing from store through gRPC serialization)
        .bytes("mvccpb.Event.kv")
        .bytes("mvccpb.Event.prev_kv")
        .bytes("etcdserverpb.RangeResponse.kvs")
        .bytes("etcdserverpb.PutResponse.prev_kv")
        .bytes("etcdserverpb.DeleteRangeResponse.prev_kvs")
        .compile_protos(&["proto/patched/rpc.proto"], &[proto_root])?;

    std::fs::remove_dir_all("proto/patched")?;
    Ok(())
}

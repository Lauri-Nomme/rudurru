fn git_version() -> String {
    let describe = std::process::Command::new("git")
        .args(["describe", "--always", "--dirty", "--long"])
        .output();
    match describe {
        Ok(out) if out.status.success() => {
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        }
        _ => "unknown".to_string(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-env=GIT_REVISION={}", git_version());

    let proto_root = "proto";
    println!("cargo:rerun-if-changed={proto_root}");

    tonic_prost_build::configure()
        .build_server(true)
        .compile_protos(
            &[
                "proto/auth.proto",
                "proto/kv.proto",
                "proto/rpc.proto",
            ],
            &[proto_root],
        )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

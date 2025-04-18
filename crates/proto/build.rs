use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir =
        PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR environment variable not set"));

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("world_descriptor.bin"))
        .compile(&["proto/world.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto");

    Ok(())
}

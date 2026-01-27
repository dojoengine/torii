use std::io::Write;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let target = std::env::var("TARGET")?;
    let feature_client = std::env::var("CARGO_FEATURE_CLIENT").is_ok();
    let feature_server = std::env::var("CARGO_FEATURE_SERVER").is_ok();

    if target.contains("wasm32") {
        if feature_server {
            return Err(format!(
                "feature `server` is not supported on target `{target}`"
            )
            .into());
        }

        wasm_tonic_build::configure()
            .build_server(false)
            .build_client(feature_client)
            .file_descriptor_set_path(out_dir.join("world_descriptor.bin"))
            .compile_protos(&["proto/world.proto"], &["proto"])?;
    } else {
        tonic_build::configure()
            .build_server(feature_server)
            .build_client(feature_client)
            .file_descriptor_set_path(out_dir.join("world_descriptor.bin"))
            .compile(&["proto/world.proto"], &["proto"])?;
    }

    std::io::stdout().write_all(b"cargo:rerun-if-changed=proto\n")?;

    Ok(())
}

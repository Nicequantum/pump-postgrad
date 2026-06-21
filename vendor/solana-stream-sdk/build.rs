use std::path::PathBuf;

fn main() {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &["proto/shredstream.proto"],
            &[PathBuf::from("proto")],
        )
        .unwrap();
}
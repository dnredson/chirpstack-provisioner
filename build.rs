fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc is required");
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/internal_bootstrap.proto"], &["proto"])
        .expect("compile ChirpStack bootstrap proto");
}

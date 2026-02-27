fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc");
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/replay.proto"], &["proto"])
        .expect("compile replay proto");
}

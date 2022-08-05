fn main() {
    let proto_file = "./src/ring/proto/ring.proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("./src/ring")
        .compile(&[proto_file], &["."])
        .unwrap_or_else(|e| panic!("protobuf comile error: {}", e));

    println!("cargo:rerun-if-changed={}", proto_file);
}

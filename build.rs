fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir should exist");
    let output = std::path::Path::new(&crate_dir)
        .join("include")
        .join("firelite.h");

    std::fs::create_dir_all(output.parent().expect("header parent should exist"))
        .expect("failed to create include directory");

    let cfg = cbindgen::Config::from_file("cbindgen.toml").unwrap_or_default();
    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(cfg)
        .generate()
        .expect("Unable to generate C header")
        .write_to_file(output);
}

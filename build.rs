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

    // ponytail: cdylib on Windows MSVC produces `firelite.dll.lib` as the
    // import library, but `#[link(name = "firelite")]` looks for `firelite.lib`.
    // The FFI round-trip test in tests/ffi_roundtrip.rs needs that file to
    // exist. Copy if absent — best-effort, no-op once present.
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
    let target_dir = std::path::Path::new("target").join(&profile);
    #[cfg(windows)]
    {
        let src = target_dir.join("firelite.dll.lib");
        let dst = target_dir.join("firelite.lib");
        if src.exists() && !dst.exists() {
            let _ = std::fs::copy(&src, &dst);
        }
    }
    // Allow integration tests to find firelite.dll when invoked from anywhere.
    println!("cargo:rustc-link-search=native={}", target_dir.display());
}

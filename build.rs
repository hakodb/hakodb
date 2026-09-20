fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir should exist");
    let output = std::path::Path::new(&crate_dir)
        .join("include")
        .join("firelite.h");

    std::fs::create_dir_all(output.parent().expect("header parent should exist"))
        .expect("failed to create include directory");

    // ponytail: absolute path — a relative "cbindgen.toml" silently misses
    // (build-script cwd is not guaranteed to be the package root), and
    // from_file().unwrap_or_default() then falls back to cbindgen DEFAULTS
    // (C++ output, no guard/prefix) with zero diagnostics. The on-disk
    // header proved it: C++ `using` aliases + <cstdarg> despite
    // language="C" in the toml. Absolute path or loud failure, never
    // silent defaults for a shipped ABI header.
    let cbindgen_toml = std::path::Path::new(&crate_dir).join("cbindgen.toml");
    let cfg = cbindgen::Config::from_file(&cbindgen_toml)
        .map_err(|e| format!("read {}: {e}", cbindgen_toml.display()))
        .expect("cbindgen.toml must load — refusing silent-default ABI header");
    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(cfg)
        .generate()
        .expect("Unable to generate C header")
        .write_to_file(output);

// ponytail: the old copy (dll.lib -> lib) was stale-by-design. It ran
// BEFORE rustc linked, so lib always lagged one build behind — and stayed
// there whenever cargo skipped the build script (no source changes). A
// stale .lib SHADOWS the fresh .dll in MinGW ld search order, producing
// undefined-reference ghosts for new symbols. Delete it instead: MinGW ld
// falls through to firelite.dll directly (always fresh), and no current
// consumer needs an MSVC import lib (benchmark + Go use MinGW; Pascal
// keeps its own .a). Deterministic, no timing, no silent staleness.
// MSVC exception: rustc emits the cdylib import library as
// `firelite.dll.lib`, which the release workflow ships (renamed to the
// conventional `firelite.lib`) — so the delete below runs on GNU/MinGW
// targets only, never on MSVC.
let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
let target = std::env::var("TARGET").unwrap_or_default();
let target_dir = std::path::Path::new("target").join(&profile);
#[cfg(windows)]
{
if !target.contains("msvc") {
    let dst = target_dir.join("firelite.lib");
    let _ = std::fs::remove_file(&dst);
}
}
    // Allow integration tests to find firelite.dll when invoked from anywhere.
    println!("cargo:rustc-link-search=native={}", target_dir.display());
}

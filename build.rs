fn main() {
    // (touch: force build-script rerun after OUT_DIR migration)
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir should exist");
    // ponytail: generated header goes to OUT_DIR, never the source tree —
    // `cargo publish --verify` (and good hygiene generally) forbids build
    // scripts from touching anything outside OUT_DIR. The checked-in
    // include/hako.h that C consumers actually use is refreshed explicitly:
    // HAKODB_REGEN_HEADER=1 cargo build. CI/on-demand diff keeps it honest.
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR should exist");
    let output = std::path::Path::new(&out_dir).join("hako.h");

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
        .with_crate(crate_dir.clone())
        .with_config(cfg)
        .generate()
        .expect("Unable to generate C header")
        .write_to_file(&output);
    // Opt-in refresh of the checked-in header for C consumers + release
    // assets. Deliberately env-gated so normal builds (and publish verify)
    // stay side-effect free.
    if std::env::var("HAKODB_REGEN_HEADER").is_ok() {
        let checked_in = std::path::Path::new(&crate_dir)
            .join("include")
            .join("hako.h");
        std::fs::copy(&output, &checked_in).expect("refresh checked-in hako.h");
        println!("cargo:warning=refreshed include/hako.h from cbindgen output");
    }

// ponytail: the old copy (dll.lib -> lib) was stale-by-design. It ran
// BEFORE rustc linked, so lib always lagged one build behind — and stayed
// there whenever cargo skipped the build script (no source changes). A
// stale .lib SHADOWS the fresh .dll in MinGW ld search order, producing
// undefined-reference ghosts for new symbols. Delete it instead: MinGW ld
// falls through to hakodb.dll directly (always fresh), and no current
// consumer needs an MSVC import lib (benchmark + Go use MinGW; Pascal
// keeps its own .a). Deterministic, no timing, no silent staleness.
// MSVC exception: rustc emits the cdylib import library as
// `hakodb.dll.lib`, which the release workflow ships (renamed to the
// conventional `hakodb.lib`) — so the delete below runs on GNU/MinGW
// targets only, never on MSVC.
let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
let target = std::env::var("TARGET").unwrap_or_default();
let target_dir = std::path::Path::new("target").join(&profile);
#[cfg(windows)]
{
if !target.contains("msvc") {
    let dst = target_dir.join("hakodb.lib");
    let _ = std::fs::remove_file(&dst);
}
}
    // Allow integration tests to find hakodb.dll when invoked from anywhere.
    println!("cargo:rustc-link-search=native={}", target_dir.display());
}

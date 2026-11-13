fn main() {
    // Header generation is opt-in via the `header` feature: release/tag CI
    // and sync scripts build with `--features header`, everything else
    // (Rust dependents above all) compiles zero header machinery — the
    // cbindgen build-dependency is optional and stays off, so dependent
    // builds pay neither its compile nor the whole-crate parse it drives.
    // Why not an env/primary-package signal: CARGO_PRIMARY_PACKAGE is only
    // set for rustc invocations, never for build scripts (rust-lang/cargo
    // #9403), and no env var reliably distinguishes "dependency build".
    // A feature forks dependent builds in theory, but this one only gates
    // build-script work — the library itself ignores it. HAKODB_SKIP_CBINDGEN=1
    // still forces a skip in header-enabled dev loops; set per-invocation,
    // not globally (it would strip headers from the build that produces
    // release assets).
    // ponytail: explicit early scope, not a flag threaded through —
    // link-search + lib hygiene below always run (dependents link
    // against them even in dev loops).
    #[cfg(feature = "header")]
    if std::env::var("HAKODB_SKIP_CBINDGEN").is_err() {
        gen_header();
    }
    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=cbindgen.toml");

    // One line of truth in every build's output: protects the header flow
    // from silent regressions (the 1.100 layout change produced none).
    println!(
        "cargo:warning=hakodb build-script: header feature={} skip_env={}",
        std::env::var_os("CARGO_FEATURE_HEADER").is_some(),
        std::env::var_os("HAKODB_SKIP_CBINDGEN").is_some(),
    );

    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR should exist");
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
    // ponytail: find the profile dir by NAME (PROFILE env), not by depth —
    // cargo 1.100 moved intermediate artifacts into a new layout, so a
    // fixed ancestor count no longer points at target/<profile>/. A shared
    // CARGO_TARGET_DIR (or any --target-dir) stays correct either way.
    let profile_dir = profile_dir_of(&out_dir, &profile);
    let target_dir = profile_dir.unwrap_or_else(|| {
        std::path::Path::new(&out_dir)
            .ancestors()
            .nth(3)
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::Path::new("target").join(&profile))
    });

// ponytail: a stale unprefixed import lib SHADOWS the fresh .dll in
// MinGW ld search order (it ran BEFORE rustc linked, so it always lagged
// a build behind), producing undefined-reference ghosts for new symbols.
// Delete it — MinGW ld falls through to hakodb.dll directly (always
// fresh). MSVC consumers link rustc's `hakodb.dll.lib` (shipped renamed
// as `hakodb.lib`), so the delete below runs on GNU/MinGW targets only.
#[cfg(windows)]
{
let target = std::env::var("TARGET").unwrap_or_default();
if !target.contains("msvc") {
    let dst = target_dir.join("hakodb.lib");
    let _ = std::fs::remove_file(&dst);
}
}
    // Allow integration tests to find hakodb.dll when invoked from anywhere.
    println!("cargo:rustc-link-search=native={}", target_dir.display());
}

#[cfg(feature = "header")]
fn gen_header() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir should exist");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR should exist");
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
    let output = std::path::Path::new(&out_dir).join("hakodb.h");

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
    let header = cbindgen::Builder::new()
        .with_crate(crate_dir.clone())
        .with_config(cfg)
        .generate()
        .expect("Unable to generate C header");
    // cbindgen 0.28: write_to_file reports a plain bool, not a Result —
    // a swallowed failure here is exactly the "build green, no header"
    // CI incident, so be loud.
    if !header.write_to_file(&output) {
        panic!("Unable to write C header to {}", output.display());
    }
    // ponytail: stable per-target copy next to the binaries. OUT_DIR hides
    // under the build dir — unusable as a consumer path, and its depth is
    // NOT stable (cargo 1.100 layout change), so the copy target is the
    // profile dir found by name. The profile dir is stable across hosts,
    // triples, and profiles, so release CI, sync scripts, and the
    // C++ bench all take `target/<...>/hakodb.h` from the build that
    // produced it. There is no checked-in header anymore (deleted in
    // 0.9.1: one header pretending to serve all targets caused the stale
    // socket-API incident). Writing inside target/ keeps `cargo publish
    // --verify` and source-tree hygiene intact.
    let profile_dir = profile_dir_of(&out_dir, &profile)
        .expect("profile dir not found above OUT_DIR — refusing to ship a header consumers can't find");
    std::fs::copy(&output, profile_dir.join("hakodb.h")).expect("Unable to copy C header");
}

/// The profile dir (`target/<profile>/`) above OUT_DIR, located by NAME:
/// the first ancestor whose final component matches the PROFILE env value
/// ("release"/"debug"). Depth-independent — survives cargo layout changes.
fn profile_dir_of(out_dir: &str, profile: &str) -> Option<std::path::PathBuf> {
    std::path::Path::new(out_dir)
        .ancestors()
        .find(|p| p.file_name().is_some_and(|n| n == profile))
        .map(|p| p.to_path_buf())
}

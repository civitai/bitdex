fn main() {
    // Files that are include_str!'d / include_bytes!'d into the binary must
    // be declared here so sccache / CI caches rebuild when they change.
    // Without these hints, asset-only edits can silently cache-miss in CI
    // and serve a stale binary.
    println!("cargo:rerun-if-changed=static/index.html");    // src/server.rs
    println!("cargo:rerun-if-changed=bitdex.default.toml");  // src/bin/server.rs

    #[cfg(target_os = "windows")]
    println!("cargo:rustc-link-lib=advapi32");
}

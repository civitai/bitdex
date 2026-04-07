fn main() {
    // static/index.html is include_str!'d into server.rs. Without this hint,
    // sccache / CI caches can serve a stale binary after HTML-only edits.
    println!("cargo:rerun-if-changed=static/index.html");

    #[cfg(target_os = "windows")]
    println!("cargo:rustc-link-lib=advapi32");
}

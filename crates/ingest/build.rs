use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=NPCAP_SDK_DIR");
    println!("cargo:rerun-if-env-changed=NPCAP_LIB_DIR");
    println!("cargo:rerun-if-env-changed=LIB");

    if env::var_os("CARGO_FEATURE_PCAP").is_none() {
        return;
    }

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_else(|_| String::from("x86_64"));
    let lib_subdir = if arch == "x86_64" { "x64" } else { "" };

    if let Some(lib_dir) = env::var_os("NPCAP_LIB_DIR") {
        let dir = PathBuf::from(lib_dir);
        if dir.join("wpcap.lib").exists() {
            println!("cargo:rustc-link-search=native={}", dir.display());
            return;
        }
    }

    if let Some(lib_env) = env::var_os("LIB") {
        for part in env::split_paths(&lib_env) {
            if part.join("wpcap.lib").exists() {
                println!("cargo:rustc-link-search=native={}", part.display());
                return;
            }
        }
    }

    let mut candidates = Vec::new();
    if let Some(sdk_dir) = env::var_os("NPCAP_SDK_DIR") {
        candidates.push(PathBuf::from(sdk_dir));
    }
    candidates.push(PathBuf::from(r"C:\Program Files\Npcap SDK"));
    candidates.push(PathBuf::from(r"C:\Program Files (x86)\Npcap SDK"));

    for root in candidates {
        let mut dirs = Vec::new();
        dirs.push(root.clone());
        dirs.push(root.join("Lib"));
        if !lib_subdir.is_empty() {
            dirs.push(root.join("Lib").join(lib_subdir));
            dirs.push(root.join(lib_subdir));
        }

        for dir in dirs {
            if dir.join("wpcap.lib").exists() {
                println!("cargo:rustc-link-search=native={}", dir.display());
                return;
            }
        }
    }

    println!(
        "cargo:warning=pcap feature enabled, but wpcap.lib not found; install Npcap SDK or set NPCAP_SDK_DIR"
    );
}

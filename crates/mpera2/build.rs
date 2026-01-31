use std::env;
use std::process::Command;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    dbg!(&crate_dir);
    let tinygrad_dir = std::path::Path::new(&crate_dir)
        .join("../../tinygrad")
        .canonicalize()
        .expect("failed to resolve tinygrad path");

    let status = Command::new("bash")
        .args(["-lc", "source ./.venv/bin/activate && uv --version"])
        .current_dir(&crate_dir)
        .status()
        .expect("Failed to activate venv");

    if !status.success() {
        panic!("venv activation failed failed");
    }

    let status = Command::new("uv")
        .args(["pip", "install", "-e", "."])
        .current_dir(&tinygrad_dir)
        .status()
        .expect("failed to execute uv");

    if !status.success() {
        panic!("uv pip install -e . failed");
    }

    let pwd = tinygrad_dir.to_str().expect("non-utf8 path");

    println!("cargo:warning=tinygrad installed from {}", pwd);
    println!("cargo:rustc-env=TINYGRAD_PWD={}", pwd);
    println!("cargo:rerun-if-changed={}", pwd);
}

//! Build script for syncode-contracts
//!
//! Generates TypeScript type definitions from Rust types annotated with `#[derive(TS)]`.
//! Sets TS_RS_EXPORT_DIR so ts-rs exports .d.ts files to the frontend's src/types/.

use std::env;
use std::path::PathBuf;

fn main() {
    // Point ts-rs exports to the frontend types directory
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let frontend_types = PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("frontend")
        .join("src")
        .join("types");

    // Create the output directory if it doesn't exist
    std::fs::create_dir_all(&frontend_types).ok();

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rustc-env=TS_RS_EXPORT_DIR={}", frontend_types.display());
}

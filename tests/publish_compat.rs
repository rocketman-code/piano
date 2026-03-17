//! Verify workspace settings that affect published piano-runtime manifests.
//!
//! `cargo publish` embeds the workspace's effective resolver into member
//! manifests. Resolver "3" (edition 2024 default) breaks Cargo < 1.84,
//! violating piano-runtime's MSRV of 1.59. The workspace must explicitly
//! set `resolver = "2"` to prevent this (rust-lang/cargo#11047).

use std::path::Path;

#[test]
fn workspace_resolver_is_2() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = std::fs::read_to_string(manifest_dir.join("Cargo.toml"))
        .expect("failed to read workspace Cargo.toml");
    let doc: toml_edit::DocumentMut = cargo_toml.parse().expect("failed to parse Cargo.toml");

    let resolver = doc
        .get("workspace")
        .and_then(|ws| ws.get("resolver"))
        .and_then(|r| r.as_str());

    assert_eq!(
        resolver,
        Some("2"),
        "workspace resolver must be explicitly \"2\" to prevent cargo publish from \
         embedding resolver = \"3\" into piano-runtime's published manifest, which \
         breaks Cargo < 1.84 (rust-lang/cargo#11047)"
    );
}

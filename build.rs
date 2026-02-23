use std::path::Path;

fn main() {
    // Read piano-runtime version at compile time so the CLI injects the correct
    // crates.io dependency, not its own package version.
    let runtime_cargo = Path::new("piano-runtime").join("Cargo.toml");
    println!("cargo::rerun-if-changed={}", runtime_cargo.display());

    let version = match std::fs::read_to_string(&runtime_cargo) {
        Ok(contents) => contents
            .parse::<toml_edit::DocumentMut>()
            .expect("failed to parse piano-runtime/Cargo.toml")
            .get("package")
            .and_then(|p| p.get("version"))
            .and_then(|v| v.as_str())
            .expect("missing [package].version in piano-runtime/Cargo.toml")
            .to_owned(),
        Err(_) => {
            // When installed from crates.io the workspace sibling is absent;
            // fall back to the CLI package version (kept in sync at publish time).
            std::env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION not set")
        }
    };

    println!("cargo::rustc-env=PIANO_RUNTIME_VERSION={version}");
}

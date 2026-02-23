use std::path::Path;

fn main() {
    // Read piano-runtime version at compile time so the CLI injects the correct
    // crates.io dependency, not its own package version.
    let runtime_cargo = Path::new("piano-runtime").join("Cargo.toml");
    println!("cargo::rerun-if-changed={}", runtime_cargo.display());

    let contents =
        std::fs::read_to_string(&runtime_cargo).expect("failed to read piano-runtime/Cargo.toml");

    let version = contents
        .parse::<toml_edit::DocumentMut>()
        .expect("failed to parse piano-runtime/Cargo.toml")
        .get("package")
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .expect("missing [package].version in piano-runtime/Cargo.toml")
        .to_owned();

    println!("cargo::rustc-env=PIANO_RUNTIME_VERSION={version}");
}

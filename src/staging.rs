//! Staging overlay: mirror a directory tree with symlinks, overlaying
//! instrumented file copies. Preserves all path resolution (modules,
//! include!, include_str!) by maintaining identical directory structure.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::error::{Error, io_context};

/// Guard that removes the staging directory tree on drop.
pub struct StagingGuard(pub PathBuf);

impl Drop for StagingGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Create a staging overlay of `ws_root` at `staging_root`.
///
/// For each file in `instrumented`, writes the provided content at the
/// corresponding relative path under `staging_root`. All other files and
/// directories are symlinked to the originals. Only ancestor directories
/// of instrumented files are created as real directories; all other
/// directories are symlinked wholesale.
///
/// Skips `target/` and `.git/` directories.
pub fn create_staging_overlay(
    ws_root: &Path,
    staging_root: &Path,
    instrumented: &[(PathBuf, String)],
) -> Result<(), Error> {
    // Collect ancestor directories that must be "real" (not symlinks)
    let mut real_dirs: HashSet<PathBuf> = HashSet::new();
    for (rel_path, _) in instrumented {
        let mut dir = rel_path.parent();
        while let Some(d) = dir {
            if d.as_os_str().is_empty() {
                break;
            }
            real_dirs.insert(d.to_path_buf());
            dir = d.parent();
        }
    }

    // Build a set for O(1) lookup
    let instrumented_set: HashSet<&Path> = instrumented.iter().map(|(p, _)| p.as_path()).collect();

    // Remove stale staging dir if present
    let _ = std::fs::remove_dir_all(staging_root);
    std::fs::create_dir_all(staging_root)
        .map_err(io_context("create staging directory", staging_root))?;

    populate_overlay(
        ws_root,
        staging_root,
        Path::new(""),
        &real_dirs,
        instrumented,
        &instrumented_set,
    )
}

fn populate_overlay(
    ws_root: &Path,
    staging_root: &Path,
    rel_prefix: &Path,
    real_dirs: &HashSet<PathBuf>,
    instrumented: &[(PathBuf, String)],
    instrumented_set: &HashSet<&Path>,
) -> Result<(), Error> {
    let source_dir = ws_root.join(rel_prefix);
    let staging_dir = staging_root.join(rel_prefix);
    std::fs::create_dir_all(&staging_dir)
        .map_err(io_context("create staging subdirectory", &staging_dir))?;

    let entries =
        std::fs::read_dir(&source_dir).map_err(io_context("read source directory", &source_dir))?;

    for entry in entries {
        let entry = entry.map_err(io_context("read directory entry", &source_dir))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Skip directories that are never referenced by source code
        if name_str == "target" || name_str == ".git" {
            continue;
        }

        let rel_path = if rel_prefix.as_os_str().is_empty() {
            PathBuf::from(&name)
        } else {
            rel_prefix.join(&name)
        };
        let staging_path = staging_dir.join(&name);
        let original_abs = std::fs::canonicalize(entry.path())
            .map_err(io_context("canonicalize path", &entry.path()))?;

        let file_type = entry
            .file_type()
            .map_err(io_context("read file type", &entry.path()))?;

        if file_type.is_dir() {
            if real_dirs.contains(&rel_path) {
                // This directory contains instrumented files; recurse
                populate_overlay(
                    ws_root,
                    staging_root,
                    &rel_path,
                    real_dirs,
                    instrumented,
                    instrumented_set,
                )?;
            } else {
                // No instrumented files in subtree, symlink entire directory
                std::os::unix::fs::symlink(&original_abs, &staging_path)
                    .map_err(io_context("create directory symlink", &staging_path))?;
            }
        } else if instrumented_set.contains(rel_path.as_path()) {
            // Instrumented file: write the modified content
            let content = &instrumented.iter().find(|(p, _)| p == &rel_path).unwrap().1;
            std::fs::write(&staging_path, content)
                .map_err(io_context("write instrumented file", &staging_path))?;
        } else {
            // Non-instrumented file: symlink to original
            std::os::unix::fs::symlink(&original_abs, &staging_path)
                .map_err(io_context("create file symlink", &staging_path))?;
        }
    }

    Ok(())
}

/// Remove stale staging directories from a previous crashed run.
pub fn clean_stale_staging(target_piano_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(target_piano_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name.to_string_lossy().starts_with("staging-") && entry.path().is_dir() {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_fake_project(root: &Path) {
        // src/main.rs
        std::fs::create_dir_all(root.join("src/args")).unwrap();
        std::fs::write(root.join("src/main.rs"), "fn main() {}").unwrap();
        std::fs::write(root.join("src/args.rs"), "mod command;").unwrap();
        std::fs::write(root.join("src/args/command.rs"), "pub fn run() {}").unwrap();
        std::fs::write(root.join("src/utils.rs"), "pub fn help() {}").unwrap();
        // data file for include!
        std::fs::write(root.join("data.txt"), "hello").unwrap();
        // subdirectory with no instrumented files
        std::fs::create_dir_all(root.join("templates")).unwrap();
        std::fs::write(root.join("templates/page.html"), "<h1>hi</h1>").unwrap();
    }

    #[test]
    fn overlay_creates_correct_structure() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path().join("project");
        let staging = tmp.path().join("staging");
        create_fake_project(&project);

        let instrumented = vec![
            (PathBuf::from("src/main.rs"), "// instrumented main".into()),
            (PathBuf::from("src/args.rs"), "// instrumented args".into()),
            (
                PathBuf::from("src/args/command.rs"),
                "// instrumented command".into(),
            ),
        ];

        create_staging_overlay(&project, &staging, &instrumented).unwrap();

        // Instrumented files are real files with modified content
        assert_eq!(
            std::fs::read_to_string(staging.join("src/main.rs")).unwrap(),
            "// instrumented main"
        );
        assert_eq!(
            std::fs::read_to_string(staging.join("src/args.rs")).unwrap(),
            "// instrumented args"
        );
        assert_eq!(
            std::fs::read_to_string(staging.join("src/args/command.rs")).unwrap(),
            "// instrumented command"
        );

        // Non-instrumented file is a symlink
        assert!(staging.join("src/utils.rs").is_symlink());
        assert_eq!(
            std::fs::read_to_string(staging.join("src/utils.rs")).unwrap(),
            "pub fn help() {}"
        );

        // data.txt is a symlink
        assert!(staging.join("data.txt").is_symlink());
        assert_eq!(
            std::fs::read_to_string(staging.join("data.txt")).unwrap(),
            "hello"
        );

        // templates/ is a symlinked directory (no instrumented files inside)
        assert!(staging.join("templates").is_symlink());
        assert_eq!(
            std::fs::read_to_string(staging.join("templates/page.html")).unwrap(),
            "<h1>hi</h1>"
        );

        // Ancestor directories of instrumented files are real dirs, not symlinks
        assert!(staging.join("src").is_dir());
        assert!(!staging.join("src").is_symlink());
        assert!(staging.join("src/args").is_dir());
        assert!(!staging.join("src/args").is_symlink());
    }

    #[test]
    fn overlay_skips_target_and_git() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path().join("project");
        create_fake_project(&project);
        std::fs::create_dir_all(project.join("target/debug")).unwrap();
        std::fs::write(project.join("target/debug/binary"), "bin").unwrap();
        std::fs::create_dir_all(project.join(".git/objects")).unwrap();
        std::fs::write(project.join(".git/HEAD"), "ref: refs/heads/main").unwrap();

        let staging = tmp.path().join("staging");
        let instrumented = vec![(PathBuf::from("src/main.rs"), "// inst".into())];

        create_staging_overlay(&project, &staging, &instrumented).unwrap();

        assert!(!staging.join("target").exists());
        assert!(!staging.join(".git").exists());
    }

    #[test]
    fn staging_guard_cleans_up_on_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join("staging");
        std::fs::create_dir_all(staging.join("src")).unwrap();
        std::fs::write(staging.join("src/main.rs"), "fn main() {}").unwrap();

        assert!(staging.exists());
        {
            let _guard = StagingGuard(staging.clone());
        } // guard drops here
        assert!(!staging.exists());
    }

    #[test]
    fn clean_stale_staging_removes_old_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let target_piano = tmp.path().join("target/piano");
        std::fs::create_dir_all(target_piano.join("staging-mylib")).unwrap();
        std::fs::create_dir_all(target_piano.join("staging-otherlib")).unwrap();
        std::fs::write(target_piano.join("config.json"), "{}").unwrap();

        clean_stale_staging(&target_piano);

        assert!(!target_piano.join("staging-mylib").exists());
        assert!(!target_piano.join("staging-otherlib").exists());
        // Non-staging files are untouched
        assert!(target_piano.join("config.json").exists());
    }
}

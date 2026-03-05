use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no functions matched {specs}{hint}")]
    NoTargetsFound { specs: String, hint: String },

    #[error("failed to parse {}: {source}", path.display())]
    ParseError {
        path: PathBuf,
        #[source]
        source: syn::Error,
    },

    #[error("build failed: {0}")]
    BuildFailed(String),

    #[error("run failed: {0}")]
    RunFailed(String),

    #[error("no instrumented binary found -- run `piano build` first")]
    NoBinary,

    #[error("no piano runs found -- run `piano profile` to generate one")]
    NoRuns,

    #[error("need at least 2 runs to diff -- run `piano profile` to generate one")]
    NotEnoughRuns,

    #[error(
        "piano diff takes 0 or 2 arguments\n\n  piano diff              compare the two most recent runs\n  piano diff <A> <B>      compare two specific runs (path or tag)"
    )]
    DiffArgCount,

    #[error("no run found for tag '{tag}' -- run piano tag to see available tags")]
    RunNotFound { tag: String },

    #[error("no Cargo.toml found in {0} or any parent directory")]
    NoProjectFound(PathBuf),

    #[error("failed to read run file {}: {source}", path.display())]
    RunReadError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to read tags directory {}: {source}", path.display())]
    TagReadError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid run data in {}: {reason}", path.display())]
    InvalidRunData { path: PathBuf, reason: String },

    #[error("{0}")]
    InvalidTagName(String),

    #[error(
        "no profiling data written -- no functions were instrumented (all functions may be const, unsafe, or extern)\n\n  run `piano build --list-skipped` for details"
    )]
    NoFunctionsInstrumented,

    #[error("profiling data was not written -- check disk space and permissions for {}", .0.display())]
    NoDataWritten(PathBuf),

    #[error("failed to {operation} {}: {source}", path.display())]
    IoWithContext {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Create a closure that wraps a `std::io::Error` with file path context.
///
/// Usage: `.map_err(io_context("read", &path))?`
pub fn io_context(
    operation: &'static str,
    path: impl Into<PathBuf>,
) -> impl FnOnce(std::io::Error) -> Error {
    let path = path.into();
    move |source| Error::IoWithContext {
        operation,
        path,
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_functions_instrumented_message() {
        let err = Error::NoFunctionsInstrumented;
        let msg = err.to_string();
        assert!(
            msg.contains("no functions were instrumented"),
            "should mention no functions instrumented: {msg}"
        );
        assert!(
            msg.contains("--list-skipped"),
            "should include --list-skipped hint: {msg}"
        );
        assert!(
            !msg.contains("disk space"),
            "should not mention disk space: {msg}"
        );
    }

    #[test]
    fn no_data_written_message() {
        let err = Error::NoDataWritten(PathBuf::from("/tmp/runs"));
        let msg = err.to_string();
        assert!(
            msg.contains("disk space"),
            "should mention disk space: {msg}"
        );
        assert!(msg.contains("/tmp/runs"), "should include path: {msg}");
    }
}

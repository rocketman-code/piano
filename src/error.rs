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

    #[error("failed to read run file {}: {source}", path.display())]
    RunReadError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid run data in {}: {reason}", path.display())]
    InvalidRunData { path: PathBuf, reason: String },

    #[error("invalid tag name: {0}")]
    InvalidTagName(String),

    #[error("profiling data was not written -- check disk space and permissions for {}", .0.display())]
    NoDataWritten(PathBuf),

    #[error("{0}")]
    Io(#[from] std::io::Error),
}

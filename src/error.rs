use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no functions matched: {0}")]
    NoTargetsFound(String),

    #[error("failed to parse {}: {source}", path.display())]
    ParseError {
        path: PathBuf,
        #[source]
        source: syn::Error,
    },

    #[error("build failed: {0}")]
    BuildFailed(String),

    #[error("no piano runs found in {} -- run `piano build` and execute the instrumented binary first", .0.display())]
    NoRuns(PathBuf),

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

    #[error("{0}")]
    Io(#[from] std::io::Error),
}

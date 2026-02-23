use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no functions match pattern '{0}'")]
    NoTargetsFound(String),

    #[error("failed to parse {}: {source}", path.display())]
    ParseError {
        path: PathBuf,
        #[source]
        source: syn::Error,
    },

    #[error("build failed: {0}")]
    BuildFailed(String),

    #[error("no runs found in {}", .0.display())]
    NoRuns(PathBuf),

    #[error("HOME environment variable not set")]
    HomeNotFound,

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

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("provision error: {0}")]
    Provision(String),

    #[error("checksum mismatch — expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("rclone rcd startup timed out after {seconds}s")]
    StartupTimeout { seconds: u64 },

    #[error("rclone is not ready (state: {state})")]
    NotReady { state: String },

    #[error("rclone rc error: {0}")]
    Rc(String),

    #[error("async job failed: {0}")]
    JobFailed(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

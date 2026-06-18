use std::path::PathBuf;
use std::time::Duration;

/// Per-OS rclone download descriptor.
#[derive(Debug, Clone)]
pub struct PlatformAsset {
    pub url: String,
    pub sha256: String,
}

/// Configuration for the rclone engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Directory where the rclone binary and config are stored.
    /// The engine places the binary at `<data_dir>/bin/rclone[.exe]`.
    pub data_dir: PathBuf,

    /// Pinned rclone version string (e.g. "v1.74.3").
    pub rclone_version: String,

    /// Download URL and expected SHA-256 for the current platform.
    pub asset: PlatformAsset,

    /// How long to wait for `rclone rcd` to pass the health-check.
    pub startup_timeout: Duration,
}

impl EngineConfig {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        rclone_version: impl Into<String>,
        download_url: impl Into<String>,
        sha256: impl Into<String>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            rclone_version: rclone_version.into(),
            asset: PlatformAsset {
                url: download_url.into(),
                sha256: sha256.into(),
            },
            startup_timeout: Duration::from_secs(15),
        }
    }

    /// Path to the rclone binary inside the data directory.
    pub fn binary_path(&self) -> PathBuf {
        let name = if cfg!(target_os = "windows") { "rclone.exe" } else { "rclone" };
        self.data_dir.join("bin").join(name)
    }

    /// Path to the rclone config file managed by this engine instance.
    pub fn config_path(&self) -> PathBuf {
        self.data_dir.join("rclone.conf")
    }
}

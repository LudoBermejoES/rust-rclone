use std::path::PathBuf;
use std::time::Duration;

/// Per-OS rclone download descriptor.
#[derive(Debug, Clone)]
pub struct PlatformAsset {
    pub url: String,
    pub sha256: String,
}

/// The rclone release "platform slug" for the OS/arch this binary was built for,
/// e.g. `"windows-amd64"`, `"osx-arm64"`, `"linux-amd64"`. Returns `None` on an
/// unsupported target. rclone download URLs follow
/// `https://downloads.rclone.org/<version>/rclone-<version>-<slug>.zip`.
pub fn current_platform_slug() -> Option<&'static str> {
    let os = match std::env::consts::OS {
        "macos" => "osx",
        "windows" => "windows",
        "linux" => "linux",
        _ => return None,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => return None,
    };
    Some(match (os, arch) {
        ("osx", "amd64") => "osx-amd64",
        ("osx", "arm64") => "osx-arm64",
        ("windows", "amd64") => "windows-amd64",
        ("windows", "arm64") => "windows-arm64",
        ("linux", "amd64") => "linux-amd64",
        ("linux", "arm64") => "linux-arm64",
        _ => return None,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_platform_slug_matches_host_and_arch() {
        // On every CI/dev host we support, the slug must resolve and pair the OS
        // with the arch — so the host downloads its own build, never another OS's.
        let slug = current_platform_slug().expect("supported platform");
        assert!(
            ["osx-amd64", "osx-arm64", "windows-amd64", "windows-arm64", "linux-amd64", "linux-arm64"]
                .contains(&slug),
            "unexpected slug: {slug}"
        );
        #[cfg(target_os = "windows")]
        assert!(slug.starts_with("windows-"));
        #[cfg(target_os = "macos")]
        assert!(slug.starts_with("osx-"));
        #[cfg(target_os = "linux")]
        assert!(slug.starts_with("linux-"));
    }
}

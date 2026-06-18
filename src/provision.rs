use std::path::Path;

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::config::EngineConfig;
use crate::error::{Error, Result};
use crate::state::{EngineState, SharedState};

const VERSION_FILE: &str = "version.json";

#[derive(serde::Serialize, serde::Deserialize)]
struct VersionJson {
    rclone_version: String,
    sha256: String,
}

/// Returns `true` if the binary exists and matches the configured version.
pub fn is_installed(cfg: &EngineConfig) -> bool {
    let bin = cfg.binary_path();
    if !bin.exists() {
        return false;
    }
    let version_file = cfg.data_dir.join(VERSION_FILE);
    let Ok(content) = std::fs::read_to_string(&version_file) else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<VersionJson>(&content) else {
        return false;
    };
    v.rclone_version == cfg.rclone_version
}

/// Download, verify, and place the rclone binary atomically.
///
/// - If already installed with the same version, this is a fast no-op.
/// - Progress is reported via `on_progress`.
/// - On failure the partial download is removed; no partial state is left.
pub async fn provision(
    cfg: &EngineConfig,
    state: &SharedState,
    on_progress: &impl Fn(EngineState),
) -> Result<()> {
    let bin_dir = cfg.data_dir.join("bin");
    tokio::fs::create_dir_all(&bin_dir).await?;

    if is_installed(cfg) {
        info!("rclone {} already installed, skipping provision", cfg.rclone_version);
        return Ok(());
    }

    let part_path = cfg.data_dir.join("rclone.part");
    let final_bin = cfg.binary_path();

    info!(
        "Downloading rclone {} from {}",
        cfg.rclone_version, cfg.asset.url
    );

    download_file(&cfg.asset.url, &part_path, state, on_progress).await?;

    // Verify checksum
    if !cfg.asset.sha256.is_empty() {
        state.set(EngineState::Installing);
        on_progress(EngineState::Installing);
        let actual = sha256_of_file(&part_path).await?;
        if actual != cfg.asset.sha256 {
            let _ = tokio::fs::remove_file(&part_path).await;
            return Err(Error::ChecksumMismatch {
                expected: cfg.asset.sha256.clone(),
                actual,
            });
        }
        info!("rclone checksum ok");
    }

    // The rclone release for macOS/Linux is a zip with a single binary inside.
    // For Windows it is also a zip. Extract the binary then place it atomically.
    extract_binary(&part_path, &final_bin).await?;
    let _ = tokio::fs::remove_file(&part_path).await;

    // Mark executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&final_bin)?.permissions();
        perms.set_mode(perms.mode() | 0o111);
        std::fs::set_permissions(&final_bin, perms)?;
    }

    // Write version marker
    let v = VersionJson {
        rclone_version: cfg.rclone_version.clone(),
        sha256: cfg.asset.sha256.clone(),
    };
    tokio::fs::write(
        cfg.data_dir.join(VERSION_FILE),
        serde_json::to_string_pretty(&v)?,
    )
    .await?;

    info!("rclone {} provisioned at {}", cfg.rclone_version, final_bin.display());
    Ok(())
}

async fn download_file(
    url: &str,
    dest: &Path,
    state: &SharedState,
    on_progress: &impl Fn(EngineState),
) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client.get(url).send().await?.error_for_status()?;
    let total = response.content_length();

    let mut file = tokio::fs::File::create(dest).await?;
    let mut downloaded: u64 = 0;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        let s = EngineState::Downloading { downloaded, total };
        state.set(s.clone());
        on_progress(s);
    }
    file.flush().await?;
    Ok(())
}

async fn sha256_of_file(path: &Path) -> Result<String> {
    let data = tokio::fs::read(path).await?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    Ok(hex::encode(hasher.finalize()))
}

/// Extract the rclone binary from the downloaded zip archive.
/// rclone releases ship as `rclone-v<ver>-<os>-<arch>/rclone[.exe]` inside a zip.
async fn extract_binary(zip_path: &Path, dest: &Path) -> Result<()> {
    let zip_path = zip_path.to_owned();
    let dest = dest.to_owned();

    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&zip_path)
            .map_err(|e| Error::Provision(format!("open zip: {e}")))?;
        let mut archive = zip::ZipArchive::new(file)
            .map_err(|e| Error::Provision(format!("read zip: {e}")))?;

        let bin_name = if cfg!(target_os = "windows") { "rclone.exe" } else { "rclone" };

        for i in 0..archive.len() {
            let mut entry = archive.by_index(i)
                .map_err(|e| Error::Provision(format!("zip entry {i}: {e}")))?;
            let name = entry.name().to_string();
            // Match any path ending in rclone or rclone.exe (one subdir deep)
            if name.ends_with(bin_name) && !entry.is_dir() {
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| Error::Provision(format!("mkdir: {e}")))?;
                }
                let mut out = std::fs::File::create(&dest)
                    .map_err(|e| Error::Provision(format!("create binary: {e}")))?;
                std::io::copy(&mut entry, &mut out)
                    .map_err(|e| Error::Provision(format!("extract binary: {e}")))?;
                return Ok(());
            }
        }
        Err(Error::Provision(format!(
            "rclone binary '{bin_name}' not found in zip"
        )))
    })
    .await
    .map_err(|e| Error::Provision(e.to_string()))??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg(dir: &std::path::Path) -> EngineConfig {
        EngineConfig::new(dir, "v1.74.3", "https://example.com/rclone.zip", "deadbeef")
    }

    #[tokio::test]
    async fn is_installed_missing_binary() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_cfg(dir.path());
        assert!(!is_installed(&cfg));
    }

    #[tokio::test]
    async fn provision_fails_on_bad_url() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_cfg(dir.path());
        let state = SharedState::new();
        let result = provision(&cfg, &state, &|_| {}).await;
        assert!(result.is_err());
        // Partial file must not remain
        assert!(!dir.path().join("rclone.part").exists());
    }
}

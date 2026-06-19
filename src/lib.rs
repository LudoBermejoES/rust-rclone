//! # rust-rclone
//!
//! Download-on-demand rclone HTTP sidecar for offline Rust desktop apps.
//!
//! ## Architecture
//!
//! rclone is **never bundled**. When the user enables cloud sync,
//! [`RcloneEngine::provision`] downloads the official per-OS binary
//! (version-pinned, SHA-256-verified) into the resolved app-data directory.
//! While your app runs, [`RcloneEngine::start`] spawns `rclone rcd` on an
//! ephemeral loopback port. Cloud operations are then available via the rc
//! HTTP API through [`RcloneEngine::rc`].
//!
//! ## State machine
//!
//! ```text
//! NotInstalled → Downloading{…} → Installing → (Error on failure)
//!                                     │
//!                                  [provision ok]
//!                                     │
//!                                  Stopped (ready to start)
//!                                     │
//!                                   start()
//!                                     │
//!                                 Starting → Ready
//!                                               │
//!                                             stop()
//!                                               │
//!                                           Stopped
//! ```
//!
//! ## Minimal usage
//!
//! ```rust,no_run
//! use rust_rclone::{RcloneEngine, EngineConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let cfg = EngineConfig::new(
//!         "/path/to/app-data/rclone",
//!         "v1.74.3",
//!         "https://downloads.rclone.org/v1.74.3/rclone-v1.74.3-osx-arm64.zip",
//!         "<sha256-hex>",
//!     );
//!     let engine = RcloneEngine::new(cfg);
//!
//!     engine.provision(|state| println!("Progress: {state}")).await?;
//!     engine.start().await?;
//!
//!     let rc = engine.rc();
//!     let jobid = rc.copy_async("/local/project", "myremote:corylus/MyNovel", vec![
//!         "- index.sqlite*".to_string(),
//!     ]).await?;
//!     let status = rc.wait_for_job(jobid, std::time::Duration::from_millis(500),
//!         std::time::Duration::from_secs(60)).await?;
//!     println!("copy ok: {}", status.is_ok());
//!
//!     engine.stop().await?;
//!     Ok(())
//! }
//! ```

mod client;
mod config;
mod error;
mod lifecycle;
mod provision;
mod state;

pub use client::{BisyncOutput, ConfigQuestion, DirectoryItem, JobStatus, RcClient, SyncConflict};
pub use config::{current_platform_slug, EngineConfig};
pub use error::{Error, Result};
pub use state::EngineState;

use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tracing::{error, info};

use lifecycle::{DaemonProcess, wait_for_ready};
use state::SharedState;

/// The rclone engine.
///
/// Cheap to clone — all state is behind an `Arc`.
#[derive(Clone)]
pub struct RcloneEngine {
    config: Arc<RwLock<EngineConfig>>,
    state: SharedState,
    daemon: Arc<Mutex<Option<DaemonProcess>>>,
}

impl RcloneEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            state: SharedState::new(),
            daemon: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the current data directory.
    pub fn data_dir(&self) -> std::path::PathBuf {
        self.config.read().unwrap().data_dir.clone()
    }

    /// Replace the data directory and re-probe install state.
    ///
    /// Call this from `setup()` once the Tauri app handle is available,
    /// passing the bundle-scoped per-user app-data dir. The engine uses the
    /// directory directly (the `rclone/` subdir is provided by the caller,
    /// matching `downloaded-asset-storage` rules).
    pub fn set_data_dir(&self, data_dir: std::path::PathBuf) {
        let mut cfg = self.config.write().unwrap();
        cfg.data_dir = data_dir;
        drop(cfg);
        let installed = provision::is_installed(&self.config.read().unwrap());
        if !installed {
            self.state.set(EngineState::NotInstalled);
        } else if self.state.get() == EngineState::NotInstalled {
            self.state.set(EngineState::Stopped);
        }
    }

    /// Current observable state.
    pub fn state(&self) -> EngineState {
        self.state.get()
    }

    /// `true` if the binary is downloaded and the version matches.
    pub fn is_installed(&self) -> bool {
        provision::is_installed(&self.config.read().unwrap())
    }

    /// `true` if the daemon is running and accepting rc requests.
    pub fn is_ready(&self) -> bool {
        self.state.get() == EngineState::Ready
    }

    /// Download and verify the rclone binary.
    ///
    /// - Fast no-op if already installed at the same version.
    /// - Progress events are sent to `on_progress`.
    pub async fn provision(&self, on_progress: impl Fn(EngineState) + Send + 'static) -> Result<()> {
        self.state.set(EngineState::Downloading { downloaded: 0, total: None });
        let cfg = self.config.read().unwrap().clone();
        let result = provision::provision(&cfg, &self.state, &on_progress).await;
        match &result {
            Ok(()) => {
                if !matches!(self.state.get(), EngineState::Ready | EngineState::Starting) {
                    self.state.set(EngineState::Stopped);
                }
            }
            Err(e) => {
                error!("rclone provision failed: {e}");
                self.state.set(EngineState::Error { message: e.to_string() });
            }
        }
        result
    }

    /// Spawn `rclone rcd` and wait until the health-check passes.
    pub async fn start(&self) -> Result<()> {
        if !self.is_installed() {
            return Err(Error::Provision("call provision() before start()".into()));
        }

        let cfg = self.config.read().unwrap().clone();
        self.state.set(EngineState::Starting);

        // Kill any orphaned daemons from a previous run (e.g. one left behind by a
        // SIGKILLed dev process) before spawning a fresh one, so daemons never pile up.
        crate::lifecycle::reap_stray_daemons(&cfg.config_path());

        let daemon = match DaemonProcess::spawn(&cfg).await {
            Ok(d) => d,
            Err(e) => {
                self.state.set(EngineState::Error { message: e.to_string() });
                return Err(e);
            }
        };

        let port = daemon.port;
        let mut guard = self.daemon.lock().await;
        *guard = Some(daemon);
        drop(guard);

        match wait_for_ready(port, cfg.startup_timeout, &self.state).await {
            Ok(()) => {
                info!("rclone engine ready on port {port}");
                Ok(())
            }
            Err(e) => {
                self.state.set(EngineState::Error { message: e.to_string() });
                if let Some(d) = self.daemon.lock().await.take() {
                    d.stop().await;
                }
                Err(e)
            }
        }
    }

    /// Stop the `rclone rcd` daemon. Safe to call even if already stopped.
    pub async fn stop(&self) -> Result<()> {
        let mut guard = self.daemon.lock().await;
        if let Some(d) = guard.take() {
            d.stop().await;
        }
        self.state.set(EngineState::Stopped);
        Ok(())
    }

    /// Returns an `RcClient` pointed at the running daemon.
    ///
    /// Panics if the daemon is not started (check `is_ready()` first).
    pub fn rc(&self) -> RcClient {
        let port = match self.bound_port() {
            Some(p) => p,
            None => panic!("rclone daemon not started; call start() first"),
        };
        RcClient::new(port)
    }

    /// Returns the loopback port the daemon is bound to, or `None` if not running.
    pub fn bound_port(&self) -> Option<u16> {
        // We access the daemon synchronously using try_lock to avoid async.
        // If the lock is held we conservatively return None.
        self.daemon
            .try_lock()
            .ok()
            .and_then(|g| g.as_ref().map(|d| d.port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> EngineConfig {
        let tmp = tempfile::tempdir().unwrap();
        EngineConfig::new(tmp.path(), "v1.74.3", "https://example.com/rclone.zip", "deadbeef")
    }

    #[tokio::test]
    async fn state_starts_not_installed() {
        let engine = RcloneEngine::new(test_config());
        assert_eq!(engine.state(), EngineState::NotInstalled);
        assert!(!engine.is_installed());
        assert!(!engine.is_ready());
    }

    #[tokio::test]
    async fn start_before_provision_returns_error() {
        let engine = RcloneEngine::new(test_config());
        let err = engine.start().await.unwrap_err();
        assert!(matches!(err, Error::Provision(_)));
    }

    #[tokio::test]
    async fn provision_fails_on_bad_url() {
        let engine = RcloneEngine::new(test_config());
        let result = engine.provision(|_| {}).await;
        assert!(result.is_err());
        assert!(matches!(engine.state(), EngineState::Error { .. }));
    }

    #[tokio::test]
    async fn set_data_dir_updates_state() {
        let engine = RcloneEngine::new(test_config());
        let new_dir = tempfile::tempdir().unwrap();
        engine.set_data_dir(new_dir.path().join("rclone"));
        assert_eq!(engine.state(), EngineState::NotInstalled);
    }
}

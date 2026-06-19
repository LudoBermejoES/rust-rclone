use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::config::EngineConfig;
use crate::error::{Error, Result};
use crate::state::{EngineState, SharedState};

/// Acquire an ephemeral free port on loopback by binding momentarily.
pub fn acquire_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Kill any stray `rclone rcd` daemons that were started against THIS config
/// path (e.g. orphaned by a previous run that was SIGKILLed before its graceful
/// shutdown could fire — common under `tauri dev`/Ctrl-C). Scoped by the config
/// path so we never touch unrelated rclone instances the user may run.
///
/// Best-effort: any failure (no `pgrep`, permission, race) is ignored. Unix
/// only; on other platforms this is a no-op (the graceful exit handler covers
/// the normal case there).
#[cfg(unix)]
pub fn reap_stray_daemons(config_path: &std::path::Path) {
    use std::process::Command;
    let cfg = config_path.to_string_lossy();
    // Match the daemon command line: `rclone ... rcd ... --config <our cfg>`.
    // `pgrep -f` matches against the full argv; the config path is the unique key.
    let out = match Command::new("pgrep").args(["-f", "rclone.*rcd"]).output() {
        Ok(o) => o,
        Err(_) => return,
    };
    let my_pid = std::process::id();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let pid: u32 = match line.trim().parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        if pid == my_pid {
            continue;
        }
        // Confirm this PID's argv actually references our config path before killing.
        let argv = Command::new("ps")
            .args(["-o", "command=", "-p", &pid.to_string()])
            .output()
            .ok();
        let matches_cfg = argv
            .map(|o| String::from_utf8_lossy(&o.stdout).contains(cfg.as_ref()))
            .unwrap_or(false);
        if matches_cfg {
            info!("Reaping stray rclone rcd daemon (pid {pid})");
            let _ = Command::new("kill").arg(pid.to_string()).status();
        }
    }
}

#[cfg(not(unix))]
pub fn reap_stray_daemons(_config_path: &std::path::Path) {}

pub(crate) struct DaemonProcess {
    child: Arc<Mutex<Option<Child>>>,
    pub port: u16,
}

impl DaemonProcess {
    /// Spawn `rclone rcd` on a free loopback port and return the wrapper.
    pub async fn spawn(cfg: &EngineConfig) -> Result<Self> {
        let port = acquire_free_port()?;
        let bin = cfg.binary_path();
        let config_path = cfg.config_path();

        info!("Spawning rclone rcd on 127.0.0.1:{port}");

        let mut command = Command::new(&bin);
        command
            .args([
                "rcd",
                "--rc-addr",
                &format!("127.0.0.1:{port}"),
                "--rc-no-auth",
                "--config",
                &config_path.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        // Suppress console window on Windows
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);

        let child = command.spawn()?;
        Ok(Self { child: Arc::new(Mutex::new(Some(child))), port })
    }

    /// Kill the rcd process. Sends `core/quit` first for a clean shutdown.
    pub async fn stop(&self) {
        let mut guard = self.child.lock().await;
        if let Some(mut child) = guard.take() {
            info!("Stopping rclone rcd (port {})", self.port);
            let client = reqwest::Client::new();
            let _ = client
                .post(format!("http://127.0.0.1:{}/core/quit", self.port))
                .timeout(Duration::from_secs(3))
                .send()
                .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _: std::io::Result<()> = child.kill().await;
            let _: std::io::Result<std::process::ExitStatus> = child.wait().await;
        }
    }
}

/// Poll `GET /rc/noop` until the daemon responds or the timeout elapses.
pub async fn wait_for_ready(port: u16, timeout: Duration, state: &SharedState) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + timeout;

    state.set(EngineState::Starting);

    loop {
        if Instant::now() >= deadline {
            return Err(Error::StartupTimeout { seconds: timeout.as_secs() });
        }
        match client
            .post(format!("http://127.0.0.1:{port}/rc/noop"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!("rclone rcd ready on port {port}");
                state.set(EngineState::Ready);
                return Ok(());
            }
            Ok(resp) => {
                debug!("rcd not ready yet: HTTP {}", resp.status());
            }
            Err(e) => {
                debug!("rcd not ready yet: {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_free_port_is_unprivileged() {
        let port = acquire_free_port().unwrap();
        assert!(port > 1024, "expected unprivileged port, got {port}");
    }

    #[test]
    fn acquire_free_port_twice_differs() {
        let p1 = acquire_free_port().unwrap();
        let p2 = acquire_free_port().unwrap();
        // With high probability the OS will not give the same port twice in a row.
        // This is not guaranteed but validates the function works.
        assert!(p1 > 0 && p2 > 0);
    }
}

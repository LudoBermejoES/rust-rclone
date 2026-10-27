//! Daemons must not outlive their host (openspec change
//! rclone-orphan-daemons-windows, in the Corylus repository).
//!
//! These run a real rclone. They find it on PATH, or where Corylus installs it;
//! without one, each test says loudly that it did not run.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rust_rclone::{EngineConfig, RcloneEngine};

const BIN: &str = if cfg!(windows) { "rclone.exe" } else { "rclone" };

fn installed_rclone() -> Option<PathBuf> {
    if let Ok(p) = which::which("rclone") {
        return Some(p);
    }
    let base = if cfg!(windows) {
        PathBuf::from(std::env::var_os("APPDATA")?)
    } else {
        PathBuf::from(std::env::var_os("HOME")?).join("Library").join("Application Support")
    };
    Some(base.join("com.corylus.app").join("rclone").join("bin").join(BIN)).filter(|p| p.exists())
}

/// A data dir holding a link to (or copy of) the installed rclone.
fn data_dir_with_rclone(dir: &Path) -> Option<EngineConfig> {
    let src = installed_rclone()?;
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).ok()?;
    let dst = bin.join(BIN);
    std::fs::hard_link(&src, &dst).or_else(|_| std::fs::copy(&src, &dst).map(|_| ())).ok()?;
    std::fs::write(dir.join("version.json"), r#"{"rclone_version":"test","sha256":""}"#).ok()?;
    Some(EngineConfig::new(dir, "test", "https://unused", ""))
}

macro_rules! cfg_or_skip {
    ($dir:expr) => {
        match data_dir_with_rclone($dir) {
            Some(cfg) => cfg,
            None => {
                eprintln!("\n=== SKIPPED (no rclone): this test did NOT run ===\n");
                return;
            }
        }
    };
}

fn recorded_pid(dir: &Path) -> Option<u32> {
    std::fs::read_to_string(dir.join("rclone.pid")).ok()?.trim().parse().ok()
}

/// Whether `pid` is a live (non-zombie) process.
fn alive(pid: u32) -> bool {
    if cfg!(windows) {
        let out = std::process::Command::new("tasklist").args(["/FI", &format!("PID eq {pid}"), "/NH"]).output().unwrap();
        String::from_utf8_lossy(&out.stdout).split_whitespace().any(|w| w == pid.to_string())
    } else {
        let out = std::process::Command::new("ps").args(["-o", "stat=", "-p", &pid.to_string()]).output().unwrap();
        let stat = String::from_utf8_lossy(&out.stdout).trim().to_string();
        !stat.is_empty() && !stat.starts_with('Z')
    }
}

fn gone_within(pid: u32, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while alive(pid) {
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    true
}

#[tokio::test]
async fn a_daemon_left_by_an_earlier_session_is_stopped_on_the_next_start() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = cfg_or_skip!(tmp.path());

    let first = RcloneEngine::new(cfg.clone());
    first.start().await.expect("first start");
    let leftover = recorded_pid(tmp.path()).expect("pid recorded");
    // The host "crashes": no stop(), no drop — the daemon keeps running.
    std::mem::forget(first);
    assert!(alive(leftover));

    let second = RcloneEngine::new(cfg);
    second.start().await.expect("second start");
    assert!(gone_within(leftover, Duration::from_secs(5)), "leftover daemon {leftover} still running");
    let current = recorded_pid(tmp.path()).expect("new pid recorded");
    assert_ne!(current, leftover);
    assert!(alive(current));

    second.stop().await.unwrap();
    assert!(recorded_pid(tmp.path()).is_none(), "a clean stop removes the pid file");
    assert!(gone_within(current, Duration::from_secs(5)));
}

#[tokio::test]
async fn a_recorded_pid_that_is_not_this_engines_rclone_is_left_alone() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = cfg_or_skip!(tmp.path());
    let mut other = if cfg!(windows) {
        std::process::Command::new("ping").args(["-n", "60", "127.0.0.1"]).stdout(std::process::Stdio::null()).spawn().unwrap()
    } else {
        std::process::Command::new("sleep").arg("60").spawn().unwrap()
    };
    std::fs::write(tmp.path().join("rclone.pid"), other.id().to_string()).unwrap();

    let engine = RcloneEngine::new(cfg);
    engine.start().await.expect("start");
    assert!(alive(other.id()), "an unrelated process must not be killed");
    engine.stop().await.unwrap();
    let _ = other.kill();
}

/// Windows ties the daemon to its host with a kill-on-close job object, so a
/// host that dies without cleaning up takes its daemon with it.
#[cfg(windows)]
#[test]
fn a_host_killed_abruptly_takes_its_daemon_with_it() {
    let tmp = tempfile::tempdir().unwrap();
    if data_dir_with_rclone(tmp.path()).is_none() {
        eprintln!("\n=== SKIPPED (no rclone): this test did NOT run ===\n");
        return;
    }
    // Re-run this test binary as the host, running only the helper below.
    let mut host = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "hold_a_daemon_until_killed", "--ignored", "--nocapture"])
        .env("RCLONE_HOLD_DATA_DIR", tmp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let daemon = loop {
        if let Some(pid) = recorded_pid(tmp.path()) {
            break pid;
        }
        assert!(Instant::now() < deadline, "host never started its daemon");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(alive(daemon));

    host.kill().unwrap(); // TerminateProcess: no exit handler runs.
    let _ = host.wait();
    assert!(gone_within(daemon, Duration::from_secs(5)), "daemon {daemon} outlived its host");
}

/// Helper for the test above; does nothing unless that test starts it.
#[cfg(windows)]
#[tokio::test]
#[ignore]
async fn hold_a_daemon_until_killed() {
    let Some(dir) = std::env::var_os("RCLONE_HOLD_DATA_DIR") else { return };
    let cfg = EngineConfig::new(PathBuf::from(dir), "test", "https://unused", "");
    let engine = RcloneEngine::new(cfg);
    engine.start().await.expect("host start");
    std::thread::sleep(Duration::from_secs(120));
}

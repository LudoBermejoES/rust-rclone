//! Integration tests for rust-rclone.
//!
//! These tests require the `rclone` binary to be on PATH and will download/start
//! a real rclone rcd. Run with:
//!   cargo test --test integration -- --ignored
//!
//! (Marked `#[ignore]` so they are skipped in CI unless rclone is available.)

use std::time::Duration;

use rust_rclone::{EngineConfig, EngineState, RcloneEngine};

fn find_rclone_binary() -> Option<std::path::PathBuf> {
    // Look for rclone on PATH for a real integration test
    which::which("rclone").ok()
}

/// Build an EngineConfig that points to the already-installed system rclone.
/// This lets us test lifecycle and rc without downloading anything.
fn system_rclone_config() -> Option<(EngineConfig, std::path::PathBuf)> {
    let rclone_bin = find_rclone_binary()?;
    let tmp = tempfile::tempdir().ok()?;
    let data_dir = tmp.path().to_path_buf();

    // Place a symlink/copy of the system binary so is_installed passes
    let bin_dir = data_dir.join("bin");
    std::fs::create_dir_all(&bin_dir).ok()?;
    let dest = if cfg!(windows) {
        bin_dir.join("rclone.exe")
    } else {
        bin_dir.join("rclone")
    };
    std::fs::copy(&rclone_bin, &dest).ok()?;
    // Write a version.json so is_installed() returns true
    let version_json = serde_json::json!({
        "rclone_version": "v1.74.3",
        "sha256": ""
    });
    std::fs::write(data_dir.join("version.json"), version_json.to_string()).ok()?;

    let cfg = EngineConfig::new(
        &data_dir,
        "v1.74.3",
        "https://unused",
        "", // sha256 check skipped when empty
    );

    Some((cfg, tmp.into_path()))
}

#[tokio::test]
#[ignore]
async fn lifecycle_start_stop() {
    let Some((cfg, _tmp)) = system_rclone_config() else {
        eprintln!("skipping: rclone not on PATH");
        return;
    };
    let engine = RcloneEngine::new(cfg);
    assert!(engine.is_installed());

    engine.start().await.expect("start");
    assert_eq!(engine.state(), EngineState::Ready);

    let port = engine.bound_port().expect("port");
    assert!(port > 1024);

    engine.stop().await.expect("stop");
    assert_eq!(engine.state(), EngineState::Stopped);
}

#[tokio::test]
#[ignore]
async fn rc_sync_copy_local_dirs() {
    let Some((cfg, _tmp)) = system_rclone_config() else {
        eprintln!("skipping: rclone not on PATH");
        return;
    };
    let engine = RcloneEngine::new(cfg);
    engine.start().await.expect("start");

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("file.txt"), b"hello").unwrap();

    let rc = engine.rc();
    let jobid = rc
        .copy_async(
            &src.path().to_string_lossy(),
            &dst.path().to_string_lossy(),
            vec![],
        )
        .await
        .expect("copy_async");

    let status = rc
        .wait_for_job(jobid, Duration::from_millis(200), Duration::from_secs(15))
        .await
        .expect("wait_for_job");

    assert!(status.is_ok(), "copy failed: {:?}", status.error);
    assert!(dst.path().join("file.txt").exists(), "file not copied");

    engine.stop().await.expect("stop");
}

#[tokio::test]
#[ignore]
async fn rc_bisync_conflict_detection() {
    let Some((cfg, _tmp)) = system_rclone_config() else {
        eprintln!("skipping: rclone not on PATH");
        return;
    };
    let engine = RcloneEngine::new(cfg);
    engine.start().await.expect("start");

    let path1 = tempfile::tempdir().unwrap();
    let path2 = tempfile::tempdir().unwrap();
    std::fs::write(path1.path().join("doc.txt"), b"original").unwrap();

    let rc = engine.rc();

    // First sync: establish baseline
    let jid = rc
        .bisync_async(
            &path1.path().to_string_lossy(),
            &path2.path().to_string_lossy(),
            true,  // resync
            false, // force
            vec![],
        )
        .await
        .expect("bisync resync");
    let status = rc
        .wait_for_job(jid, Duration::from_millis(200), Duration::from_secs(15))
        .await
        .expect("bisync wait");
    assert!(status.is_ok(), "resync failed: {:?}", status.error);
    assert!(path2.path().join("doc.txt").exists());

    // Create a conflict: edit the same file on both sides
    std::fs::write(path1.path().join("doc.txt"), b"edit from path1").unwrap();
    std::fs::write(path2.path().join("doc.txt"), b"edit from path2").unwrap();

    // Second sync: should detect conflict
    let jid2 = rc
        .bisync_async(
            &path1.path().to_string_lossy(),
            &path2.path().to_string_lossy(),
            false, // no resync
            true,  // force (override all-changed safety abort)
            vec![],
        )
        .await
        .expect("bisync conflict");
    let status2 = rc
        .wait_for_job(jid2, Duration::from_millis(200), Duration::from_secs(15))
        .await
        .expect("bisync conflict wait");

    let conflicts = status2.conflicts(&path1.path().to_string_lossy());
    assert!(!conflicts.is_empty(), "expected conflicts, got none");
    assert_eq!(conflicts[0].conflict1, "doc.txt.conflict1");
    assert_eq!(conflicts[0].conflict2, "doc.txt.conflict2");

    // Both versions must exist (no data loss)
    assert!(path1.path().join("doc.txt.conflict1").exists());
    assert!(path1.path().join("doc.txt.conflict2").exists());

    engine.stop().await.expect("stop");
}

#[tokio::test]
#[ignore]
async fn rc_loopback_only() {
    let Some((cfg, _tmp)) = system_rclone_config() else {
        eprintln!("skipping: rclone not on PATH");
        return;
    };
    let engine = RcloneEngine::new(cfg);
    engine.start().await.expect("start");

    let port = engine.bound_port().expect("port");

    // Connecting to 0.0.0.0 on the same port should fail (daemon only binds 127.0.0.1)
    let non_loopback = format!("http://0.0.0.0:{port}/rc/noop");
    let client = reqwest::Client::new();
    let result = client
        .post(&non_loopback)
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    // Either connection refused or a transport error — not a success
    assert!(result.is_err(), "daemon should not be reachable on 0.0.0.0");

    engine.stop().await.expect("stop");
}

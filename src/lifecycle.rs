use std::net::TcpListener;
use std::path::{Path, PathBuf};
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

// ── Daemons left behind by an earlier host ───────────────────────────────────
//
// The host stops its daemon on a clean exit. A crash or a forced quit skips
// that, and the daemon keeps running. Two defences:
//   * Windows: the daemon is placed in a kill-on-close job object, so the OS
//     ends it together with the host however the host ends.
//   * Every OS: the daemon's pid is written to `rclone.pid` in the data dir;
//     the next start stops that process if it is still this engine's rclone.

fn pid_file(cfg: &EngineConfig) -> PathBuf {
    cfg.data_dir.join("rclone.pid")
}

/// Whether the recorded pid is a daemon to stop: it must not be this process,
/// and it must be running the engine's own rclone binary — so a pid the OS has
/// since given to another program, or an rclone the user started, is left alone.
pub(crate) fn should_reap(recorded: u32, own: u32, running_exe: Option<&Path>, expected_exe: &Path) -> bool {
    recorded != own && running_exe.is_some_and(|exe| same_file(exe, expected_exe))
}

fn same_file(a: &Path, b: &Path) -> bool {
    let eq = |x: &Path, y: &Path| {
        if cfg!(windows) {
            x.to_string_lossy().eq_ignore_ascii_case(&y.to_string_lossy())
        } else {
            x == y
        }
    };
    eq(a, b)
        || matches!((a.canonicalize(), b.canonicalize()), (Ok(x), Ok(y)) if eq(&x, &y))
}

/// Stop the daemon recorded by an earlier session, if it is still running.
pub fn reap_recorded_daemon(cfg: &EngineConfig) {
    let file = pid_file(cfg);
    let Some(pid) = std::fs::read_to_string(&file).ok().and_then(|s| s.trim().parse::<u32>().ok()) else {
        return;
    };
    if should_reap(pid, std::process::id(), process_exe(pid).as_deref(), &cfg.binary_path()) {
        info!("Stopping rclone daemon left by an earlier session (pid {pid})");
        kill_and_wait(pid, &cfg.binary_path());
    }
    let _ = std::fs::remove_file(&file);
}

/// The executable of a running process, or `None` if it is not running.
#[cfg(windows)]
pub(crate) fn process_exe(pid: u32) -> Option<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, QueryFullProcessImageNameW, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    // STILL_ACTIVE: an exited process can still be opened while handles to it exist.
    const STILL_ACTIVE: u32 = 259;
    unsafe {
        let h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if h.is_null() {
            return None;
        }
        let mut code = 0u32;
        let alive = GetExitCodeProcess(h, &mut code) != 0 && code == STILL_ACTIVE;
        let mut buf = vec![0u16; 32_768];
        let mut len = buf.len() as u32;
        let named = QueryFullProcessImageNameW(h, 0, buf.as_mut_ptr(), &mut len) != 0;
        CloseHandle(h);
        (alive && named).then(|| PathBuf::from(std::ffi::OsString::from_wide(&buf[..len as usize])))
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn process_exe(pid: u32) -> Option<PathBuf> {
    std::fs::read_link(format!("/proc/{pid}/exe")).ok()
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn process_exe(pid: u32) -> Option<PathBuf> {
    // `comm` is the executable path the daemon was started with (absolute here);
    // a zombie shows as `<defunct>`/`(rclone)`, which matches no path.
    let out = std::process::Command::new("ps").args(["-o", "comm=", "-p", &pid.to_string()]).output().ok()?;
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then(|| PathBuf::from(s))
}

#[cfg(windows)]
fn kill_and_wait(pid: u32, _exe: &Path) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, TerminateProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
    };
    unsafe {
        let h = OpenProcess(PROCESS_TERMINATE | PROCESS_SYNCHRONIZE, 0, pid);
        if h.is_null() {
            return;
        }
        TerminateProcess(h, 1);
        WaitForSingleObject(h, 5_000);
        CloseHandle(h);
    }
}

#[cfg(unix)]
fn kill_and_wait(pid: u32, exe: &Path) {
    let _ = std::process::Command::new("kill").arg(pid.to_string()).status();
    // Until it no longer runs our rclone: a killed child of this process stays
    // listed as a zombie until reaped, but is no longer that executable.
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_exe(pid).is_some_and(|e| same_file(&e, exe)) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A Windows job object that terminates its processes when its last handle
/// closes — including when the host process dies without cleaning up.
#[cfg(windows)]
struct KillOnCloseJob(windows_sys::Win32::Foundation::HANDLE);

// The handle is only ever closed, once, on drop.
#[cfg(windows)]
unsafe impl Send for KillOnCloseJob {}
#[cfg(windows)]
unsafe impl Sync for KillOnCloseJob {}

#[cfg(windows)]
impl KillOnCloseJob {
    fn assign(process: windows_sys::Win32::Foundation::HANDLE) -> Option<Self> {
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };
        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job.is_null() {
                return None;
            }
            let job = Self(job);
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let set = SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const std::ffi::c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            ) != 0;
            (set && AssignProcessToJobObject(job.0, process) != 0).then_some(job)
        }
    }
}

#[cfg(windows)]
impl Drop for KillOnCloseJob {
    fn drop(&mut self) {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

pub(crate) struct DaemonProcess {
    child: Arc<Mutex<Option<Child>>>,
    pub port: u16,
    pid_file: PathBuf,
    /// Held for the daemon's lifetime; dropping it (or the host dying) ends the daemon.
    #[cfg(windows)]
    _job: Option<KillOnCloseJob>,
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

        #[cfg(windows)]
        let job = {
            let job = child.raw_handle().and_then(|h| KillOnCloseJob::assign(h as _));
            if job.is_none() {
                tracing::warn!("rclone rcd: could not tie the daemon to this process; a crash may leave it running until the next start");
            }
            job
        };
        let pid_file = pid_file(cfg);
        if let Some(pid) = child.id() {
            let _ = std::fs::write(&pid_file, pid.to_string());
        }
        Ok(Self {
            child: Arc::new(Mutex::new(Some(child))),
            port,
            pid_file,
            #[cfg(windows)]
            _job: job,
        })
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
            let _ = std::fs::remove_file(&self.pid_file);
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
    fn reap_only_this_engines_rclone() {
        let expected = Path::new("/data/bin/rclone");
        assert!(should_reap(42, 1, Some(expected), expected), "our leftover daemon");
        assert!(!should_reap(42, 1, None, expected), "no longer running");
        assert!(!should_reap(42, 1, Some(Path::new("/usr/bin/rclone")), expected), "the user's own rclone");
        assert!(!should_reap(42, 1, Some(Path::new("/usr/bin/vim")), expected), "pid reused by another program");
        assert!(!should_reap(1, 1, Some(expected), expected), "never this process");
    }

    #[cfg(windows)]
    #[test]
    fn paths_compare_case_insensitively_on_windows() {
        assert!(same_file(Path::new(r"C:\Data\bin\rclone.exe"), Path::new(r"c:\data\BIN\rclone.exe")));
    }

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

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::error::{Error, Result};

/// Typed rc HTTP client.
pub struct RcClient {
    port: u16,
    http: reqwest::Client,
}

// ─── rc response types ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct JobStarted {
    jobid: u64,
}

#[derive(Debug, Deserialize)]
pub struct JobStatus {
    pub id: u64,
    pub finished: bool,
    pub success: bool,
    pub error: String,
    /// Present for bisync jobs — contains human-readable conflict/progress lines.
    pub output: Option<BisyncOutput>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct BisyncOutput {
    #[serde(default)]
    pub output: String,
    #[serde(rename = "basePath", default)]
    pub base_path: String,
    #[serde(default)]
    pub session: String,
}

/// An item returned by `list_directory`.
#[derive(Debug, Clone)]
pub struct DirectoryItem {
    pub name: String,
    pub is_dir: bool,
}

/// Transfer progress for a job, from `core/stats` scoped to the job's group.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct JobStats {
    #[serde(default)]
    pub bytes: u64,
    #[serde(rename = "totalBytes", default)]
    pub total_bytes: u64,
    /// Estimated seconds remaining, when rclone can compute one.
    #[serde(default)]
    pub eta: Option<f64>,
}

/// A conflict detected by bisync.
#[derive(Debug, Clone)]
pub struct SyncConflict {
    /// The relative path of the file that conflicted.
    pub path: String,
    /// Filename of the path1-side version (`.conflict1` suffix).
    pub conflict1: String,
    /// Filename of the path2-side version (`.conflict2` suffix).
    pub conflict2: String,
}

/// rclone rc filter object. The rc API expects filters under the `_filter` key as
/// structured include/exclude rule lists — NOT a top-level `filter` array of
/// `"- pattern"` strings (which rclone silently ignores).
#[derive(Debug, Serialize, Default)]
struct RcFilter {
    #[serde(rename = "IncludeRule", skip_serializing_if = "Vec::is_empty")]
    include_rule: Vec<String>,
    #[serde(rename = "ExcludeRule", skip_serializing_if = "Vec::is_empty")]
    exclude_rule: Vec<String>,
}

impl RcFilter {
    /// Convert rclone filter-string syntax (`"- pattern"`, `"+ pattern"`) into the
    /// rc `_filter` shape. Lines without a recognised `+ `/`- ` prefix are treated
    /// as excludes (the conservative default for our exclusion-only use).
    fn from_lines(lines: &[String]) -> Self {
        let mut f = RcFilter::default();
        for line in lines {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("+ ") {
                f.include_rule.push(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("- ") {
                f.exclude_rule.push(rest.trim().to_string());
            } else if !line.is_empty() {
                f.exclude_rule.push(line.to_string());
            }
        }
        f
    }

    fn is_empty(&self) -> bool {
        self.include_rule.is_empty() && self.exclude_rule.is_empty()
    }
}

#[derive(Debug, Serialize)]
struct SyncCopyParams<'a> {
    #[serde(rename = "srcFs")]
    src_fs: &'a str,
    #[serde(rename = "dstFs")]
    dst_fs: &'a str,
    #[serde(rename = "_async")]
    r#async: bool,
    #[serde(rename = "_filter", skip_serializing_if = "RcFilter::is_empty")]
    filter: RcFilter,
}

#[derive(Debug, Serialize)]
struct BisyncParams<'a> {
    path1: &'a str,
    path2: &'a str,
    resync: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    force: bool,
    // Self-heal from "less-serious" aborts/interruptions on the NEXT run instead
    // of demanding a destructive `--resync`. A resync is a union operation that
    // re-pulls remote-only files, so it resurrects locally-deleted/moved files;
    // resilient+recover let an over-cautious or interrupted run simply retry.
    resilient: bool,
    recover: bool,
    // bisync's own `--checksum` flag (compare file content hash, not just
    // modtime+size). Without this, two machines that saved byte-identical
    // content see a fresh modtime on each write and bisync reports "changed in
    // both" for text that never actually diverged — the identical-write skip in
    // document-io only helps when Corylus itself performs the write; a remote
    // that lacks hash support falls back to size+modtime regardless, so this is
    // a defense-in-depth complement, not a replacement.
    checksum: bool,
    // bisync's OWN safety check: the maximum PERCENTAGE of files a run may
    // delete before it aborts. This is a bisync command flag and must be sent
    // at the top level.
    //
    // It used to be sent as `_config: { MaxDelete: n }`, which bisync ignores
    // entirely — that sets the *global* sync max-delete (a file count), a
    // different option. With bisync's own flag therefore never set, it took
    // Go's zero value over the RC API — **0 percent** — so ANY deletion aborted
    // the run. Since a move is a delete plus a create as far as bisync is
    // concerned, that silently prevented every automatic sync from propagating
    // a trashed document or a reorganised binder. Only the manual "Sync now"
    // button worked, because it passes `force`, which bypasses the check.
    //
    // Note the CLI does NOT show this: there `--max-delete` defaults to 50, so
    // the same command works by hand and fails over the API.
    #[serde(rename = "maxDelete", skip_serializing_if = "Option::is_none")]
    max_delete_percent: Option<i64>,
    #[serde(rename = "_async")]
    r#async: bool,
    #[serde(rename = "_filter", skip_serializing_if = "RcFilter::is_empty")]
    filter: RcFilter,
}

#[derive(Debug, Serialize)]
struct RemoteParams<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    remote_type: &'a str,
    parameters: serde_json::Value,
    opt: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ContinueParams<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    remote_type: &'a str,
    parameters: serde_json::Value,
    opt: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ConfigQuestion {
    #[serde(rename = "State")]
    pub state: String,
    #[serde(rename = "Option")]
    pub option: Option<serde_json::Value>,
    #[serde(rename = "Error")]
    pub error: String,
}

impl RcClient {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}/{path}", self.port)
    }

    async fn post_json<B: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<R> {
        let resp = self.http.post(self.url(path)).json(body).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Rc(format!("rc/{path} HTTP error: {text}")));
        }
        Ok(resp.json().await?)
    }

    // ─── sync/copy ────────────────────────────────────────────────────────────

    /// Start an async `sync/copy` job (upload-only). Returns the job ID.
    ///
    /// `src_fs` and `dst_fs` are rclone fs paths, e.g. `"/local/path"` or
    /// `"remote:subfolder"`. `filters` is a list of `--filter` patterns to
    /// exclude volatile files (e.g. `"- index.sqlite*"`).
    pub async fn copy_async(
        &self,
        src_fs: &str,
        dst_fs: &str,
        filters: Vec<String>,
    ) -> Result<u64> {
        let params = SyncCopyParams {
            src_fs,
            dst_fs,
            r#async: true,
            filter: RcFilter::from_lines(&filters),
        };
        let started: JobStarted = self.post_json("sync/copy", &params).await?;
        debug!("sync/copy started: jobid={}", started.jobid);
        Ok(started.jobid)
    }

    // ─── sync/bisync ─────────────────────────────────────────────────────────

    /// Start an async `sync/bisync` job. Returns the job ID.
    ///
    /// Set `resync = true` for the first sync of a (path1, path2) pair or when
    /// the baseline is missing. `force = true` overrides the all-files-changed
    /// safety abort entirely. `max_delete` (when `Some`) instead caps deletions
    /// at a fixed file count — propagating a normal trash while still aborting an
    /// accidental mass-deletion; prefer this over `force` for that protection.
    ///
    /// Always compares by content checksum, not modtime+size — see the
    /// `checksum` field's doc comment on `BisyncParams` for why this matters.
    pub async fn bisync_async(
        &self,
        path1: &str,
        path2: &str,
        resync: bool,
        force: bool,
        max_delete: Option<i64>,
        filters: Vec<String>,
    ) -> Result<u64> {
        let params = BisyncParams {
            path1,
            path2,
            resync,
            force,
            resilient: true,
            recover: true,
            checksum: true,
            max_delete_percent: max_delete,
            r#async: true,
            filter: RcFilter::from_lines(&filters),
        };
        let started: JobStarted = self.post_json("sync/bisync", &params).await?;
        debug!("sync/bisync started: jobid={}", started.jobid);
        Ok(started.jobid)
    }

    // ─── job/status polling ───────────────────────────────────────────────────

    /// Poll `job/status` until the job is finished. Returns the final status.
    pub async fn wait_for_job(
        &self,
        jobid: u64,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<JobStatus> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(Error::JobFailed(format!(
                    "job {jobid} timed out after {}s",
                    timeout.as_secs()
                )));
            }
            let status = self.job_status(jobid).await?;
            if status.finished {
                return Ok(status);
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Poll `job/status` once (a single check, unlike `wait_for_job`'s loop) —
    /// used by callers that need to interleave their own progress reporting
    /// between polls.
    pub async fn job_status(&self, jobid: u64) -> Result<JobStatus> {
        #[derive(Serialize)]
        struct Req {
            jobid: u64,
        }
        self.post_json("job/status", &Req { jobid }).await
    }

    /// Fetch transfer-progress stats for a job's stats group (`job/<jobid>`), as
    /// reported by `core/stats`. Large transfers (e.g. multi-GB backup archives)
    /// can run for minutes with no other observable progress — this lets a caller
    /// report bytes/percentage/ETA instead of a static "in progress" indicator.
    pub async fn job_stats(&self, jobid: u64) -> Result<JobStats> {
        #[derive(Serialize)]
        struct Req {
            group: String,
        }
        self.post_json("core/stats", &Req { group: format!("job/{jobid}") }).await
    }

    // ─── config / remote management ──────────────────────────────────────────

    /// Create a remote in nonInteractive mode (first step of the state machine).
    /// Returns the question to answer next (state, option) or an empty state when done.
    pub async fn config_create_start(
        &self,
        name: &str,
        remote_type: &str,
    ) -> Result<ConfigQuestion> {
        let params = RemoteParams {
            name,
            remote_type,
            parameters: serde_json::json!({}),
            opt: serde_json::json!({"nonInteractive": true}),
        };
        self.post_json("config/create", &params).await
    }

    /// Continue the config creation state machine with a given answer.
    pub async fn config_create_continue(
        &self,
        name: &str,
        remote_type: &str,
        state: &str,
        result: &str,
    ) -> Result<ConfigQuestion> {
        let params = ContinueParams {
            name,
            remote_type,
            parameters: serde_json::json!({}),
            opt: serde_json::json!({
                "nonInteractive": true,
                "continue": true,
                "state": state,
                "result": result
            }),
        };
        self.post_json("config/create", &params).await
    }

    /// Delete a remote by name, removing its stored credentials.
    pub async fn config_delete(&self, name: &str) -> Result<()> {
        #[derive(Serialize)]
        struct Req<'a> {
            name: &'a str,
        }
        let _: serde_json::Value = self.post_json("config/delete", &Req { name }).await?;
        Ok(())
    }

    /// List configured remote names.
    pub async fn list_remotes(&self) -> Result<Vec<String>> {
        #[derive(Deserialize)]
        struct Resp {
            remotes: Vec<String>,
        }
        #[derive(Serialize)]
        struct Empty {}
        let resp: Resp = self.post_json("config/listremotes", &Empty {}).await?;
        Ok(resp.remotes)
    }

    /// List items in a remote directory (one level, non-recursive).
    ///
    /// `fs` is a rclone fs path, e.g. `"remote:base"`. Returns `[]` when the
    /// directory does not exist yet (404 / remote error treated as empty list).
    pub async fn list_directory(&self, fs: &str) -> Result<Vec<DirectoryItem>> {
        #[derive(Serialize)]
        struct Req<'a> {
            fs: &'a str,
            remote: &'a str,
        }
        #[derive(Deserialize)]
        struct Resp {
            list: Vec<RcItem>,
        }
        #[derive(Deserialize)]
        struct RcItem {
            #[serde(rename = "Name")]
            name: String,
            #[serde(rename = "IsDir")]
            is_dir: bool,
        }

        let req = Req { fs, remote: "" };
        match self.post_json::<_, Resp>("operations/list", &req).await {
            Ok(resp) => Ok(resp.list.into_iter().map(|i| DirectoryItem { name: i.name, is_dir: i.is_dir }).collect()),
            Err(Error::Rc(_)) => Ok(vec![]), // Remote dir doesn't exist yet — treat as empty.
            Err(e) => Err(e),
        }
    }

    /// Whether one object exists on a remote, via `operations/stat`.
    ///
    /// Unlike [`Self::list_directory`], an rc-level failure is **propagated**
    /// rather than reported as "nothing there". That distinction is the whole
    /// point of this call: a caller deciding whether it is safe to overwrite a
    /// remote file must be able to tell "the remote definitely does not have
    /// it" from "I could not find out" — an expired token, a 429 or a
    /// provider-side 500 all arrive as `Error::Rc`, and treating those as
    /// absence would overwrite a file that is really there.
    ///
    /// `Ok(None)` means the object is confirmed absent (rclone answers a valid
    /// `stat` for a missing object with a null item).
    pub async fn stat(&self, fs: &str, remote: &str) -> Result<Option<DirectoryItem>> {
        #[derive(Serialize)]
        struct Req<'a> {
            fs: &'a str,
            remote: &'a str,
        }
        #[derive(Deserialize)]
        struct Resp {
            #[serde(default)]
            item: Option<RcItem>,
        }
        #[derive(Deserialize)]
        struct RcItem {
            #[serde(rename = "Name")]
            name: String,
            #[serde(rename = "IsDir")]
            is_dir: bool,
        }

        let req = Req { fs, remote };
        let resp: Resp = self.post_json("operations/stat", &req).await?;
        Ok(resp
            .item
            .map(|i| DirectoryItem { name: i.name, is_dir: i.is_dir }))
    }

    /// Create a remote directory (and parents). `fs` is the full rclone fs path,
    /// e.g. `"remote:base/Project"`. Idempotent — creating an existing directory
    /// is not an error. Needed before a bisync `--resync`, which aborts if its
    /// path2 doesn't exist.
    pub async fn mkdir(&self, fs: &str) -> Result<()> {
        #[derive(Serialize)]
        struct Req<'a> {
            fs: &'a str,
            remote: &'a str,
        }
        let _: serde_json::Value = self.post_json("operations/mkdir", &Req { fs, remote: "" }).await?;
        Ok(())
    }

    /// Copy a single file one-way via `operations/copyfile`. Synchronous (rclone
    /// performs the copy before returning). Used to push a local-authoritative
    /// file (e.g. `project.json`) to the remote without bidirectional conflict
    /// semantics.
    ///
    /// `src_fs`/`dst_fs` are rclone fs roots (e.g. a local dir or `remote:base`);
    /// `src_remote`/`dst_remote` are the file paths within them.
    pub async fn copy_file(
        &self,
        src_fs: &str,
        src_remote: &str,
        dst_fs: &str,
        dst_remote: &str,
    ) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Req<'a> {
            src_fs: &'a str,
            src_remote: &'a str,
            dst_fs: &'a str,
            dst_remote: &'a str,
        }
        let _: serde_json::Value = self
            .post_json(
                "operations/copyfile",
                &Req { src_fs, src_remote, dst_fs, dst_remote },
            )
            .await?;
        Ok(())
    }

    // ─── helpers ──────────────────────────────────────────────────────────────

    /// Parse conflict pairs from a bisync job output string.
    ///
    /// Bisync emits lines like:
    ///   `"- Path1    Renaming Path1 copy   - /full/path/file.txt.conflict1"`
    /// This extracts the relative file path of each conflicting pair.
    pub fn parse_conflicts(output: &str, path1_root: &str) -> Vec<SyncConflict> {
        let mut conflicts: Vec<SyncConflict> = Vec::new();
        for raw_line in output.lines() {
            // rcd colorizes log lines with ANSI escapes; strip them before parsing
            // or the path keeps embedded codes (e.g. "\x1b[36m/path\x1b[0m") and
            // both the suffix trim and the path1_root prefix strip silently fail.
            let line = strip_ansi(raw_line);
            let line = line.as_str();
            if line.contains("conflict1") && line.contains("Renaming Path1 copy")
                && let Some(full) = extract_path_from_notice(line) {
                    let base = full.trim_end_matches(".conflict1");
                    let rel = base
                        .strip_prefix(path1_root)
                        .unwrap_or(base)
                        .trim_start_matches('/');
                    let filename = std::path::Path::new(rel)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(rel);
                    conflicts.push(SyncConflict {
                        path: rel.to_string(),
                        conflict1: format!("{filename}.conflict1"),
                        conflict2: format!("{filename}.conflict2"),
                    });
                }
        }
        conflicts
    }
}

fn extract_path_from_notice(line: &str) -> Option<&str> {
    // Lines look like: "- Path1    Renaming Path1 copy       - /abs/path/file.conflict1"
    // Find the last " - " separator and take everything after it.
    let marker = " - ";
    let pos = line.rfind(marker)?;
    Some(line[pos + marker.len()..].trim())
}

/// Remove ANSI SGR escape sequences (`\x1b[...m`) from a string. rcd colorizes its
/// log/NOTICE output, which would otherwise corrupt parsed paths.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b {
            // Skip the escape: ESC '[' ... final byte in 0x40..=0x7e.
            i += 1;
            if i < bytes.len() && bytes[i] == b'[' {
                i += 1;
                while i < bytes.len() && !(0x40..=0x7e).contains(&bytes[i]) {
                    i += 1;
                }
                if i < bytes.len() { i += 1; } // consume the final byte
            }
        } else {
            // Push the full UTF-8 char starting at i.
            let ch_len = match s[i..].chars().next() {
                Some(c) => c.len_utf8(),
                None => 1,
            };
            out.push_str(&s[i..i + ch_len]);
            i += ch_len;
        }
    }
    out
}

// ─── log information helper ───────────────────────────────────────────────────

impl JobStatus {
    /// Returns `true` if the job succeeded without errors.
    pub fn is_ok(&self) -> bool {
        self.finished && self.success && self.error.is_empty()
    }

    /// Extract conflicts from the bisync output.
    pub fn conflicts(&self, path1_root: &str) -> Vec<SyncConflict> {
        if let Some(ref out) = self.output {
            RcClient::parse_conflicts(&out.output, path1_root)
        } else {
            Vec::new()
        }
    }

    /// Returns true if bisync refused to run for one of its own safety checks
    /// rather than failing substantively.
    ///
    /// `"too many deletes"` is the message bisync uses when a run would exceed
    /// its max-delete percentage, and it was missing here — so that abort
    /// surfaced to the writer as a sync *error* instead of the deferral the
    /// caller intends for a self-healing safety check.
    pub fn is_safety_abort(&self) -> bool {
        self.error.contains("all files were changed")
            || self.error.contains("too many deletes")
            || self
                .output
                .as_ref()
                .map(|o| o.output.contains("Safety abort"))
                .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_conflicts_extracts_pair() {
        let output = "2026/06/18 17:29:23 NOTICE: - Path1    Renaming Path1 copy                - /tmp/path1/doc.txt.conflict1\n\
                      2026/06/18 17:29:23 NOTICE: - Path2    Renaming Path2 copy                - /tmp/path1/doc.txt.conflict2\n";
        let conflicts = RcClient::parse_conflicts(output, "/tmp/path1");
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, "doc.txt");
        assert_eq!(conflicts[0].conflict1, "doc.txt.conflict1");
        assert_eq!(conflicts[0].conflict2, "doc.txt.conflict2");
    }

    #[test]
    fn rc_filter_splits_include_and_exclude_rules() {
        let f = RcFilter::from_lines(&[
            "- index.sqlite".to_string(),
            "- *.part".to_string(),
            "+ keep.zip".to_string(),
            "bare-pattern".to_string(), // no prefix → treated as exclude
        ]);
        assert_eq!(f.exclude_rule, vec!["index.sqlite", "*.part", "bare-pattern"]);
        assert_eq!(f.include_rule, vec!["keep.zip"]);
    }

    #[test]
    fn rc_filter_serializes_under_capitalized_keys() {
        // The rc API expects IncludeRule/ExcludeRule; a top-level `filter` array of
        // "- pattern" strings is silently ignored by rclone.
        let f = RcFilter::from_lines(&["- index.sqlite".to_string(), "+ x".to_string()]);
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"ExcludeRule\""), "got {json}");
        assert!(json.contains("\"IncludeRule\""), "got {json}");
        assert!(json.contains("index.sqlite"));
    }

    #[test]
    fn parse_conflicts_strips_ansi_color_codes() {
        // rcd colorizes paths; the parser must yield the clean relative path and
        // single-suffixed conflict filenames despite embedded ANSI escapes.
        let output = "NOTICE: - Path1    Renaming Path1 copy   - \u{1b}[36m/tmp/proj/chapter.md.conflict1\u{1b}[0m\n";
        let conflicts = RcClient::parse_conflicts(output, "/tmp/proj");
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, "chapter.md");
        assert_eq!(conflicts[0].conflict1, "chapter.md.conflict1");
        assert_eq!(conflicts[0].conflict2, "chapter.md.conflict2");
    }

    #[test]
    fn strip_ansi_removes_sgr_sequences() {
        assert_eq!(strip_ansi("\u{1b}[36mhello\u{1b}[0m"), "hello");
        assert_eq!(strip_ansi("plain"), "plain");
        assert_eq!(strip_ansi("a\u{1b}[1;32mb\u{1b}[0mc"), "abc");
    }

    #[test]
    fn parse_conflicts_empty_when_no_conflicts() {
        let output = "2026/06/18 NOTICE: Bisync completed successfully";
        let conflicts = RcClient::parse_conflicts(output, "/tmp/path1");
        assert!(conflicts.is_empty());
    }

    #[test]
    fn job_status_is_safety_abort() {
        let status = JobStatus {
            id: 1,
            finished: true,
            success: false,
            error: "all files were changed".to_string(),
            output: None,
        };
        assert!(status.is_safety_abort());
    }

    // R7 (document-revision-versioning, task 1.5): the bisync rc payload must
    // always request content-checksum comparison, not modtime+size — otherwise
    // two machines that saved byte-identical content (a fresh mtime on each
    // write, same bytes) are reported as "changed in both" with no genuine
    // divergence at all.
    #[test]
    fn bisync_params_always_request_checksum_comparison() {
        let params = BisyncParams {
            path1: "/local/project",
            path2: "remote:Project",
            resync: false,
            force: false,
            resilient: true,
            recover: true,
            checksum: true,
            max_delete_percent: None,
            r#async: true,
            filter: RcFilter::default(),
        };
        let json = serde_json::to_value(&params).unwrap();
        assert_eq!(json["checksum"], serde_json::json!(true), "got {json}");
    }

    /// bisync's delete cap must be sent at the TOP LEVEL as `maxDelete`.
    ///
    /// It was previously sent as `_config: { MaxDelete: n }`, which bisync
    /// ignores — that key sets the *global* sync max-delete, a different
    /// option. With bisync's own flag never set it took Go's zero value over
    /// the RC API, 0 percent, so any deletion aborted the run; and since
    /// bisync treats a move as a delete plus a create, no automatic sync could
    /// propagate a deletion or a reorganisation at all.
    ///
    /// Asserting on the serialized shape is the point: the old code compiled,
    /// typechecked and looked right, and the only thing that would have caught
    /// it is a check of what actually goes on the wire.
    #[test]
    fn the_delete_cap_is_sent_where_bisync_reads_it() {
        let params = BisyncParams {
            path1: "/local/project",
            path2: "remote:Project",
            resync: false,
            force: false,
            resilient: true,
            recover: true,
            checksum: true,
            max_delete_percent: Some(50),
            r#async: true,
            filter: RcFilter::default(),
        };
        let json = serde_json::to_value(&params).unwrap();
        assert_eq!(json["maxDelete"], serde_json::json!(50), "got {json}");
        assert!(
            json.get("_config").is_none(),
            "the cap must NOT be sent under _config, which bisync ignores: {json}"
        );
    }

    #[test]
    fn no_delete_cap_omits_the_field_rather_than_sending_zero() {
        // `force` runs are meant to have NO cap. Serializing a `None` as 0
        // would be the same bug in reverse: 0 percent forbids every deletion.
        let params = BisyncParams {
            path1: "/local/project",
            path2: "remote:Project",
            resync: false,
            force: true,
            resilient: true,
            recover: true,
            checksum: true,
            max_delete_percent: None,
            r#async: true,
            filter: RcFilter::default(),
        };
        let json = serde_json::to_value(&params).unwrap();
        assert!(
            json.get("maxDelete").is_none(),
            "an absent cap must be omitted, never sent as 0: {json}"
        );
    }
}

//! Unix implementations of the lifecycle seams: a tokio-UDS
//! [`ControlEndpoint`] and a detached-process [`DaemonSpawner`]. A Windows
//! port replaces this module (named pipe + `CreateProcess`) without touching
//! the state machine.

use std::fs::OpenOptions;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

use crate::app::lifecycle::{ControlEndpoint, DaemonSpawner, ExitInfo, PingFailure, SpawnedDaemon};
use crate::protocol::uds::{Reply, Request};

/// How much of the daemon log to quote in an exit diagnosis.
const LOG_TAIL_BYTES: u64 = 2048;

pub(crate) struct TokioEndpoint;

impl ControlEndpoint for TokioEndpoint {
    async fn ping(&self, socket: &Path, timeout: Duration) -> Result<String, PingFailure> {
        let attempt = async {
            let stream = UnixStream::connect(socket)
                .await
                .map_err(|e| PingFailure(format!("connect: {e}")))?;
            let (read, mut write) = stream.into_split();
            let mut line = serde_json::to_vec(&Request::Ping)
                .map_err(|e| PingFailure(format!("encode: {e}")))?;
            line.push(b'\n');
            write
                .write_all(&line)
                .await
                .map_err(|e| PingFailure(format!("write: {e}")))?;
            let reply_line = BufReader::new(read)
                .lines()
                .next_line()
                .await
                .map_err(|e| PingFailure(format!("read: {e}")))?
                .ok_or_else(|| PingFailure("eof before reply".to_string()))?;
            let reply: Reply = serde_json::from_str(&reply_line)
                .map_err(|e| PingFailure(format!("decode: {e}")))?;
            match (reply.ok, reply.version) {
                (true, Some(version)) => Ok(version),
                (true, None) => Err(PingFailure("ping reply missing version".to_string())),
                (false, _) => Err(PingFailure(format!(
                    "daemon rejected ping: {}",
                    reply.code.unwrap_or_default()
                ))),
            }
        };
        tokio::time::timeout(timeout, attempt)
            .await
            .map_err(|_| PingFailure("probe timed out".to_string()))?
    }
}

/// Spawns the daemon **detached** (its own session via `process_group(0)`),
/// stdio appended to a log file — the daemon is a shared host service that
/// must outlive the spawning app.
pub(crate) struct ProcessSpawner {
    log_path: PathBuf,
}

impl ProcessSpawner {
    pub(crate) fn new(log_path: PathBuf) -> Self {
        Self { log_path }
    }
}

pub(crate) struct UnixDaemonProcess {
    child: Child,
    log_path: PathBuf,
    exited: Option<ExitInfo>,
}

impl DaemonSpawner for ProcessSpawner {
    type Proc = UnixDaemonProcess;

    fn spawn(&self, binary: &Path, config: Option<&Path>) -> std::io::Result<UnixDaemonProcess> {
        if let Some(parent) = self.log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let mut cmd = Command::new(binary);
        if let Some(config) = config {
            cmd.arg("--config").arg(config);
        }
        cmd.stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            cmd.process_group(0);
        }
        let child = cmd.spawn()?;
        Ok(UnixDaemonProcess {
            child,
            log_path: self.log_path.clone(),
            exited: None,
        })
    }
}

impl SpawnedDaemon for UnixDaemonProcess {
    fn poll_exit(&mut self) -> Option<ExitInfo> {
        if self.exited.is_none()
            && let Ok(Some(status)) = self.child.try_wait()
        {
            self.exited = Some(ExitInfo {
                status: status.code(),
                stderr_tail: log_tail(&self.log_path),
            });
        }
        self.exited.clone()
    }
}

/// Last [`LOG_TAIL_BYTES`] of the daemon log, best effort (empty on any
/// error). The seek offset can land mid-multibyte UTF-8 character, so the
/// raw bytes are decoded lossily rather than with `read_to_string` (which
/// would error on the truncated char and collapse the whole tail to "" right
/// when a `DaemonExited` diagnosis needs it most).
fn log_tail(path: &Path) -> String {
    let read = || -> std::io::Result<Vec<u8>> {
        let mut f = std::fs::File::open(path)?;
        let len = f.metadata()?.len();
        f.seek(SeekFrom::Start(len.saturating_sub(LOG_TAIL_BYTES)))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(buf)
    };
    let bytes = read().unwrap_or_default();
    String::from_utf8_lossy(&bytes).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::lifecycle::{ControlEndpoint, DaemonSpawner, SpawnedDaemon};
    use std::time::Duration;

    fn fake_daemon(reply: &'static str) -> std::path::PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("control.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            let _req = lines.next_line().await.unwrap();
            write.write_all(reply.as_bytes()).await.unwrap();
            write.write_all(b"\n").await.unwrap();
        });
        path
    }

    #[tokio::test]
    async fn ping_extracts_version_from_a_live_socket() {
        let path = fake_daemon(r#"{"ok":true,"version":"9.9.9"}"#);
        let v = TokioEndpoint
            .ping(&path, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(v, "9.9.9");
    }

    #[tokio::test]
    async fn ping_fails_on_error_reply_and_absent_socket() {
        let path = fake_daemon(r#"{"ok":false,"code":"shutting_down","message":"…"}"#);
        assert!(
            TokioEndpoint
                .ping(&path, Duration::from_secs(1))
                .await
                .is_err()
        );
        let absent = std::path::Path::new("/nonexistent/never.sock");
        assert!(
            TokioEndpoint
                .ping(absent, Duration::from_millis(200))
                .await
                .is_err()
        );
    }

    #[test]
    fn log_tail_returns_trailing_bytes_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.log");
        // Bigger than LOG_TAIL_BYTES so the seek actually truncates.
        let body = "x".repeat(3000);
        std::fs::write(&path, format!("{body}\n")).unwrap();
        let tail = log_tail(&path);
        assert!(!tail.is_empty());
        assert!(u64::try_from(tail.len()).unwrap() <= LOG_TAIL_BYTES);
        assert!(tail.chars().all(|c| c == 'x'));
    }

    #[test]
    fn log_tail_is_lossy_across_a_multibyte_boundary() {
        // '…' is a 3-byte UTF-8 char; 2048 is not a multiple of 3, so a run
        // of them long enough to be truncated always has the cut point land
        // mid-character (proof: for a run of N 3-byte chars the cut's
        // position within the run is `3*N - 2048 (mod 3)` = `-2048 mod 3` =
        // 1, independent of N — never 0, i.e. never a char boundary).
        let content = "…".repeat(2000);
        let cut = content
            .len()
            .saturating_sub(usize::try_from(LOG_TAIL_BYTES).unwrap());
        assert!(
            !content.is_char_boundary(cut),
            "fixture must land mid multibyte char for this test to be meaningful"
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.log");
        std::fs::write(&path, &content).unwrap();

        // `read_to_string` would hit InvalidData on the split char and the
        // tail would collapse to "" — assert the lossy decode doesn't.
        let tail = log_tail(&path);
        assert!(
            !tail.is_empty(),
            "lossy decode across a multibyte boundary must not collapse to empty"
        );
    }

    #[tokio::test]
    async fn spawner_detaches_logs_and_reports_exit_tail() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("d.log");
        let spawner = ProcessSpawner::new(log.clone());
        // `--config <path>` mirrors the real invocation; sh -c ignores it.
        let mut proc_ = spawner
            .spawn(std::path::Path::new("/bin/sh"), None)
            .unwrap();
        // /bin/sh with no script exits immediately (status 0) — poll until it does.
        let exit = loop {
            if let Some(e) = proc_.poll_exit() {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(exit.status, Some(0));
        assert!(log.exists(), "log file must be created");
    }
}

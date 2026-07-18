//! Windows implementations of the lifecycle seams: a named-pipe
//! [`ControlEndpoint`] and a detached-process [`DaemonSpawner`]. The Windows
//! counterpart of [`super::platform`] (Unix/UDS); the find-or-spawn state
//! machine in [`super::lifecycle`] is untouched — it selects between the two
//! at the single wiring point in [`super::mod`].

use std::fs::OpenOptions;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::os::windows::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

use crate::app::lifecycle::{
    ControlEndpoint, DaemonHello, DaemonSpawner, ExitInfo, PingFailure, SpawnedDaemon,
};
use crate::protocol::uds::{Reply, Request};

/// How much of the daemon log to quote in an exit diagnosis.
const LOG_TAIL_BYTES: u64 = 2048;

// `CreateProcess` creation flag (winbase.h): `DETACHED_PROCESS` gives the child
// no console at all — the Windows analog of the Unix `process_group(0)` session
// detach — so the shared host daemon outlives the app that spawned it and never
// receives that app's console control events. `CREATE_NEW_PROCESS_GROUP` and
// `CREATE_NO_WINDOW` are omitted: both are redundant with a console-less
// detached child (and the former conflicts with `DETACHED_PROCESS`).
const DETACHED_PROCESS: u32 = 0x0000_0008;

pub(crate) struct TokioEndpoint;

impl ControlEndpoint for TokioEndpoint {
    async fn ping(&self, socket: &Path, timeout: Duration) -> Result<DaemonHello, PingFailure> {
        let attempt = async {
            // The control-socket `Path` carries the pipe name on Windows
            // (`\\.\pipe\datamancer\<user>\control`; see `crate::paths`).
            // `connect_verified` retries `ERROR_PIPE_BUSY` and verifies the
            // pipe's owner SID is this user's (review B1) before we trust the
            // daemon's hello.
            let stream = crate::win_pipe::connect_verified(socket)
                .await
                .map_err(|e| PingFailure(format!("connect: {e}")))?;
            let (read, mut write) = tokio::io::split(stream);
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
                (true, Some(version)) => Ok(DaemonHello {
                    version,
                    credential_backend: reply.credential_backend,
                }),
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

/// Spawns the daemon **detached** (no inherited console, own process group),
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

pub(crate) struct WindowsDaemonProcess {
    child: Child,
    log_path: PathBuf,
    exited: Option<ExitInfo>,
}

impl DaemonSpawner for ProcessSpawner {
    type Proc = WindowsDaemonProcess;

    fn spawn(&self, binary: &Path, config: Option<&Path>) -> std::io::Result<WindowsDaemonProcess> {
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
            .stderr(log)
            .creation_flags(DETACHED_PROCESS);
        let child = cmd.spawn()?;
        Ok(WindowsDaemonProcess {
            child,
            log_path: self.log_path.clone(),
            exited: None,
        })
    }
}

impl SpawnedDaemon for WindowsDaemonProcess {
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
/// error). The seek offset can land mid-multibyte UTF-8 character, so the raw
/// bytes are decoded lossily rather than with `read_to_string` (which would
/// error on the truncated char and collapse the whole tail to "").
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

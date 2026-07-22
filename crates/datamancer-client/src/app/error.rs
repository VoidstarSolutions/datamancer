use std::path::PathBuf;
use std::time::Duration;

use crate::ClientError;
#[cfg(not(windows))]
use crate::iceoryx2::Iceoryx2ClientError;

/// Why a spawned daemon never became ready (inside
/// [`EnsureError::ReadyTimeout`]).
#[derive(Debug)]
pub enum ReadyDiagnosis {
    /// The spawned process exited before the socket answered — and a
    /// subsequent connect never succeeded either (a lost spawn race whose
    /// winner answers is success, not this).
    DaemonExited {
        status: Option<i32>,
        /// Tail of the daemon log (best effort; empty if unreadable).
        stderr_tail: String,
    },
    /// The process appears alive but the socket never answered a ping.
    Unresponsive {
        /// The final probe's diagnostic reason (connect refused, stale
        /// socket, bad reply…). `None` only if no probe ran.
        last_ping_failure: Option<String>,
    },
}

impl std::fmt::Display for ReadyDiagnosis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DaemonExited {
                status,
                stderr_tail,
            } => {
                let status = status.map_or_else(|| "unknown".to_string(), |s| s.to_string());
                write!(
                    f,
                    "daemon exited (status: {status}), stderr tail: {stderr_tail}"
                )
            }
            Self::Unresponsive {
                last_ping_failure: Some(reason),
            } => write!(f, "daemon unresponsive: {reason}"),
            Self::Unresponsive {
                last_ping_failure: None,
            } => write!(f, "daemon unresponsive (no probe completed)"),
        }
    }
}

/// Failure to find-or-spawn-and-connect a daemon.
#[derive(Debug, thiserror::Error)]
pub enum EnsureError {
    /// Also returned when the daemon-log path can't be resolved
    /// (`EnsureConfig::log_path` unset and `paths::default_daemon_log`
    /// fails) — both stem from the same no-home-dir condition.
    #[error(
        "no control-socket (or daemon-log) path: no platform default derivable \
         (no home/runtime dir); set EnsureConfig::control_socket/log_path explicitly"
    )]
    NoSocketPath,
    #[error("failed to spawn datamancerd at {binary}: {source}", binary = binary.display())]
    SpawnFailed {
        binary: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("daemon not ready within {timeout:?}: {diagnosis}")]
    ReadyTimeout {
        timeout: Duration,
        diagnosis: ReadyDiagnosis,
    },
    #[error("version skew: daemon {daemon} incompatible with client {client}")]
    VersionSkew { daemon: String, client: String },
    /// The data-plane client failed to connect. On unix this is the iceoryx2
    /// client (control + shm); on Windows the hybrid's WS-loopback data client.
    #[cfg(not(windows))]
    #[error(transparent)]
    Connect(#[from] ClientError<Iceoryx2ClientError>),
    #[cfg(windows)]
    #[error(transparent)]
    Connect(#[from] ClientError<crate::ws::WsClientError>),
    /// The Windows hybrid's admin (named-pipe control) plane failed to connect.
    /// Distinct from [`Self::Connect`] (the WS data plane) — the two planes are
    /// independent connections on Windows.
    #[cfg(windows)]
    #[error("admin control pipe connect failed: {0}")]
    AdminConnect(#[from] crate::pipe_control::PipeControlError),
}

//! App-facing facade for datamancerd (spec 2026-07-05, cycle 1): find a
//! running daemon or spawn one, connect, and expose typed health.
//!
//! Adds **no** protocol semantics — every capability maps to control-surface
//! ops a hand-rolled client could issue. Spawn is detached and unsupervised:
//! the daemon is a shared host service that outlives the app that spawned it;
//! if it dies, the event stream ends and the app calls `AppHandle::ensure`
//! again (reconnect-by-recreate).

mod error;
mod lifecycle;

pub use error::{EnsureError, ReadyDiagnosis};

use std::path::PathBuf;
use std::time::Duration;

/// Parameters for `AppHandle::ensure` (`AppHandle` lands with the facade).
#[derive(Debug, Clone)]
pub struct EnsureConfig {
    /// The datamancerd binary to spawn if none is running. Explicit — no
    /// `PATH` search (a bundling app knows its sidecar's location; guessing
    /// invites version skew and PATH hijack).
    pub daemon_binary: PathBuf,
    /// Daemon config file. `None` = the daemon's platform default (which
    /// self-scaffolds on first run).
    pub config_path: Option<PathBuf>,
    /// Control socket. `None` = `crate::default_control_socket()`.
    pub control_socket: Option<PathBuf>,
    /// This client's name for `open-client` (unique per daemon).
    pub client_name: String,
    /// Bound on spawn-to-ready. Default 10 s.
    pub ready_timeout: Duration,
    /// Spawned daemon's stdout/stderr destination. `None` = the platform
    /// default (`crate::paths::default_daemon_log()`, Task 5).
    pub log_path: Option<PathBuf>,
    /// Forwarded to the iceoryx2 client (idle poll sleep).
    pub poll_interval: Duration,
    /// Forwarded to the iceoryx2 client (local event buffer bound).
    pub event_buffer: usize,
}

impl EnsureConfig {
    /// Defaults: 10 s ready timeout, 1 ms poll, 8192-event buffer, platform
    /// socket/config/log paths.
    #[must_use]
    pub fn new(daemon_binary: impl Into<PathBuf>, client_name: impl Into<String>) -> Self {
        Self {
            daemon_binary: daemon_binary.into(),
            config_path: None,
            control_socket: None,
            client_name: client_name.into(),
            ready_timeout: Duration::from_secs(10),
            log_path: None,
            poll_interval: Duration::from_millis(1),
            event_buffer: 8192,
        }
    }
}

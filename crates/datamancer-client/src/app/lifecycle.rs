//! Find-or-spawn-and-await-readiness, as a state machine over two seams so
//! the logic tests against fakes and a Windows port is additive: a
//! [`ControlEndpoint`] (UDS today, named pipe later) and a [`DaemonSpawner`]
//! (detached unix spawn today, `CreateProcess` later).

// Scaffolding: `platform.rs` (Task 5) supplies the real `ControlEndpoint`/
// `DaemonSpawner` impls but doesn't wire this state machine into anything
// yet — that's `AppHandle` (Task 6). Every item here is otherwise
// unreachable from the plain `lib` target under `--all-targets` (which
// doesn't count `#[cfg(test)]` usage). Remove once `AppHandle` lands.
#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use tokio::time::Instant;

use crate::app::EnsureConfig;
use crate::app::error::{EnsureError, ReadyDiagnosis};

/// Interval between readiness probes while awaiting a spawned daemon.
const READY_POLL: Duration = Duration::from_millis(100);
/// Per-probe bound (connect + ping round-trip).
const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// A failed readiness probe (absent socket, refused, stale socket, no/bad
/// reply). The reason is diagnostic only: every failure means "not ready".
#[derive(Debug, Clone)]
pub(crate) struct PingFailure(pub String);

/// One control-surface probe: ping the socket, return the daemon version.
pub(crate) trait ControlEndpoint {
    async fn ping(&self, socket: &Path, timeout: Duration) -> Result<String, PingFailure>;
}

/// A spawned daemon's exit observation (best effort).
#[derive(Debug, Clone)]
pub(crate) struct ExitInfo {
    pub status: Option<i32>,
    /// Tail of the daemon's log/stderr; empty if unavailable.
    pub stderr_tail: String,
}

/// Handle onto a spawned daemon process, for exit polling only — the spawn
/// is detached and deliberately unsupervised.
pub(crate) trait SpawnedDaemon: Send {
    /// `Some` once the process has exited (idempotent thereafter).
    fn poll_exit(&mut self) -> Option<ExitInfo>;
}

/// Spawns the daemon binary, detached, stdio to a log file.
pub(crate) trait DaemonSpawner {
    type Proc: SpawnedDaemon;
    fn spawn(&self, binary: &Path, config: Option<&Path>) -> std::io::Result<Self::Proc>;
}

/// Find a ready daemon on `socket` or spawn one and await readiness.
/// Returns the daemon's version (from `ping`).
///
/// A spawned process exiting is **not** failure while the deadline holds:
/// losing the single-instance race to another app's daemon that then answers
/// is success. The exit is only reported as the diagnosis if no daemon ever
/// answers.
pub(crate) async fn ensure_daemon<E: ControlEndpoint, S: DaemonSpawner>(
    endpoint: &E,
    spawner: &S,
    cfg: &EnsureConfig,
    socket: &Path,
) -> Result<String, EnsureError> {
    if let Ok(version) = endpoint.ping(socket, PROBE_TIMEOUT).await {
        return Ok(version);
    }
    let mut proc_ = spawner
        .spawn(&cfg.daemon_binary, cfg.config_path.as_deref())
        .map_err(|source| EnsureError::SpawnFailed {
            binary: cfg.daemon_binary.clone(),
            source,
        })?;
    let deadline = Instant::now() + cfg.ready_timeout;
    let mut observed_exit: Option<ExitInfo> = None;
    loop {
        if let Ok(version) = endpoint.ping(socket, PROBE_TIMEOUT).await {
            return Ok(version);
        }
        if observed_exit.is_none() {
            observed_exit = proc_.poll_exit();
        }
        if Instant::now() >= deadline {
            let diagnosis = match observed_exit {
                Some(ExitInfo {
                    status,
                    stderr_tail,
                }) => ReadyDiagnosis::DaemonExited {
                    status,
                    stderr_tail,
                },
                None => ReadyDiagnosis::Unresponsive,
            };
            return Err(EnsureError::ReadyTimeout {
                timeout: cfg.ready_timeout,
                diagnosis,
            });
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

/// Compatibility floor: equal major version, and equal minor while major
/// is 0 (cargo semver convention). Unparseable versions are incompatible.
pub(crate) fn version_compatible(client: &str, daemon: &str) -> bool {
    fn major_minor(v: &str) -> Option<(u64, u64)> {
        let mut parts = v.split('.');
        Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
    }
    match (major_minor(client), major_minor(daemon)) {
        (Some((cmaj, cmin)), Some((dmaj, dmin))) => cmaj == dmaj && (cmaj != 0 || cmin == dmin),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{EnsureConfig, EnsureError, ReadyDiagnosis};
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Ping outcomes served in order; the last entry repeats forever.
    struct ScriptedEndpoint {
        script: Vec<Result<String, PingFailure>>,
        calls: AtomicUsize,
    }
    impl ScriptedEndpoint {
        fn new(script: Vec<Result<String, PingFailure>>) -> Self {
            Self {
                script,
                calls: AtomicUsize::new(0),
            }
        }
    }
    impl ControlEndpoint for ScriptedEndpoint {
        async fn ping(&self, _: &Path, _: Duration) -> Result<String, PingFailure> {
            let i = self.calls.fetch_add(1, Ordering::SeqCst);
            self.script[i.min(self.script.len() - 1)].clone()
        }
    }

    struct ScriptedProc {
        /// `poll_exit` returns `None` this many times, then `Some(exit)`.
        alive_polls: usize,
        exit: Option<ExitInfo>,
    }
    impl SpawnedDaemon for ScriptedProc {
        fn poll_exit(&mut self) -> Option<ExitInfo> {
            if self.alive_polls > 0 {
                self.alive_polls -= 1;
                return None;
            }
            self.exit.clone()
        }
    }

    struct ScriptedSpawner {
        result: Mutex<Option<std::io::Result<ScriptedProc>>>,
        spawned: AtomicUsize,
    }
    impl ScriptedSpawner {
        fn ok(proc_: ScriptedProc) -> Self {
            Self {
                result: Mutex::new(Some(Ok(proc_))),
                spawned: AtomicUsize::new(0),
            }
        }
        fn fails() -> Self {
            Self {
                result: Mutex::new(Some(Err(std::io::Error::from(
                    std::io::ErrorKind::NotFound,
                )))),
                spawned: AtomicUsize::new(0),
            }
        }
        /// A spawner the test expects never to be called.
        fn unreachable() -> Self {
            Self {
                result: Mutex::new(None),
                spawned: AtomicUsize::new(0),
            }
        }
    }
    impl DaemonSpawner for ScriptedSpawner {
        type Proc = ScriptedProc;
        fn spawn(&self, _: &Path, _: Option<&Path>) -> std::io::Result<ScriptedProc> {
            self.spawned.fetch_add(1, Ordering::SeqCst);
            self.result
                .lock()
                .unwrap()
                .take()
                .expect("unexpected spawn")
        }
    }

    fn fail() -> Result<String, PingFailure> {
        Err(PingFailure("connection refused".to_string()))
    }
    fn cfg() -> EnsureConfig {
        let mut c = EnsureConfig::new("/bundle/datamancerd", "test-app");
        c.ready_timeout = Duration::from_millis(300);
        c
    }

    #[tokio::test]
    async fn already_running_daemon_is_used_without_spawning() {
        let ep = ScriptedEndpoint::new(vec![Ok("0.1.0".to_string())]);
        let sp = ScriptedSpawner::unreachable();
        let v = ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock"))
            .await
            .unwrap();
        assert_eq!(v, "0.1.0");
        assert_eq!(sp.spawned.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn spawns_then_waits_for_readiness() {
        let ep = ScriptedEndpoint::new(vec![fail(), fail(), Ok("0.1.0".to_string())]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: usize::MAX,
            exit: None,
        });
        let v = ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock"))
            .await
            .unwrap();
        assert_eq!(v, "0.1.0");
        assert_eq!(sp.spawned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn lost_spawn_race_still_succeeds_when_winner_answers() {
        // Our spawn exits immediately (single-instance lock held by the
        // winner), but a later ping answers: SUCCESS per the spec.
        let ep = ScriptedEndpoint::new(vec![fail(), fail(), Ok("0.1.0".to_string())]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: 0,
            exit: Some(ExitInfo {
                status: Some(1),
                stderr_tail: "already running".into(),
            }),
        });
        let v = ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock"))
            .await
            .unwrap();
        assert_eq!(v, "0.1.0");
    }

    #[tokio::test]
    async fn timeout_with_dead_child_diagnoses_daemon_exited() {
        let ep = ScriptedEndpoint::new(vec![fail()]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: 0,
            exit: Some(ExitInfo {
                status: Some(2),
                stderr_tail: "bad config".into(),
            }),
        });
        match ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::ReadyTimeout {
                diagnosis:
                    ReadyDiagnosis::DaemonExited {
                        status: Some(2),
                        stderr_tail,
                    },
                ..
            }) => assert_eq!(stderr_tail, "bad config"),
            other => panic!("expected DaemonExited diagnosis, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_with_live_child_diagnoses_unresponsive() {
        let ep = ScriptedEndpoint::new(vec![fail()]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: usize::MAX,
            exit: None,
        });
        match ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::ReadyTimeout {
                diagnosis: ReadyDiagnosis::Unresponsive,
                ..
            }) => {}
            other => panic!("expected Unresponsive diagnosis, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn spawn_io_failure_is_spawn_failed() {
        let ep = ScriptedEndpoint::new(vec![fail()]);
        let sp = ScriptedSpawner::fails();
        match ensure_daemon(&ep, &sp, &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::SpawnFailed { binary, .. }) => {
                assert_eq!(binary, Path::new("/bundle/datamancerd"));
            }
            other => panic!("expected SpawnFailed, got {other:?}"),
        }
    }

    #[test]
    fn version_compatibility_is_major_and_pre_1_minor() {
        assert!(version_compatible("0.1.0", "0.1.9"));
        assert!(!version_compatible("0.1.0", "0.2.0")); // pre-1.0: minor breaks
        assert!(version_compatible("1.2.0", "1.9.3")); // post-1.0: major only
        assert!(!version_compatible("1.0.0", "2.0.0"));
        assert!(!version_compatible("0.1.0", "garbage"));
    }
}

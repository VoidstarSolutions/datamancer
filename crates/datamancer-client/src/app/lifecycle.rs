//! Find-or-spawn-and-await-readiness, as a state machine over two seams so
//! the logic tests against fakes and a Windows port is additive: a
//! [`ControlEndpoint`] (UDS today, named pipe later) and a [`DaemonSpawner`]
//! (detached unix spawn today, `CreateProcess` later).

use std::path::{Path, PathBuf};
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
/// Surfaced to callers via `ReadyDiagnosis::Unresponsive::last_ping_failure`.
#[derive(Debug, Clone)]
pub(crate) struct PingFailure(pub String);

/// A `ping` handshake result: the daemon's version and (if reported) its
/// active credential backend. `credential_backend` is `Option` because an
/// older daemon's pong predates the field — the probe stays compatible with
/// it rather than treating the absence as a protocol error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DaemonHello {
    pub version: String,
    pub credential_backend: Option<String>,
}

/// One control-surface probe: ping the socket, return the daemon's hello.
pub(crate) trait ControlEndpoint {
    async fn ping(&self, socket: &Path, timeout: Duration) -> Result<DaemonHello, PingFailure>;
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

/// What a spawning config spawns: the binary and the log its stdio goes to.
/// `None` from [`spawn_plan`] means attach only.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SpawnPlan<'a> {
    pub binary: &'a Path,
    pub log_path: PathBuf,
}

/// Resolve what `cfg` would spawn. Attach-only (`daemon_binary: None`)
/// resolves nothing, so it never fails on a log path it would not use; a
/// spawning config with no explicit `log_path` needs `default_log` (the
/// platform default) and fails [`EnsureError::NoSocketPath`] without it.
pub(crate) fn spawn_plan(
    cfg: &EnsureConfig,
    default_log: impl FnOnce() -> Option<PathBuf>,
) -> Result<Option<SpawnPlan<'_>>, EnsureError> {
    let Some(binary) = cfg.daemon_binary.as_deref() else {
        return Ok(None);
    };
    let log_path = cfg
        .log_path
        .clone()
        .or_else(default_log)
        .ok_or(EnsureError::NoSocketPath)?;
    Ok(Some(SpawnPlan { binary, log_path }))
}

/// The spawn side of [`ensure_daemon`]: a spawner and the binary it runs,
/// together. `None` in its place means attach only.
pub(crate) struct Spawn<'a, S> {
    pub spawner: &'a S,
    pub binary: &'a Path,
}

/// Find a ready daemon on `socket` or, given a [`Spawn`], spawn one and await
/// readiness. Returns the daemon's hello (from `ping`). Without a `Spawn`
/// (attach only) a failed first probe is [`EnsureError::NoDaemon`].
///
/// A spawned process exiting is **not** failure while the deadline holds:
/// losing the single-instance race to another app's daemon that then answers
/// is success. The exit is only reported as the diagnosis if no daemon ever
/// answers.
pub(crate) async fn ensure_daemon<E: ControlEndpoint, S: DaemonSpawner>(
    endpoint: &E,
    spawn: Option<Spawn<'_, S>>,
    cfg: &EnsureConfig,
    socket: &Path,
) -> Result<DaemonHello, EnsureError> {
    let first_failure = match endpoint.ping(socket, PROBE_TIMEOUT).await {
        Ok(hello) => return Ok(hello),
        Err(failure) => failure,
    };
    let Some(Spawn { spawner, binary }) = spawn else {
        return Err(EnsureError::NoDaemon {
            socket: socket.to_path_buf(),
            last_ping_failure: Some(first_failure.0),
        });
    };
    let mut proc_ = spawner
        .spawn(binary, cfg.config_path.as_deref())
        .map_err(|source| EnsureError::SpawnFailed {
            binary: binary.to_path_buf(),
            source,
        })?;
    let deadline = Instant::now() + cfg.ready_timeout;
    let mut observed_exit: Option<ExitInfo> = None;
    let mut last_failure: Option<PingFailure>;
    loop {
        match endpoint.ping(socket, PROBE_TIMEOUT).await {
            Ok(hello) => return Ok(hello),
            Err(f) => last_failure = Some(f),
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
                None => ReadyDiagnosis::Unresponsive {
                    last_ping_failure: last_failure.take().map(|f| f.0),
                },
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
        script: Vec<Result<DaemonHello, PingFailure>>,
        calls: AtomicUsize,
    }
    impl ScriptedEndpoint {
        fn new(script: Vec<Result<DaemonHello, PingFailure>>) -> Self {
            Self {
                script,
                calls: AtomicUsize::new(0),
            }
        }
    }
    impl ControlEndpoint for ScriptedEndpoint {
        async fn ping(&self, _: &Path, _: Duration) -> Result<DaemonHello, PingFailure> {
            let i = self.calls.fetch_add(1, Ordering::SeqCst);
            self.script[i.min(self.script.len() - 1)].clone()
        }
    }

    /// Build a scripted `DaemonHello` with no reported credential backend
    /// (the common case in these tests, which don't exercise that field).
    fn hello(version: &str) -> DaemonHello {
        DaemonHello {
            version: version.to_string(),
            credential_backend: None,
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

    fn fail() -> Result<DaemonHello, PingFailure> {
        Err(PingFailure("connection refused".to_string()))
    }
    const BIN: &str = "/bundle/datamancerd";

    fn cfg() -> EnsureConfig {
        let mut c = EnsureConfig::new(BIN, "test-app");
        c.ready_timeout = Duration::from_millis(300);
        c
    }

    /// The spawn side `AppHandle::ensure` builds for [`cfg`].
    fn spawning(sp: &ScriptedSpawner) -> Spawn<'_, ScriptedSpawner> {
        Spawn {
            spawner: sp,
            binary: Path::new(BIN),
        }
    }

    #[tokio::test]
    async fn already_running_daemon_is_used_without_spawning() {
        let ep = ScriptedEndpoint::new(vec![Ok(hello("0.1.0"))]);
        let sp = ScriptedSpawner::unreachable();
        let v = ensure_daemon(&ep, Some(spawning(&sp)), &cfg(), Path::new("/tmp/x.sock"))
            .await
            .unwrap();
        assert_eq!(v.version, "0.1.0");
        assert_eq!(sp.spawned.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn spawns_then_waits_for_readiness() {
        let ep = ScriptedEndpoint::new(vec![fail(), fail(), Ok(hello("0.1.0"))]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: usize::MAX,
            exit: None,
        });
        let v = ensure_daemon(&ep, Some(spawning(&sp)), &cfg(), Path::new("/tmp/x.sock"))
            .await
            .unwrap();
        assert_eq!(v.version, "0.1.0");
        assert_eq!(sp.spawned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn lost_spawn_race_still_succeeds_when_winner_answers() {
        // Our spawn exits immediately (single-instance lock held by the
        // winner), but a later ping answers: SUCCESS per the spec.
        let ep = ScriptedEndpoint::new(vec![fail(), fail(), Ok(hello("0.1.0"))]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: 0,
            exit: Some(ExitInfo {
                status: Some(1),
                stderr_tail: "already running".into(),
            }),
        });
        let v = ensure_daemon(&ep, Some(spawning(&sp)), &cfg(), Path::new("/tmp/x.sock"))
            .await
            .unwrap();
        assert_eq!(v.version, "0.1.0");
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
        match ensure_daemon(&ep, Some(spawning(&sp)), &cfg(), Path::new("/tmp/x.sock")).await {
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
        match ensure_daemon(&ep, Some(spawning(&sp)), &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::ReadyTimeout {
                diagnosis: ReadyDiagnosis::Unresponsive { .. },
                ..
            }) => {}
            other => panic!("expected Unresponsive diagnosis, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unresponsive_diagnosis_carries_last_ping_failure() {
        // Endpoint never answers; spawned proc never exits → Unresponsive,
        // and the last probe's reason must surface.
        let ep = ScriptedEndpoint::new(vec![Err(PingFailure("connect refused (test)".into()))]);
        let sp = ScriptedSpawner::ok(ScriptedProc {
            alive_polls: usize::MAX,
            exit: None,
        });
        let err = ensure_daemon(&ep, Some(spawning(&sp)), &cfg(), Path::new("/tmp/x.sock"))
            .await
            .unwrap_err();
        // The rendered error message (the surface apps actually log) must
        // carry the reason too.
        assert!(
            err.to_string().contains("connect refused (test)"),
            "rendered error should carry the ping-failure reason: {err}"
        );
        match err {
            EnsureError::ReadyTimeout {
                diagnosis: ReadyDiagnosis::Unresponsive { last_ping_failure },
                ..
            } => assert_eq!(last_ping_failure.as_deref(), Some("connect refused (test)")),
            other => panic!("expected Unresponsive with reason, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn spawn_io_failure_is_spawn_failed() {
        let ep = ScriptedEndpoint::new(vec![fail()]);
        let sp = ScriptedSpawner::fails();
        match ensure_daemon(&ep, Some(spawning(&sp)), &cfg(), Path::new("/tmp/x.sock")).await {
            Err(EnsureError::SpawnFailed { binary, .. }) => {
                assert_eq!(binary, Path::new("/bundle/datamancerd"));
            }
            other => panic!("expected SpawnFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn attach_only_with_no_daemon_is_no_daemon_without_spawning() {
        let ep = ScriptedEndpoint::new(vec![Err(PingFailure("connect refused (test)".into()))]);
        let cfg = EnsureConfig::attach_only("test-app");
        // `AppHandle::ensure` passes no spawn side for this config.
        assert_eq!(spawn_plan(&cfg, || None).unwrap(), None);
        let err = ensure_daemon(
            &ep,
            None::<Spawn<'_, ScriptedSpawner>>,
            &cfg,
            Path::new("/tmp/x.sock"),
        )
        .await
        .unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("/tmp/x.sock") && rendered.contains("connect refused (test)"),
            "rendered error should name the socket and the reason: {rendered}"
        );
        match err {
            EnsureError::NoDaemon {
                socket,
                last_ping_failure,
            } => {
                assert_eq!(socket, Path::new("/tmp/x.sock"));
                assert_eq!(last_ping_failure.as_deref(), Some("connect refused (test)"));
            }
            other => panic!("expected NoDaemon, got {other:?}"),
        }
        assert_eq!(ep.calls.load(Ordering::SeqCst), 1, "one probe, no retry");
    }

    #[tokio::test]
    async fn attach_only_uses_a_running_daemon() {
        let ep = ScriptedEndpoint::new(vec![Ok(hello("0.1.0"))]);
        let cfg = EnsureConfig::attach_only("test-app");
        let v = ensure_daemon(
            &ep,
            None::<Spawn<'_, ScriptedSpawner>>,
            &cfg,
            Path::new("/tmp/x.sock"),
        )
        .await
        .unwrap();
        assert_eq!(v.version, "0.1.0");
    }

    #[test]
    fn spawn_plan_needs_a_log_path_only_when_spawning() {
        // Attach-only: no plan, and the default resolver is never consulted.
        let attach = EnsureConfig::attach_only("test-app");
        assert_eq!(
            spawn_plan(&attach, || panic!("must not resolve")).unwrap(),
            None
        );

        // Spawning, no explicit log, no platform default: NoSocketPath, as
        // before attach-only existed.
        assert!(matches!(
            spawn_plan(&cfg(), || None),
            Err(EnsureError::NoSocketPath)
        ));

        // Spawning with the platform default.
        assert_eq!(
            spawn_plan(&cfg(), || Some(PathBuf::from("/var/log/d.log"))).unwrap(),
            Some(SpawnPlan {
                binary: Path::new(BIN),
                log_path: PathBuf::from("/var/log/d.log"),
            })
        );

        // An explicit log path wins and the default is never consulted.
        let mut explicit = cfg();
        explicit.log_path = Some(PathBuf::from("/explicit.log"));
        assert_eq!(
            spawn_plan(&explicit, || panic!("must not resolve")).unwrap(),
            Some(SpawnPlan {
                binary: Path::new(BIN),
                log_path: PathBuf::from("/explicit.log"),
            })
        );
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

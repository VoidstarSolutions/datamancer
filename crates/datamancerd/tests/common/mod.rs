//! Test support shared by the live e2e suites that spawn the real
//! `datamancerd` binary: one guard that stops the daemon when a test ends.
//!
//! Not a test target itself (cargo only builds `tests/*.rs` as targets);
//! each suite pulls it in with `mod common;`.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
#[cfg(unix)]
use std::time::Duration;

/// The command that starts the daemon under test from `config_path`.
///
/// iceoryx2's own log (dead-node detection, cleanup, service open/create
/// retries) goes to the daemon's inherited stderr at `trace`, so a rejected
/// open is diagnosable from the `--nocapture` output (#96). Set here once so
/// every suite's daemon logs the same way.
///
/// The credential store is pinned to a `credentials.json` beside the config;
/// the `ALPACA_*` env fallback still applies.
pub fn daemon_command(config_path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_datamancerd"));
    command
        .arg("--config")
        .arg(config_path)
        .env("IOX2_LOG_LEVEL", "trace")
        .env(
            "DATAMANCER_CREDENTIALS_FILE",
            config_path.with_file_name("credentials.json"),
        );
    command
}

/// How long [`DaemonGuard`] waits for a graceful exit before it falls back to
/// a kill. The daemon's own drain is bounded by `shutdown_timeout_secs` (30 s
/// by default), but a test daemon with a handful of clients drains in well
/// under a second.
#[cfg(unix)]
pub const GRACEFUL_STOP: Duration = Duration::from_secs(15);

/// A spawned `datamancerd` that is stopped when the guard goes out of scope,
/// **including on panic**, which a bare stop at the end of a test body skips.
///
/// Unix: sends the `shutdown` control op over the control socket and waits up
/// to `GRACEFUL_STOP` for the process to exit, so the daemon's drain removes
/// its iceoryx2 services and node before the next test spawns a daemon. Falls
/// back to a kill, reported on stderr, if the op cannot be sent, is refused,
/// or the daemon does not exit in time; a drain that stops working then shows
/// up in the `--nocapture` log instead of silently leaving services behind
/// with a dead owner.
///
/// Windows: kill. The Windows daemon runs no iceoryx2 node, so there is
/// nothing for a drain to release between tests.
pub struct DaemonGuard {
    child: Child,
    control: PathBuf,
    /// Set once the child has been waited on (`kill_abruptly`, or a
    /// `try_wait` that saw it exit): `Drop` then has nothing to stop.
    reaped: bool,
}

impl DaemonGuard {
    /// Take ownership of a spawned daemon and the control endpoint it was
    /// configured with (a UDS path on Unix, a named pipe on Windows).
    pub fn new(child: Child, control: PathBuf) -> Self {
        Self {
            child,
            control,
            reaped: false,
        }
    }

    /// The daemon's control endpoint.
    #[allow(dead_code)] // not every suite reads it back
    pub fn control_endpoint(&self) -> &Path {
        &self.control
    }

    /// The daemon's process id, for a test that signals it directly.
    #[allow(dead_code)] // only the SIGTERM test signals the daemon itself
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// Non-blocking exit check, for a test that stops the daemon itself and
    /// asserts on how it exited. Once this reports an exit the guard treats
    /// the child as reaped and `Drop` does nothing further.
    #[allow(dead_code)] // only the SIGTERM test waits on the daemon itself
    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let status = self.child.try_wait()?;
        if status.is_some() {
            self.reaped = true;
        }
        Ok(status)
    }

    /// Simulate a crash: kill the daemon outright and reap it, so it never
    /// runs its drain and its iceoryx2 node and per-client services stay
    /// registered with a dead owner. `Drop` then has nothing left to stop.
    #[allow(dead_code)] // only the crash-restart test simulates a crash
    pub fn kill_abruptly(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.reaped = true;
    }
}

impl Drop for DaemonGuard {
    // Nothing here may panic: a panic in `Drop` while a failing test unwinds
    // aborts the process.
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        #[cfg(unix)]
        let stopped = request_shutdown(&self.control) && wait_for_exit(&mut self.child);
        #[cfg(not(unix))]
        let stopped = false;
        if !stopped {
            #[cfg(unix)]
            eprintln!(
                "e2e: graceful shutdown failed or timed out after {GRACEFUL_STOP:?}; \
                 killing the daemon (its iceoryx2 services are left behind)"
            );
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

/// Send `Request::Shutdown` as one control frame and read the reply if one
/// arrives. Returns `false` if the daemon could not be reached or refused the
/// op. A missing reply counts as sent: the daemon replies before it drains,
/// best-effort. Blocking I/O is fine here: `Drop` is synchronous.
#[cfg(unix)]
fn request_shutdown(socket: &Path) -> bool {
    use std::io::{BufRead as _, Write as _};

    use datamancer_client::protocol::uds::{Reply, Request};

    let Ok(mut stream) = std::os::unix::net::UnixStream::connect(socket) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
    let Ok(mut frame) = serde_json::to_vec(&Request::Shutdown) else {
        return false;
    };
    frame.push(b'\n');
    if stream.write_all(&frame).is_err() {
        return false;
    }
    let mut line = String::new();
    let _ = std::io::BufReader::new(stream).read_line(&mut line);
    match serde_json::from_str::<Reply>(&line) {
        Ok(reply) => reply.ok,
        Err(_) => true,
    }
}

/// Poll `child` until it exits or [`GRACEFUL_STOP`] passes. Returns whether it
/// exited.
#[cfg(unix)]
fn wait_for_exit(child: &mut Child) -> bool {
    let deadline = std::time::Instant::now() + GRACEFUL_STOP;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            _ => return false,
        }
    }
}

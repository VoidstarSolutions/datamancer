//! Test support shared by the live e2e suites that spawn the real
//! `datamancerd` binary: the commands that start it, a guard that stops it
//! when a test ends, and, on Unix, one spawn helper and a control connection.
//!
//! Not a test target itself (cargo only builds `tests/*.rs` as targets);
//! each suite pulls it in with `mod common;`.

#[cfg(unix)]
use std::io::{BufRead as _, BufReader, Write as _};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
#[cfg(unix)]
use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(unix)]
use std::time::{Duration, Instant};

/// The command that starts the daemon under test from `config_path`.
///
/// iceoryx2's own log (dead-node detection, cleanup, service open/create
/// retries) goes to the daemon's inherited stderr at `trace`, so a rejected
/// open is diagnosable from the `--nocapture` output (#96). Set here once so
/// every suite's daemon logs the same way.
pub fn daemon_command(config_path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_datamancerd"));
    command
        .arg("--config")
        .arg(config_path)
        .env("IOX2_LOG_LEVEL", "trace");
    command
}

/// The `ALPACA_*` variables the daemon reads as a credential fallback.
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub const ALPACA_ENV: [&str; 4] = [
    "ALPACA_PAPER_API_KEY_ID",
    "ALPACA_PAPER_API_SECRET_KEY",
    "ALPACA_LIVE_API_KEY_ID",
    "ALPACA_LIVE_API_SECRET_KEY",
];

/// [`daemon_command`] for a daemon that must touch nothing outside its test:
/// its credential store is `credentials.json` beside `config_path`, and no
/// `ALPACA_*` variable reaches it.
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub fn hermetic_command(config_path: &Path) -> Command {
    let mut command = daemon_command(config_path);
    let store = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("credentials.json");
    command.env("DATAMANCER_CREDENTIALS_FILE", store);
    for name in ALPACA_ENV {
        command.env_remove(name);
    }
    command
}

/// Makes every daemon's `service_prefix` unique (`<pid>-<n>`), so a reply's
/// advertised service name identifies the daemon that sent it.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
static PREFIX_SEQ: AtomicU32 = AtomicU32::new(0);

/// A `[server].service_prefix` no other daemon of this test process uses.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub fn unique_prefix() -> String {
    format!(
        "datamancerd-e2e-{}-{}",
        std::process::id(),
        PREFIX_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Serializes a suite's daemons: `datamancerd` takes a **global**
/// single-instance lock (`<data dir>/datamancerd.lock`, one per user per
/// host), so two daemons can never be up at once, and under cargo's default
/// parallelism every test but the lock winner would fail with "daemon socket
/// never appeared". The lock is not scoped to a config file, so an isolated
/// data dir per test is not an option; the tests take turns instead.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
static DAEMON_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A daemon from [`spawn_daemon`]: the [`DaemonGuard`] plus the suite lock
/// and the prefix the daemon was configured with. A leaked daemon keeps
/// holding the single-instance lock and makes every later spawn fail, which
/// is why the stop lives in the guard's `Drop`.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub struct Daemon {
    // Field order is drop order: stop the child before releasing the lock, or
    // the next test races the dying daemon for the single-instance lock.
    guard: DaemonGuard,
    _lock: std::sync::MutexGuard<'static, ()>,
    /// The `[server].service_prefix` this daemon was started with.
    pub service_prefix: String,
}

#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
impl Daemon {
    /// The UDS control socket.
    pub fn socket(&self) -> &Path {
        self.guard.control_endpoint()
    }

    /// Simulate a crash; see [`DaemonGuard::kill_abruptly`]. Dropping the
    /// daemon afterwards releases the suite lock.
    pub fn kill_abruptly(&mut self) {
        self.guard.kill_abruptly();
    }

    /// Send `shutdown` and wait up to `timeout` for the daemon to exit.
    /// Returns whether it exited.
    pub fn shutdown_within(&mut self, timeout: Duration) -> bool {
        let exited = request_shutdown(self.guard.control_endpoint())
            && wait_for_exit_within(&mut self.guard.child, timeout);
        self.guard.reaped |= exited;
        exited
    }
}

/// Write a config with the `[provider.*]` sections in `providers`, this
/// daemon's `[server]` and `[diagnostics]`, and any further `sections`, start
/// the daemon from it with `command` ([`daemon_command`] or
/// [`hermetic_command`]), and wait for its control socket to appear.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub fn spawn_daemon(
    dir: &Path,
    service_prefix: String,
    providers: &str,
    sections: &str,
    command: fn(&Path) -> Command,
) -> Daemon {
    spawn_daemon_with_server(dir, service_prefix, providers, "", sections, command)
}

/// [`spawn_daemon`] with further `[server]` keys in `server`.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub fn spawn_daemon_with_server(
    dir: &Path,
    service_prefix: String,
    providers: &str,
    server: &str,
    sections: &str,
    command: fn(&Path) -> Command,
) -> Daemon {
    // Poisoning just means an earlier test panicked; its `Daemon` has already
    // stopped that daemon, so the lock is still meaningful.
    let lock = DAEMON_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let socket = dir.join("admin.sock");
    let config_path = dir.join("datamancerd.toml");
    let config = format!(
        r#"
{providers}

[server]
admin_socket = "{}"
service_prefix = "{service_prefix}"
{server}

[diagnostics]
publish_interval_ms = 200

{sections}
"#,
        socket.display()
    );
    std::fs::write(&config_path, config).expect("write config");

    let child = command(&config_path).spawn().expect("spawn datamancerd");
    let daemon = Daemon {
        guard: DaemonGuard::new(child, socket),
        _lock: lock,
        service_prefix,
    };

    let deadline = Instant::now() + Duration::from_secs(10);
    while !daemon.socket().exists() {
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        std::thread::sleep(Duration::from_millis(50));
    }
    daemon
}

/// A held control connection: any number of requests on the same
/// line-oriented socket.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub struct Conn {
    writer: UnixStream,
    reader: BufReader<UnixStream>,
}

#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
impl Conn {
    /// Connect to the control socket at `socket`. A read that sees no reply
    /// within 30 s fails, so a wedged daemon fails its test.
    pub fn connect(socket: &Path) -> Self {
        let stream = UnixStream::connect(socket).expect("connect socket");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .expect("set read timeout");
        let writer = stream.try_clone().expect("clone");
        let reader = BufReader::new(stream);
        Self { writer, reader }
    }

    /// Send one JSON line and read one JSON reply line.
    pub fn request(&mut self, line: &str) -> serde_json::Value {
        self.writer.write_all(line.as_bytes()).expect("write");
        self.writer.write_all(b"\n").expect("write nl");
        self.writer.flush().expect("flush");
        let mut reply = String::new();
        self.reader.read_line(&mut reply).expect("read reply");
        serde_json::from_str(&reply).expect("parse reply")
    }

    /// `open-client` and attach a data-plane subscriber to the advertised
    /// service.
    pub fn open_client(
        &mut self,
        name: &str,
        node: &iceoryx2::node::Node<iceoryx2::prelude::ipc_threadsafe::Service>,
    ) -> datamancer::transport::DataSubscriber {
        let reply = self.request(&format!(r#"{{"op":"open-client","client":"{name}"}}"#));
        assert_eq!(reply["ok"], true, "open-client rejected: {reply}");
        let service = reply["service"].as_str().expect("service name");
        let service_name: datamancer::transport::DataServiceName = service
            .parse()
            .unwrap_or_else(|e| panic!("not a data-service name: {service}: {e}"));
        datamancer::transport::DataSubscriber::open(node, &service_name).expect("subscriber")
    }
}

/// One request on a connection of its own, which closes once the reply is
/// read.
#[cfg(unix)]
#[allow(
    dead_code,
    reason = "not every suite that includes this module uses it"
)]
pub fn round_trip(socket: &Path, request: &str) -> serde_json::Value {
    Conn::connect(socket).request(request)
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
    /// `shutdown_within` that saw it exit): `Drop` then has nothing to stop.
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
    pub fn control_endpoint(&self) -> &Path {
        &self.control
    }

    /// Simulate a crash: kill the daemon outright and reap it, so it never
    /// runs its drain and its iceoryx2 node and per-client services stay
    /// registered with a dead owner. `Drop` then has nothing left to stop.
    #[allow(
        dead_code,
        reason = "not every suite that includes this module uses it"
    )]
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
    wait_for_exit_within(child, GRACEFUL_STOP)
}

/// Poll `child` until it exits or `timeout` passes. Returns whether it
/// exited.
#[cfg(unix)]
fn wait_for_exit_within(child: &mut Child, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
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

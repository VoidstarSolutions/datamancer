//! End-to-end daemon tests.
//!
//! These spawn the real `datamancerd` binary and talk to it over its
//! Unix-domain control socket. They need a live **iceoryx2 runtime** (the
//! daemon creates one node per process at startup) and so are `#[ignore]`d in
//! normal CI — run them explicitly:
//!
//! ```text
//! cargo test -p datamancerd --test daemon_e2e -- --ignored
//! ```
//!
//! The headline per-symbol-agreement and live-flow tests additionally need
//! Alpaca credentials in the environment; they live here as `#[ignore]`d
//! placeholders to be filled in once a hermetic replay provider is wired
//! (roadmap RE-PLAN: `[provider.replay]`).

// This suite talks to the daemon over a Unix-domain control socket and manages
// the process POSIX-style; the Windows named-pipe harness port is Phase 5 (#29).
// Compile on Unix only until then.
#![cfg(unix)]
#![forbid(unsafe_code)]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use common::DaemonGuard;

/// Makes every daemon's `service_prefix` unique (`<pid>-<n>`), so a reply's
/// advertised service name identifies the daemon that sent it.
static PREFIX_SEQ: AtomicU32 = AtomicU32::new(0);

/// Serializes the whole suite: `datamancerd` takes a **global** single-instance
/// lock (`<data dir>/datamancerd.lock`, one per user per host), so two daemons
/// can never be up at once — under cargo's default parallelism every test but
/// the lock winner would fail with "daemon socket never appeared". The lock is
/// deliberately not scoped to a config file, so an isolated data dir per test
/// is not an option; the tests take turns instead.
static DAEMON_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// This suite's daemon: the shared [`DaemonGuard`] plus the suite lock and
/// the prefix the daemon was configured with. A leaked daemon keeps holding
/// the single-instance lock and makes every later run of this suite fail at
/// `spawn_daemon`, which is why the stop lives in the guard's `Drop`.
struct Daemon {
    // Field order is drop order: stop the child before releasing the lock, or
    // the next test races the dying daemon for the single-instance lock.
    guard: DaemonGuard,
    _lock: std::sync::MutexGuard<'static, ()>,
    /// The `[server].service_prefix` this daemon was started with, for tests
    /// that assert on a reply's full service name.
    service_prefix: String,
}

impl Daemon {
    /// The UDS control socket.
    fn socket(&self) -> &Path {
        self.guard.control_endpoint()
    }
}

impl Daemon {
    /// Simulate a crash: SIGKILL the daemon and reap it, so it never runs its
    /// drain and its iceoryx2 node and per-client services stay registered
    /// with a dead owner. Dropping the guard afterwards is a no-op on the
    /// reaped child and releases the suite lock.
    fn kill_abruptly(&mut self) {
        self.guard.kill_abruptly();
    }
}

/// Spawn the daemon with a memory-backed config and wait for its control
/// socket to appear.
fn spawn_daemon(dir: &Path) -> Daemon {
    let service_prefix = format!(
        "datamancerd-e2e-{}-{}",
        std::process::id(),
        PREFIX_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    spawn_daemon_with_prefix(dir, service_prefix)
}

/// [`spawn_daemon`] with an explicit `[server].service_prefix`, for a test
/// that needs two daemons configured identically (the crash-restart case).
fn spawn_daemon_with_prefix(dir: &Path, service_prefix: String) -> Daemon {
    // Poisoning just means an earlier test panicked; its `Daemon` guard has
    // already killed that daemon, so the lock is still meaningful.
    let lock = DAEMON_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let socket = dir.join("admin.sock");
    let config_path = dir.join("datamancerd.toml");
    let config = format!(
        r#"
[provider.alpaca]
account_type = "paper"

[provider.alpaca_crypto]
account_type = "paper"
venue = "us"

[server]
admin_socket = "{}"
service_prefix = "{service_prefix}"

[diagnostics]
publish_interval_ms = 200
"#,
        socket.display()
    );
    std::fs::write(&config_path, config).expect("write config");

    let child = common::daemon_command(&config_path)
        .spawn()
        .expect("spawn datamancerd");

    let daemon = Daemon {
        guard: DaemonGuard::new(child, socket),
        _lock: lock,
        service_prefix,
    };

    // Wait for the socket to appear (daemon bound).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !daemon.socket().exists() {
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        std::thread::sleep(Duration::from_millis(50));
    }
    daemon
}

/// Send one JSON line and read one JSON reply line.
fn round_trip(socket: &Path, request: &str) -> serde_json::Value {
    let stream = UnixStream::connect(socket).expect("connect socket");
    let mut writer = stream.try_clone().expect("clone");
    writer.write_all(request.as_bytes()).expect("write");
    writer.write_all(b"\n").expect("write nl");
    writer.flush().expect("flush");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read reply");
    serde_json::from_str(&line).expect("parse reply")
}

#[test]
#[ignore = "needs a live iceoryx2 runtime"]
fn control_round_trip_list_and_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    let reply = round_trip(daemon.socket(), r#"{"op":"list-clients"}"#);
    assert_eq!(reply["ok"], serde_json::Value::Bool(true));
    assert!(reply["clients"].is_array());

    let snap = round_trip(daemon.socket(), r#"{"op":"snapshot"}"#);
    assert_eq!(snap["ok"], serde_json::Value::Bool(true));
    assert!(snap["snapshot"].is_object());

    // Unknown op -> structured error reply with a stable code.
    let err = round_trip(daemon.socket(), r#"{"op":"frobnicate"}"#);
    assert_eq!(err["ok"], serde_json::Value::Bool(false));
    assert_eq!(err["code"], "bad_request");
}

#[test]
#[ignore = "needs a live iceoryx2 runtime"]
fn open_client_creates_a_service_then_closes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    // open-client over its own long-lived connection.
    let stream = UnixStream::connect(daemon.socket()).expect("connect");
    let mut writer = stream.try_clone().expect("clone");
    let mut reader = BufReader::new(stream);

    writer
        .write_all(br#"{"op":"open-client","client":"exec-1"}"#)
        .expect("write");
    writer.write_all(b"\n").expect("nl");
    writer.flush().expect("flush");
    let mut line = String::new();
    reader.read_line(&mut line).expect("read");
    let reply: serde_json::Value = serde_json::from_str(&line).expect("parse");
    assert_eq!(reply["ok"], serde_json::Value::Bool(true));
    // The advertised name is the real service name: `spawn_daemon`'s
    // per-daemon `service_prefix`, this daemon process's boot id, the wire
    // version, then the client id.
    let service = reply["service"].as_str().expect("service name");
    let name: datamancer::transport::DataServiceName = service
        .parse()
        .unwrap_or_else(|e| panic!("not a data-service name: {service}: {e}"));
    let prefix = name.prefix().as_str();
    assert!(
        prefix.starts_with(&format!("{}/", daemon.service_prefix)),
        "advertised prefix {prefix:?} is not boot-scoped under {:?}",
        daemon.service_prefix
    );

    // The client appears in list-clients (separate connection).
    let listing = round_trip(daemon.socket(), r#"{"op":"list-clients"}"#);
    let clients = listing["clients"].as_array().expect("array");
    assert!(clients.iter().any(|c| c == "exec-1"));

    // Dropping the connection (EOF) tears the client down.
    drop(writer);
    drop(reader);
    std::thread::sleep(Duration::from_millis(300));
    let after = round_trip(daemon.socket(), r#"{"op":"list-clients"}"#);
    let clients = after["clients"].as_array().expect("array");
    assert!(
        !clients.iter().any(|c| c == "exec-1"),
        "client not torn down"
    );
}

/// A daemon that starts after its predecessor crashed must serve its first
/// client (#96). The predecessor owns a live data service when it is killed
/// with SIGKILL; if the kill lands while it is removing a service, that
/// service stays marked for destruction with no owner and iceoryx2 0.10's
/// dead-node cleanup never visits it, so any later open of the same name
/// fails `IsMarkedForDestruction` (the nightly signature before #92). Both
/// daemons here share one configured `service_prefix` and number their first
/// client 0; the fix is the per-boot segment in the advertised name, so this
/// test pins two things that hold whatever the kill timing: the successor's
/// first `open-client` succeeds, and its service name differs from the
/// victim's.
#[test]
#[ignore = "spawns the daemon twice, needs a live iceoryx2 runtime"]
fn a_restarted_daemon_serves_its_first_client_after_a_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut victim = spawn_daemon(dir.path());
    // The successor is configured identically: same `service_prefix`, so the
    // only thing keeping its names apart from the victim's is the boot id.
    let configured_prefix = victim.service_prefix.clone();

    // Hold the client's connection open so its data service exists at the
    // moment the daemon dies.
    let stream = UnixStream::connect(victim.socket()).expect("connect");
    let mut writer = stream.try_clone().expect("clone");
    let mut reader = BufReader::new(stream);
    writer
        .write_all(br#"{"op":"open-client","client":"crash-victim"}"#)
        .expect("write");
    writer.write_all(b"\n").expect("nl");
    writer.flush().expect("flush");
    let mut line = String::new();
    reader.read_line(&mut line).expect("read");
    let reply: serde_json::Value = serde_json::from_str(&line).expect("parse");
    if reply["ok"] != serde_json::Value::Bool(true) {
        dump_iceoryx2_state("victim open-client rejected");
    }
    assert_eq!(
        reply["ok"],
        serde_json::Value::Bool(true),
        "victim open-client: {reply}"
    );
    let victim_service = reply["service"]
        .as_str()
        .expect("victim service name")
        .to_string();

    victim.kill_abruptly();
    // Release the suite lock before the successor spawns; the connection is
    // dead with the daemon.
    drop(victim);
    drop(writer);
    drop(reader);

    let dir = tempfile::tempdir().expect("tempdir");
    let successor = spawn_daemon_with_prefix(dir.path(), configured_prefix.clone());
    let reply = round_trip(
        successor.socket(),
        r#"{"op":"open-client","client":"crash-successor"}"#,
    );
    if reply["ok"] != serde_json::Value::Bool(true) {
        dump_iceoryx2_state("successor open-client rejected");
    }
    assert_eq!(
        reply["ok"],
        serde_json::Value::Bool(true),
        "first open after a crashed predecessor was rejected: {reply}"
    );

    // Both daemons numbered their first client 0 under the same configured
    // prefix; the boot id is what keeps the successor off the dead daemon's
    // (possibly zombie) service name. This holds whatever the kill timing.
    let successor_service = reply["service"].as_str().expect("successor service name");
    assert_ne!(
        successor_service, victim_service,
        "successor re-created its crashed predecessor's service name"
    );
    for service in [victim_service.as_str(), successor_service] {
        let name: datamancer::transport::DataServiceName = service
            .parse()
            .unwrap_or_else(|e| panic!("not a data-service name: {service}: {e}"));
        let prefix = name.prefix().as_str();
        assert!(
            prefix.starts_with(&format!("{configured_prefix}/")),
            "{service}: prefix {prefix:?} is not boot-scoped under {configured_prefix:?}"
        );
        assert_eq!(name.client_id(), 0, "{service}: first client of each boot is id 0");
    }
}

/// Print what iceoryx2 left under its root (`/tmp/iceoryx2`, the 0.10 Linux
/// default) and in shared memory, for a failure only a leftover service or
/// node can explain. Best-effort: a missing directory prints nothing.
fn dump_iceoryx2_state(why: &str) {
    fn walk(dir: &std::path::Path, depth: usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            eprintln!("{:indent$}{}", "", path.display(), indent = depth * 2);
            if path.is_dir() {
                walk(&path, depth + 1);
            }
        }
    }
    eprintln!("daemon_e2e: {why}; iceoryx2 state:");
    walk(std::path::Path::new("/tmp/iceoryx2"), 1);
    if let Ok(entries) = std::fs::read_dir("/dev/shm") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.contains("iox2") {
                eprintln!("  /dev/shm/{name}");
            }
        }
    }
}

/// A bounded historical bar query over a month-long, long-closed range: real
/// bars, then `SessionClosing`, then the service disappears on its own.
///
/// The opening connection is held open for the query's whole lifetime rather
/// than using `round_trip` to send `open-query` itself: a dropped connection
/// aborts the queries it opened (see
/// `dropping_the_control_connection_aborts_its_queries` below), and
/// `round_trip` drops its connection the moment it has the reply — a real run
/// against Alpaca confirmed the daemon reaping the query (via that
/// connection's EOF, logged as `query cancelled`) within milliseconds of the
/// `open-query` reply, well before the very next `list-queries` round trip
/// could ever observe it in flight. Holding the connection open here proves
/// "still in flight" and "self-reaped without cancel-query" for real, instead
/// of merely proving EOF-teardown a second time.
///
/// Uses `bar1m` over `2024-01-01..2024-02-01` rather than the daily-bar,
/// single-week range this docstring's title once suggested: in two
/// independent real runs against Alpaca paper credentials, an equity
/// `bar1d` request over `2024-01-01..2024-01-08` connected successfully
/// (TCP+TLS to `data.alpaca.markets` completed) and then never received a
/// response — 30+ seconds with zero further activity, confirmed by
/// `RUST_LOG=debug` request tracing. The `bar1m`/month-long request below,
/// by contrast, paginates and completes in ~1-2 seconds every time it was
/// run. The cause is **unconfirmed**: this branch's own reading of the fetch
/// path found it identical for both intervals (only the `TimeFrame` differs),
/// so the two candidate explanations — an Alpaca-side quirk for that exact
/// short/holiday-adjacent daily range, or simple rate limiting from repeated
/// development runs — are both at least as plausible as a defect in our
/// daily-bar path. Recorded as an observation to reproduce deliberately, not
/// as a known bug.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime and Alpaca credentials"]
fn open_query_streams_bars_then_reaps_its_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    let stream = UnixStream::connect(daemon.socket()).expect("connect");
    let mut writer = stream.try_clone().expect("clone");
    let mut reader = BufReader::new(stream);

    writer
        .write_all(
            br#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1m","from":1704067200000000000,"to":1706745600000000000}"#,
        )
        .expect("write");
    writer.write_all(b"\n").expect("nl");
    writer.flush().expect("flush");
    let mut line = String::new();
    reader.read_line(&mut line).expect("read");
    let reply: serde_json::Value = serde_json::from_str(&line).expect("parse");
    assert_eq!(reply["ok"], true, "open-query rejected: {reply}");
    let id = reply["query"].as_u64().expect("query id");
    // The advertised name is the real service name under `spawn_daemon`'s
    // per-daemon `service_prefix` and this process's boot id, not the
    // daemon's out-of-the-box default ("datamancerd"), carrying the query id.
    let service = reply["service"].as_str().expect("service name");
    let name: datamancer::transport::DataServiceName = service
        .parse()
        .unwrap_or_else(|e| panic!("not a data-service name: {service}: {e}"));
    let prefix = name.prefix().as_str();
    assert!(
        prefix.starts_with(&format!("{}/", daemon.service_prefix)),
        "advertised prefix {prefix:?} is not boot-scoped under {:?}",
        daemon.service_prefix
    );
    assert_eq!(name.client_id(), id);

    // The query is in flight immediately after the reply. `list-queries` owns
    // nothing, so checking it over a fresh `round_trip` connection cannot
    // itself cancel anything.
    let listed = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    assert!(
        listed["queries"]
            .as_array()
            .expect("queries array")
            .contains(&serde_json::json!(id)),
        "expected {id} in {listed}"
    );

    // Let the bounded fetch finish — the opening connection (`writer`/
    // `reader`) is still alive here — then confirm the daemon reaped the
    // query on its own: nobody called cancel-query, and the connection that
    // opened it never dropped, so this can only be the query's own
    // completion, not EOF teardown. Poll rather than a single fixed sleep:
    // real Alpaca REST latency (and any rate-limit backoff) varies.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut after = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    while after["queries"]
        .as_array()
        .expect("queries array")
        .contains(&serde_json::json!(id))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "query {id} never self-reaped: {after}"
        );
        std::thread::sleep(std::time::Duration::from_millis(500));
        after = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    }

    drop((writer, reader));
}

#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn open_query_rejects_an_inverted_range_and_an_unserved_pair() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    let inverted = round_trip(
        daemon.socket(),
        r#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1d","from":200,"to":100}"#,
    );
    assert_eq!(inverted["ok"], false);
    assert_eq!(inverted["code"], "bad_request");

    // Alpaca's crypto provider reports `Surface::History => false`, so routing
    // rejects the pair before any session opens.
    let crypto = round_trip(
        daemon.socket(),
        r#"{"op":"open-query","provider":"alpaca","asset_class":"crypto","symbol":"BTC/USD","kind":"bar1d","from":100,"to":200}"#,
    );
    assert_eq!(crypto["ok"], false);
    assert_eq!(crypto["code"], "unsupported_event_kind");
}

#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime and Alpaca credentials"]
fn cancel_query_reports_unknown_for_a_stale_id_and_caps_concurrency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    let stale = round_trip(daemon.socket(), r#"{"op":"cancel-query","query":9999}"#);
    assert_eq!(stale["ok"], false);
    assert_eq!(stale["code"], "unknown_query");

    // max_queries defaults to 2. Each open-query must stay in flight for the
    // cap to mean anything, so this holds three control connections open
    // concurrently rather than reusing `round_trip` per open: `round_trip`
    // closes its connection right after reading the reply, and a closed
    // connection's queries are reaped by the same EOF teardown that
    // `dropping_the_control_connection_aborts_its_queries` exercises below —
    // which would silently free the cap slot before the next open landed and
    // let all three succeed.
    let open = r#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1m","from":1704067200000000000,"to":1706745600000000000}"#;
    let mut conns = Vec::new();
    let mut replies = Vec::new();
    for _ in 0..3 {
        let stream = UnixStream::connect(daemon.socket()).expect("connect");
        let mut writer = stream.try_clone().expect("clone");
        let mut reader = BufReader::new(stream);
        writer.write_all(open.as_bytes()).expect("write");
        writer.write_all(b"\n").expect("nl");
        writer.flush().expect("flush");
        let mut line = String::new();
        reader.read_line(&mut line).expect("read");
        replies.push(serde_json::from_str::<serde_json::Value>(&line).expect("parse"));
        conns.push((writer, reader)); // keep each connection open past this loop
    }
    assert_eq!(replies[0]["ok"], true, "{:?}", replies[0]);
    assert_eq!(replies[1]["ok"], true, "{:?}", replies[1]);
    assert_eq!(replies[2]["ok"], false, "{:?}", replies[2]);
    assert_eq!(replies[2]["code"], "service_cap_exceeded");

    // Confirm the daemon really does see two concurrent queries, not just two
    // successful replies.
    let listed = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    let ids = listed["queries"].as_array().expect("queries array");
    assert_eq!(
        ids.len(),
        2,
        "expected exactly 2 in-flight queries: {listed}"
    );

    let first_id = replies[0]["query"].as_u64().expect("id");
    let cancelled = round_trip(
        daemon.socket(),
        &format!(r#"{{"op":"cancel-query","query":{first_id}}}"#),
    );
    assert_eq!(cancelled["ok"], true, "{cancelled}");

    drop(conns); // release the still-open connections before the daemon guard runs
}

/// A dropped control connection must abort the queries it opened — otherwise a
/// crashed backtest keeps burning provider rate limit.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime and Alpaca credentials"]
fn dropping_the_control_connection_aborts_its_queries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    // `round_trip` opens and drops a connection per call, so the query it
    // opens is torn down when it returns.
    let opened = round_trip(
        daemon.socket(),
        r#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1m","from":1704067200000000000,"to":1706745600000000000}"#,
    );
    assert_eq!(opened["ok"], true, "{opened}");

    std::thread::sleep(std::time::Duration::from_millis(500));
    let listed = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    assert_eq!(
        listed["queries"],
        serde_json::json!([]),
        "EOF must reap the connection's queries: {listed}"
    );
}

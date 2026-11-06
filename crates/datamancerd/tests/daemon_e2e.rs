//! End-to-end daemon tests.
//!
//! These spawn the real `datamancerd` binary and talk to it over its
//! Unix-domain control socket. They need a live **iceoryx2 runtime** (the
//! daemon creates one node per process at startup) and so are `#[ignore]`d in
//! normal CI — run them explicitly:
//!
//! ```text
//! cargo test -p datamancerd --all-features --test daemon_e2e -- --ignored
//! ```
//!
//! The tests that hit real Alpaca additionally need paper credentials in the
//! environment; the hermetic ones at the bottom of this file run against the
//! scripted provider (feature `scripted-provider`) and need nothing.

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
#[cfg(feature = "scripted-provider")]
use datamancer::transport::{DataServiceName, DataSubscriber, TransportError};
use datamancer::{ControlKind, MarketEvent};
use iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};

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

/// `spawn_daemon` configures neither `[cache]` nor `[tap_log]`.
#[test]
#[ignore = "needs a live iceoryx2 runtime"]
fn subscribe_refuses_persistence_the_daemon_has_no_backend_for() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    let stream = UnixStream::connect(daemon.socket()).expect("connect");
    let mut writer = stream.try_clone().expect("clone");
    let mut reader = BufReader::new(stream);
    writer
        .write_all(b"{\"op\":\"open-client\",\"client\":\"exec-1\"}\n")
        .expect("write");
    let mut line = String::new();
    reader.read_line(&mut line).expect("read");
    let reply: serde_json::Value = serde_json::from_str(&line).expect("parse");
    assert_eq!(reply["ok"], serde_json::Value::Bool(true), "{reply}");

    for persistence in ["cached", "tap_only", "cached_with_tap"] {
        let reply = round_trip(
            daemon.socket(),
            &format!(
                r#"{{"op":"subscribe","client":"exec-1","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade","scope":"live","persistence":"{persistence}"}}"#
            ),
        );
        assert_eq!(reply["ok"], serde_json::Value::Bool(false), "{reply}");
        assert_eq!(reply["code"], "persistence_required", "{reply}");
    }
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
        assert_eq!(
            name.client_id(),
            0,
            "{service}: first client of each boot is id 0"
        );
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

/// A bounded historical bar query nobody attaches to: in flight after the
/// reply, then gone once the daemon's attach grace passes, without
/// `cancel-query` and with the opening connection still up. No bar is
/// published.
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
/// Uses `bar1m` over `2024-01-01..2024-02-01` rather than a daily-bar,
/// single-week range: in two
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
fn open_query_nobody_attaches_to_is_reaped_after_the_grace() {
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

    // Nobody attaches, so the daemon gives the query up once its attach grace
    // passes. The opening connection (`writer`/`reader`) is still alive here
    // and nobody called cancel-query, so this can only be the daemon's own
    // reap, not EOF teardown.
    wait_until_reaped(&daemon, id);

    drop((writer, reader));
}

/// Poll `list-queries` until query `id` is gone, failing after 30 s. Poll
/// rather than sleep once: real Alpaca REST latency (and any rate-limit
/// backoff) varies. `list-queries` owns nothing, so a fresh `round_trip`
/// connection cannot itself cancel anything.
fn wait_until_reaped(daemon: &Daemon, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut after = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    while after["queries"]
        .as_array()
        .expect("queries array")
        .contains(&serde_json::json!(id))
    {
        assert!(
            Instant::now() < deadline,
            "query {id} never self-reaped: {after}"
        );
        std::thread::sleep(Duration::from_millis(500));
        after = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    }
}

/// `open-query` streams bars: a subscriber on the advertised data service
/// receives the queried instrument's bars and then the terminal
/// `SessionClosing`, after which the daemon reaps the query on its own
/// (#108). Until now only the reap was asserted; a query whose events were
/// dropped before the sink would have passed. Same request as
/// [`open_query_nobody_attaches_to_is_reaped_after_the_grace`].
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime and Alpaca credentials"]
fn open_query_delivers_bars_to_a_subscriber_then_closes() {
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
    let service = reply["service"].as_str().expect("service name");
    let name: datamancer::transport::DataServiceName = service
        .parse()
        .unwrap_or_else(|e| panic!("not a data-service name: {service}: {e}"));

    // Attach as a consumer would: a node of our own, opening the advertised
    // name verbatim.
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut subscriber =
        datamancer::transport::DataSubscriber::open(&node, &name).expect("subscriber");

    // Drain to the end of the stream, which the reap closes.
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut bars = 0u64;
    let mut closed = false;
    loop {
        assert!(
            Instant::now() < deadline,
            "the stream did not end within 60 s ({bars} bars so far)"
        );
        let events = match subscriber.poll() {
            Ok(events) => events,
            Err(datamancer::transport::TransportError::ServiceClosed) => break,
            Err(e) => panic!("poll: {e}"),
        };
        for event in events {
            match event {
                MarketEvent::Bar(bar) => {
                    assert_eq!(bar.instrument.symbol(), "AAPL", "{bar:?}");
                    bars += 1;
                }
                MarketEvent::Control(control) => {
                    if control.kind == ControlKind::SessionClosing {
                        closed = true;
                    }
                }
                other => panic!("unexpected event on a bar query: {other:?}"),
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(closed, "the stream ended without SessionClosing");
    assert!(bars > 0, "SessionClosing arrived before any bar");
    assert_eq!(
        subscriber.held_len(),
        0,
        "bars held back for an announcement that never came"
    );

    // The pump reaps the query once the stream has drained and flushed.
    wait_until_reaped(&daemon, id);
    drop((subscriber, node, writer, reader));
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

    // The equity provider serves no crypto, so routing rejects the pair before
    // any session opens.
    let crypto = round_trip(
        daemon.socket(),
        r#"{"op":"open-query","provider":"alpaca","asset_class":"crypto","symbol":"BTC/USD","kind":"bar1d","from":100,"to":200}"#,
    );
    assert_eq!(crypto["ok"], false);
    assert_eq!(crypto["code"], "unsupported_event_kind");
}

/// `spawn_daemon` configures no `[cache]`, so its query policy degrades to none.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn open_query_on_a_disabled_provider_is_refused_at_open_without_a_cache() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path());

    let removed = round_trip(
        daemon.socket(),
        r#"{"op":"remove-provider","provider":"alpaca"}"#,
    );
    assert_eq!(removed["ok"], true, "{removed}");

    let reply = round_trip(
        daemon.socket(),
        r#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1d","from":100,"to":200}"#,
    );
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(reply["code"], "provider_disabled", "{reply}");
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

// ---------------------------------------------------------------------------
// Hermetic tests against the scripted provider (feature `scripted-provider`)
// ---------------------------------------------------------------------------

/// Spawn a daemon whose only enabled provider is the scripted one, with the
/// given `[provider.scripted]` knobs and further config sections.
#[cfg(feature = "scripted-provider")]
fn spawn_scripted_daemon(dir: &Path, scripted: &str, sections: &str) -> Daemon {
    let lock = DAEMON_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let service_prefix = format!(
        "datamancerd-e2e-{}-{}",
        std::process::id(),
        PREFIX_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let socket = dir.join("admin.sock");
    let config_path = dir.join("datamancerd.toml");
    let config = format!(
        r#"
[provider.scripted]
{scripted}

[server]
admin_socket = "{}"
service_prefix = "{service_prefix}"

[diagnostics]
publish_interval_ms = 200

{sections}
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
    let deadline = Instant::now() + Duration::from_secs(10);
    while !daemon.socket().exists() {
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        std::thread::sleep(Duration::from_millis(50));
    }
    daemon
}

/// A held control connection: `open-client` plus any number of further
/// requests on the same line-oriented socket.
#[cfg(feature = "scripted-provider")]
struct Conn {
    writer: UnixStream,
    reader: BufReader<UnixStream>,
}

#[cfg(feature = "scripted-provider")]
impl Conn {
    fn open(daemon: &Daemon) -> Self {
        let stream = UnixStream::connect(daemon.socket()).expect("connect");
        let writer = stream.try_clone().expect("clone");
        let reader = BufReader::new(stream);
        Self { writer, reader }
    }

    fn request(&mut self, line: &str) -> serde_json::Value {
        self.writer.write_all(line.as_bytes()).expect("write");
        self.writer.write_all(b"\n").expect("nl");
        self.writer.flush().expect("flush");
        let mut reply = String::new();
        self.reader.read_line(&mut reply).expect("read reply");
        serde_json::from_str(&reply).expect("parse reply")
    }

    /// `open-client` and attach a data-plane subscriber to the advertised
    /// service.
    fn open_client(
        &mut self,
        name: &str,
        node: &iceoryx2::node::Node<ipc_threadsafe::Service>,
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

/// `open-query` for `symbol`'s trades over a fixed window.
#[cfg(feature = "scripted-provider")]
fn open_trade_query(conn: &mut Conn, symbol: &str) -> (u64, DataServiceName) {
    let reply = conn.request(&format!(
        r#"{{"op":"open-query","provider":"scripted","asset_class":"equity","symbol":"{symbol}","kind":"trade","from":1000000000,"to":2000000000}}"#
    ));
    assert_eq!(reply["ok"], true, "open-query rejected: {reply}");
    let service = reply["service"].as_str().expect("service name");
    let name = service
        .parse()
        .unwrap_or_else(|e| panic!("not a data-service name: {service}: {e}"));
    (reply["query"].as_u64().expect("query id"), name)
}

/// Every trade price a query delivers, read to the end of its stream, which
/// must arrive within 30 s and follow a terminal `SessionClosing`.
#[cfg(feature = "scripted-provider")]
fn drain_trade_query(subscriber: &mut DataSubscriber) -> Vec<f64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut prices = Vec::new();
    let mut closing = false;
    loop {
        assert!(
            Instant::now() < deadline,
            "the stream did not end within 30 s, after {} trades",
            prices.len()
        );
        let events = match subscriber.poll() {
            Ok(events) => events,
            Err(TransportError::ServiceClosed) => break,
            Err(e) => panic!("poll: {e}"),
        };
        for event in events {
            assert!(!closing, "{event:?} after SessionClosing");
            match event {
                MarketEvent::Trade(t) => prices.push(t.price.to_f64()),
                MarketEvent::Control(c) if c.kind == ControlKind::SessionClosing => closing = true,
                MarketEvent::Control(_) => {}
                other => panic!("unexpected event on a trade query: {other:?}"),
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(closing, "the stream ended without SessionClosing");
    prices
}

#[cfg(feature = "scripted-provider")]
fn now_nanos() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits")
}

/// What one subscriber observed, in delivery order.
#[cfg(feature = "scripted-provider")]
#[derive(Debug, Clone, PartialEq)]
enum Seen {
    /// `(price, source_ts, seq)`.
    Trade(f64, i64, u64),
    Gap,
}

#[cfg(feature = "scripted-provider")]
impl Seen {
    fn is_gap(&self) -> bool {
        matches!(self, Seen::Gap)
    }
}

/// Poll until `want` data events (gaps count) have been seen or `budget`
/// elapses.
#[cfg(feature = "scripted-provider")]
fn collect(
    subscriber: &mut datamancer::transport::DataSubscriber,
    want: usize,
    budget: Duration,
) -> Vec<Seen> {
    let deadline = Instant::now() + budget;
    let mut seen = Vec::new();
    while seen.len() < want && Instant::now() < deadline {
        for event in subscriber.poll().expect("poll") {
            match event {
                MarketEvent::Trade(t) => {
                    seen.push(Seen::Trade(t.price.to_f64(), t.source_ts.0, t.seq.0));
                }
                MarketEvent::Control(c) => {
                    if matches!(c.kind, ControlKind::Gap { .. }) {
                        seen.push(Seen::Gap);
                    }
                }
                other => panic!("unexpected event on a trade subscription: {other:?}"),
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    seen
}

/// `subscribe` with `scope: live_backfill` against the scripted provider:
/// the client sees the history, then the live prints that arrived during the
/// backfill in arrival order, then the live tail, with one monotonic `seq`
/// and no `Gap`; the reply carries the backfill scope, and a second client
/// subscribing pure-live to the same pair is told the backfill scope.
#[test]
#[cfg(feature = "scripted-provider")]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    reason = "a handful of scripted prints; the indices are tiny"
)]
fn subscribe_live_backfill_serves_history_then_buffered_prints_then_live() {
    let dir = tempfile::tempdir().expect("tempdir");
    // 5 history prints after a 400 ms wait; a live print every 50 ms, so
    // several land while the backfill is in flight and are buffered.
    let daemon = spawn_scripted_daemon(
        dir.path(),
        "history_prints = 5\nhistory_delay_ms = 400\nlive_interval_ms = 50\n",
        "",
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");

    let mut first = Conn::open(&daemon);
    let mut subscriber = first.open_client("backfill-1", &node);
    let from = now_nanos() - 60_000_000_000;
    let subscribed_at = now_nanos();
    let reply = first.request(&format!(
        r#"{{"op":"subscribe","client":"backfill-1","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","scope":"live_backfill","backfill_from":{from}}}"#
    ));
    assert_eq!(reply["ok"], true, "subscribe rejected: {reply}");
    assert_eq!(reply["scope"], "live_backfill", "{reply}");
    assert_eq!(reply["backfill_from"], from, "{reply}");
    assert!(
        reply.get("joined").is_none(),
        "the creator is not a joiner: {reply}"
    );
    assert!(
        reply.get("seed").is_none(),
        "a backfill never seeds: {reply}"
    );

    // A second client, pure-live on the same pair, is told the actual scope.
    let mut second = Conn::open(&daemon);
    let _second_subscriber = second.open_client("live-2", &node);
    let reply = second.request(
        r#"{"op":"subscribe","client":"live-2","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade"}"#,
    );
    assert_eq!(reply["ok"], true, "second subscribe rejected: {reply}");
    assert_eq!(reply["scope"], "live_backfill", "{reply}");
    assert_eq!(reply["backfill_from"], from, "{reply}");
    assert_eq!(reply["joined"], true, "the joiner is told so: {reply}");

    // 5 history + at least 8 live prints (~400 ms of buffered prints plus a
    // live tail).
    let mut seen = collect(&mut subscriber, 5, Duration::from_secs(10));
    let history_seen_at = now_nanos();
    let rest = 13_usize.saturating_sub(seen.len());
    seen.extend(collect(&mut subscriber, rest, Duration::from_secs(10)));
    assert!(seen.len() >= 13, "too few events: {seen:?}");
    assert!(
        !seen.iter().any(Seen::is_gap),
        "no Gap at a healthy seam: {seen:?}"
    );

    let trades: Vec<(f64, i64, u64)> = seen
        .iter()
        .map(|s| match s {
            Seen::Trade(p, ts, seq) => (*p, *ts, *seq),
            Seen::Gap => unreachable!(),
        })
        .collect();
    // History first: prices 1..=5 at ascending source_ts inside the window
    // (the last one sits at the live edge, past the request time, and every
    // one precedes the first live print).
    let first_live_ts = trades[5].1;
    for (i, (price, ts, seq)) in trades.iter().take(5).enumerate() {
        assert!(
            (price - (1.0 + i as f64)).abs() < 1e-9,
            "history {i}: {trades:?}"
        );
        assert!(
            *ts >= from && *ts < first_live_ts,
            "history {i} ts: {trades:?}"
        );
        assert_eq!(*seq, i as u64, "history seq: {trades:?}");
    }
    // Then the live prints, in arrival order, seq continuing without a hole.
    let live = &trades[5..];
    assert!(
        live[0].0 >= 100.0,
        "live tail starts after history: {trades:?}"
    );
    for (i, (price, ts, seq)) in live.iter().enumerate() {
        assert!(
            (price - (100.0 + i as f64)).abs() < 1e-9,
            "live {i}: {trades:?}"
        );
        assert!(
            *ts >= subscribed_at,
            "live {i} ts before subscribe: {trades:?}"
        );
        assert_eq!(*seq, 5 + i as u64, "live seq: {trades:?}");
    }
    assert!(
        live[0].1 < history_seen_at,
        "the first live print should be buffered behind the history: {trades:?}"
    );

    drop((subscriber, second, first, node));
}

/// The seam on a cold provider socket: the scripted provider's
/// `subscribe` returns only after `connect_delay_ms` (the lazy connect, auth
/// and acknowledgement a first subscriber waits for) and emits nothing
/// before then. The daemon's reply waits for that return, the backfill's
/// live edge is fixed after it (the scripted history's last print sits at
/// `edge - 1`, so the edge is readable off the stream), every live print is
/// at or after the edge, `seq` is contiguous and no `Gap` is emitted: no
/// uncovered window between the history and the first live print.
#[test]
#[cfg(feature = "scripted-provider")]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    reason = "a handful of scripted prints; the indices are tiny"
)]
fn subscribe_live_backfill_fixes_its_edge_after_a_cold_socket_connects() {
    const CONNECT_DELAY_NS: i64 = 500_000_000;
    let dir = tempfile::tempdir().expect("tempdir");
    // 3 history prints after a 400 ms wait, a live print every 50 ms, and a
    // 500 ms "connect" before the first subscribe returns.
    let daemon = spawn_scripted_daemon(
        dir.path(),
        "history_prints = 3\nhistory_delay_ms = 400\nlive_interval_ms = 50\nconnect_delay_ms = 500\n",
        "",
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::open(&daemon);
    let mut subscriber = conn.open_client("cold", &node);
    let from = now_nanos() - 60_000_000_000;
    let subscribed_at = now_nanos();
    let reply = conn.request(&format!(
        r#"{{"op":"subscribe","client":"cold","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","scope":"live_backfill","backfill_from":{from}}}"#
    ));
    let replied_at = now_nanos();
    assert_eq!(reply["ok"], true, "subscribe rejected: {reply}");
    assert_eq!(reply["scope"], "live_backfill", "{reply}");
    // The reply waited for the provider's acknowledgement.
    assert!(
        replied_at - subscribed_at >= CONNECT_DELAY_NS,
        "the subscribe must wait for the cold socket: {subscribed_at} -> {replied_at}"
    );

    // 3 history + at least 4 live prints.
    let seen = collect(&mut subscriber, 7, Duration::from_secs(10));
    assert!(seen.len() >= 7, "too few events: {seen:?}");
    assert!(
        !seen.iter().any(Seen::is_gap),
        "a cold socket is not loss, no Gap: {seen:?}"
    );
    let trades: Vec<(f64, i64, u64)> = seen
        .iter()
        .map(|s| match s {
            Seen::Trade(p, ts, seq) => (*p, *ts, *seq),
            Seen::Gap => unreachable!(),
        })
        .collect();
    for (i, (price, ts, seq)) in trades.iter().take(3).enumerate() {
        assert!(
            (price - (1.0 + i as f64)).abs() < 1e-9,
            "history {i}: {trades:?}"
        );
        assert!(*ts >= from, "history {i} ts: {trades:?}");
        assert_eq!(*seq, i as u64, "history seq: {trades:?}");
    }
    // The live edge, read off the last history print: fixed after the
    // provider acknowledged the pair (at or after request + delay) and
    // before the reply went out.
    let edge = trades[2].1 + 1;
    assert!(
        edge >= subscribed_at + CONNECT_DELAY_NS,
        "the live edge must follow the acknowledgement: edge {edge}, request {subscribed_at}"
    );
    assert!(
        edge <= replied_at,
        "the live edge is fixed before the reply: edge {edge}, reply {replied_at}"
    );
    // Every live print is at or after the edge, in order, seq contiguous:
    // the history reached the point the live side covers from.
    let live = &trades[3..];
    for (i, (price, ts, seq)) in live.iter().enumerate() {
        assert!(
            (price - (100.0 + i as f64)).abs() < 1e-9,
            "live {i}: {trades:?}"
        );
        assert!(
            *ts >= edge,
            "live {i} ts before the edge {edge}: {trades:?}"
        );
        assert_eq!(*seq, 3 + i as u64, "live seq: {trades:?}");
    }
    drop((subscriber, conn, node));
}

/// `subscribe` pure-live: `latest` seeds the stream with the scripted print
/// ahead of the live tail, and with it off the live tail comes first.
#[test]
#[cfg(feature = "scripted-provider")]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn subscribe_live_seeds_from_the_latest_knob() {
    for (latest, first_price) in [(true, 999.0), (false, 100.0)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let daemon = spawn_scripted_daemon(
            dir.path(),
            &format!("latest = {latest}\nlive_interval_ms = 50\n"),
            "",
        );
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .expect("iceoryx2 node");
        let mut conn = Conn::open(&daemon);
        let mut subscriber = conn.open_client("seeded", &node);
        let reply = conn.request(
            r#"{"op":"subscribe","client":"seeded","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade"}"#,
        );
        assert_eq!(reply["ok"], true, "subscribe rejected: {reply}");
        assert_eq!(reply["scope"], "live", "{reply}");

        let seen = collect(&mut subscriber, 1, Duration::from_secs(10));
        assert!(
            matches!(seen.first(), Some(Seen::Trade(price, ..)) if (price - first_price).abs() < 1e-9),
            "latest = {latest}: {seen:?}"
        );
    }
}

/// `seed:false` opens a session with no data event ahead of the first live print,
/// a joiner asking for the seed is told the session's choice, and an explicit
/// `seed:true` on another pair delivers the latest value first.
#[test]
#[cfg(feature = "scripted-provider")]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn subscribe_seed_false_delivers_the_first_live_print_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = true\nlive_interval_ms = 300\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");

    let mut unseeded = Conn::open(&daemon);
    let mut unseeded_sub = unseeded.open_client("unseeded", &node);
    let reply = unseeded.request(
        r#"{"op":"subscribe","client":"unseeded","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","scope":"live","seed":false}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["scope"], "live", "{reply}");
    assert_eq!(reply["seed"], false, "{reply}");

    let mut joiner = Conn::open(&daemon);
    let _joiner_sub = joiner.open_client("joiner", &node);
    let reply = joiner.request(
        r#"{"op":"subscribe","client":"joiner","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","seed":true}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["joined"], true, "{reply}");
    assert_eq!(reply["seed"], false, "{reply}");

    let seen = collect(&mut unseeded_sub, 1, Duration::from_secs(10));
    assert!(
        matches!(seen.first(), Some(Seen::Trade(price, _, 0)) if (price - 100.0).abs() < 1e-9),
        "the first event must be the first live print: {seen:?}"
    );

    let mut seeded = Conn::open(&daemon);
    let mut seeded_sub = seeded.open_client("seeded", &node);
    let reply = seeded.request(
        r#"{"op":"subscribe","client":"seeded","provider":"scripted","asset_class":"equity","symbol":"MSFT","kind":"trade","seed":true}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["seed"], true, "{reply}");
    let seen = collect(&mut seeded_sub, 2, Duration::from_secs(10));
    assert!(seen.len() >= 2, "{seen:?}");
    assert_eq!(seen[0], Seen::Trade(999.0, 1, 0), "seed first: {seen:?}");
    assert!(
        matches!(seen[1], Seen::Trade(price, _, 1) if (price - 100.0).abs() < 1e-9),
        "then live: {seen:?}"
    );
}

/// `seed:false` inside `open-client` reaches the session it creates: a joiner
/// is told the session does not seed.
#[test]
#[cfg(feature = "scripted-provider")]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn open_client_subscriptions_carry_seed_false_to_the_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = true\nlive_interval_ms = 300\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");

    let mut opener = Conn::open(&daemon);
    let reply = opener.request(
        r#"{"op":"open-client","client":"opener","subscriptions":[{"provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","seed":false}]}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");

    let mut joiner = Conn::open(&daemon);
    let _joiner_sub = joiner.open_client("joiner", &node);
    let reply = joiner.request(
        r#"{"op":"subscribe","client":"joiner","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade"}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["joined"], true, "{reply}");
    assert_eq!(reply["seed"], false, "{reply}");
}

/// `subscribe` to an id no provider owns answers `unknown_provider`, and to a
/// compiled-in provider with no config section, `provider_disabled`.
#[test]
#[cfg(feature = "scripted-provider")]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn subscribe_refuses_an_unowned_id_and_a_disabled_provider() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::open(&daemon);
    let _subscriber = conn.open_client("refused", &node);

    for (provider, code) in [
        ("nobody", "unknown_provider"),
        ("alpaca", "provider_disabled"),
    ] {
        let reply = conn.request(&format!(
            r#"{{"op":"subscribe","client":"refused","provider":"{provider}","asset_class":"equity","symbol":"AAPL","kind":"trade"}}"#
        ));
        assert_eq!(reply["ok"], false, "{reply}");
        assert_eq!(reply["code"], code, "{reply}");
    }
}

/// A scripted daemon for the query tests: one query slot, a memory cache and
/// no fetch delay.
#[cfg(feature = "scripted-provider")]
fn spawn_query_daemon(dir: &Path, prints: u32) -> Daemon {
    spawn_scripted_daemon(
        dir,
        &format!("history_delay_ms = 0\nhistory_prints = {prints}\n"),
        "[iceoryx2]\nmax_queries = 1\n\n[cache]\nbackend = \"memory\"\n",
    )
}

/// The scripted provider's upstream history fetches so far.
#[cfg(feature = "scripted-provider")]
fn scripted_history_fetches(daemon: &Daemon) -> u64 {
    let reply = round_trip(daemon.socket(), r#"{"op":"snapshot"}"#);
    reply["snapshot"]["providers"]
        .as_array()
        .expect("providers array")
        .iter()
        .find(|provider| provider["provider"] == "scripted")
        .unwrap_or_else(|| panic!("no scripted provider: {reply}"))["history_fetches"]
        .as_u64()
        .expect("history_fetches")
}

/// Warm the cache with a query of `prints` trades, then run it again with the
/// attach delayed well past a cached read's end: every trade still arrives,
/// then `SessionClosing`, then the end of the stream.
#[cfg(feature = "scripted-provider")]
fn assert_a_cached_query_survives_a_late_attach(prints: u32) {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_query_daemon(dir.path(), prints);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut conn = Conn::open(&daemon);
    let expected: Vec<f64> = (1..=prints).map(f64::from).collect();

    let (warm, service) = open_trade_query(&mut conn, "AAPL");
    let mut subscriber = DataSubscriber::open(&node, &service).expect("subscriber");
    assert_eq!(drain_trade_query(&mut subscriber), expected);
    wait_until_reaped(&daemon, warm);
    let fetches = scripted_history_fetches(&daemon);
    assert!(fetches > 0, "the first query must reach the provider");

    let (_, service) = open_trade_query(&mut conn, "AAPL");
    std::thread::sleep(Duration::from_millis(300));
    let mut subscriber = DataSubscriber::open(&node, &service).expect("subscriber");
    assert_eq!(drain_trade_query(&mut subscriber), expected);
    assert_eq!(
        scripted_history_fetches(&daemon),
        fetches,
        "the window was not served from the cache"
    );
}

/// More trades than the data service's history and subscriber buffer hold.
#[cfg(feature = "scripted-provider")]
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_cached_query_is_held_for_a_late_attach() {
    assert_a_cached_query_survives_a_late_attach(100);
}

/// Few enough trades that the whole query fits in one burst.
#[cfg(feature = "scripted-provider")]
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_short_cached_query_is_held_for_a_late_attach() {
    assert_a_cached_query_survives_a_late_attach(3);
}

/// A query nobody attaches to holds its slot for the grace alone: the cap
/// refuses a second query meanwhile, the first is then reaped on its own with
/// its service gone, so a late attach fails, and the slot serves a new query.
#[cfg(feature = "scripted-provider")]
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_query_nobody_attaches_to_releases_its_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_query_daemon(dir.path(), 3);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut conn = Conn::open(&daemon);

    let (abandoned, service) = open_trade_query(&mut conn, "AAPL");
    let refused = conn.request(
        r#"{"op":"open-query","provider":"scripted","asset_class":"equity","symbol":"MSFT","kind":"trade","from":1000000000,"to":2000000000}"#,
    );
    assert_eq!(refused["ok"], false, "{refused}");
    assert_eq!(refused["code"], "service_cap_exceeded", "{refused}");

    wait_until_reaped(&daemon, abandoned);
    assert!(
        DataSubscriber::open(&node, &service).is_err(),
        "attaching to a reaped query must fail"
    );

    let (_, service) = open_trade_query(&mut conn, "MSFT");
    let mut subscriber = DataSubscriber::open(&node, &service).expect("subscriber");
    assert_eq!(drain_trade_query(&mut subscriber), [1.0, 2.0, 3.0]);
}

/// A query waiting for its client does not hold up shutdown. The query is
/// still in flight long after its fetch ended, and the daemon then exits well
/// inside what is left of the grace.
#[cfg(feature = "scripted-provider")]
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn shutdown_does_not_wait_for_a_query_to_be_attached() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut daemon = spawn_query_daemon(dir.path(), 3);
    let mut conn = Conn::open(&daemon);
    let (id, _) = open_trade_query(&mut conn, "AAPL");
    std::thread::sleep(Duration::from_millis(300));
    let listed = round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    assert!(
        listed["queries"]
            .as_array()
            .expect("queries array")
            .contains(&serde_json::json!(id)),
        "the query must be waiting for its client: {listed}"
    );

    let requested = Instant::now();
    assert_eq!(conn.request(r#"{"op":"shutdown"}"#)["ok"], true);
    while daemon.guard.try_wait().expect("try_wait").is_none() {
        assert!(
            requested.elapsed() < Duration::from_secs(4),
            "the daemon is still up"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The next trade on `events`, skipping controls.
#[cfg(all(feature = "scripted-provider", feature = "ws"))]
async fn next_trade(
    events: &mut (impl futures::Stream<Item = MarketEvent> + Unpin),
) -> datamancer::Trade {
    use futures::StreamExt as _;

    let trade = async {
        loop {
            match events.next().await.expect("stream ended") {
                MarketEvent::Trade(trade) => break trade,
                MarketEvent::Control(_) => {}
                other => panic!("unexpected event on a trade subscription: {other:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), trade)
        .await
        .expect("no trade arrived")
}

/// A typed client connected to the scripted daemon's WS listener, once it is
/// up.
#[cfg(all(feature = "scripted-provider", feature = "ws"))]
async fn connect_ws(
    port: u16,
) -> (
    datamancer_client::ws::WsClient,
    <datamancer_client::ws::WsClient as datamancer_client::Client>::Events,
) {
    use datamancer_client::Client as _;
    use datamancer_client::ws::{WsClient, WsConfig};

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let config = WsConfig {
            url: format!("ws://127.0.0.1:{port}/"),
            auth_token: None,
            event_buffer: 256,
        };
        match WsClient::connect(config).await {
            Ok(connected) => return connected,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("ws never became ready: {e}"),
        }
    }
}

/// `subscribe` over WS against the scripted provider: the creator reads the
/// backfill it opened, then its history and a live print under one contiguous
/// `seq`; a pure-live joiner reads the session's scope and receives trades.
#[tokio::test]
#[cfg(all(feature = "scripted-provider", feature = "ws"))]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
async fn ws_subscribe_replies_with_the_scope_and_streams_history_then_live() {
    use datamancer::{
        AssetClass, EventKind, Instrument, LiveScope, ProviderId, Subscribed, Timestamp,
    };
    use datamancer_client::Client as _;
    use datamancer_client::spec::SubscriptionSpec;

    const PORT: u16 = 19023;

    let dir = tempfile::tempdir().expect("tempdir");
    let _daemon = spawn_scripted_daemon(
        dir.path(),
        "history_prints = 3\nhistory_delay_ms = 200\nlive_interval_ms = 50\n",
        &format!("[ws]\nenabled = true\nbind = \"127.0.0.1\"\nport = {PORT}\n"),
    );
    let instrument = Instrument::new(
        ProviderId::from_static("scripted"),
        AssetClass::Equity,
        "AAPL",
    );
    let spec = SubscriptionSpec::new(&instrument, EventKind::Trade).expect("spec");
    let from = Timestamp(now_nanos() - 60_000_000_000);
    let backfill = LiveScope::LiveBackfill { from };

    let (mut creator, mut events) = connect_ws(PORT).await;
    let created = creator
        .subscribe(&spec.clone().with_backfill_from(from))
        .await
        .expect("subscribe");
    assert_eq!(created, Subscribed::Created(backfill));

    let (mut joiner, mut joiner_events) = connect_ws(PORT).await;
    assert_eq!(
        joiner.subscribe(&spec).await.expect("join"),
        Subscribed::Joined { session: backfill }
    );

    for (seq, price) in [1.0, 2.0, 3.0, 100.0].into_iter().enumerate() {
        let trade = next_trade(&mut events).await;
        assert!((trade.price.to_f64() - price).abs() < 1e-9, "{trade:?}");
        assert_eq!(trade.seq.0, seq as u64, "{trade:?}");
    }
    next_trade(&mut joiner_events).await;
}

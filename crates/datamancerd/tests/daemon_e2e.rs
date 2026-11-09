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
//! The tests whose `#[ignore]` reason names Alpaca credentials read them from
//! the environment. The hermetic tests against the scripted provider are in
//! `scripted_e2e.rs`.

// This suite talks to the daemon over a Unix-domain control socket and manages
// the process POSIX-style; the Windows named-pipe harness port is Phase 5 (#29).
// Compile on Unix only until then.
#![cfg(unix)]
#![forbid(unsafe_code)]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use common::{Daemon, round_trip};
use datamancer::{ControlKind, MarketEvent};
use iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};

/// The real-provider sections every daemon of this suite runs with.
const PROVIDERS: &str = r#"
[provider.alpaca]
account_type = "paper"

[provider.alpaca_crypto]
account_type = "paper"
venue = "us"
"#;

/// Spawn the daemon with both Alpaca providers and wait for its control
/// socket to appear.
fn spawn_daemon(dir: &Path) -> Daemon {
    spawn_daemon_with_prefix(dir, common::unique_prefix())
}

/// [`spawn_daemon`] with an explicit `[server].service_prefix`, for a test
/// that needs two daemons configured identically (the crash-restart case).
fn spawn_daemon_with_prefix(dir: &Path, service_prefix: String) -> Daemon {
    common::spawn_daemon(dir, service_prefix, PROVIDERS, "", common::daemon_command)
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

#[test]
#[ignore = "spawns the daemon binary and needs a live iceoryx2 runtime"]
fn subscribe_refuses_a_backfill_older_than_the_limit() {
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

    let reply = round_trip(
        daemon.socket(),
        r#"{"op":"subscribe","client":"exec-1","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade","scope":"live_backfill","backfill_from":1}"#,
    );
    assert_eq!(reply["code"], "backfill_too_old", "{reply}");
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

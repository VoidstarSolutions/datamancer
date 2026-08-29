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

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// Spawn the daemon with a memory-backed config and wait for its control
/// socket to appear. Returns the child and the socket path.
fn spawn_daemon(dir: &std::path::Path) -> (Child, PathBuf) {
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
service_prefix = "datamancerd-e2e"

[diagnostics]
publish_interval_ms = 200
"#,
        socket.display()
    );
    std::fs::write(&config_path, config).expect("write config");

    let bin = env!("CARGO_BIN_EXE_datamancerd");
    let child = Command::new(bin)
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("spawn datamancerd");

    // Wait for the socket to appear (daemon bound).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        std::thread::sleep(Duration::from_millis(50));
    }
    (child, socket)
}

/// Send one JSON line and read one JSON reply line.
fn round_trip(socket: &std::path::Path, request: &str) -> serde_json::Value {
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
    let (mut child, socket) = spawn_daemon(dir.path());

    let reply = round_trip(&socket, r#"{"op":"list-clients"}"#);
    assert_eq!(reply["ok"], serde_json::Value::Bool(true));
    assert!(reply["clients"].is_array());

    let snap = round_trip(&socket, r#"{"op":"snapshot"}"#);
    assert_eq!(snap["ok"], serde_json::Value::Bool(true));
    assert!(snap["snapshot"].is_object());

    // Unknown op -> structured error reply with a stable code.
    let err = round_trip(&socket, r#"{"op":"frobnicate"}"#);
    assert_eq!(err["ok"], serde_json::Value::Bool(false));
    assert_eq!(err["code"], "bad_request");

    child.kill().expect("kill");
    let _ = child.wait();
}

#[test]
#[ignore = "needs a live iceoryx2 runtime"]
fn open_client_creates_a_service_then_closes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, socket) = spawn_daemon(dir.path());

    // open-client over its own long-lived connection.
    let stream = UnixStream::connect(&socket).expect("connect");
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
    assert!(
        reply["service"]
            .as_str()
            .is_some_and(|s| s.contains("datamancerd-e2e/data/"))
    );

    // The client appears in list-clients (separate connection).
    let listing = round_trip(&socket, r#"{"op":"list-clients"}"#);
    let clients = listing["clients"].as_array().expect("array");
    assert!(clients.iter().any(|c| c == "exec-1"));

    // Dropping the connection (EOF) tears the client down.
    drop(writer);
    drop(reader);
    std::thread::sleep(Duration::from_millis(300));
    let after = round_trip(&socket, r#"{"op":"list-clients"}"#);
    let clients = after["clients"].as_array().expect("array");
    assert!(
        !clients.iter().any(|c| c == "exec-1"),
        "client not torn down"
    );

    child.kill().expect("kill");
    let _ = child.wait();
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
/// run. This looks like a genuine gap in the equity daily-bar historical
/// path (or an Alpaca-side quirk for that exact short range/holiday-adjacent
/// window), worth a follow-up outside this task's scope.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime and Alpaca credentials"]
fn open_query_streams_bars_then_reaps_its_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, socket) = spawn_daemon(dir.path());

    let stream = UnixStream::connect(&socket).expect("connect");
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
    // `service_prefix` is "datamancerd-e2e" in `spawn_daemon`'s config above,
    // not the daemon's own out-of-the-box default ("datamancerd").
    assert_eq!(reply["service"], format!("datamancerd-e2e/data/{id}"));

    // The query is in flight immediately after the reply. `list-queries` owns
    // nothing, so checking it over a fresh `round_trip` connection cannot
    // itself cancel anything.
    let listed = round_trip(&socket, r#"{"op":"list-queries"}"#);
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
    let mut after = round_trip(&socket, r#"{"op":"list-queries"}"#);
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
        after = round_trip(&socket, r#"{"op":"list-queries"}"#);
    }

    drop((writer, reader));
    let _ = child.kill();
}

#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn open_query_rejects_an_inverted_range_and_an_unserved_pair() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, socket) = spawn_daemon(dir.path());

    let inverted = round_trip(
        &socket,
        r#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1d","from":200,"to":100}"#,
    );
    assert_eq!(inverted["ok"], false);
    assert_eq!(inverted["code"], "bad_request");

    // Alpaca's crypto provider reports `Surface::History => false`, so routing
    // rejects the pair before any session opens.
    let crypto = round_trip(
        &socket,
        r#"{"op":"open-query","provider":"alpaca","asset_class":"crypto","symbol":"BTC/USD","kind":"bar1d","from":100,"to":200}"#,
    );
    assert_eq!(crypto["ok"], false);
    assert_eq!(crypto["code"], "unsupported_event_kind");

    let _ = child.kill();
}

#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime and Alpaca credentials"]
fn cancel_query_reports_unknown_for_a_stale_id_and_caps_concurrency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, socket) = spawn_daemon(dir.path());

    let stale = round_trip(&socket, r#"{"op":"cancel-query","query":9999}"#);
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
        let stream = UnixStream::connect(&socket).expect("connect");
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
    let listed = round_trip(&socket, r#"{"op":"list-queries"}"#);
    let ids = listed["queries"].as_array().expect("queries array");
    assert_eq!(
        ids.len(),
        2,
        "expected exactly 2 in-flight queries: {listed}"
    );

    let first_id = replies[0]["query"].as_u64().expect("id");
    let cancelled = round_trip(
        &socket,
        &format!(r#"{{"op":"cancel-query","query":{first_id}}}"#),
    );
    assert_eq!(cancelled["ok"], true, "{cancelled}");

    drop(conns); // release the still-open connections before killing the daemon
    let _ = child.kill();
}

/// A dropped control connection must abort the queries it opened — otherwise a
/// crashed backtest keeps burning provider rate limit.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime and Alpaca credentials"]
fn dropping_the_control_connection_aborts_its_queries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, socket) = spawn_daemon(dir.path());

    // `round_trip` opens and drops a connection per call, so the query it
    // opens is torn down when it returns.
    let opened = round_trip(
        &socket,
        r#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1m","from":1704067200000000000,"to":1706745600000000000}"#,
    );
    assert_eq!(opened["ok"], true, "{opened}");

    std::thread::sleep(std::time::Duration::from_millis(500));
    let listed = round_trip(&socket, r#"{"op":"list-queries"}"#);
    assert_eq!(
        listed["queries"],
        serde_json::json!([]),
        "EOF must reap the connection's queries: {listed}"
    );

    let _ = child.kill();
}

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

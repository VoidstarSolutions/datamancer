//! End-to-end WS client surface tests. `#[ignore]`d — they bind a socket and
//! spawn the real `datamancerd` binary (which needs a live iceoryx2 runtime);
//! run with:
//!
//! ```text
//! cargo test -p datamancerd --features ws --test ws_e2e -- --ignored
//! ```
//!
//! They exercise wiring (handshake/auth, snapshot reply + id echo, subscribe
//! reply + id echo, teardown), not live market data. The spawned daemon is the
//! binary built WITH the `ws` feature, so it runs the ws listener.

#![cfg(feature = "ws")]

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use futures::{SinkExt as _, StreamExt as _};
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// The host diagnostics plane (`datamancer/diagnostics`) is a single-publisher
/// iceoryx2 service, so only one daemon may run at a time. Serialize the tests
/// (they otherwise run on parallel threads within this one test binary).
static DAEMON_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Spawn the daemon from a written config file. Returns the child; the caller is
/// responsible for waiting on WS readiness.
fn spawn_daemon(dir: &std::path::Path, port: u16, auth_token: Option<&str>) -> Child {
    let socket = dir.join("admin.sock");
    let config_path = dir.join("datamancerd.toml");
    let auth_line = match auth_token {
        Some(t) => format!("auth_token = \"{t}\"\n"),
        None => String::new(),
    };
    let config = format!(
        r#"
[provider.alpaca_crypto]
account_type = "paper"
venue = "us"

[server]
admin_socket = "{socket}"
service_prefix = "datamancerd-ws-e2e-{port}"

[diagnostics]
publish_interval_ms = 200

[ws]
enabled = true
bind = "127.0.0.1"
port = {port}
{auth_line}"#,
        socket = socket.display(),
    );
    std::fs::write(&config_path, config).expect("write config");

    let bin: PathBuf = env!("CARGO_BIN_EXE_datamancerd").into();
    Command::new(bin)
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("spawn datamancerd")
}

/// Retry `connect_async` until the ws listener is up or the deadline elapses.
async fn connect_when_ready(
    port: u16,
) -> tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
> {
    let url = format!("ws://127.0.0.1:{port}/");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match connect_async(&url).await {
            Ok((ws, _resp)) => return ws,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("ws never became ready: {e}"),
        }
    }
}

/// Read text frames until one parses to a JSON object whose `id` equals
/// `want_id`, returning that value. Skips interleaved event frames.
async fn read_reply<S>(ws: &mut S, want_id: u64) -> serde_json::Value
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("reply timed out")
            .expect("stream ended")
            .expect("ws error");
        if let Message::Text(t) = msg {
            let v: serde_json::Value = serde_json::from_str(&t).expect("parse frame");
            if v.get("id").and_then(serde_json::Value::as_u64) == Some(want_id) {
                return v;
            }
        }
    }
    panic!("no reply with id {want_id} arrived");
}

#[tokio::test]
#[ignore = "spawns the binary; needs a live iceoryx2 runtime; run with --ignored"]
async fn subscribe_reply_echoes_id_and_snapshot_returns() {
    let _guard = DAEMON_LOCK.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let port = 19011;
    let mut child = spawn_daemon(dir.path(), port, None);

    let mut ws = connect_when_ready(port).await;

    // snapshot -> reply echoes id 7 with a snapshot object.
    ws.send(Message::text(r#"{"id":7,"op":"snapshot"}"#))
        .await
        .expect("send snapshot");
    let snap = read_reply(&mut ws, 7).await;
    assert_eq!(snap["id"], 7);
    assert_eq!(snap["ok"], serde_json::Value::Bool(true));
    assert!(snap["snapshot"].is_object(), "expected snapshot object");

    // subscribe paper crypto BTC/USD trade -> reply echoes id 8, ok.
    ws.send(Message::text(
        r#"{"id":8,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
    ))
    .await
    .expect("send subscribe");
    let sub = read_reply(&mut ws, 8).await;
    assert_eq!(sub["id"], 8);
    assert_eq!(sub["ok"], serde_json::Value::Bool(true), "subscribe reply: {sub}");

    // Close cleanly; the server tears down its side.
    let _ = ws.close(None).await;

    child.kill().expect("kill");
    let _ = child.wait();
}

#[tokio::test]
#[ignore = "spawns the binary; needs a live iceoryx2 runtime; run with --ignored"]
async fn missing_bearer_token_is_rejected() {
    let _guard = DAEMON_LOCK.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let port = 19012;
    let mut child = spawn_daemon(dir.path(), port, Some("secret"));

    // Wait for the listener to bind: a bare connect (no Authorization header)
    // must be *rejected* with a handshake error once the server is up. Retry
    // until we get a definite rejection (not a connection-refused during boot).
    let url = format!("ws://127.0.0.1:{port}/");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut rejected = false;
    while Instant::now() < deadline {
        match connect_async(&url).await {
            Ok(_) => panic!("handshake succeeded without a bearer token"),
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(
                    resp.status(),
                    tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED,
                    "expected 401"
                );
                rejected = true;
                break;
            }
            // Connection refused / io error while the daemon is still booting.
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    assert!(rejected, "server never rejected the unauthenticated handshake");

    child.kill().expect("kill");
    let _ = child.wait();
}

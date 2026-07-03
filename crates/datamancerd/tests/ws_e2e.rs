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

// Fixed ports (19011/19012/19013) can collide with anything else already bound
// on the host — acceptable for these `#[ignore]`d, manually-run tests, which are
// also serialized by `DAEMON_LOCK` so they never collide with each other.

/// Spawn the daemon from a written config file. Returns the child; the caller is
/// responsible for waiting on WS readiness.
fn spawn_daemon(dir: &std::path::Path, port: u16, auth_token: Option<&str>) -> Child {
    spawn_daemon_full(dir, port, auth_token, None)
}

/// As `spawn_daemon`, but also lets the caller pin a tiny `[ws] channel_depth`
/// (used by the overrun test to force slow-consumer backpressure quickly).
fn spawn_daemon_full(
    dir: &std::path::Path,
    port: u16,
    auth_token: Option<&str>,
    channel_depth: Option<usize>,
) -> Child {
    let socket = dir.join("admin.sock");
    let config_path = dir.join("datamancerd.toml");
    let auth_line = match auth_token {
        Some(t) => format!("auth_token = \"{t}\"\n"),
        None => String::new(),
    };
    let depth_line = match channel_depth {
        Some(d) => format!("channel_depth = {d}\n"),
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
{auth_line}{depth_line}"#,
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

/// SIGTERM the daemon while a live WS client is connected and assert the client
/// observes a graceful close (a `Close` frame or a clean end-of-stream) — not a
/// hang. This exercises the daemon-shutdown → per-connection teardown path
/// (`ws_shutdown` → listener drains `JoinSet` → each `handle_connection` breaks
/// its read loop → `session.close()` → writer emits the WS Close). All reads are
/// bounded so a regression FAILS instead of hanging.
#[tokio::test]
#[ignore = "spawns the binary; needs a live iceoryx2 runtime; run with --ignored"]
async fn graceful_shutdown_closes_live_connection() {
    let _guard = DAEMON_LOCK.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let port = 19013;
    let mut child = spawn_daemon(dir.path(), port, None);

    let mut ws = connect_when_ready(port).await;

    // Subscribe so the connection holds a live client session before shutdown.
    ws.send(Message::text(
        r#"{"id":1,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
    ))
    .await
    .expect("send subscribe");
    let sub = read_reply(&mut ws, 1).await;
    assert_eq!(sub["ok"], serde_json::Value::Bool(true), "subscribe reply: {sub}");

    // Graceful termination (NOT SIGKILL): the daemon must tear down live WS
    // connections on its drain path.
    let pid = child.id().cast_signed();
    assert_eq!(
        unsafe { libc::kill(pid, libc::SIGTERM) },
        0,
        "SIGTERM failed"
    );

    // The client must observe a graceful close within the shutdown window:
    // either a `Close` frame or a clean end-of-stream (`None`). A regression that
    // leaves the connection blocked would time out here and FAIL the test.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut saw_close = false;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Close(_))) | None) => {
                saw_close = true;
                break;
            }
            // Interleaved event/reply frames or a benign error on the closing
            // stream: keep reading until Close/EOF or the deadline.
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) => {
                // A transport error as the peer goes away still means the server
                // side closed rather than hanging.
                saw_close = true;
                break;
            }
            Err(_) => break, // per-read timeout; fall through to overall deadline
        }
    }
    assert!(
        saw_close,
        "client never observed a graceful close after SIGTERM (connection hung)"
    );

    // The child must exit within the shutdown window (bounded try_wait poll so a
    // wedged daemon fails rather than hanging the test).
    let exit_deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break status,
            None if Instant::now() < exit_deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            None => {
                child.kill().expect("kill wedged daemon");
                panic!("daemon did not exit within the shutdown window after SIGTERM");
            }
        }
    };
    assert!(status.success(), "daemon exited non-zero after SIGTERM: {status:?}");
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

/// Happy-path auth: with a configured bearer token AND a correct
/// `Authorization: Bearer secret` header, the upgrade succeeds and a `snapshot`
/// request returns `ok`. Complements `missing_bearer_token_is_rejected` (reject
/// path) by covering the accept path end-to-end.
#[tokio::test]
#[ignore = "spawns the binary; needs a live iceoryx2 runtime; run with --ignored"]
async fn correct_bearer_token_is_accepted() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

    let _guard = DAEMON_LOCK.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let port = 19014;
    let mut child = spawn_daemon(dir.path(), port, Some("secret"));

    // Retry the authenticated handshake until the listener is up (connection
    // refused during boot is transient); a definite success ends the loop.
    let url = format!("ws://127.0.0.1:{port}/");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut ws = loop {
        let mut request = url.as_str().into_client_request().expect("build request");
        request
            .headers_mut()
            .insert("authorization", "Bearer secret".parse().expect("header"));
        match connect_async(request).await {
            Ok((ws, _resp)) => break ws,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("authenticated handshake never succeeded: {e}"),
        }
    };

    // snapshot -> reply echoes id 3 with ok=true and a snapshot object.
    ws.send(Message::text(r#"{"id":3,"op":"snapshot"}"#))
        .await
        .expect("send snapshot");
    let snap = read_reply(&mut ws, 3).await;
    assert_eq!(snap["ok"], serde_json::Value::Bool(true), "snapshot reply: {snap}");
    assert!(snap["snapshot"].is_object(), "expected snapshot object");

    let _ = ws.close(None).await;
    child.kill().expect("kill");
    let _ = child.wait();
}

/// Slow-consumer overrun tears the connection down (I-1). With a tiny
/// `channel_depth = 1`, subscribing to an active symbol and then NOT reading
/// fills the bounded outbound channel; the pump's `Rejected` must cancel the
/// read loop, which runs the shared teardown and closes the socket. The client's
/// read side must observe a Close / EOF / transport error within a bounded
/// window — a regression that silently stalls would time out and FAIL.
///
/// MARKET-ACTIVITY DEPENDENCY (read before relying on this test): the daemon's
/// outbound path is `pump -> bounded mpsc(channel_depth) -> writer -> TCP`. To
/// force `Rejected`, the mpsc must stay full, which only happens once the
/// client's TCP receive buffer is also full and back-pressures the writer. We
/// therefore (a) pin `channel_depth = 1` AND (b) connect with a tiny client
/// `SO_RCVBUF` so only a handful of un-read event frames are needed to overrun —
/// but it still requires SOME real Alpaca trades to arrive within the window.
///
/// To avoid a false FAIL on a quiet or light feed, this test first runs a
/// bounded warm-up read phase after subscribing: it reads frames and COUNTS
/// real event frames (an `EventFrame`-tagged object, i.e. one with a `"type"`
/// field such as `"trade"`/`"quote"`/`"bar"` — as opposed to the subscribe's
/// own `WsReply`, which has `"id"`/`"ok"` and no `"type"`). A single lone trade
/// is NOT enough: forcing the depth-1 channel + tiny rcvbuf to overrun needs a
/// SUSTAINED burst, so the warm-up must observe at least
/// `MIN_EVENTS_TO_PROVE_FLOW` events before the overrun assertion is trusted.
/// If fewer arrive within the warm-up window, the feed is too light to force an
/// overrun and this test soft-passes (prints an "inconclusive" message and
/// returns) without asserting teardown — that is a market-data gap, not a code
/// defect. Only once the feed is confirmed actively producing does the test
/// stop reading and assert teardown within the bounded window; a regression
/// that lets the connection stall while events are actively flowing now
/// genuinely FAILS the test. The bounded reads guarantee failure-not-hang
/// either way. Net: this test asserts teardown ONLY when market events are
/// confirmed flowing fast enough to overrun; it soft-passes (inconclusive) on a
/// quiet or light feed, so it never false-fails, and fails only if events were
/// actively flowing AND the connection did not tear down.
#[tokio::test]
#[ignore = "spawns the binary; needs a live iceoryx2 runtime + live market activity; run with --ignored"]
async fn slow_consumer_overrun_tears_down_connection() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

    // A single lone trade does NOT prove the feed is producing fast enough to
    // overrun the depth-1 channel + tiny rcvbuf; require a small sustained burst
    // in warm-up before trusting the teardown assertion (else soft-pass).
    const MIN_EVENTS_TO_PROVE_FLOW: usize = 10;

    let _guard = DAEMON_LOCK.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let port = 19015;
    // Pin the outbound channel to depth 1 so minimal un-drained events overrun.
    let mut child = spawn_daemon_full(dir.path(), port, None, Some(1));

    // Connect with a tiny receive buffer so the client's TCP window fills after
    // only a few un-read frames, back-pressuring the writer and overflowing the
    // depth-1 mpsc quickly. Retry until the listener is up.
    let sock_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut ws = loop {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        let _ = socket.set_recv_buffer_size(2048); // best-effort; kernel clamps to its min
        match socket.connect(sock_addr).await {
            Ok(tcp) => {
                let request = format!("ws://127.0.0.1:{port}/")
                    .into_client_request()
                    .expect("request");
                match tokio_tungstenite::client_async(request, tcp).await {
                    Ok((ws, _resp)) => break ws,
                    Err(_) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => panic!("ws handshake never succeeded: {e}"),
                }
            }
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("tcp connect never succeeded: {e}"),
        }
    };

    // Subscribe to an active paper symbol (same as the subscribe test), then stop
    // reading so events queue and overrun the depth-1 channel.
    ws.send(Message::text(
        r#"{"id":1,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
    ))
    .await
    .expect("send subscribe");
    let sub = read_reply(&mut ws, 1).await;
    assert_eq!(sub["ok"], serde_json::Value::Bool(true), "subscribe reply: {sub}");

    // Bounded warm-up: keep reading (draining, so we don't overrun yet) and COUNT
    // real event frames (an `EventFrame`-tagged object, i.e. one with a `"type"`
    // field). The subscribe reply above (and any other control frame) has
    // `"id"`/`"ok"` and no `"type"`, so it does not count. A single lone trade
    // does NOT prove the feed is producing fast enough to overrun the depth-1
    // channel + tiny rcvbuf, so we require a small SUSTAINED burst before trusting
    // the overrun assertion; a quiet or light feed (fewer events) soft-passes
    // rather than false-failing.
    let warmup_deadline = Instant::now() + Duration::from_secs(15);
    let mut events_seen = 0usize;
    while events_seen < MIN_EVENTS_TO_PROVE_FLOW && Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t)
                    && v.get("type").is_some()
                {
                    events_seen += 1;
                }
            }
            Ok(Some(Ok(_))) => {} // other frame kinds (ping/etc.); keep reading
            // Connection ended during warm-up, or the warm-up window elapsed.
            Ok(Some(Err(_)) | None) | Err(_) => break,
        }
    }
    if events_seen < MIN_EVENTS_TO_PROVE_FLOW {
        eprintln!(
            "inconclusive: only {events_seen} market event(s) in warm-up window \
             (need {MIN_EVENTS_TO_PROVE_FLOW} to force overrun); skipping overrun assertion"
        );
        child.kill().expect("kill");
        let _ = child.wait();
        return;
    }

    // Deliberately stop reading for a beat so the pump fills the depth-1 channel
    // and hits `Rejected`, cancelling the read loop into teardown.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now the server should be tearing us down: within ~15s the read side must
    // see a Close frame, EOF (`None`), or a transport error. Each read is bounded
    // so a regression (silent stall) FAILS here instead of hanging.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut torn_down = false;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            // Close frame, EOF, or a transport error as the peer goes away all
            // mean the server tore the connection down rather than stalling.
            Ok(Some(Ok(Message::Close(_)) | Err(_)) | None) => {
                torn_down = true;
                break;
            }
            Ok(Some(Ok(_))) => {} // queued event/reply frame draining out; keep reading
            Err(_) => break, // per-read timeout; fall through to overall deadline
        }
    }
    assert!(
        torn_down,
        "client never observed teardown after slow-consumer overrun (connection stalled)"
    );

    child.kill().expect("kill");
    let _ = child.wait();
}

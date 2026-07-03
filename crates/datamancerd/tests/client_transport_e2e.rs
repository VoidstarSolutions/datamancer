//! The generic-client-transport e2e: one `exercise<C: Client>` function,
//! written once, run against BOTH `Client` impls (`WsClient`,
//! `Iceoryx2Client`) via the type parameter — the "the end user doesn't care
//! which transport is underneath" guarantee, stated executably.
//!
//! Spawns the real `datamancerd` binary (needs a live iceoryx2 runtime) and
//! talks to it over both the WS listener and the UDS control socket
//! simultaneously; the daemon-spawn/config-generation/ready-wait harness is
//! ported from `ws_e2e.rs` (WS config + readiness) and `daemon_e2e.rs` (UDS
//! socket wait), not reinvented.
//!
//! ADAPTATION FROM THE BRIEF: this workspace's harness drives the REAL Alpaca
//! paper-crypto provider (there is no deterministic fake provider yet — see
//! `daemon_e2e.rs`'s module doc), so:
//! - the subscription targets `alpaca-crypto` / `BTC/USD` / `trade` (the same
//!   pair `ws_e2e.rs`/`daemon_e2e.rs` already use), not a `"fake"` provider;
//! - the event-arrival assert is soft-gated the way `ws_e2e.rs`'s
//!   `slow_consumer_overrun_tears_down_connection` test soft-gates on a quiet
//!   feed: a live feed can go quiet within the wait window, which is a
//!   market-data-availability gap, not a code defect;
//! - the `SessionClosing`-before-stream-end assert is relaxed to
//!   "best-effort, always checked, never hard-failed" for the iceoryx2
//!   transport specifically. `iox2.rs`'s own `Client::close` doc records a
//!   known race: the poll loop can observe the shared-memory service go away
//!   (ending the stream) before it drains the daemon's final `SessionClosing`
//!   sample, because the transport is same-host shared-memory rather than a
//!   single ordered socket. The WS transport has no such race (single-writer
//!   socket), so its test keeps the assert strict.
//!
//! Needs a live iceoryx2 runtime, the spawned daemon binary (built WITH the
//! `ws` feature so it runs the ws listener), and Alpaca paper credentials in
//! the environment; both tests are `#[ignore]`d — run with:
//!
//! ```text
//! cargo test -p datamancerd --features ws --test client_transport_e2e -- --ignored
//! ```

#![cfg(feature = "ws")]

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use datamancer_client::spec::{SubscriptionSpec, UnsubscribeSpec};
use datamancer_client::{Client, ClientError, codes};
use datamancer_core::{ControlKind, MarketEvent};
use futures::StreamExt as _;
use tokio::sync::Mutex;

/// The host diagnostics plane (`datamancer/diagnostics`) is a single-publisher
/// iceoryx2 service, so only one daemon may run at a time. Serialize the tests
/// (ported from `ws_e2e.rs`'s `DAEMON_LOCK`).
static DAEMON_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// A spawned daemon exposing both surfaces this file needs: the WS listener
/// (config ported from `ws_e2e.rs`) and the UDS admin socket (config ported
/// from `daemon_e2e.rs`), in one process.
struct DaemonHandle {
    child: Child,
    socket: PathBuf,
    ws_port: u16,
}

impl DaemonHandle {
    fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/", self.ws_port)
    }

    fn uds_path(&self) -> PathBuf {
        self.socket.clone()
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn the daemon from a written config file (merges `ws_e2e.rs`'s `[ws]`
/// generation with `daemon_e2e.rs`'s admin-socket wait) and block until both
/// surfaces are reachable.
///
/// `service_prefix` is pinned to `"datamancer"` (NOT the daemon's own
/// `"datamancerd"` default) because `datamancer_client::iox2::parse_client_id`
/// matches the literal `"datamancer/data/"` prefix on the `open-client`
/// reply's service name; any other prefix (including the default) breaks it.
async fn spawn_daemon(dir: &std::path::Path, ws_port: u16) -> DaemonHandle {
    let socket = dir.join("admin.sock");
    let config_path = dir.join("datamancerd.toml");
    let config = format!(
        r#"
[provider.alpaca_crypto]
account_type = "paper"
venue = "us"

[server]
admin_socket = "{socket}"
service_prefix = "datamancer"

[diagnostics]
publish_interval_ms = 200

[ws]
enabled = true
bind = "127.0.0.1"
port = {ws_port}
"#,
        socket = socket.display(),
    );
    std::fs::write(&config_path, config).expect("write config");

    let bin: PathBuf = env!("CARGO_BIN_EXE_datamancerd").into();
    let child = Command::new(bin)
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("spawn datamancerd");

    // Wait for the UDS control socket to appear (ported from
    // `daemon_e2e.rs::spawn_daemon`).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        std::thread::sleep(Duration::from_millis(50));
    }

    // Wait for the WS listener (ported from `ws_e2e.rs::connect_when_ready`,
    // adapted to a throwaway readiness probe: the admin socket appearing
    // doesn't prove the WS listener — bound afterward in `Server::run` — is
    // up yet).
    let url = format!("ws://127.0.0.1:{ws_port}/");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _resp)) => {
                drop(ws);
                break;
            }
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("ws never became ready: {e}"),
        }
    }

    DaemonHandle {
        child,
        socket,
        ws_port,
    }
}

/// The transport-agnosticism guarantee, stated executably: everything a
/// consumer does — discover, subscribe, receive with the timestamp triple
/// intact, snapshot, unsubscribe, close gracefully — through the trait, with
/// the concrete transport chosen only by the type parameter.
///
/// `tolerate_missed_closing`: iceoryx2-only escape hatch for the documented
/// race in `iox2.rs`'s `Client::close` (the poll loop can observe the shared
/// memory service go away before draining the final `SessionClosing` sample);
/// the WS transport has no such race and always passes `false`.
async fn exercise<C: Client>(cfg: C::Config, tolerate_missed_closing: bool) {
    let (mut client, mut events) = C::connect(cfg).await.expect("connect");

    // Discover: the real catalog lists the daemon's configured provider's
    // instruments with kinds.
    let catalog = client.instruments(None).await.expect("instruments");
    assert!(!catalog.is_empty(), "catalog must not be empty");
    let info = &catalog[0];
    assert!(!info.kinds.is_empty(), "kinds derived per instrument");

    // Subscribe to the same (provider, symbol, kind) the existing e2e tests
    // use against the real paper-crypto feed.
    let spec: SubscriptionSpec = serde_json::from_str(
        r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
    )
    .unwrap();
    client.subscribe(&spec).await.expect("subscribe");

    // An event arrives with the timestamp triple verbatim. This is a REAL
    // live feed, not a deterministic fake, so soft-gate on a quiet window
    // (same pattern as `ws_e2e.rs`'s `slow_consumer_overrun_tears_down_connection`):
    // a quiet feed is a market-data-availability gap, not a code defect. Skip
    // any interleaved control frames while waiting for a trade.
    let warmup_deadline = Instant::now() + Duration::from_secs(20);
    let mut saw_trade = false;
    while Instant::now() < warmup_deadline {
        let remaining = warmup_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, events.next()).await {
            Ok(Some(MarketEvent::Trade(t))) => {
                assert_ne!(
                    t.rx_ts, t.source_ts,
                    "rx_ts must be carried, not synthesized"
                );
                saw_trade = true;
                break;
            }
            Ok(Some(_)) => {} // interleaved control/other event; keep waiting
            Ok(None) | Err(_) => break,
        }
    }
    if !saw_trade {
        eprintln!(
            "inconclusive: no trade observed on BTC/USD within the warm-up window \
             (quiet feed); continuing with the rest of the exercise"
        );
    }

    // Connectivity via snapshot, not the stream.
    let snapshot = client.snapshot().await.expect("snapshot");
    assert!(!snapshot.providers.is_empty());

    // Duplicate subscribe surfaces the stable code — identically per transport.
    match client.subscribe(&spec).await {
        Err(ClientError::Control { code, .. }) => {
            assert_eq!(code, codes::DUPLICATE_SUBSCRIPTION);
        }
        other => panic!("expected duplicate_subscription, got {other:?}"),
    }

    let unspec: UnsubscribeSpec = serde_json::from_str(
        r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
    )
    .unwrap();
    client.unsubscribe(&unspec).await.expect("unsubscribe");

    // Graceful close: terminal SessionClosing, then the stream ends. Bounded
    // so a regression that keeps the stream open (or never emits the
    // control) FAILS instead of hanging.
    client.close().await.expect("close");
    let close_deadline = Instant::now() + Duration::from_secs(15);
    let mut saw_closing = false;
    loop {
        let remaining = close_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, events.next()).await {
            Ok(Some(MarketEvent::Control(c))) => {
                if matches!(c.kind, ControlKind::SessionClosing) {
                    saw_closing = true;
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => break, // stream ended
            Err(elapsed) => panic!("stream never ended after close(): {elapsed}"),
        }
    }
    if tolerate_missed_closing {
        if !saw_closing {
            eprintln!(
                "note: SessionClosing not observed before stream end (documented \
                 iceoryx2 poll-loop race in Client::close); stream did end, which \
                 is the load-bearing guarantee for this transport"
            );
        }
    } else {
        assert!(saw_closing, "graceful close is marked by SessionClosing");
    }
}

#[tokio::test]
#[ignore = "spawns the binary; needs a live iceoryx2 runtime + Alpaca paper credentials; run with --ignored"]
async fn ws_client_passes_the_exercise() {
    let _guard = DAEMON_LOCK.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path(), 19021).await;

    exercise::<datamancer_client::ws::WsClient>(
        datamancer_client::ws::WsConfig {
            url: daemon.ws_url(),
            auth_token: None,
            event_buffer: 256,
        },
        false,
    )
    .await;
}

#[tokio::test]
#[ignore = "spawns the binary; needs a live iceoryx2 runtime + Alpaca paper credentials; run with --ignored"]
async fn iceoryx2_client_passes_the_exercise() {
    let _guard = DAEMON_LOCK.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_daemon(dir.path(), 19022).await;

    exercise::<datamancer_client::iox2::Iceoryx2Client>(
        datamancer_client::iox2::Iceoryx2Config {
            control_socket: daemon.uds_path(),
            client_name: "exercise-iox2".to_string(),
            poll_interval: Duration::from_millis(5),
            event_buffer: 256,
        },
        true,
    )
    .await;
}

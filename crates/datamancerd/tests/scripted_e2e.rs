//! End-to-end tests against the scripted provider: the backfill seam over
//! the control socket and iceoryx2, the latest-value seed, the refusals for
//! an unowned and a disabled provider, health while trades flow, a removed
//! and a reconfigured provider reaching open sessions, shutdown with a
//! stalled consumer, a third client backfill waiting under
//! `max_concurrent_backfills`, and, with feature `ws`, the backfill seam over
//! WS. The daemon runs with `[provider.scripted]` as its only provider, a
//! credential store in the test's tempdir, and no `ALPACA_*` variables, so
//! the suite needs no network and no credentials.
//!
//! The daemon tests need a live **iceoryx2 runtime** and are `#[ignore]`d;
//! they share the daemon's single-instance lock, so run them one at a time:
//!
//! ```text
//! cargo test -p datamancerd --all-features --test scripted_e2e -- --ignored --test-threads=1
//! ```

#![cfg(feature = "scripted-provider")]
#![cfg(unix)]
#![forbid(unsafe_code)]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::{Conn, Daemon};
use datamancer::{ControlKind, MarketEvent};
use iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};

/// Spawn a hermetic daemon whose only provider is the scripted one, with the
/// given `[provider.scripted]` knobs and further config sections.
fn spawn_scripted_daemon(dir: &Path, scripted: &str, sections: &str) -> Daemon {
    common::spawn_daemon(
        dir,
        common::unique_prefix(),
        &format!("[provider.scripted]\n{scripted}"),
        sections,
        common::hermetic_command,
    )
}

/// The hermetic command pins the credential store beside the config and
/// removes every `ALPACA_*` variable from the daemon's environment.
#[test]
fn a_hermetic_command_pins_the_store_and_scrubs_the_env() {
    use std::ffi::OsStr;

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("datamancerd.toml");
    let command = common::hermetic_command(&config_path);
    let envs: Vec<(&OsStr, Option<&OsStr>)> = command.get_envs().collect();

    let store = dir.path().join("credentials.json");
    assert!(
        envs.contains(&(
            OsStr::new("DATAMANCER_CREDENTIALS_FILE"),
            Some(store.as_os_str())
        )),
        "{envs:?}"
    );
    for name in common::ALPACA_ENV {
        assert!(envs.contains(&(OsStr::new(name), None)), "{name}: {envs:?}");
    }
}

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
#[derive(Debug, Clone, PartialEq)]
enum Seen {
    /// `(price, source_ts, seq)`.
    Trade(f64, i64, u64),
    Gap,
}

impl Seen {
    fn is_gap(&self) -> bool {
        matches!(self, Seen::Gap)
    }
}

/// Poll until `want` data events (gaps count) have been seen or `budget`
/// elapses.
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

    let mut first = Conn::connect(daemon.socket());
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
    let mut second = Conn::connect(daemon.socket());
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
    // Then the live prints, in arrival order, seq continuing after the one
    // slot the provider's `ProviderConnected` takes.
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
        // Slot 5 is the provider's `ProviderConnected`, which the data plane
        // does not carry.
        assert_eq!(*seq, 6 + i as u64, "live seq: {trades:?}");
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
/// at or after the edge, `seq` has no hole but the provider's connect
/// control, and no `Gap` is emitted: no uncovered window between the history
/// and the first live print.
#[test]
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
    let mut conn = Conn::connect(daemon.socket());
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
    // Every live print is at or after the edge, in order; slot 3 is the
    // provider's `ProviderConnected`, which the data plane does not carry.
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
        assert_eq!(*seq, 4 + i as u64, "live seq: {trades:?}");
    }
    drop((subscriber, conn, node));
}

/// `subscribe` pure-live: `latest` seeds the stream with the scripted print
/// ahead of the live tail, and with it off the live tail comes first.
#[test]
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
        let mut conn = Conn::connect(daemon.socket());
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
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn subscribe_seed_false_delivers_the_first_live_print_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = true\nlive_interval_ms = 300\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");

    let mut unseeded = Conn::connect(daemon.socket());
    let mut unseeded_sub = unseeded.open_client("unseeded", &node);
    let reply = unseeded.request(
        r#"{"op":"subscribe","client":"unseeded","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","scope":"live","seed":false}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["scope"], "live", "{reply}");
    assert_eq!(reply["seed"], false, "{reply}");

    let mut joiner = Conn::connect(daemon.socket());
    let _joiner_sub = joiner.open_client("joiner", &node);
    let reply = joiner.request(
        r#"{"op":"subscribe","client":"joiner","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","seed":true}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["joined"], true, "{reply}");
    assert_eq!(reply["seed"], false, "{reply}");

    let seen = collect(&mut unseeded_sub, 1, Duration::from_secs(10));
    assert!(
        matches!(seen.first(), Some(Seen::Trade(price, ..)) if (price - 100.0).abs() < 1e-9),
        "the first event must be the first live print: {seen:?}"
    );

    let mut seeded = Conn::connect(daemon.socket());
    let mut seeded_sub = seeded.open_client("seeded", &node);
    let reply = seeded.request(
        r#"{"op":"subscribe","client":"seeded","provider":"scripted","asset_class":"equity","symbol":"MSFT","kind":"trade","seed":true}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert!(
        reply.get("seed").is_none(),
        "a seeded reply writes no seed: {reply}"
    );
    let seen = collect(&mut seeded_sub, 2, Duration::from_secs(10));
    assert!(seen.len() >= 2, "{seen:?}");
    // The provider's `ProviderConnected` holds a `seq` slot the data plane
    // does not carry, so only the order is pinned.
    let Seen::Trade(seed_price, seed_ts, seed_seq) = seen[0] else {
        panic!("seed first: {seen:?}");
    };
    assert!(
        (seed_price - 999.0).abs() < 1e-9 && seed_ts == 1,
        "seed first: {seen:?}"
    );
    assert!(
        matches!(seen[1], Seen::Trade(price, _, seq) if (price - 100.0).abs() < 1e-9 && seq > seed_seq),
        "then live: {seen:?}"
    );
}

/// A `seed:false` client joining an `always_on` seeding anchor before its seed
/// is delivered gets no seed: its first event is a live print.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_seed_false_joiner_of_a_seeding_anchor_starts_at_a_live_print() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(
        dir.path(),
        "latest = true\nlatest_delay_ms = 5000\nlive_interval_ms = 8000\n",
        "[[startup_session]]\nprovider = \"scripted\"\nasset_class = \"equity\"\nsymbol = \"AAPL\"\nkind = \"trade\"\nalways_on = true\n",
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut subscriber = conn.open_client("unseeded", &node);
    let reply = conn.request(
        r#"{"op":"subscribe","client":"unseeded","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","seed":false}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["joined"], true, "{reply}");
    assert!(reply.get("seed").is_none(), "the anchor seeds: {reply}");

    let seen = collect(&mut subscriber, 1, Duration::from_secs(20));
    assert!(
        matches!(seen.first(), Some(Seen::Trade(price, ..)) if *price >= 100.0 && (price - 999.0).abs() > 1e-9),
        "the first event must be a live print, not the seed: {seen:?}"
    );
}

/// `seed:false` inside `open-client` reaches the session it creates: the
/// opener's first event is a live print, and a joiner is told the session does
/// not seed.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn open_client_subscriptions_carry_seed_false_to_the_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(
        dir.path(),
        "latest = true\nlatest_delay_ms = 1000\nlive_interval_ms = 3000\n",
        "",
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");

    let mut opener = Conn::connect(daemon.socket());
    let reply = opener.request(
        r#"{"op":"open-client","client":"opener","subscriptions":[{"provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","seed":false}]}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    let service: datamancer::transport::DataServiceName = reply["service"]
        .as_str()
        .expect("service name")
        .parse()
        .expect("a data-service name");
    let mut opener_sub =
        datamancer::transport::DataSubscriber::open(&node, &service).expect("subscriber");
    let seen = collect(&mut opener_sub, 1, Duration::from_secs(10));
    assert!(
        matches!(seen.first(), Some(Seen::Trade(price, ..)) if *price >= 100.0 && (price - 999.0).abs() > 1e-9),
        "the opener's first event must be a live print, not the seed: {seen:?}"
    );

    let mut joiner = Conn::connect(daemon.socket());
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
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn subscribe_refuses_an_unowned_id_and_a_disabled_provider() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
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

/// A `live_backfill` start older than `[session] max_backfill_days` is refused
/// on the wire as `backfill_too_old`.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn subscribe_refuses_a_backfill_older_than_the_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let _subscriber = conn.open_client("aged", &node);

    let reply = conn.request(
        r#"{"op":"subscribe","client":"aged","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","scope":"live_backfill","backfill_from":1}"#,
    );
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(reply["code"], "backfill_too_old", "{reply}");
}

/// The next trade on `events`, skipping controls.
#[cfg(feature = "ws")]
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
#[cfg(feature = "ws")]
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
/// backfill it opened, then its history and a live print under one `seq` whose
/// only hole is the provider's connect control; a pure-live joiner reads the
/// session's scope and receives trades.
#[tokio::test]
#[cfg(feature = "ws")]
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
    let backfill = LiveScope::backfill(from);

    let (mut creator, mut events) = connect_ws(PORT).await;
    let created = creator
        .subscribe(&spec.clone().with_backfill_from(from))
        .await
        .expect("subscribe");
    assert_eq!(created, Subscribed::created(backfill));

    let (mut joiner, mut joiner_events) = connect_ws(PORT).await;
    assert_eq!(
        joiner.subscribe(&spec).await.expect("join"),
        Subscribed::joined(backfill).with_backfill_in_flight()
    );

    // Slot 3 is the provider's `ProviderConnected`, which WS does not carry.
    for (seq, price) in [(0, 1.0), (1, 2.0), (2, 3.0), (4, 100.0)] {
        let trade = next_trade(&mut events).await;
        assert!((trade.price.to_f64() - price).abs() < 1e-9, "{trade:?}");
        assert_eq!(trade.seq.0, seq, "{trade:?}");
    }
    next_trade(&mut joiner_events).await;
}

/// Subscribe `client` pure-live to scripted `AAPL` trades on `conn`.
fn subscribe_aapl(conn: &mut Conn, client: &str) {
    let reply = conn.request(&format!(
        r#"{{"op":"subscribe","client":"{client}","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade"}}"#
    ));
    assert_eq!(reply["ok"], true, "subscribe rejected: {reply}");
}

/// The scripted provider's entry in a `health` reply.
fn scripted_health(conn: &mut Conn) -> serde_json::Value {
    let reply = conn.request(r#"{"op":"health"}"#);
    assert_eq!(reply["ok"], true, "health rejected: {reply}");
    reply["health"]["providers"]
        .as_array()
        .expect("providers")
        .iter()
        .find(|p| p["provider"] == "scripted")
        .cloned()
        .unwrap_or_else(|| panic!("scripted is not enumerated: {reply}"))
}

/// The `connection` of the one authoritative session in a `snapshot` reply.
fn session_connection(conn: &mut Conn) -> serde_json::Value {
    let reply = conn.request(r#"{"op":"snapshot"}"#);
    assert_eq!(reply["ok"], true, "snapshot rejected: {reply}");
    let sessions = reply["snapshot"]["authoritative_sessions"]
        .as_array()
        .expect("authoritative_sessions");
    assert_eq!(sessions.len(), 1, "{reply}");
    sessions[0]["connection"].clone()
}

/// Trades seen within `window`.
fn trades_within(
    subscriber: &mut datamancer::transport::DataSubscriber,
    window: Duration,
) -> usize {
    collect(subscriber, usize::MAX, window)
        .iter()
        .filter(|s| !s.is_gap())
        .count()
}

/// While scripted trades flow, `health` reads the provider `connected` and
/// the session's connection is up.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn health_reads_connected_while_scripted_trades_flow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = false\nlive_interval_ms = 20\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut subscriber = conn.open_client("health", &node);
    subscribe_aapl(&mut conn, "health");

    let seen = collect(&mut subscriber, 3, Duration::from_secs(10));
    assert!(seen.len() >= 3, "trades never flowed: {seen:?}");
    assert_eq!(scripted_health(&mut conn)["state"], "connected");
    assert!(
        session_connection(&mut conn).get("up").is_some(),
        "the session is not up"
    );
}

/// `remove-provider scripted` stops an open session's trades, with the
/// session down on a `ProviderDisconnected` whose cause is `disabled`.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn remove_provider_stops_open_sessions_with_provider_disconnected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = false\nlive_interval_ms = 20\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut subscriber = conn.open_client("removed", &node);
    subscribe_aapl(&mut conn, "removed");
    let seen = collect(&mut subscriber, 3, Duration::from_secs(10));
    assert!(seen.len() >= 3, "trades never flowed: {seen:?}");

    let reply = conn.request(r#"{"op":"remove-provider","provider":"scripted"}"#);
    assert_eq!(reply["ok"], true, "remove-provider rejected: {reply}");
    // Drain what was in flight, then expect silence.
    trades_within(&mut subscriber, Duration::from_millis(300));
    let after = trades_within(&mut subscriber, Duration::from_millis(500));
    assert_eq!(after, 0, "trades still flow after remove-provider");

    let connection = session_connection(&mut conn);
    assert_eq!(connection["down"]["cause"], "disabled", "{connection}");
    assert_eq!(scripted_health(&mut conn)["state"], "disabled");
}

/// `configure-provider scripted` after `remove-provider scripted` resumes an
/// open session's trades, and `health` reads the provider connected again.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn configure_after_remove_resumes_open_sessions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = false\nlive_interval_ms = 20\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut subscriber = conn.open_client("resumed", &node);
    subscribe_aapl(&mut conn, "resumed");
    let seen = collect(&mut subscriber, 3, Duration::from_secs(10));
    assert!(seen.len() >= 3, "trades never flowed: {seen:?}");

    let reply = conn.request(r#"{"op":"remove-provider","provider":"scripted"}"#);
    assert_eq!(reply["ok"], true, "remove-provider rejected: {reply}");
    trades_within(&mut subscriber, Duration::from_millis(300));
    assert_eq!(scripted_health(&mut conn)["state"], "disabled");

    let reply = conn.request(
        r#"{"op":"configure-provider","provider":"scripted","settings":{"latest":false,"live_interval_ms":20}}"#,
    );
    assert_eq!(reply["ok"], true, "configure-provider rejected: {reply}");
    let resumed = trades_within(&mut subscriber, Duration::from_secs(1));
    assert!(
        resumed >= 10,
        "a 20 ms interval should resume many trades in 1 s, got {resumed}"
    );
    assert_eq!(scripted_health(&mut conn)["state"], "connected");
    assert!(
        session_connection(&mut conn).get("up").is_some(),
        "the session is not up"
    );
}

/// `configure-provider scripted` with a new `live_interval_ms` reaches a
/// session that is already open.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn configure_provider_reaches_an_open_sessions_live_interval() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = false\nlive_interval_ms = 2000\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut subscriber = conn.open_client("reconfigured", &node);
    subscribe_aapl(&mut conn, "reconfigured");
    // Let the subscribe land before the change.
    std::thread::sleep(Duration::from_millis(300));

    let reply = conn.request(
        r#"{"op":"configure-provider","provider":"scripted","settings":{"latest":false,"live_interval_ms":20}}"#,
    );
    assert_eq!(reply["ok"], true, "configure-provider rejected: {reply}");
    let seen = trades_within(&mut subscriber, Duration::from_secs(1));
    assert!(
        seen >= 10,
        "a 20 ms interval should give many trades in 1 s, got {seen}"
    );
}

/// A client whose subscriber holds its port and stops reading does not keep
/// the daemon from exiting on `shutdown`: `shutdown_timeout_secs` (2 s here)
/// bounds the whole exit, so it is gone within 4 s.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn shutdown_exits_with_a_stalled_client_consumer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut daemon = common::spawn_daemon_with_server(
        dir.path(),
        common::unique_prefix(),
        "[provider.scripted]\nlatest = false\nlive_interval_ms = 1\n",
        "shutdown_timeout_secs = 2",
        "",
        common::hermetic_command,
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let _stalled = conn.open_client("stalled", &node);
    subscribe_aapl(&mut conn, "stalled");
    // Never poll: let every buffer between the provider and the port fill.
    std::thread::sleep(Duration::from_secs(3));

    assert!(
        daemon.shutdown_within(Duration::from_secs(4)),
        "the daemon did not exit within 4 s of shutdown"
    );
}

/// A client whose subscriber holds its port and stops reading leaves no pump
/// behind once it is closed: nothing publishes on its data service within 2 s
/// of the `close-client` reply, round after round.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_closed_client_with_a_stalled_consumer_leaves_no_pump() {
    use iceoryx2::service::port_factory::PortFactory as _;

    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = false\nlive_interval_ms = 1\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    for round in 0..3 {
        let client = format!("stalled-{round}");
        let reply = conn.request(&format!(r#"{{"op":"open-client","client":"{client}"}}"#));
        assert_eq!(reply["ok"], true, "open-client rejected: {reply}");
        let service = reply["service"].as_str().expect("service name").to_string();
        let name: datamancer::transport::DataServiceName =
            service.parse().expect("a data-service name");
        let _stalled =
            datamancer::transport::DataSubscriber::open(&node, &name).expect("subscriber");
        subscribe_aapl(&mut conn, &client);
        // Never poll: let the queue fill so the pump waits on it.
        std::thread::sleep(Duration::from_secs(1));

        let service_name: iceoryx2::prelude::ServiceName =
            service.as_str().try_into().expect("service name");
        let data = node
            .service_builder(&service_name)
            .publish_subscribe::<datamancer::transport::DataPayload>()
            .open()
            .expect("data service");
        let publishers = || data.dynamic_config().number_of_publishers();
        assert_eq!(publishers(), 1, "round {round}: the pump publishes");

        let reply = conn.request(&format!(r#"{{"op":"close-client","client":"{client}"}}"#));
        assert_eq!(reply["ok"], true, "close-client rejected: {reply}");
        let deadline = Instant::now() + Duration::from_secs(2);
        while publishers() > 0 {
            assert!(
                Instant::now() < deadline,
                "round {round}: a publisher outlived its closed client"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// With `[server] max_concurrent_backfills = 2`, a third client backfill is not
/// refused: it waits for one of the first two histories to end, then fetches
/// its own.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_third_backfill_waits_for_one_of_two_to_finish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = common::spawn_daemon_with_server(
        dir.path(),
        common::unique_prefix(),
        "[provider.scripted]\nhistory_prints = 1\nhistory_delay_ms = 1500\nlive_interval_ms = 1000\n",
        "max_concurrent_backfills = 2",
        "",
        common::hermetic_command,
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut subscriber = conn.open_client("capped", &node);
    let from = now_nanos() - 60_000_000_000;
    let started = Instant::now();
    for symbol in ["AAPL", "MSFT", "NVDA"] {
        let reply = conn.request(&format!(
            r#"{{"op":"subscribe","client":"capped","provider":"scripted","asset_class":"equity","symbol":"{symbol}","kind":"trade","scope":"live_backfill","backfill_from":{from}}}"#
        ));
        assert_eq!(reply["ok"], true, "{symbol} refused: {reply}");
    }

    // Each history is one print priced 1.0; the live prints start at 100.
    let mut history_at = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while history_at.len() < 3 && Instant::now() < deadline {
        for event in subscriber.poll().expect("poll") {
            if let MarketEvent::Trade(t) = event
                && t.price.to_f64() < 100.0
            {
                history_at.push(started.elapsed());
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(history_at.len(), 3, "histories seen at {history_at:?}");
    assert!(
        history_at[1].saturating_sub(history_at[0]) < Duration::from_secs(1),
        "two fetch at once: {history_at:?}"
    );
    assert!(
        history_at[2] >= Duration::from_millis(2900),
        "the third waits for a turn: {history_at:?}"
    );
    drop((subscriber, conn, node));
}

//! End-to-end tests against the scripted provider: the backfill seam over
//! the control socket and iceoryx2, the latest-value seed, the refusals for
//! an unowned and a disabled provider, health while trades flow, a removed
//! and a reconfigured provider reaching open sessions, shutdown with a
//! stalled consumer, a third client backfill waiting under
//! `max_concurrent_backfills`, a stalled query's slot and the daemon
//! answering behind it, the daemon answering while a subscribe waits for a
//! cold provider, and, with feature `ws`, the backfill seam over WS. The
//! daemon runs with `[provider.scripted]` as its only provider, a credential
//! store in the test's tempdir, and no `ALPACA_*` variables, so the suite
//! needs no network and no credentials.
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
use datamancer::transport::{DataServiceName, DataSubscriber, TransportError};
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

/// `open-query` for `symbol`'s trades over a fixed window.
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

/// Poll `list-queries` until `id` is gone, within 30 s.
fn wait_until_reaped(daemon: &Daemon, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let listed = common::round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
        if !listed["queries"]
            .as_array()
            .expect("queries array")
            .contains(&serde_json::json!(id))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "query {id} never self-reaped: {listed}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A scripted daemon for the query tests: one query slot, a memory cache and
/// no fetch delay.
fn spawn_query_daemon(dir: &Path, prints: u32) -> Daemon {
    spawn_scripted_daemon(
        dir,
        &format!("history_delay_ms = 0\nhistory_prints = {prints}\n"),
        "[iceoryx2]\nmax_queries = 1\n\n[cache]\nbackend = \"memory\"\n",
    )
}

/// The scripted provider's upstream history fetches so far.
fn scripted_history_fetches(daemon: &Daemon) -> u64 {
    let reply = common::round_trip(daemon.socket(), r#"{"op":"snapshot"}"#);
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
fn assert_a_cached_query_survives_a_late_attach(prints: u32) {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_query_daemon(dir.path(), prints);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut conn = Conn::connect(daemon.socket());
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
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_cached_query_is_held_for_a_late_attach() {
    assert_a_cached_query_survives_a_late_attach(100);
}

/// Few enough trades that the whole query fits in one burst.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_short_cached_query_is_held_for_a_late_attach() {
    assert_a_cached_query_survives_a_late_attach(3);
}

/// A query nobody attaches to holds its slot for the grace alone: the cap
/// refuses a second query meanwhile, the first is then reaped on its own with
/// its service gone, so a late attach fails, and the slot serves a new query.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_query_nobody_attaches_to_releases_its_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_query_daemon(dir.path(), 3);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut conn = Conn::connect(daemon.socket());

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
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn shutdown_does_not_wait_for_a_query_to_be_attached() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut daemon = spawn_query_daemon(dir.path(), 3);
    let mut conn = Conn::connect(daemon.socket());
    let (id, _) = open_trade_query(&mut conn, "AAPL");
    std::thread::sleep(Duration::from_millis(300));
    let listed = common::round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    assert!(
        listed["queries"]
            .as_array()
            .expect("queries array")
            .contains(&serde_json::json!(id)),
        "the query must be waiting for its client: {listed}"
    );

    assert!(
        daemon.shutdown_within(Duration::from_secs(4)),
        "the daemon is still up"
    );
}

/// More trades than a query service's history and its subscriber's buffer
/// hold, so a subscriber that holds its port and does not read stalls the
/// query's pump in its send.
const STALL_PRINTS: u32 = 500;

/// A scripted daemon for the stalled-query tests: `max_queries` query slots,
/// a memory cache, [`STALL_PRINTS`] trades per query and a 2 s
/// `shutdown_timeout_secs`.
fn spawn_stall_daemon(dir: &Path, max_queries: usize) -> Daemon {
    common::spawn_daemon_with_server(
        dir,
        common::unique_prefix(),
        &format!("[provider.scripted]\nhistory_delay_ms = 0\nhistory_prints = {STALL_PRINTS}\n"),
        "shutdown_timeout_secs = 2",
        &format!("[iceoryx2]\nmax_queries = {max_queries}\n\n[cache]\nbackend = \"memory\"\n"),
        common::hermetic_command,
    )
}

/// One request on a connection of its own; `None` if no reply arrives within
/// `timeout`.
fn request_within(socket: &Path, request: &str, timeout: Duration) -> Option<serde_json::Value> {
    use std::io::{BufRead, BufReader, Write};

    let mut stream = std::os::unix::net::UnixStream::connect(socket).expect("connect socket");
    stream
        .set_read_timeout(Some(timeout))
        .expect("read timeout");
    stream.write_all(request.as_bytes()).expect("write");
    stream.write_all(b"\n").expect("write nl");
    let mut reply = String::new();
    match BufReader::new(stream).read_line(&mut reply) {
        Ok(n) if n > 0 => Some(serde_json::from_str(&reply).expect("parse reply")),
        _ => None,
    }
}

/// Whether `list-queries` names `id`.
fn is_listed(daemon: &Daemon, id: u64) -> bool {
    let listed = common::round_trip(daemon.socket(), r#"{"op":"list-queries"}"#);
    listed["queries"]
        .as_array()
        .expect("queries array")
        .contains(&serde_json::json!(id))
}

/// A live count of the publishers on `service`.
fn publisher_count(
    node: &iceoryx2::node::Node<ipc_threadsafe::Service>,
    service: &DataServiceName,
) -> impl Fn() -> usize {
    let name: iceoryx2::prelude::ServiceName = service
        .to_string()
        .as_str()
        .try_into()
        .expect("service name");
    let data = node
        .service_builder(&name)
        .publish_subscribe::<datamancer::transport::DataPayload>()
        .open()
        .expect("data service");
    move || {
        use iceoryx2::service::port_factory::PortFactory as _;
        data.dynamic_config().number_of_publishers()
    }
}

/// Waits up to 5 s for `publishers` to reach zero.
fn assert_no_publisher_soon(publishers: impl Fn() -> usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while publishers() > 0 {
        assert!(
            Instant::now() < deadline,
            "the query's pump still holds its publisher"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A query whose subscriber holds its port and stops reading frees its
/// `max_queries` slot on `cancel-query`: the cancel answers `ok` at once, the
/// query leaves `list-queries`, a second cancel answers `unknown_query`, and
/// a new query opens and is served in full while the stalled subscriber
/// still holds its port.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_cancelled_stalled_query_frees_its_slot_at_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_stall_daemon(dir.path(), 1);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut conn = Conn::connect(daemon.socket());

    let (id, service) = open_trade_query(&mut conn, "AAPL");
    let _stalled = DataSubscriber::open(&node, &service).expect("subscriber");
    let publishers = publisher_count(&node, &service);
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        is_listed(&daemon, id),
        "the stalled query must be in flight"
    );
    assert_eq!(publishers(), 1, "the pump publishes");

    let cancel = format!(r#"{{"op":"cancel-query","query":{id}}}"#);
    let cancelled = request_within(daemon.socket(), &cancel, Duration::from_secs(2))
        .expect("cancel-query did not answer");
    assert_eq!(cancelled["ok"], true, "{cancelled}");
    assert!(!is_listed(&daemon, id), "a cancelled query is still listed");
    let again = conn.request(&cancel);
    assert_eq!(again["code"], "unknown_query", "{again}");
    assert_no_publisher_soon(publishers);

    let reply = conn.request(
        r#"{"op":"open-query","provider":"scripted","asset_class":"equity","symbol":"MSFT","kind":"trade","from":1000000000,"to":2000000000}"#,
    );
    assert_eq!(
        reply["ok"], true,
        "the cancelled query kept its slot: {reply}"
    );
    let service: DataServiceName = reply["service"]
        .as_str()
        .expect("service name")
        .parse()
        .expect("data-service name");
    let mut subscriber = DataSubscriber::open(&node, &service).expect("subscriber");
    assert_eq!(
        drain_trade_query(&mut subscriber).len(),
        STALL_PRINTS as usize
    );
}

/// A query whose subscriber holds its port and stops reading frees its
/// `max_queries` slot when its connection ends: the query leaves
/// `list-queries` and a new one opens.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_stalled_query_frees_its_slot_when_its_connection_ends() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_stall_daemon(dir.path(), 1);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut opener = Conn::connect(daemon.socket());
    let (id, service) = open_trade_query(&mut opener, "AAPL");
    let _stalled = DataSubscriber::open(&node, &service).expect("subscriber");
    let publishers = publisher_count(&node, &service);
    // Nothing reports a pump parked on the full queue; this outlasts filling it.
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        is_listed(&daemon, id),
        "the stalled query must be in flight"
    );
    assert_eq!(publishers(), 1, "the pump publishes");

    drop(opener);
    let deadline = Instant::now() + Duration::from_secs(5);
    while is_listed(&daemon, id) {
        assert!(
            Instant::now() < deadline,
            "a stalled query kept its slot after its connection ended"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_no_publisher_soon(publishers);

    let mut conn = Conn::connect(daemon.socket());
    let reply = conn.request(
        r#"{"op":"open-query","provider":"scripted","asset_class":"equity","symbol":"MSFT","kind":"trade","from":1000000000,"to":2000000000}"#,
    );
    assert_eq!(reply["ok"], true, "the ended query kept its slot: {reply}");
}

/// With every `max_queries` slot held by a query whose subscriber holds its
/// port and does not read, the daemon still answers `ping`. The queries number
/// the test machine's parallelism plus one, so they outnumber the daemon
/// runtime's workers even if it has fewer than this machine reports.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn ping_answers_with_every_query_stalled() {
    let workers = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    let dir = tempfile::tempdir().expect("tempdir");
    let stalled_queries = workers + 1;
    let daemon = spawn_stall_daemon(dir.path(), stalled_queries);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut conn = Conn::connect(daemon.socket());

    let mut stalled = Vec::new();
    for i in 0..stalled_queries {
        let (_, service) = open_trade_query(&mut conn, &format!("Q{i}"));
        stalled.push(DataSubscriber::open(&node, &service).expect("subscriber"));
    }
    std::thread::sleep(Duration::from_secs(1));

    let ping = request_within(daemon.socket(), r#"{"op":"ping"}"#, Duration::from_secs(5))
        .expect("ping did not answer with every query stalled");
    assert_eq!(ping["ok"], true, "{ping}");
}

/// A query whose subscriber holds its port and stops reading does not keep
/// the daemon from exiting on `shutdown`: `shutdown_timeout_secs` (2 s here)
/// bounds the whole exit, so it is gone within 4 s.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn shutdown_exits_with_a_stalled_query_consumer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut daemon = spawn_stall_daemon(dir.path(), 1);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("node");
    let mut conn = Conn::connect(daemon.socket());
    let (id, service) = open_trade_query(&mut conn, "AAPL");
    let _stalled = DataSubscriber::open(&node, &service).expect("subscriber");
    std::thread::sleep(Duration::from_millis(500));
    assert!(is_listed(&daemon, id), "the query must still be in flight");

    assert!(
        daemon.shutdown_within(Duration::from_secs(4)),
        "the daemon did not exit within 4 s of shutdown"
    );
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
/// behind once it is closed: `close-client` answers at once, and nothing
/// publishes on its data service within 5 s of the reply, round after round.
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

        let started = Instant::now();
        let reply = conn.request(&format!(r#"{{"op":"close-client","client":"{client}"}}"#));
        let took = started.elapsed();
        assert_eq!(reply["ok"], true, "close-client rejected: {reply}");
        assert!(
            took < Duration::from_secs(2),
            "round {round}: close-client answered after {took:?}"
        );
        // The close gives the stalled reader its 2 s, then aborts the pump.
        let deadline = Instant::now() + Duration::from_secs(5);
        while publishers() > 0 {
            assert!(
                Instant::now() < deadline,
                "round {round}: a publisher outlived its closed client"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// How long the scripted provider takes to connect in the cold-subscribe
/// tests.
const COLD_CONNECT: Duration = Duration::from_secs(3);

/// A scripted daemon whose every subscribe waits [`COLD_CONNECT`] for the
/// provider.
fn spawn_cold_daemon(dir: &Path) -> Daemon {
    spawn_scripted_daemon(dir, "connect_delay_ms = 3000\n", "")
}

/// A `subscribe` of `client` to scripted `symbol` trades.
fn subscribe_line(client: &str, symbol: &str) -> String {
    format!(
        r#"{{"op":"subscribe","client":"{client}","provider":"scripted","asset_class":"equity","symbol":"{symbol}","kind":"trade"}}"#
    )
}

/// Send `request` on `conn` from a thread of its own; the thread returns the
/// reply, or `None` if the connection closed first, and how long it took.
fn request_in_background(
    conn: Conn,
    request: String,
) -> std::thread::JoinHandle<(Option<serde_json::Value>, Duration)> {
    std::thread::spawn(move || {
        let mut conn = conn;
        let started = Instant::now();
        let reply = conn.try_request(&request);
        (reply, started.elapsed())
    })
}

/// Wait until the scripted provider has been asked for `count` subscribes,
/// which each happen before the provider's connect. Every read is a `snapshot`
/// of its own that must answer within a second, so a daemon that stops
/// answering fails here.
fn wait_for_subscribes(socket: &Path, count: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // A snapshot the daemon is slow to answer counts as "not yet".
        if let Some(reply) = request_within(socket, r#"{"op":"snapshot"}"#, Duration::from_secs(1))
        {
            let subscribes = reply["snapshot"]["providers"]
                .as_array()
                .expect("providers")
                .iter()
                .find(|p| p["provider"] == "scripted")
                .unwrap_or_else(|| panic!("scripted is not enumerated: {reply}"))["subscribes"]
                .as_u64()
                .expect("subscribes");
            if subscribes >= count {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{count} subscribes never reached the provider"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Poll `subscriber` until a `SessionClosing` arrives, or the service closes
/// or `budget` elapses first. Returns whether it arrived.
fn saw_session_closing(subscriber: &mut DataSubscriber, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        match subscriber.poll() {
            Ok(events) => {
                if events.iter().any(|event| {
                    matches!(event, MarketEvent::Control(c) if matches!(c.kind, ControlKind::SessionClosing))
                }) {
                    return true;
                }
            }
            Err(TransportError::ServiceClosed) => return false,
            Err(e) => panic!("poll: {e:?}"),
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

/// `ping` answers while another client's first subscribe waits for a cold
/// provider, and that subscribe still answers only once the provider has the
/// pair.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn ping_answers_during_a_cold_subscribe() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let _subscriber = conn.open_client("cold", &node);
    let subscribe = request_in_background(conn, subscribe_line("cold", "AAPL"));
    wait_for_subscribes(daemon.socket(), 1);

    let ping = request_within(daemon.socket(), r#"{"op":"ping"}"#, Duration::from_secs(1))
        .expect("ping did not answer during a cold subscribe");
    assert_eq!(ping["ok"], true, "{ping}");

    let (reply, took) = subscribe.join().expect("subscribe thread");
    let reply = reply.expect("subscribe reply");
    assert_eq!(reply["ok"], true, "{reply}");
    assert!(
        took >= COLD_CONNECT,
        "the reply must wait for the provider: {took:?}"
    );
}

/// A second client's subscribe to another pair, sent while the first
/// client's subscribe waits for a cold provider, waits for its own connect
/// only.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn another_clients_subscribe_is_not_held_by_a_cold_subscribe() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut first = Conn::connect(daemon.socket());
    let _first_subscriber = first.open_client("first", &node);
    let mut second = Conn::connect(daemon.socket());
    let _second_subscriber = second.open_client("second", &node);

    let held = request_in_background(first, subscribe_line("first", "AAPL"));
    wait_for_subscribes(daemon.socket(), 1);
    let started = Instant::now();
    let reply = second.request(&subscribe_line("second", "MSFT"));
    let took = started.elapsed();
    assert_eq!(reply["ok"], true, "{reply}");
    assert!(
        took < COLD_CONNECT + Duration::from_secs(2),
        "the second subscribe waited behind the first: {took:?}"
    );

    let (reply, _) = held.join().expect("subscribe thread");
    assert_eq!(reply.expect("first reply")["ok"], true);
}

/// `open-client` with an initial subscription to a cold provider answers once
/// the provider has the pair, and `ping` answers meanwhile.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn open_client_with_a_cold_subscription_leaves_the_actor_answering() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let conn = Conn::connect(daemon.socket());
    let open = request_in_background(
        conn,
        r#"{"op":"open-client","client":"cold","subscriptions":[{"provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade"}]}"#
            .to_string(),
    );
    wait_for_subscribes(daemon.socket(), 1);

    let ping = request_within(daemon.socket(), r#"{"op":"ping"}"#, Duration::from_secs(1))
        .expect("ping did not answer during a cold open-client");
    assert_eq!(ping["ok"], true, "{ping}");

    let (reply, took) = open.join().expect("open-client thread");
    let reply = reply.expect("open-client reply");
    assert_eq!(reply["ok"], true, "{reply}");
    assert!(reply["service"].is_string(), "{reply}");
    assert!(
        took >= COLD_CONNECT,
        "the reply must wait for the provider: {took:?}"
    );
}

/// An `open-client` whose second initial subscription is refused answers
/// that refusal and leaves no client: `list-clients` does not name it and the
/// name opens again.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_refused_initial_subscription_leaves_no_client() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());

    let reply = conn.request(
        r#"{"op":"open-client","client":"refused","subscriptions":[{"provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade"},{"provider":"nobody","asset_class":"equity","symbol":"AAPL","kind":"trade"}]}"#,
    );
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(reply["code"], "unknown_provider", "{reply}");

    let listed = conn.request(r#"{"op":"list-clients"}"#);
    assert_eq!(listed["clients"], serde_json::json!([]), "{listed}");
    let _subscriber = conn.open_client("refused", &node);
}

/// `shutdown` while a subscribe waits for a cold provider: the daemon exits
/// in bounded time, and the subscribe answers `ok`, `session_closed` or
/// `shutting_down`, or its connection closes with the daemon.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn shutdown_exits_with_a_subscribe_in_flight() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let _subscriber = conn.open_client("cold", &node);
    let subscribe = request_in_background(conn, subscribe_line("cold", "AAPL"));
    wait_for_subscribes(daemon.socket(), 1);

    assert!(
        daemon.shutdown_within(Duration::from_secs(8)),
        "the daemon did not exit within 8 s of shutdown"
    );
    let (reply, _) = subscribe.join().expect("subscribe thread");
    if let Some(reply) = reply {
        assert!(
            reply["ok"] == true
                || reply["code"] == "session_closed"
                || reply["code"] == "shutting_down",
            "{reply}"
        );
    }
}

/// `close-client` while that client's subscribe waits for a cold provider
/// answers at once, and the subscribe answers `session_closed`.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_client_closed_during_its_subscribe_reports_session_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let _subscriber = conn.open_client("closed", &node);
    let subscribe = request_in_background(conn, subscribe_line("closed", "AAPL"));
    wait_for_subscribes(daemon.socket(), 1);

    let closed = request_within(
        daemon.socket(),
        r#"{"op":"close-client","client":"closed"}"#,
        Duration::from_secs(1),
    )
    .expect("close-client did not answer during the subscribe");
    assert_eq!(closed["ok"], true, "{closed}");

    let (reply, _) = subscribe.join().expect("subscribe thread");
    let reply = reply.expect("subscribe reply");
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(reply["code"], "session_closed", "{reply}");
}

/// An `unsubscribe` waiting behind a cold subscribe of a client is answered
/// `session_closed` when that client is closed.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_client_closed_behind_a_queued_unsubscribe_reports_session_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let _subscriber = conn.open_client("x", &node);
    let _subscribe = request_in_background(conn, subscribe_line("x", "AAPL"));
    wait_for_subscribes(daemon.socket(), 1);
    let unsubscribe = request_in_background(
        Conn::connect(daemon.socket()),
        r#"{"op":"unsubscribe","client":"x","provider":"scripted","asset_class":"equity","symbol":"MSFT","kind":"trade"}"#
            .to_string(),
    );
    // Nothing reports an unsubscribe queued behind the subscribe.
    std::thread::sleep(Duration::from_millis(300));

    let closed = request_within(
        daemon.socket(),
        r#"{"op":"close-client","client":"x"}"#,
        Duration::from_secs(1),
    )
    .expect("close-client did not answer during the unsubscribe");
    assert_eq!(closed["ok"], true, "{closed}");

    let (reply, _) = unsubscribe.join().expect("unsubscribe thread");
    let reply = reply.expect("unsubscribe reply");
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(reply["code"], "session_closed", "{reply}");
}

/// `ping` answers while one connection's `unsubscribe` of a client waits
/// behind another connection's cold subscribe of that client.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn ping_answers_while_an_unsubscribe_waits_behind_a_cold_subscribe() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let _subscriber = conn.open_client("x", &node);
    let subscribe = request_in_background(conn, subscribe_line("x", "AAPL"));
    wait_for_subscribes(daemon.socket(), 1);
    let unsubscribe = request_in_background(
        Conn::connect(daemon.socket()),
        r#"{"op":"unsubscribe","client":"x","provider":"scripted","asset_class":"equity","symbol":"MSFT","kind":"trade"}"#
            .to_string(),
    );
    std::thread::sleep(Duration::from_millis(300));

    let ping = request_within(daemon.socket(), r#"{"op":"ping"}"#, Duration::from_secs(1))
        .expect("ping did not answer behind a waiting unsubscribe");
    assert_eq!(ping["ok"], true, "{ping}");

    let (reply, _) = subscribe.join().expect("subscribe thread");
    assert_eq!(reply.expect("subscribe reply")["ok"], true);
    let (reply, took) = unsubscribe.join().expect("unsubscribe thread");
    let reply = reply.expect("unsubscribe reply");
    assert_eq!(reply["code"], "not_subscribed", "{reply}");
    assert!(
        took >= Duration::from_secs(1),
        "the unsubscribe must wait behind the subscribe: {took:?}"
    );
}

/// [`common::hermetic_command`] with the daemon's log, at `debug`, written to
/// `daemon.log` beside the config.
fn logged_command(config_path: &Path) -> std::process::Command {
    let log = std::fs::File::create(config_path.with_file_name("daemon.log")).expect("log file");
    let mut command = common::hermetic_command(config_path);
    command
        .env("RUST_LOG", "datamancerd=debug")
        .env("NO_COLOR", "1")
        .stdout(log);
    command
}

/// `shutdown` while a subscribe waits on a provider whose connect now takes a
/// minute, and a client behind one has been closed: the drain flushes the tap
/// log without waiting for either client, and the daemon exits inside its 10 s
/// `shutdown_timeout_secs` with the trades of a session that was streaming in
/// the log.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn shutdown_flushes_the_tap_log_with_a_subscribe_in_flight() {
    use datamancer::storage::{TursoTapLog, TursoTapLogConfig};
    use datamancer::{AssetClass, EventKind, Instrument, ProviderId, ReplayRequest, TapLog as _};

    let dir = tempfile::tempdir().expect("tempdir");
    let tap_path = dir.path().join("taplog.db");
    let mut daemon = common::spawn_daemon_with_server(
        dir.path(),
        common::unique_prefix(),
        "[provider.scripted]\nlatest = false\nlive_interval_ms = 20\n",
        "shutdown_timeout_secs = 10",
        &format!(
            "[tap_log]\nbackend = \"embedded\"\npath = \"{}\"\n",
            tap_path.display()
        ),
        logged_command,
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut tapped = conn.open_client("tapped", &node);
    let reply = conn.request(
        r#"{"op":"subscribe","client":"tapped","provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade","persistence":"tap_only"}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    assert!(
        trades_within(&mut tapped, Duration::from_secs(2)) > 0,
        "the tapped session streams nothing"
    );
    let reply = conn.request(
        r#"{"op":"configure-provider","provider":"scripted","settings":{"latest":false,"live_interval_ms":20,"connect_delay_ms":60000}}"#,
    );
    assert_eq!(reply["ok"], true, "{reply}");
    let _subscriber = conn.open_client("slow", &node);
    let _subscribe = request_in_background(conn, subscribe_line("slow", "MSFT"));
    let mut closing_conn = Conn::connect(daemon.socket());
    let _closing = closing_conn.open_client("closing", &node);
    let _closing_subscribe = request_in_background(closing_conn, subscribe_line("closing", "GOOG"));
    wait_for_subscribes(daemon.socket(), 3);
    let reply = request_within(
        daemon.socket(),
        r#"{"op":"close-client","client":"closing"}"#,
        Duration::from_secs(2),
    )
    .expect("close-client did not answer during the subscribe");
    assert_eq!(reply["ok"], true, "{reply}");

    assert!(
        daemon.shutdown_within(Duration::from_secs(9)),
        "the daemon did not exit within 9 s of shutdown"
    );
    let log = std::fs::read_to_string(dir.path().join("daemon.log")).expect("daemon log");
    let phases = log
        .lines()
        .find(|line| line.contains("drain phases"))
        .unwrap_or_else(|| panic!("no drain phases logged:\n{log}"));
    let first_flush = phases.find("tap-log-flush");
    let cut_off = phases.find("client-close-timeout");
    assert!(
        first_flush.is_some() && first_flush < cut_off,
        "the tap log must be flushed before the waiting client is cut off: {phases}"
    );
    assert!(!log.contains("drain exceeded timeout"), "{log}");

    let replayed = tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            use futures::StreamExt as _;

            let log = TursoTapLog::open(TursoTapLogConfig::embedded(&tap_path))
                .await
                .expect("open tap log");
            let mut events = log
                .as_replay_source()
                .open(ReplayRequest {
                    instruments: vec![Instrument::new(
                        ProviderId::from_static("scripted"),
                        AssetClass::Equity,
                        "AAPL",
                    )],
                    kinds: vec![EventKind::Trade],
                    from: datamancer::Timestamp(0),
                    to: datamancer::Timestamp(i64::MAX),
                })
                .await
                .expect("replay");
            let mut replayed = 0_u64;
            while events.next().await.is_some() {
                replayed += 1;
            }
            replayed
        });
    assert!(replayed > 0, "the tap log holds no trades");
}

/// A client closed while its subscribe waits on a slow provider counts
/// toward `max_clients` until its close finishes: with a cap of 2, two such
/// clients make the next `open-client` `service_cap_exceeded`, and once
/// their connects return the cap admits a client again.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_client_closed_mid_subscribe_holds_its_slot_until_it_closes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(
        dir.path(),
        "connect_delay_ms = 3000\n",
        "[iceoryx2]\nmax_clients = 2\n",
    );
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut subscribes = Vec::new();
    let mut ports = Vec::new();
    for (i, symbol) in ["AAPL", "MSFT"].into_iter().enumerate() {
        let client = format!("closed-{i}");
        let mut conn = Conn::connect(daemon.socket());
        ports.push(conn.open_client(&client, &node));
        subscribes.push(request_in_background(conn, subscribe_line(&client, symbol)));
        wait_for_subscribes(daemon.socket(), i as u64 + 1);
        let closed = round_trip_within(
            daemon.socket(),
            &format!(r#"{{"op":"close-client","client":"{client}"}}"#),
        );
        assert_eq!(closed["ok"], true, "{closed}");
    }

    let mut conn = Conn::connect(daemon.socket());
    let refused = conn.request(r#"{"op":"open-client","client":"third"}"#);
    assert_eq!(refused["code"], "service_cap_exceeded", "{refused}");

    for subscribe in subscribes {
        let _ = subscribe.join().expect("subscribe thread");
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let reply = conn.request(r#"{"op":"open-client","client":"third"}"#);
        if reply["ok"] == true {
            break;
        }
        assert_eq!(reply["code"], "service_cap_exceeded", "{reply}");
        assert!(
            Instant::now() < deadline,
            "the closed clients never released their slots"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `close-client` on an idle client ends its stream with `SessionClosing`.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_closed_idle_client_ends_with_session_closing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_scripted_daemon(dir.path(), "latest = false\n", "");
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    let mut conn = Conn::connect(daemon.socket());
    let mut subscriber = conn.open_client("idle", &node);
    let closed = conn.request(r#"{"op":"close-client","client":"idle"}"#);
    assert_eq!(closed["ok"], true, "{closed}");
    assert!(
        saw_session_closing(&mut subscriber, Duration::from_secs(5)),
        "no SessionClosing after close-client"
    );
}

/// `close-client` during an `open-client` whose initial subscription waits
/// for a cold provider answers at once, the open answers `session_closed`,
/// and the stream ends with `SessionClosing` once the connect returns.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_client_closed_during_its_cold_open_ends_with_session_closing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_cold_daemon(dir.path());
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");
    // The opener learns its service only from the reply, which waits on the
    // connect; client ids are sequential, so the next one's name is known.
    let reply = Conn::connect(daemon.socket()).request(r#"{"op":"open-client","client":"namer"}"#);
    let namer = reply["service"].as_str().expect("service name");
    let service: DataServiceName = namer
        .rsplit_once('/')
        .map(|(prefix, _)| format!("{prefix}/1"))
        .expect("an id after the last slash")
        .parse()
        .expect("data-service name");
    let open = request_in_background(
        Conn::connect(daemon.socket()),
        r#"{"op":"open-client","client":"cold","subscriptions":[{"provider":"scripted","asset_class":"equity","symbol":"AAPL","kind":"trade"}]}"#
            .to_string(),
    );
    wait_for_subscribes(daemon.socket(), 1);
    let mut subscriber = DataSubscriber::open(&node, &service).expect("subscriber");

    let closed = round_trip_within(daemon.socket(), r#"{"op":"close-client","client":"cold"}"#);
    assert_eq!(closed["ok"], true, "{closed}");
    let (reply, _) = open.join().expect("open-client thread");
    let reply = reply.expect("open-client reply");
    assert_eq!(reply["code"], "session_closed", "{reply}");
    assert!(
        saw_session_closing(&mut subscriber, COLD_CONNECT + Duration::from_secs(3)),
        "no SessionClosing after the connect returned"
    );
}

/// A connection's EOF tears down the client it opened, not one since reopened
/// under that name on another connection: the reopened client stays listed
/// and keeps streaming.
#[test]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
fn a_connection_eof_leaves_a_client_reopened_under_its_name() {
    let dir = tempfile::tempdir().expect("tempdir");
    let daemon = spawn_stall_daemon(dir.path(), 2);
    let node = NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("iceoryx2 node");

    // The opener stays armed for `x` after closing it, and for this query,
    // whose cancel is queued behind the client's teardown.
    let mut opener = Conn::connect(daemon.socket());
    let _first = opener.open_client("x", &node);
    let (query, service) = open_trade_query(&mut opener, "AAPL");
    let _stalled = DataSubscriber::open(&node, &service).expect("subscriber");
    let closed = opener.request(r#"{"op":"close-client","client":"x"}"#);
    assert_eq!(closed["ok"], true, "{closed}");

    let mut reopened = Conn::connect(daemon.socket());
    let mut subscriber = reopened.open_client("x", &node);
    subscribe_aapl(&mut reopened, "x");

    drop(opener);
    let deadline = Instant::now() + Duration::from_secs(5);
    while is_listed(&daemon, query) {
        assert!(
            Instant::now() < deadline,
            "the connection's EOF was never handled"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let listed = reopened.request(r#"{"op":"list-clients"}"#);
    assert_eq!(listed["clients"], serde_json::json!(["x"]), "{listed}");
    // Drop what was buffered before the EOF was handled.
    trades_within(&mut subscriber, Duration::from_millis(500));
    assert!(
        trades_within(&mut subscriber, Duration::from_secs(2)) > 0,
        "the reopened client stopped streaming"
    );
}

/// With feature `ws`, a connection still waiting on a provider past the WS
/// step's limit is aborted, and the drain flushes the tap log again after the
/// closes, since that connection may release its session after the first
/// flush.
#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "ws")]
#[ignore = "spawns the daemon, needs a live iceoryx2 runtime"]
async fn shutdown_flushes_the_tap_log_again_after_aborting_a_ws_connection() {
    use datamancer::{AssetClass, EventKind, Instrument, ProviderId};
    use datamancer_client::Client as _;
    use datamancer_client::spec::SubscriptionSpec;

    const PORT: u16 = 19027;

    let dir = tempfile::tempdir().expect("tempdir");
    let mut daemon = common::spawn_daemon_with_server(
        dir.path(),
        common::unique_prefix(),
        "[provider.scripted]\nconnect_delay_ms = 60000\n",
        "shutdown_timeout_secs = 10",
        &format!("[ws]\nenabled = true\nbind = \"127.0.0.1\"\nport = {PORT}\n"),
        logged_command,
    );
    let instrument = Instrument::new(
        ProviderId::from_static("scripted"),
        AssetClass::Equity,
        "AAPL",
    );
    let spec = SubscriptionSpec::new(&instrument, EventKind::Trade).expect("spec");
    let (mut client, _events) = connect_ws(PORT).await;
    let _subscribing = tokio::spawn(async move {
        let _ = client.subscribe(&spec).await;
        client
    });
    wait_for_subscribes(daemon.socket(), 1);

    assert!(
        daemon.shutdown_within(Duration::from_secs(9)),
        "the daemon did not exit within 9 s of shutdown"
    );
    let log = std::fs::read_to_string(dir.path().join("daemon.log")).expect("daemon log");
    let phases = log
        .lines()
        .find(|line| line.contains("drain phases"))
        .unwrap_or_else(|| panic!("no drain phases logged:\n{log}"));
    assert_eq!(
        phases.matches("tap-log-flush").count(),
        2,
        "the tap log is flushed again after the aborted connection: {phases}"
    );
}

/// One request on a connection of its own, answered within a second.
fn round_trip_within(socket: &Path, request: &str) -> serde_json::Value {
    request_within(socket, request, Duration::from_secs(1)).expect("no reply within 1 s")
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

//! iceoryx2 runtime integration tests.
//!
//! These need a live iceoryx2 runtime (shared memory + service discovery), so
//! they are `#[ignore]`d and CI runs them in a step of their own. Run
//! explicitly with:
//!
//! ```text
//! cargo test -p datamancer-transport-iceoryx2 --test iceoryx2_runtime -- --ignored
//! ```
//!
//! Each test uses a distinct client id / process-unique node so concurrent
//! runs do not collide on service names.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datamancer_core::{
    AssetClass, CacheSnapshot, ConnectionState, Control, ControlKind, EventSink, HealthView,
    Instrument, MarketEvent, Price, ProviderId, ProviderSnapshot, Quantity, Seq, SystemSnapshot,
    Timestamp, Trade,
};
use datamancer_transport_iceoryx2::{
    DataServiceName, DataSubscriber, Iceoryx2DataSink, Iceoryx2DiagnosticsPublisher,
    Iceoryx2DiagnosticsSubscriber, Iceoryx2HealthPublisher, Iceoryx2HealthSubscriber, Polled,
    ServicePrefix, TransportError,
};
use iceoryx2::prelude::{Node, NodeBuilder, ipc_threadsafe};

/// The `[server].service_prefix` a daemon would configure; every service name
/// in these tests is scoped by it.
const PREFIX: &str = "transport-runtime-test";

/// The data-service name a daemon would derive for `client` under [`PREFIX`]
/// scoped to this test process.
fn name(client: u64) -> DataServiceName {
    let prefix = ServicePrefix::new(PREFIX)
        .expect("valid prefix")
        .boot_scoped(0, std::process::id())
        .expect("fits");
    DataServiceName::new(&prefix, client)
}

fn unique_client_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    // Mix in the process id so parallel test binaries never collide.
    let pid = u64::from(std::process::id());
    pid.wrapping_mul(1_000_000)
        .wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed))
}

fn node() -> Node<ipc_threadsafe::Service> {
    NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .expect("create iceoryx2 node")
}

fn trade(symbol: &str, seq: u64, price: i64) -> MarketEvent {
    let ts = i64::try_from(seq).unwrap();
    MarketEvent::Trade(Trade {
        instrument: Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Crypto,
            symbol,
        ),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts + 1),
        seq: Seq(seq),
        price: Price(price),
        size: Quantity::from_raw(10),
    })
}

/// What one poll of a service that still has its publisher returned.
fn arrived(sub: &mut DataSubscriber) -> Vec<MarketEvent> {
    match sub.poll().expect("poll") {
        Polled::Events(events) => events,
        Polled::Closed => panic!("the service closed"),
    }
}

/// Drain the subscriber until it has produced `expected` events or attempts run
/// out (bounded so a missing announcement does not hang the test).
fn drain_until(sub: &mut DataSubscriber, expected: usize) -> Vec<MarketEvent> {
    let mut out = Vec::new();
    for _ in 0..1000 {
        out.extend(arrived(sub));
        if out.len() >= expected {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    out
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn data_plane_reconstructs_logical_stream() {
    let node = node();
    let client = unique_client_id();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber");

    // Two symbols' trades, interleaved.
    let events = vec![
        trade("BTC/USD", 0, 100),
        trade("ETH/USD", 0, 200),
        trade("BTC/USD", 1, 101),
        trade("ETH/USD", 1, 201),
    ];
    for ev in &events {
        sink.publish(ev.clone()).await;
    }

    let received = drain_until(&mut sub, events.len());

    // Per-pair determinism only: assert seq is monotonic WITHIN each
    // (instrument, kind) pair, with no cross-pair ordering claim.
    for symbol in ["BTC/USD", "ETH/USD"] {
        let seqs: Vec<u64> = received
            .iter()
            .filter_map(|e| match e {
                MarketEvent::Trade(t) if t.instrument.symbol() == symbol => Some(t.seq.0),
                _ => None,
            })
            .collect();
        assert_eq!(seqs, vec![0, 1], "per-pair seq order for {symbol} trades");
    }
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn flush_drains_before_exit() {
    let node = node();
    let client = unique_client_id();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber");

    sink.publish(trade("BTC/USD", 0, 100)).await;
    sink.publish(MarketEvent::Control(Control {
        source_ts: Timestamp(1),
        rx_ts: Timestamp(2),
        seq: Seq::SYNTHETIC,
        kind: ControlKind::SessionClosing,
    }))
    .await;
    sink.flush().await.expect("flush");

    let received = drain_until(&mut sub, 2);
    assert!(received.iter().any(
        |e| matches!(e, MarketEvent::Control(c) if matches!(c.kind, ControlKind::SessionClosing))
    ));
}

/// A subscriber whose queue is full makes `publish` wait, without holding the
/// thread: a reader that drains lets the waiting `publish` complete, and a
/// `publish` dropped while it waits never delivers its sample.
#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_full_subscriber_makes_publish_wait_until_it_reads() {
    use std::sync::Arc;

    let node = node();
    let sink = Arc::new(Iceoryx2DataSink::new(&node, name(unique_client_id())).expect("sink"));
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber");

    // Publish until one waits; dropping it at the timeout abandons that sample.
    let mut filled = 0;
    loop {
        let ev = trade("BTC/USD", filled, 100);
        match tokio::time::timeout(Duration::from_millis(200), sink.publish(ev)).await {
            Ok(outcome) => {
                assert!(matches!(
                    outcome,
                    datamancer_core::PublishOutcome::Delivered
                ));
                filled += 1;
                assert!(filled < 1000, "publish never waited on the full queue");
            }
            Err(_) => break,
        }
    }

    // The next publish waits while nobody reads...
    let waiting = tokio::spawn({
        let sink = sink.clone();
        async move { sink.publish(trade("BTC/USD", filled + 1, 100)).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !waiting.is_finished(),
        "publish completed into a full queue"
    );

    // ...and completes once the reader drains.
    let mut seqs = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !waiting.is_finished() || seqs.len() < usize::try_from(filled).unwrap() + 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "drain timed out: {seqs:?}"
        );
        for ev in arrived(&mut sub) {
            if let MarketEvent::Trade(t) = ev {
                seqs.push(t.seq.0);
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(matches!(
        waiting.await.expect("publish task"),
        datamancer_core::PublishOutcome::Delivered
    ));
    let mut expected: Vec<u64> = (0..filled).collect();
    expected.push(filled + 1);
    assert_eq!(seqs, expected, "the dropped publish delivered nothing");
}

/// A subscriber that attaches between publishes gets each sample at most
/// once: what it receives from the history and live is strictly increasing in
/// `seq`.
#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_late_subscriber_receives_strictly_increasing_seqs() {
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(unique_client_id())).expect("sink");
    for seq in 0..3 {
        sink.publish(trade("BTC/USD", seq, 100)).await;
    }
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber");
    for seq in 3..6 {
        sink.publish(trade("BTC/USD", seq, 100)).await;
    }
    sink.flush().await.expect("flush");

    let seqs: Vec<u64> = drain_until(&mut sub, 6)
        .iter()
        .filter_map(|e| match e {
            MarketEvent::Trade(t) => Some(t.seq.0),
            _ => None,
        })
        .collect();
    assert_eq!(seqs, vec![0, 1, 2, 3, 4, 5]);
}

/// A client data service takes one subscriber: a second is refused while
/// the first is attached, and a new one attaches once it has gone.
#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_data_service_refuses_a_second_subscriber() {
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(unique_client_id())).expect("sink");
    let first = DataSubscriber::open(&node, sink.data_service_name()).expect("first");
    assert!(
        DataSubscriber::open(&node, sink.data_service_name()).is_err(),
        "a second subscriber attached"
    );
    drop(first);
    let mut next = DataSubscriber::open(&node, sink.data_service_name()).expect("reattach");
    let ev = trade("BTC/USD", 1, 100);
    sink.publish(ev.clone()).await;
    assert_eq!(drain_until(&mut next, 1), vec![ev]);
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn two_clients_same_symbol_see_identical_seq() {
    let node = node();
    let client_a = unique_client_id();
    let client_b = unique_client_id();
    let sink_a = Iceoryx2DataSink::new(&node, name(client_a)).expect("sink a");
    let sink_b = Iceoryx2DataSink::new(&node, name(client_b)).expect("sink b");
    let mut sub_a = DataSubscriber::open(&node, sink_a.data_service_name()).expect("sub a");
    let mut sub_b = DataSubscriber::open(&node, sink_b.data_service_name()).expect("sub b");

    let ev = trade("BTC/USD", 7, 100);
    sink_a.publish(ev.clone()).await;
    sink_b.publish(ev.clone()).await;

    let a = drain_until(&mut sub_a, 1);
    let b = drain_until(&mut sub_b, 1);
    assert_eq!(a, vec![ev.clone()]);
    assert_eq!(b, vec![ev]);
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn late_joiner_resolves_all_symbols() {
    let node = node();
    let client = unique_client_id();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");

    // Publish before any subscriber attaches; history retains the samples.
    for ev in [trade("BTC/USD", 0, 100), trade("ETH/USD", 0, 200)] {
        sink.publish(ev).await;
    }

    // Late joiner attaches after the fact. iceoryx2 delivers retained history to
    // a new subscriber on the publisher's next send (mirrors a live stream that
    // keeps ticking), so publish one more sample + flush to trigger delivery and
    // republish the full symbol table for resolution.
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("late subscriber");
    sink.publish(trade("BTC/USD", 1, 101)).await;
    sink.flush().await.expect("flush");

    let received = drain_until(&mut sub, 3);
    for symbol in ["BTC/USD", "ETH/USD"] {
        assert!(
            received.iter().any(|e| matches!(
                e,
                MarketEvent::Trade(t) if t.instrument.symbol() == symbol
            )),
            "late joiner resolved {symbol}"
        );
    }
}

#[test]
#[ignore = "needs the iceoryx2 runtime"]
fn opening_a_service_nobody_created_fails() {
    let client = unique_client_id();
    let node = node();
    let name = name(client);
    assert_eq!(
        DataSubscriber::open(&node, &name).err(),
        Some(TransportError::ServiceNotFound(name.to_string())),
        "the attach must not create the service"
    );
}

#[tokio::test(start_paused = true)]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_sink_waits_until_its_subscriber_attaches() {
    const WITHIN: Duration = Duration::from_secs(5);
    const JUST_INSIDE: Duration = Duration::from_millis(4_900);
    let client = unique_client_id();
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    let started = tokio::time::Instant::now();

    let (attached, sub) = tokio::join!(sink.wait_for_subscriber(WITHIN), async {
        tokio::time::sleep(JUST_INSIDE).await;
        DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber")
    });

    assert!(attached, "an attach just inside the wait is seen");
    assert!(started.elapsed() < WITHIN);
    drop(sub);
}

#[tokio::test(start_paused = true)]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_sink_stops_waiting_when_no_subscriber_attaches() {
    const WITHIN: Duration = Duration::from_secs(5);
    let client = unique_client_id();
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    let started = tokio::time::Instant::now();

    assert!(!sink.wait_for_subscriber(WITHIN).await);
    assert_eq!(started.elapsed(), WITHIN);
}

#[tokio::test(start_paused = true)]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_sink_does_not_count_a_subscriber_that_left() {
    let client = unique_client_id();
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    drop(DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber"));

    assert!(!sink.wait_for_subscriber(Duration::from_millis(50)).await);
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_sample_whose_symbol_was_announced_before_the_attach_stays_held_at_close() {
    let client = unique_client_id();
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    sink.publish(trade("AAPL", 0, 100)).await;
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber");
    assert!(sink.wait_for_subscriber(Duration::from_secs(1)).await);
    sink.publish(trade("AAPL", 1, 100)).await;
    drop(sink);

    while sub.poll().expect("poll") != Polled::Closed {}
    assert_eq!(sub.held_len(), 2);
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_poll_that_fails_on_an_announcement_keeps_the_data_it_drained() {
    let client = unique_client_id();
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber");
    // A symbol holding the tuple's field separator announces a malformed tuple.
    sink.publish(trade("GOOD", 0, 100)).await;
    sink.publish(trade("BAD\u{1f}X", 1, 100)).await;

    let failed = sub.poll();
    assert!(
        matches!(failed, Err(TransportError::Interning(_))),
        "{failed:?}"
    );

    let delivered = arrived(&mut sub);
    assert!(
        matches!(delivered.as_slice(), [MarketEvent::Trade(t)] if t.seq.0 == 0),
        "{delivered:?}"
    );
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn a_subscriber_receives_everything_before_its_service_closes() {
    let client = unique_client_id();
    let node = node();
    let sink = Iceoryx2DataSink::new(&node, name(client)).expect("sink");
    let mut sub = DataSubscriber::open(&node, sink.data_service_name()).expect("subscriber");
    assert_eq!(sub.poll().expect("idle poll"), Polled::Events(Vec::new()));

    sink.publish(trade("BTC/USD", 0, 100)).await;
    drop(sink);

    assert_eq!(drain_until(&mut sub, 1).len(), 1);
    assert_eq!(sub.poll().expect("poll"), Polled::Closed);
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn diagnostics_subscriber_reconstructs_snapshot() {
    let node = node();
    let publisher = Iceoryx2DiagnosticsPublisher::new(&node).expect("diag publisher");
    let subscriber = Iceoryx2DiagnosticsSubscriber::open(&node).expect("diag subscriber");

    let snapshot = SystemSnapshot::new(
        Timestamp(1_700_000_000),
        vec![ProviderSnapshot::new(
            ProviderId::from_static("alpaca"),
            ConnectionState::Connected,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            None,
        )],
        CacheSnapshot::new(vec![], None),
        vec![],
        vec![],
    );
    publisher.publish(&snapshot).expect("publish snapshot");

    let mut received = None;
    for _ in 0..200 {
        if let Some(s) = subscriber.receive().expect("receive") {
            received = Some(s);
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(received, Some(snapshot));
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn health_subscriber_reconstructs_view() {
    let node = node();
    let publisher = Iceoryx2HealthPublisher::new(&node).expect("health publisher");
    let subscriber = Iceoryx2HealthSubscriber::open(&node).expect("health subscriber");

    let snapshot = SystemSnapshot::new(
        Timestamp(1_700_000_000),
        vec![ProviderSnapshot::new(
            ProviderId::from_static("alpaca"),
            ConnectionState::Connected,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            None,
        )],
        CacheSnapshot::new(vec![], None),
        vec![],
        vec![],
    );
    let mut view = HealthView::from_snapshot(&snapshot, HealthView::DEFAULT_QUIET_AFTER_NS);
    view.daemon.version = Some("0.4.0".to_string());
    view.daemon.credential_backend = Some("keychain".to_string());
    publisher.publish(&view).expect("publish health view");

    let mut received = None;
    for _ in 0..200 {
        if let Some(v) = subscriber.receive().expect("receive") {
            received = Some(v);
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(received, Some(view));
}

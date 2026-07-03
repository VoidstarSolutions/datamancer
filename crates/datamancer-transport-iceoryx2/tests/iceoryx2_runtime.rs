//! iceoryx2 runtime integration tests.
//!
//! These need a live iceoryx2 runtime (shared memory + service discovery), so
//! they are `#[ignore]`d in CI like `alpaca_real.rs`. Run explicitly with:
//!
//! ```text
//! cargo test -p datamancer-transport-iceoryx2 --test iceoryx2_runtime -- --ignored
//! ```
//!
//! They compile in normal CI (protecting the runtime API surface) but do not
//! run. Each test uses a distinct client id / process-unique node so concurrent
//! runs do not collide on service names.

use std::sync::atomic::{AtomicU64, Ordering};

use datamancer_core::{
    AssetClass, CacheSnapshot, ConnectionState, Control, ControlKind, EventSink, Instrument,
    MarketEvent, Price, ProviderId, ProviderSnapshot, Quantity, Seq, SystemSnapshot, Timestamp,
    Trade,
};
use datamancer_transport_iceoryx2::{
    DataSubscriber, Iceoryx2DataSink, Iceoryx2DiagnosticsPublisher, Iceoryx2DiagnosticsSubscriber,
};
use iceoryx2::prelude::{Node, NodeBuilder, ipc_threadsafe};

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

/// Drain the subscriber until it has produced `expected` events or attempts run
/// out (bounded so a missing announcement does not hang the test).
fn drain_until(sub: &mut DataSubscriber, expected: usize) -> Vec<MarketEvent> {
    let mut out = Vec::new();
    for _ in 0..1000 {
        out.extend(sub.poll().expect("poll"));
        if out.len() >= expected {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    out
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn data_plane_reconstructs_logical_stream() {
    let node = node();
    let client = unique_client_id();
    let sink = Iceoryx2DataSink::new(&node, client).expect("sink");
    let mut sub = DataSubscriber::open(&node, client).expect("subscriber");

    // Two symbols interleaved; per-symbol seq is monotonic.
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

    // Per-symbol determinism only: assert seq is monotonic WITHIN each symbol,
    // with no cross-symbol ordering claim (multiplex_is_per_symbol_only).
    for symbol in ["BTC/USD", "ETH/USD"] {
        let seqs: Vec<u64> = received
            .iter()
            .filter_map(|e| match e {
                MarketEvent::Trade(t) if t.instrument.symbol() == symbol => Some(t.seq.0),
                _ => None,
            })
            .collect();
        assert_eq!(seqs, vec![0, 1], "per-symbol seq order for {symbol}");
    }
}

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn flush_drains_before_exit() {
    let node = node();
    let client = unique_client_id();
    let sink = Iceoryx2DataSink::new(&node, client).expect("sink");
    let mut sub = DataSubscriber::open(&node, client).expect("subscriber");

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

#[tokio::test]
#[ignore = "needs the iceoryx2 runtime"]
async fn two_clients_same_symbol_see_identical_seq() {
    let node = node();
    let client_a = unique_client_id();
    let client_b = unique_client_id();
    let sink_a = Iceoryx2DataSink::new(&node, client_a).expect("sink a");
    let sink_b = Iceoryx2DataSink::new(&node, client_b).expect("sink b");
    let mut sub_a = DataSubscriber::open(&node, client_a).expect("sub a");
    let mut sub_b = DataSubscriber::open(&node, client_b).expect("sub b");

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
    let sink = Iceoryx2DataSink::new(&node, client).expect("sink");

    // Publish before any subscriber attaches; history retains the samples.
    for ev in [trade("BTC/USD", 0, 100), trade("ETH/USD", 0, 200)] {
        sink.publish(ev).await;
    }

    // Late joiner attaches after the fact. iceoryx2 delivers retained history to
    // a new subscriber on the publisher's next send (mirrors a live stream that
    // keeps ticking), so publish one more sample + flush to trigger delivery and
    // republish the full symbol table for resolution.
    let mut sub = DataSubscriber::open(&node, client).expect("late subscriber");
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
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(received, Some(snapshot));
}

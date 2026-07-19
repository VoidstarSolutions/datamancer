//! End-to-end tests for the Phase 2 client session: multiplex interleave,
//! runtime subscribe/unsubscribe, refcounted sharing + teardown, per-client
//! per-instrument resume buffering, connection-scoped control coalescing, and
//! the synthetic client-local controls.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::Surface;
use datamancer::{
    AssetClass, ControlKind, Datamancer, EventKind, Instrument, LiveHandle, MarketEvent,
    PersistenceOptions, Price, Provider, ProviderId, Result, Scope, Seq, Timestamp, Trade,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};

// ---------------------------------------------------------------------------
// Multi-symbol fake provider
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Shared {
    /// One sink per subscribed symbol. Each `start_live` is its own connection;
    /// `subscribe` registers that connection's sink for the pair's symbol.
    sinks: HashMap<String, mpsc::Sender<MarketEvent>>,
}

struct FakeProvider {
    id: String,
    shared: Arc<Mutex<Shared>>,
    /// When false, suppress `ProviderConnected` / `SubscriptionChanged` so a
    /// test can reason about a data-only stream.
    chatty: bool,
    live_starts: Arc<AtomicUsize>,
    unsubscribes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

struct Ctrl {
    shared: Arc<Mutex<Shared>>,
    live_starts: Arc<AtomicUsize>,
    unsubscribes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

impl FakeProvider {
    fn new(id: &str, chatty: bool) -> (Arc<Self>, Ctrl) {
        let shared = Arc::new(Mutex::new(Shared::default()));
        let live_starts = Arc::new(AtomicUsize::new(0));
        let unsubscribes = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(Self {
            id: id.to_string(),
            shared: shared.clone(),
            chatty,
            live_starts: live_starts.clone(),
            unsubscribes: unsubscribes.clone(),
            closes: closes.clone(),
        });
        let ctrl = Ctrl {
            shared,
            live_starts,
            unsubscribes,
            closes,
        };
        (provider, ctrl)
    }
}

impl Ctrl {
    async fn push(&self, symbol: &str, ev: MarketEvent) {
        let sink = self.shared.lock().await.sinks.get(symbol).cloned();
        if let Some(sink) = sink {
            let _ = sink.send(ev).await;
        }
    }

    fn live_starts(&self) -> usize {
        self.live_starts.load(Ordering::SeqCst)
    }

    fn unsubscribes(&self) -> usize {
        self.unsubscribes.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    fn closes(&self) -> usize {
        self.closes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Provider for FakeProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn supports(&self, _instrument: &Instrument, _kind: EventKind, _surface: Surface) -> bool {
        true
    }
    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        self.live_starts.fetch_add(1, Ordering::SeqCst);
        if self.chatty {
            let _ = sink
                .send(MarketEvent::Control(datamancer::Control {
                    source_ts: Timestamp(0),
                    rx_ts: Timestamp(0),
                    seq: Seq(0),
                    kind: ControlKind::ProviderConnected {
                        provider: self.id.clone(),
                    },
                }))
                .await;
        }
        Ok(Box::new(FakeLiveHandle {
            id: self.id.clone(),
            shared: self.shared.clone(),
            sink,
            chatty: self.chatty,
            unsubscribes: self.unsubscribes.clone(),
            closes: self.closes.clone(),
            symbols: Mutex::new(Vec::new()),
        }))
    }
    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        Ok(())
    }
}

struct FakeLiveHandle {
    id: String,
    shared: Arc<Mutex<Shared>>,
    sink: mpsc::Sender<MarketEvent>,
    chatty: bool,
    unsubscribes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    symbols: Mutex<Vec<String>>,
}

#[async_trait]
impl LiveHandle for FakeLiveHandle {
    async fn subscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let symbol = instrument.symbol().to_string();
        self.shared
            .lock()
            .await
            .sinks
            .insert(symbol.clone(), self.sink.clone());
        self.symbols.lock().await.push(symbol);
        if self.chatty {
            let _ = self
                .sink
                .send(MarketEvent::Control(datamancer::Control {
                    source_ts: Timestamp(0),
                    rx_ts: Timestamp(0),
                    seq: Seq(0),
                    kind: ControlKind::SubscriptionChanged {
                        provider: self.id.clone(),
                        instrument,
                        kind,
                        active: true,
                    },
                }))
                .await;
        }
        Ok(())
    }
    async fn unsubscribe(&self, instrument: Instrument, _kind: EventKind) -> Result<()> {
        self.unsubscribes.fetch_add(1, Ordering::SeqCst);
        self.shared.lock().await.sinks.remove(instrument.symbol());
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        let mut shared = self.shared.lock().await;
        for symbol in self.symbols.lock().await.iter() {
            shared.sinks.remove(symbol);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn inst(symbol: &str) -> Instrument {
    Instrument::new(ProviderId::from_static("fake"), AssetClass::Equity, symbol)
}

fn trade(symbol: &str, ts: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(symbol),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(1.0),
        size: datamancer::Quantity::from_units(1),
    })
}

fn live() -> Scope {
    Scope::Live {
        backfill_from: None,
    }
}

async fn next_ev(stream: &mut datamancer::EventStream) -> MarketEvent {
    tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timed out waiting for an event")
        .expect("stream ended unexpectedly")
}

/// Drain until `n` data events are seen, returning data as `(symbol, ts, seq)`
/// and any control kinds observed along the way.
async fn drain_data(
    stream: &mut datamancer::EventStream,
    n: usize,
) -> (Vec<(String, i64, u64)>, Vec<ControlKind>) {
    let mut data = Vec::new();
    let mut controls = Vec::new();
    while data.len() < n {
        match next_ev(stream).await {
            MarketEvent::Trade(t) => {
                data.push((t.instrument.symbol().to_string(), t.source_ts.0, t.seq.0));
            }
            MarketEvent::Control(c) => controls.push(c.kind),
            other => panic!("unexpected event {other:?}"),
        }
    }
    (data, controls)
}

async fn wait_until(mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiplex_interleaves_two_instruments_in_arrival_order() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    client
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();

    ctrl.push("AAPL", trade("AAPL", 100)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    ctrl.push("AAPL", trade("AAPL", 300)).await;

    let (data, _) = drain_data(&mut stream, 3).await;
    // Every event appears.
    assert_eq!(data.len(), 3);
    // Each symbol's substream is internally seq-monotonic (source-stamped).
    let aapl: Vec<u64> = data.iter().filter(|d| d.0 == "AAPL").map(|d| d.2).collect();
    let msft: Vec<u64> = data.iter().filter(|d| d.0 == "MSFT").map(|d| d.2).collect();
    assert_eq!(aapl, vec![0, 1]);
    assert_eq!(msft, vec![0]);
    // No cross-symbol order is asserted (arrival order only).
    let _ = client.close().await;
}

#[tokio::test]
async fn client_output_stream_does_not_restamp_seq() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    for ts in [100, 200, 300] {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    let (data, _) = drain_data(&mut stream, 3).await;
    // The seq a client observes equals the source-stamped seq, 0..2.
    assert_eq!(data.iter().map(|d| d.2).collect::<Vec<_>>(), vec![0, 1, 2]);
    let _ = client.close().await;
}

#[tokio::test]
async fn runtime_subscribe_adds_instrument_midstream() {
    let (provider, ctrl) = FakeProvider::new("fake", true);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    ctrl.push("AAPL", trade("AAPL", 100)).await;

    // Subscribe a second symbol mid-drain.
    client
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    ctrl.push("MSFT", trade("MSFT", 200)).await;

    let mut saw_msft_data = false;
    let mut saw_msft_sub = false;
    while !(saw_msft_data && saw_msft_sub) {
        match next_ev(&mut stream).await {
            MarketEvent::Trade(t) if t.instrument.symbol() == "MSFT" => saw_msft_data = true,
            MarketEvent::Control(c) => {
                if let ControlKind::SubscriptionChanged {
                    instrument, active, ..
                } = c.kind
                    && instrument.symbol() == "MSFT"
                    && active
                {
                    saw_msft_sub = true;
                }
            }
            _ => {}
        }
    }
    let _ = client.close().await;
}

#[tokio::test]
async fn runtime_unsubscribe_removes_instrument_midstream() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    client
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    let _ = drain_data(&mut stream, 1).await;

    // Unsubscribe MSFT: expect a client-local SubscriptionChanged{active:false}.
    client
        .unsubscribe(inst("MSFT"), EventKind::Trade)
        .await
        .unwrap();
    let mut saw_inactive = false;
    while !saw_inactive {
        if let MarketEvent::Control(c) = next_ev(&mut stream).await
            && let ControlKind::SubscriptionChanged {
                instrument, active, ..
            } = c.kind
            && instrument.symbol() == "MSFT"
        {
            assert!(!active, "unsubscribe surfaces active:false");
            assert_eq!(
                c.seq,
                Seq::SYNTHETIC,
                "client-local control rides SYNTHETIC"
            );
            saw_inactive = true;
        }
    }
    assert_eq!(
        client.subscriptions().await,
        vec![(inst("AAPL"), EventKind::Trade)]
    );
    let _ = client.close().await;
}

#[tokio::test]
async fn two_clients_sharing_one_authoritative_see_identical_seq_source_ts() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let a = dm.client_session();
    let b = dm.client_session();
    a.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    b.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    // Sharing: only one provider connection.
    assert_eq!(ctrl.live_starts(), 1);
    let mut sa = a.take_events().await.unwrap();
    let mut sb = b.take_events().await.unwrap();

    ctrl.push("AAPL", trade("AAPL", 100)).await;
    ctrl.push("AAPL", trade("AAPL", 200)).await;

    for stream in [&mut sa, &mut sb] {
        let (data, _) = drain_data(stream, 2).await;
        assert_eq!(
            data.iter().map(|d| (d.1, d.2)).collect::<Vec<_>>(),
            vec![(100, 0), (200, 1)]
        );
    }
    let _ = a.close().await;
    let _ = b.close().await;
}

#[tokio::test]
async fn slow_client_does_not_stall_co_subscriber() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let slow = dm.client_session();
    let fast = dm.client_session();
    slow.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    fast.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    // `slow` never takes its stream (its events buffer in its own ring); `fast`
    // must keep receiving promptly regardless.
    let mut fast_stream = fast.take_events().await.unwrap();
    for ts in [100, 200, 300] {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    let (data, _) = drain_data(&mut fast_stream, 3).await;
    assert_eq!(
        data.iter().map(|d| d.1).collect::<Vec<_>>(),
        vec![100, 200, 300]
    );
    let _ = slow.close().await;
    let _ = fast.close().await;
}

#[tokio::test]
async fn last_referrer_drop_tears_down_authoritative_session() {
    let (provider, ctrl) = FakeProvider::new("fake", true);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let a = dm.client_session();
    let b = dm.client_session();
    a.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    b.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    assert_eq!(ctrl.live_starts(), 1);

    // Drop both referrers: the authoritative session tears down.
    drop(a);
    drop(b);
    wait_until(|| ctrl.unsubscribes() == 1).await;

    // The registry slot cleared: a fresh open creates a NEW authoritative
    // session (a second provider connection).
    let c = dm.client_session();
    c.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    wait_until(|| ctrl.live_starts() == 2).await;
    let _ = c.close().await;
}

#[tokio::test]
async fn authoritative_survives_while_one_referrer_remains() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let a = dm.client_session();
    let b = dm.client_session();
    a.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    b.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    let mut sb = b.take_events().await.unwrap();

    // Drop one referrer; the symbol keeps flowing to the survivor and the
    // upstream stays subscribed (no unsubscribe yet).
    drop(a);
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    let (data, _) = drain_data(&mut sb, 1).await;
    assert_eq!(data[0].1, 100);
    assert_eq!(ctrl.unsubscribes(), 0, "upstream stays subscribed");
    assert_eq!(ctrl.live_starts(), 1, "no new connection");
    let _ = b.close().await;
}

#[tokio::test]
async fn per_client_overflow_reports_one_gap_per_affected_instrument() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(4)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    client
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    // Never take the stream: events buffer into the per-client ring (cap 4).
    // Push 5 of each so each instrument overflows. Interleave so both lose their
    // earliest events.
    for ts in [100, 200, 300, 400, 500] {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
        ctrl.push("MSFT", trade("MSFT", ts + 1000)).await;
    }

    // Barrier: let the controller drain the substreams into its ring.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = client.take_events().await.unwrap();
    // Collect gaps until both instruments are accounted for, then survivors.
    let mut gaps: HashMap<String, (i64, i64)> = HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while gaps.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "missing gaps: {gaps:?}"
        );
        if let MarketEvent::Control(c) = next_ev(&mut stream).await
            && let ControlKind::Gap {
                instrument, span, ..
            } = c.kind
        {
            gaps.insert(
                instrument.symbol().to_string(),
                (span.from_source_ts.0, span.to_source_ts.0),
            );
        }
    }
    // Exactly one Gap per affected instrument, spans within each symbol's range.
    let aapl = gaps.get("AAPL").expect("AAPL gap");
    let msft = gaps.get("MSFT").expect("MSFT gap");
    assert!(aapl.0 >= 100 && aapl.1 <= 501, "AAPL span {aapl:?}");
    assert!(msft.0 >= 1100 && msft.1 <= 1501, "MSFT span {msft:?}");
    let _ = client.close().await;
}

#[tokio::test]
async fn connection_scoped_control_appears_once_in_multiplex() {
    let (provider, ctrl) = FakeProvider::new("fake", true);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    // Two substreams on the same provider; each emits ProviderConnected.
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    client
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    // Drive a couple of data events to flush the controls through.
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;

    let mut connected = 0;
    let (mut data, mut subs) = (0, 0);
    while data < 2 {
        match next_ev(&mut stream).await {
            MarketEvent::Trade(_) => data += 1,
            MarketEvent::Control(c) => match c.kind {
                ControlKind::ProviderConnected { .. } => connected += 1,
                ControlKind::SubscriptionChanged { .. } => subs += 1,
                _ => {}
            },
            _ => {}
        }
    }
    assert_eq!(
        connected, 1,
        "ProviderConnected coalesced to one per provider"
    );
    assert_eq!(subs, 2, "per-symbol SubscriptionChanged rides through");
    let _ = client.close().await;
}

#[tokio::test]
async fn session_closing_emitted_once_on_client_close() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    let _ = drain_data(&mut stream, 1).await;

    client.close().await.unwrap();
    // Exactly one SessionClosing, then the stream ends. Substream closings are
    // suppressed.
    let mut closings = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(MarketEvent::Control(c))) if matches!(c.kind, ControlKind::SessionClosing) => {
                closings += 1;
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(elapsed) => panic!("stream did not end after close: {elapsed}"),
        }
    }
    assert_eq!(closings, 1, "exactly one SessionClosing on close");
}

#[tokio::test]
async fn unsubscribe_then_resubscribe_replays_subscription_changed() {
    let (provider, ctrl) = FakeProvider::new("fake", true);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    // Hold a second referrer so the AAPL authoritative survives the unsubscribe.
    let keeper = dm.client_session();
    keeper
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(ctrl.live_starts(), 1, "shared authoritative");
    let mut stream = client.take_events().await.unwrap();

    client
        .unsubscribe(inst("AAPL"), EventKind::Trade)
        .await
        .unwrap();
    // Re-subscribe: the authoritative still exists (keeper holds it), so the
    // cached SubscriptionChanged{active:true} is replayed to the rejoiner.
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(ctrl.live_starts(), 1, "still shared, no new connection");

    let mut saw_active = false;
    while !saw_active {
        if let MarketEvent::Control(c) = next_ev(&mut stream).await
            && let ControlKind::SubscriptionChanged {
                instrument, active, ..
            } = c.kind
            && instrument.symbol() == "AAPL"
            && active
        {
            saw_active = true;
        }
    }
    let _ = client.close().await;
    let _ = keeper.close().await;
}

#[tokio::test]
async fn client_subscribe_rejects_historical_and_backfill() {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();

    match client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(10),
            },
            PersistenceOptions::none(),
        )
        .await
    {
        Err(datamancer::Error::UnsupportedClientScope) => {}
        other => panic!("expected UnsupportedClientScope, got {other:?}"),
    }
    match client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::none(),
        )
        .await
    {
        Err(datamancer::Error::UnsupportedClientScope) => {}
        other => panic!("expected UnsupportedClientScope, got {other:?}"),
    }
}

#[tokio::test]
async fn client_subscribe_rejects_duplicate_pair() {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    match client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
    {
        Err(datamancer::Error::DuplicateSubscription { instrument, kind }) => {
            assert_eq!(instrument.symbol(), "AAPL");
            assert_eq!(kind, EventKind::Trade);
        }
        other => panic!("expected DuplicateSubscription, got {other:?}"),
    }
    let _ = client.close().await;
}

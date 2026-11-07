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
    AssetClass, ControlKind, Datamancer, EventKind, Instrument, LiveHandle, LiveScope, MarketEvent,
    PersistenceOptions, Price, Provider, ProviderId, Result, Seq, Subscribed, Timestamp, Trade,
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
    /// What `fetch_history` serves, filtered to the requested symbol.
    history: Vec<MarketEvent>,
    /// When set, `fetch_history` waits for one notification before serving,
    /// so a test can push live prints while the backfill is in flight.
    history_gate: Option<Arc<tokio::sync::Notify>>,
    /// When set, `fetch_history` serves this many events, then waits for one
    /// notification before serving the rest.
    history_pause: Option<(usize, Arc<tokio::sync::Notify>)>,
    /// When set, the next `LiveHandle::subscribe` waits for one notification.
    subscribe_gate: Option<Arc<tokio::sync::Notify>>,
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

    async fn set_history(&self, events: Vec<MarketEvent>) {
        self.shared.lock().await.history = events;
    }

    async fn gate_history(&self, gate: Arc<tokio::sync::Notify>) {
        self.shared.lock().await.history_gate = Some(gate);
    }

    async fn gate_next_subscribe(&self, gate: Arc<tokio::sync::Notify>) {
        self.shared.lock().await.subscribe_gate = Some(gate);
    }

    async fn pause_history_after(&self, served: usize, gate: Arc<tokio::sync::Notify>) {
        self.shared.lock().await.history_pause = Some((served, gate));
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
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let (history, gate, pause) = {
            let shared = self.shared.lock().await;
            (
                shared.history.clone(),
                shared.history_gate.clone(),
                shared.history_pause.clone(),
            )
        };
        if let Some(gate) = gate {
            gate.notified().await;
        }
        for (served, ev) in history.into_iter().enumerate() {
            if let Some((at, gate)) = &pause
                && *at == served
            {
                gate.notified().await;
            }
            let MarketEvent::Trade(t) = &ev else {
                continue;
            };
            if t.instrument.symbol() == request.instrument.symbol() && sink.send(ev).await.is_err()
            {
                break;
            }
        }
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
        let gate = self.shared.lock().await.subscribe_gate.take();
        if let Some(gate) = gate {
            gate.notified().await;
        }
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

fn live() -> LiveScope {
    LiveScope::Live
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

/// A subscribe whose caller gave up leaves no subscription behind, so the
/// caller's retry is a subscribe of its own.
#[tokio::test(start_paused = true)]
async fn a_subscribe_whose_caller_gave_up_leaves_no_subscription() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.gate_next_subscribe(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    let abandoned = tokio::time::timeout(
        Duration::from_secs(1),
        client.subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        ),
    )
    .await;
    assert!(abandoned.is_err(), "the subscribe must still be waiting");

    gate.notify_one();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .expect("the retry is a new subscribe");
    let _ = client.close().await;
}

/// A join cancelled once its subscriber is queued, before or after the
/// session takes it, leaves no referrer behind: once a real referrer leaves,
/// the session counts only the one left, with no further event to reap the
/// cancelled one.
#[tokio::test]
async fn a_cancelled_join_leaves_no_referrer() {
    for taken_first in [false, true] {
        let (provider, _ctrl) = FakeProvider::new("fake", false);
        let dm = Datamancer::builder()
            .provider_arc(provider)
            .build()
            .unwrap();
        let stays = dm.client_session();
        let leaves = dm.client_session();
        for client in [&stays, &leaves] {
            client
                .subscribe(
                    inst("AAPL"),
                    EventKind::Trade,
                    live(),
                    PersistenceOptions::none(),
                )
                .await
                .unwrap();
        }

        let mut join = Box::pin(dm.session(
            inst("AAPL"),
            EventKind::Trade,
            datamancer::Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        ));
        assert!(
            futures::poll!(join.as_mut()).is_pending(),
            "the join waits on the session's answer"
        );
        if taken_first {
            // Let the session add the subscriber and answer before the cancel.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(join);

        leaves.close().await.unwrap();
        let referrers = || {
            dm.snapshot_live()
                .authoritative_sessions
                .first()
                .map(|s| s.subscriber_refcount)
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while referrers() != Some(1) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "taken first: {taken_first}; referrers: {:?}",
                referrers()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = stays.close().await;
    }
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

/// Replayed history moves a stream's `seq` position, but the live timestamps
/// come only from the live tail.
#[tokio::test]
async fn a_backfill_leaves_the_live_timestamps_alone() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(vec![trade("AAPL", 1), trade("AAPL", 3)])
        .await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::LiveBackfill { from: Timestamp(0) },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();

    let (replayed, _) = drain_data(&mut stream, 2).await;
    assert_eq!(replayed.len(), 2);
    let snap = dm.snapshot().await.unwrap();
    let session = &snap.authoritative_sessions[0];
    assert_eq!(session.last_source_ts, None, "a replayed print is not live");
    assert_eq!(session.last_rx_ts, None, "a replayed print is not live");
    assert_eq!(session.latency_ns, None);
    assert_eq!(session.seq_position, Some(Seq(1)));

    ctrl.push("AAPL", trade("AAPL", 300)).await;
    drain_data(&mut stream, 1).await;
    let snap = dm.snapshot().await.unwrap();
    assert_eq!(
        snap.authoritative_sessions[0].last_source_ts,
        Some(Timestamp(300))
    );
}

/// A client subscription with `backfill_from` gets the library's seam: the
/// history first, then the live prints that arrived during the backfill in
/// arrival order, then the live tail — one monotonic `seq`, no `Gap` while
/// nothing was lost.
#[tokio::test]
async fn client_subscribe_with_backfill_serves_history_then_buffered_then_live() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("AAPL", 2)])
        .await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();

    let actual = client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::LiveBackfill { from: Timestamp(0) },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(
        actual,
        Subscribed::created(LiveScope::LiveBackfill { from: Timestamp(0) }),
        "the first subscriber creates the session"
    );
    assert_eq!(actual.history_from(), Some(Timestamp(0)));
    let mut stream = client.take_events().await.unwrap();

    // The provider is subscribed before the backfill runs, so a print that
    // lands while history is still being fetched is buffered, not lost.
    ctrl.push("AAPL", trade("AAPL", 200)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.notify_one();
    let (data, controls) = drain_data(&mut stream, 3).await;
    assert_eq!(
        data,
        vec![
            ("AAPL".to_string(), 1, 0),
            ("AAPL".to_string(), 2, 1),
            ("AAPL".to_string(), 200, 2),
        ]
    );
    assert_eq!(
        dm.snapshot().await.unwrap().authoritative_sessions[0].last_source_ts,
        Some(Timestamp(200)),
        "a print flushed at the seam is a live arrival"
    );

    // The live tail continues after the seam.
    ctrl.push("AAPL", trade("AAPL", 300)).await;
    let (tail, more_controls) = drain_data(&mut stream, 1).await;
    assert_eq!(tail, vec![("AAPL".to_string(), 300, 3)]);
    for control in controls.into_iter().chain(more_controls) {
        assert!(
            !matches!(control, ControlKind::Gap { .. }),
            "no Gap at a healthy seam, got {control:?}"
        );
    }
    let _ = client.close().await;
}

fn trades_from(first: i64, count: i64) -> Vec<MarketEvent> {
    (first..first + count).map(|ts| trade("AAPL", ts)).collect()
}

/// Reads until `count` prints arrive and answers them and how many `Gap`s came
/// with them.
async fn read_prints(stream: &mut datamancer::EventStream, count: usize) -> (Vec<i64>, usize) {
    let mut prints = Vec::new();
    let mut gaps = 0;
    while prints.len() < count {
        let next = tokio::time::timeout(Duration::from_secs(40), stream.next());
        match next.await.expect("prints stopped").expect("stream ended") {
            MarketEvent::Trade(t) => prints.push(t.source_ts.0),
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::Gap { .. }) => gaps += 1,
            _ => {}
        }
    }
    (prints, gaps)
}

/// Once the backfill has finished, a joiner hears none is running.
#[tokio::test]
async fn a_joiner_after_the_backfill_hears_none_is_in_flight() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 2)).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let backfill = LiveScope::LiveBackfill { from: Timestamp(0) };
    let none = PersistenceOptions::none();
    let creator = dm.client_session();
    creator
        .subscribe(inst("AAPL"), EventKind::Trade, backfill, none)
        .await
        .unwrap();
    let mut stream = creator.take_events().await.unwrap();
    read_prints(&mut stream, 2).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    read_prints(&mut stream, 1).await;

    let joined = dm
        .client_session()
        .subscribe(inst("AAPL"), EventKind::Trade, LiveScope::Live, none)
        .await
        .unwrap();
    assert_eq!(joined, Subscribed::joined(backfill, false));
    let _ = creator.close().await;
}

/// Options no backend can serve are refused whether the subscribe creates the
/// session or joins it, so the answer never depends on who arrived first.
#[tokio::test]
async fn client_subscribe_refuses_options_no_backend_serves() {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    let live = LiveScope::default();
    for options in [
        PersistenceOptions::cached(),
        PersistenceOptions::none().with_tap_log(true),
    ] {
        let refused = client
            .subscribe(inst("AAPL"), EventKind::Trade, live, options)
            .await;
        assert!(
            matches!(refused, Err(datamancer::Error::PersistenceRequired)),
            "creator: {refused:?}"
        );
    }

    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live,
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let joiner = dm.client_session();
    let refused = joiner
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live,
            PersistenceOptions::cached(),
        )
        .await;
    assert!(
        matches!(refused, Err(datamancer::Error::PersistenceRequired)),
        "joiner: {refused:?}"
    );
    let _ = client.close().await;
    let _ = joiner.close().await;
}

/// The pair's authoritative session is created with the first subscriber's
/// scope; a later subscriber joins it as is and is told the session's scope.
#[tokio::test]
async fn client_subscribe_reports_the_shared_sessions_actual_scope() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1)]).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let backfill = LiveScope::LiveBackfill { from: Timestamp(0) };

    // AAPL: opened with a backfill; a pure-live joiner is told so while the
    // backfill is still in flight.
    let first = dm.client_session();
    assert_eq!(
        first
            .subscribe(
                inst("AAPL"),
                EventKind::Trade,
                backfill,
                PersistenceOptions::none()
            )
            .await
            .unwrap(),
        Subscribed::created(backfill)
    );
    let second = dm.client_session();
    assert_eq!(
        second
            .subscribe(
                inst("AAPL"),
                EventKind::Trade,
                live(),
                PersistenceOptions::none()
            )
            .await
            .unwrap(),
        Subscribed::joined(backfill, true),
        "a joiner is told the session's scope, not its own request"
    );
    assert_eq!(ctrl.live_starts(), 1, "the pair is shared, not reopened");

    // MSFT: opened pure-live; a backfill request joins it and is told so.
    assert_eq!(
        first
            .subscribe(
                inst("MSFT"),
                EventKind::Trade,
                live(),
                PersistenceOptions::none()
            )
            .await
            .unwrap(),
        Subscribed::created(live())
    );
    assert_eq!(
        second
            .subscribe(
                inst("MSFT"),
                EventKind::Trade,
                backfill,
                PersistenceOptions::none()
            )
            .await
            .unwrap(),
        Subscribed::joined(live(), false),
        "a backfill request on a pure-live pair comes back pure-live"
    );

    gate.notify_waiters();
    let _ = first.close().await;
    let _ = second.close().await;
}

/// A subscriber joining while the backfill runs is told it joined, and its
/// stream carries what is left of the history, then the live tail, with no
/// `Gap` for what it missed.
#[tokio::test]
async fn a_subscriber_joining_mid_backfill_receives_the_remaining_history_then_live() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("AAPL", 2), trade("AAPL", 3)])
        .await;
    ctrl.pause_history_after(1, gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let backfill = LiveScope::LiveBackfill { from: Timestamp(0) };

    let creator = dm.client_session();
    creator
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            backfill,
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut creator_stream = creator.take_events().await.unwrap();
    let (head, _) = drain_data(&mut creator_stream, 1).await;
    assert_eq!(head, vec![("AAPL".to_string(), 1, 0)]);

    let joiner = dm.client_session();
    let outcome = joiner
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(outcome, Subscribed::joined(backfill, true));
    assert_eq!(outcome.history_from(), None);
    let mut joiner_stream = joiner.take_events().await.unwrap();

    ctrl.push("AAPL", trade("AAPL", 200)).await;
    gate.notify_one();
    let (remainder, mut controls) = drain_data(&mut joiner_stream, 3).await;
    assert_eq!(
        remainder,
        vec![
            ("AAPL".to_string(), 2, 1),
            ("AAPL".to_string(), 3, 2),
            ("AAPL".to_string(), 200, 3),
        ]
    );

    ctrl.push("AAPL", trade("AAPL", 300)).await;
    let (tail, more_controls) = drain_data(&mut joiner_stream, 1).await;
    assert_eq!(tail, vec![("AAPL".to_string(), 300, 4)]);
    controls.extend(more_controls);
    assert!(
        !controls
            .iter()
            .any(|control| matches!(control, ControlKind::Gap { .. })),
        "no Gap for what the joiner missed: {controls:?}"
    );

    let (creator_rest, _) = drain_data(&mut creator_stream, 4).await;
    assert_eq!(
        creator_rest.last(),
        Some(&("AAPL".to_string(), 300, 4)),
        "both consumers observe one stream"
    );
    let _ = joiner.close().await;
    let _ = creator.close().await;
}

/// A subscriber learns whether it created the pair's session or joined one
/// already open.
#[tokio::test]
async fn client_subscribe_reports_whether_it_joined() {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let first = dm.client_session();
    let created = first
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(created, Subscribed::created(live()));
    let second = dm.client_session();
    let joined = second
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(
        joined,
        Subscribed::joined(live(), false),
        "the second subscriber joins the open session"
    );
    let _ = first.close().await;
    let _ = second.close().await;
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

/// The wall clock in epoch nanoseconds, for a `backfill_from` that is
/// certainly not in the past.
fn now_nanos() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits")
}

/// [`datamancer::Error::BackfillFromNotInPast`] applies on both consumer
/// handles. A joiner is not checked: it takes the scope the pair already has.
#[tokio::test]
async fn client_subscribe_refuses_a_backfill_from_at_or_past_the_live_edge() {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let future = Timestamp(now_nanos() + 3_600_000_000_000);
    let backfill = LiveScope::LiveBackfill { from: future };

    let client = dm.client_session();
    let err = client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            backfill,
            PersistenceOptions::none(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, datamancer::Error::BackfillFromNotInPast { from, .. } if from == future),
        "{err:?}"
    );
    match dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            backfill.into(),
            PersistenceOptions::none(),
        )
        .await
    {
        Err(datamancer::Error::BackfillFromNotInPast { from, .. }) => assert_eq!(from, future),
        Err(other) => panic!("the single-pair handle follows the same rule: {other:?}"),
        Ok(_) => panic!("the single-pair handle must refuse the empty window"),
    }

    // A pair another subscriber holds pure-live is joined as is; the start
    // this subscriber asked for never applies, so it is not checked.
    client
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let other = dm.client_session();
    let outcome = other
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            backfill,
            PersistenceOptions::none(),
        )
        .await
        .expect("joins the pure-live session");
    assert_eq!(outcome, Subscribed::joined(live(), false));
    let _ = other.close().await;
    let _ = client.close().await;
}

//! End-to-end tests for the Phase 2 client session: multiplex interleave,
//! runtime subscribe/unsubscribe, refcounted sharing + teardown, per-client
//! per-instrument resume buffering, connection-scoped control coalescing, and
//! the synthetic client-local controls.

mod common;

use common::joined_as;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::Surface;
use datamancer::{
    AssetClass, ControlKind, Datamancer, EventKind, Instrument, LiveHandle, LiveScope, MarketEvent,
    PersistenceOptions, Price, Provider, ProviderId, Result, Scope, Seq, Subscribed, Timestamp,
    Trade,
};
use datamancer_core::{
    CacheCoverage, CacheKey, HistoricalCache, HistoryRequest, ReplaySource, TapLog,
};
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
    /// How many `fetch_history` calls have started.
    fetches: usize,
    /// When set, `fetch_history` fails with this message.
    history_error: Option<String>,
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

    async fn fetches(&self) -> usize {
        self.shared.lock().await.fetches
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
            let mut shared = self.shared.lock().await;
            shared.fetches += 1;
            if let Some(message) = shared.history_error.clone() {
                return Err(datamancer::Error::Provider {
                    provider: self.id.clone(),
                    message,
                });
            }
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
    LiveScope::default()
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
            datamancer::Scope::live(),
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

fn provider_control(kind: ControlKind, rx_ts: i64) -> MarketEvent {
    MarketEvent::Control(datamancer::Control {
        source_ts: Timestamp(rx_ts),
        rx_ts: Timestamp(rx_ts),
        seq: Seq(0),
        kind,
    })
}

/// A fan-out error from the fake provider; each copy is a clone of it.
fn fanout(message: &str) -> ControlKind {
    ControlKind::fanout_error("fake", message)
}

/// One copy of the fan-out error `kind`.
fn fanout_error(kind: &ControlKind, rx_ts: i64) -> MarketEvent {
    provider_control(kind.clone(), rx_ts)
}

/// An error about the one subscription it arrives on.
fn subscription_error(message: &str, rx_ts: i64) -> MarketEvent {
    provider_control(ControlKind::subscription_error("fake", message), rx_ts)
}

/// The provider's `ProviderConnected`.
fn connected(rx_ts: i64) -> MarketEvent {
    provider_control(
        ControlKind::ProviderConnected {
            provider: "fake".to_string(),
        },
        rx_ts,
    )
}

/// The provider's `ProviderDisconnected` after a connection reset.
fn disconnected(rx_ts: i64) -> MarketEvent {
    provider_control(
        ControlKind::ProviderDisconnected {
            provider: "fake".to_string(),
            reason: "reset".to_string(),
            cause: datamancer::DisconnectCause::Error,
        },
        rx_ts,
    )
}

/// Read until `last` trade's `source_ts`, counting the `ProviderError`s.
async fn errors_until(stream: &mut datamancer::EventStream, last: i64) -> usize {
    let mut errors = 0;
    loop {
        match next_ev(stream).await {
            MarketEvent::Trade(t) if t.source_ts.0 == last => return errors,
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::ProviderError { .. }) => {
                errors += 1;
            }
            _ => {}
        }
    }
}

async fn two_symbol_client(
    dm: &Datamancer,
) -> (datamancer::ClientSession, datamancer::EventStream) {
    let client = dm.client_session();
    for symbol in ["AAPL", "MSFT"] {
        client
            .subscribe(
                inst(symbol),
                EventKind::Trade,
                live(),
                PersistenceOptions::none(),
            )
            .await
            .unwrap();
    }
    let stream = client.take_events().await.unwrap();
    (client, stream)
}

#[tokio::test]
async fn a_provider_error_repeated_after_a_reconnect_is_delivered() {
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
    let [e0, e1] = [fanout("HTTP 403"), fanout("HTTP 403")];
    ctrl.push("AAPL", fanout_error(&e0, 10)).await;
    ctrl.push("AAPL", disconnected(11)).await;
    ctrl.push("AAPL", connected(12)).await;
    // Same text, same timestamp, but a second broadcast: its own fan-out id.
    ctrl.push("AAPL", fanout_error(&e1, 10)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    assert_eq!(
        errors_until(&mut stream, 100).await,
        2,
        "the error after the reconnect is a new one"
    );
    let _ = client.close().await;
}

#[tokio::test]
async fn a_connected_copy_between_two_error_copies_keeps_the_second_dropped() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = two_symbol_client(&dm).await;
    let e0 = fanout("re-subscribe failed");
    // A reconnect fanned out to both substreams, then one re-subscribe error
    // fanned out the same way; AAPL's pair is read before MSFT's.
    ctrl.push("AAPL", connected(5)).await;
    ctrl.push("AAPL", fanout_error(&e0, 10)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    assert_eq!(errors_until(&mut stream, 100).await, 1);
    ctrl.push("MSFT", connected(5)).await;
    ctrl.push("MSFT", fanout_error(&e0, 10)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    assert_eq!(
        errors_until(&mut stream, 200).await,
        0,
        "the second substream's copy of one error is dropped"
    );
    let _ = client.close().await;
}

#[tokio::test]
async fn a_later_error_with_the_same_text_is_delivered_and_a_copy_is_not() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = two_symbol_client(&dm).await;
    let e0 = fanout("HTTP 403");
    // One provider error fanned out to both substreams: one copy each.
    ctrl.push("AAPL", fanout_error(&e0, 10)).await;
    ctrl.push("MSFT", fanout_error(&e0, 10)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    assert_eq!(errors_until(&mut stream, 100).await, 1, "copies coalesce");
    // A second failure later, with the same text (a second fetch failing).
    ctrl.push("MSFT", subscription_error("HTTP 403", 20)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    assert_eq!(
        errors_until(&mut stream, 200).await,
        1,
        "a later failure with the same text keeps its cause"
    );
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
            LiveScope::backfill(Timestamp(0)),
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
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(
        actual,
        Subscribed::created(LiveScope::backfill(Timestamp(0))),
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

/// A backfill whose seam ring overflows reports the loss the way a resume
/// buffer does: one `Gap` right after the history, spanning the evicted
/// prints, and the evicted prints' `seq` slots left empty ahead of the
/// survivors, which follow in arrival order. The seam is the library's, so
/// its overflow is proved here rather than through a spawned daemon.
#[tokio::test]
async fn client_subscribe_with_backfill_leaves_a_real_hole_when_the_seam_ring_overflows() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("AAPL", 2)])
        .await;
    ctrl.gate_history(gate.clone()).await;
    // The seam ring is bounded like the resume buffer.
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
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();

    // Six prints land while the history is still gated; a ring of four
    // evicts the oldest two (100 and 200).
    for ts in [100, 200, 300, 400, 500, 600] {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();

    // History, the Gap, the four survivors: seven events in delivery order.
    let mut seen = Vec::new();
    while seen.len() < 7 {
        seen.push(next_ev(&mut stream).await);
    }
    let describe = |ev: &MarketEvent| match ev {
        MarketEvent::Trade(t) => format!("trade ts={} seq={}", t.source_ts.0, t.seq.0),
        MarketEvent::Control(c) => format!("control {:?} seq={}", c.kind, c.seq.0),
        other => format!("{other:?}"),
    };
    let listing: Vec<String> = seen.iter().map(describe).collect();
    let trade_at = |i: usize| match &seen[i] {
        MarketEvent::Trade(t) => (t.source_ts.0, t.seq.0),
        other => panic!("expected a trade at {i}, got {other:?}: {listing:?}"),
    };
    assert_eq!(trade_at(0), (1, 0), "{listing:?}");
    assert_eq!(trade_at(1), (2, 1), "{listing:?}");
    match &seen[2] {
        MarketEvent::Control(c) => match &c.kind {
            ControlKind::Gap { span, .. } => {
                assert_eq!(
                    c.seq.0, 2,
                    "the Gap takes the first evicted print's slot: {listing:?}"
                );
                assert_eq!(span.from_source_ts.0, 100, "{listing:?}");
                assert_eq!(span.to_source_ts.0, 201, "{listing:?}");
            }
            other => panic!("expected a Gap, got {other:?}: {listing:?}"),
        },
        other => panic!("expected the Gap after the history, got {other:?}: {listing:?}"),
    }
    // Slot 3 belonged to the second evicted print and stays empty: the hole
    // is real, as after a resume-buffer overflow.
    assert_eq!(
        (3..7).map(trade_at).collect::<Vec<_>>(),
        vec![(300, 4), (400, 5), (500, 6), (600, 7)],
        "the survivors follow the hole in arrival order: {listing:?}"
    );
    let _ = client.close().await;
}

/// A hole's size is the data lost. The provider's controls stay in the ring
/// and keep their slots after the hole.
#[tokio::test]
async fn a_seam_overflow_of_prints_leaves_a_hole_for_the_prints_only() {
    let (provider, ctrl) = FakeProvider::new("fake", true);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("AAPL", 2)])
        .await;
    ctrl.gate_history(gate.clone()).await;
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
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();

    // Prints 100 and 200 are evicted; the two leading controls stay.
    for ts in [100, 200, 300, 400, 500, 600] {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();

    let mut trades = Vec::new();
    let mut gap_seq = None;
    while trades.len() < 6 {
        match next_ev(&mut stream).await {
            MarketEvent::Trade(t) => trades.push((t.source_ts.0, t.seq.0)),
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::Gap { .. }) => {
                gap_seq = Some(c.seq.0);
            }
            MarketEvent::Control(_) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(gap_seq, Some(2), "{trades:?}");
    assert_eq!(
        trades,
        vec![(1, 0), (2, 1), (300, 6), (400, 7), (500, 8), (600, 9)],
        "the Gap takes slot 2, slot 3 stays empty, the controls take 4 and 5"
    );
    let _ = client.close().await;
}

fn trades_from(first: i64, count: i64) -> Vec<MarketEvent> {
    (first..first + count).map(|ts| trade("AAPL", ts)).collect()
}

/// Reads until `count` prints arrive and answers them and how many `Gap`s came
/// with them. Its wait outlasts the backfill stall limit.
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

/// A history far longer than every channel on the way reaches a consumer that
/// reads late, whole: the replay runs at the speed the consumer reads.
#[tokio::test]
async fn a_backfill_reaches_a_slow_consumer_whole() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 5000)).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (prints, gaps) = read_prints(&mut stream, 5000).await;
    assert_eq!(gaps, 0);
    assert_eq!(prints, (1..=5000).collect::<Vec<_>>());
    let _ = client.close().await;
}

/// The live arrivals buffered during a backfill are replayed at the same pace.
#[tokio::test]
async fn a_seam_flush_reaches_a_slow_consumer_whole() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 1)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    for ts in 100..5100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (prints, gaps) = read_prints(&mut stream, 5001).await;
    assert_eq!(gaps, 0);
    assert_eq!(prints.len(), 5001);
    let _ = client.close().await;
}

/// Reads until the print stamped `last` arrives and answers every `(ts, seq)`
/// on the way and how many `Gap`s came with them.
async fn read_through(stream: &mut datamancer::EventStream, last: i64) -> (Vec<(i64, u64)>, usize) {
    let mut prints = Vec::new();
    let mut gaps = 0;
    while prints.last().is_none_or(|p: &(i64, u64)| p.0 != last) {
        let next = tokio::time::timeout(Duration::from_secs(40), stream.next());
        match next.await.expect("prints stopped").expect("stream ended") {
            MarketEvent::Trade(t) => prints.push((t.source_ts.0, t.seq.0)),
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::Gap { .. }) => gaps += 1,
            _ => {}
        }
    }
    (prints, gaps)
}

/// Reads until a `Gap` arrives, failing on the print stamped `tail`: the loss
/// comes ahead of it.
async fn read_to_gap(stream: &mut datamancer::EventStream, tail: i64) {
    loop {
        let next = tokio::time::timeout(Duration::from_secs(40), stream.next());
        match next.await.expect("prints stopped").expect("stream ended") {
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::Gap { .. }) => return,
            MarketEvent::Trade(t) => assert!(t.source_ts.0 != tail, "no Gap before the live tail"),
            _ => {}
        }
    }
}

/// Subscribes a client to the backfill and takes its stream.
async fn backfilling_client(
    dm: &Datamancer,
) -> (datamancer::ClientSession, datamancer::EventStream) {
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let stream = client.take_events().await.unwrap();
    (client, stream)
}

/// Waits until `ctrl` has seen `count` history fetches start.
async fn wait_for_fetches(ctrl: &Ctrl, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while ctrl.fetches().await != count {
        assert!(tokio::time::Instant::now() < deadline, "fetches stuck");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Subscribes `symbol` with a backfill from 0 on a new client session.
async fn backfill_symbol(dm: &Datamancer, symbol: &str) -> datamancer::ClientSession {
    let client = dm.client_session();
    client
        .subscribe(
            inst(symbol),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    client
}

/// With room for one client backfill, a second waits until the first's
/// history ends and then fetches; a subscribe that joins takes no room.
#[tokio::test]
async fn a_client_backfill_past_the_limit_waits_for_the_history_before_it() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("MSFT", 1)])
        .await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .max_concurrent_client_backfills(std::num::NonZeroUsize::MIN)
        .build()
        .unwrap();
    let first = backfill_symbol(&dm, "AAPL").await;
    let mut first_stream = first.take_events().await.unwrap();
    wait_for_fetches(&ctrl, 1).await;
    let second = backfill_symbol(&dm, "MSFT").await;
    let mut second_stream = second.take_events().await.unwrap();
    let joiner = backfill_symbol(&dm, "AAPL").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        ctrl.fetches().await,
        1,
        "the second backfill fetched at once"
    );

    gate.notify_one();
    assert_eq!(read_prints(&mut first_stream, 1).await, (vec![1], 0));
    wait_for_fetches(&ctrl, 2).await;
    gate.notify_one();
    assert_eq!(read_prints(&mut second_stream, 1).await, (vec![1], 0));
    let _ = joiner.close().await;
    let _ = first.close().await;
    let _ = second.close().await;
}

/// A `Session`'s backfill takes no room: it fetches while a client's backfill
/// holds the only slot.
#[tokio::test]
async fn a_sessions_backfill_is_outside_the_client_limit() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("MSFT", 1)])
        .await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .max_concurrent_client_backfills(std::num::NonZeroUsize::MIN)
        .build()
        .unwrap();
    let client = backfill_symbol(&dm, "AAPL").await;
    wait_for_fetches(&ctrl, 1).await;

    let session = dm
        .session(
            inst("MSFT"),
            EventKind::Trade,
            datamancer::Scope::live_backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    wait_for_fetches(&ctrl, 2).await;
    let _ = session.close().await;
    let _ = client.close().await;
}

/// A reader stopped during its seam flush holds no fetch slot: the next
/// client backfill fetches once the history has ended.
#[tokio::test(start_paused = true)]
async fn a_reader_stopped_in_the_seam_flush_holds_no_fetch_slot() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("MSFT", 1)])
        .await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .max_concurrent_client_backfills(std::num::NonZeroUsize::MIN)
        .build()
        .unwrap();
    let first = backfill_symbol(&dm, "AAPL").await;
    let _unread = first.take_events().await.unwrap();
    wait_for_fetches(&ctrl, 1).await;
    for ts in 2..=3000 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    let _second = backfill_symbol(&dm, "MSFT").await;

    gate.notify_one();
    wait_for_fetches(&ctrl, 2).await;
}

/// A waiting client backfill starts once the one fetching closes.
#[tokio::test]
async fn a_closed_backfill_lets_a_waiting_one_fetch() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("MSFT", 1)]).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .max_concurrent_client_backfills(std::num::NonZeroUsize::MIN)
        .build()
        .unwrap();
    let first = backfill_symbol(&dm, "AAPL").await;
    wait_for_fetches(&ctrl, 1).await;
    let second = backfill_symbol(&dm, "MSFT").await;
    let mut second_stream = second.take_events().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(ctrl.fetches().await, 1);

    first.close().await.unwrap();
    wait_for_fetches(&ctrl, 2).await;
    gate.notify_one();
    assert_eq!(read_prints(&mut second_stream, 1).await, (vec![1], 0));
    let _ = second.close().await;
}

/// A disconnect that meets a full stream reaches the reader once it has read
/// its way to room, with no event after it to carry it.
#[tokio::test(start_paused = true)]
async fn a_refused_disconnect_arrives_when_the_reader_drains() {
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
    for ts in 1..=10_000 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    ctrl.push(
        "AAPL",
        MarketEvent::Control(datamancer::Control {
            source_ts: Timestamp(10_001),
            rx_ts: Timestamp(10_001),
            seq: Seq(0),
            kind: ControlKind::ProviderDisconnected {
                provider: "fake".to_string(),
                reason: "reset".to_string(),
                cause: datamancer_core::DisconnectCause::Error,
            },
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut gaps = 0;
    loop {
        let next = tokio::time::timeout(Duration::from_secs(40), stream.next());
        let ev = next
            .await
            .expect("the disconnect never came")
            .expect("stream ended");
        if let MarketEvent::Control(c) = ev {
            match c.kind {
                ControlKind::ProviderDisconnected { .. } => break,
                ControlKind::Gap { .. } => gaps += 1,
                _ => {}
            }
        }
    }
    assert!(gaps > 0, "the stream was never full");
    let _ = client.close().await;
}

/// A reader that stops is waited on for 30 s in all, then stops pacing: the
/// other gets the whole history and the stopped one's loss is a `Gap`.
#[tokio::test(start_paused = true)]
async fn a_reader_that_held_another_30s_stops_pacing() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 5000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (stopped, mut stopped_stream) = backfilling_client(&dm).await;
    let (reader, mut stream) = backfilling_client(&dm).await;
    gate.notify_one();

    let started = tokio::time::Instant::now();
    let (prints, gaps) = read_prints(&mut stream, 5000).await;
    assert_eq!(gaps, 0);
    assert_eq!(prints, (1..=5000).collect::<Vec<_>>());
    let waited = started.elapsed();
    assert!(waited >= Duration::from_secs(30), "waited {waited:?}");
    assert!(waited < Duration::from_secs(31), "waited {waited:?}");

    let (_, stopped_gaps) = read_prints(&mut stopped_stream, 1000).await;
    assert_eq!(stopped_gaps, 0, "the first prints were delivered");
    ctrl.push("AAPL", trade("AAPL", 9000)).await;
    read_to_gap(&mut stopped_stream, 9000).await;
    let _ = stopped.close().await;
    let _ = reader.close().await;
}

/// A sole reader that keeps taking events is waited on for as long as the
/// replay lasts, past 30 s in all, and loses nothing.
#[tokio::test(start_paused = true)]
async fn a_lone_steady_reader_is_waited_on_past_30s() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 6000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = backfilling_client(&dm).await;
    gate.notify_one();

    let mut prints = Vec::new();
    while prints.len() < 6000 {
        let ev = next_ev(&mut stream).await;
        assert!(
            !matches!(&ev, MarketEvent::Control(c) if matches!(c.kind, ControlKind::Gap { .. })),
            "a steady reader lost events: {ev:?}"
        );
        if let MarketEvent::Trade(t) = ev {
            prints.push(t.source_ts.0);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(prints, (1..=6000).collect::<Vec<_>>());
    let _ = client.close().await;
}

/// A sole reader that takes events and then stops has 30 s from its last
/// event, however long the replay waited on it before.
#[tokio::test(start_paused = true)]
async fn a_lone_reader_that_stops_is_released_after_30s() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 5000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = backfilling_client(&dm).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_secs(29)).await;
    for _ in 0..200 {
        next_ev(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_secs(29)).await;
    assert!(
        dm.snapshot().await.unwrap().authoritative_sessions[0].backfilling,
        "released before 30 s without an event"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_replay_over(&dm).await;
    drop(client);
    drop(stream);
}

/// Two stopped readers are both waited on from the start, so the replay goes
/// on after 30 s with nobody reading.
#[tokio::test(start_paused = true)]
async fn a_stopped_pair_stops_pacing_after_30s() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 5000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (first, _first_stream) = backfilling_client(&dm).await;
    let (second, _second_stream) = backfilling_client(&dm).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_replay_over(&dm).await;
    drop(first);
    drop(second);
}

/// Each stopped reader has its own 30 s: one that resumes after 10 s is
/// charged no further and gets the whole history, which goes on once the
/// other's 30 s are spent.
#[tokio::test(start_paused = true)]
async fn each_stopped_reader_has_its_own_30s() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 5000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (stopped, mut stopped_stream) = backfilling_client(&dm).await;
    let (resumed, mut stream) = backfilling_client(&dm).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_secs(10)).await;

    let started = tokio::time::Instant::now();
    let (prints, gaps) = read_prints(&mut stream, 5000).await;
    assert_eq!((prints.len(), gaps), (5000, 0));
    let waited = started.elapsed();
    assert!(waited >= Duration::from_secs(20), "waited {waited:?}");
    assert!(waited < Duration::from_secs(21), "waited {waited:?}");

    read_prints(&mut stopped_stream, 1000).await;
    ctrl.push("AAPL", trade("AAPL", 9000)).await;
    read_to_gap(&mut stopped_stream, 9000).await;
    let _ = stopped.close().await;
    let _ = resumed.close().await;
}

/// Charges the first reader out while the second reads, leaving the second
/// stopped as the replay's only pacer.
async fn charge_out_first_of(
    dm: &Datamancer,
    gate: &tokio::sync::Notify,
) -> (
    (datamancer::ClientSession, datamancer::EventStream),
    (datamancer::ClientSession, datamancer::EventStream),
) {
    let first = backfilling_client(dm).await;
    let mut second = backfilling_client(dm).await;
    gate.notify_one();
    let (prints, gaps) = read_prints(&mut second.1, 5000).await;
    assert_eq!((prints.len(), gaps), (5000, 0));
    (first, second)
}

/// Reads to the print stamped `tail`, checking that every history print up to
/// `last` was delivered in order or reported in a `Gap`.
async fn read_all_accounted(mut stream: datamancer::EventStream, last: i64, tail: i64) {
    let mut seen = 0;
    let mut gaps: Vec<(i64, i64)> = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(40), stream.next());
        match next.await.expect("prints stopped").expect("stream ended") {
            MarketEvent::Trade(t) if t.source_ts.0 == tail => break,
            MarketEvent::Trade(t) => {
                let ts = t.source_ts.0;
                let missed = seen + 1..ts;
                assert!(ts > seen, "{ts} came after {seen}");
                for lost in missed {
                    assert!(
                        gaps.iter().any(|g| (g.0..g.1).contains(&lost)),
                        "{lost} neither delivered nor reported"
                    );
                }
                seen = ts;
            }
            MarketEvent::Control(c) => {
                if let ControlKind::Gap { span, .. } = c.kind {
                    gaps.push((span.from_source_ts.0, span.to_source_ts.0));
                }
            }
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert!(
        seen == last || gaps.iter().any(|g| (g.0..g.1).contains(&last)),
        "the history ended at {seen}, gaps {gaps:?}"
    );
}

/// The replay has ended: a joiner hears no backfill is in flight.
async fn assert_replay_over(dm: &Datamancer) {
    let joined = dm
        .client_session()
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::default(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(
        joined,
        Subscribed::joined(LiveScope::backfill(Timestamp(0)))
    );
}

/// The replay waits on the last pacer while a reader that has stopped pacing
/// has room: it is charged like any other, so that reader gets the rest of the
/// history, what it missed as a `Gap`, and the live tail.
#[tokio::test(start_paused = true)]
async fn an_unpaced_reader_is_not_starved_by_the_last_pacer() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 20000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let ((charged, stream), (last_pacer, _unread)) = charge_out_first_of(&dm, &gate).await;

    let reader = tokio::spawn(read_all_accounted(stream, 20000, 90000));
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_replay_over(&dm).await;
    ctrl.push("AAPL", trade("AAPL", 90000)).await;
    reader.await.unwrap();
    drop(charged);
    drop(last_pacer);
}

/// A stopped reader whose only co-subscriber leaves is still waited on, and
/// stops pacing once that wait reaches 30 s in all.
#[tokio::test(start_paused = true)]
async fn a_reader_left_alone_goes_on_being_charged() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 5000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (stopped, mut stream) = backfilling_client(&dm).await;
    let (leaver, mut leaver_stream) = backfilling_client(&dm).await;
    gate.notify_one();
    read_prints(&mut leaver_stream, 2000).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    drop(leaver_stream);
    leaver.close().await.unwrap();
    tokio::time::sleep(Duration::from_secs(26)).await;
    assert_replay_over(&dm).await;

    read_prints(&mut stream, 1000).await;
    ctrl.push("AAPL", trade("AAPL", 9000)).await;
    read_to_gap(&mut stream, 9000).await;
    let _ = stopped.close().await;
}

/// A reader leaving while the replay waits on it ends the wait at once: the
/// co-subscriber gets everything and the session reaches its live loop.
#[tokio::test(start_paused = true)]
async fn a_replay_goes_on_when_the_reader_it_waits_on_leaves() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 5000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (stopped, stopped_stream) = backfilling_client(&dm).await;
    let (reader, mut stream) = backfilling_client(&dm).await;
    gate.notify_one();
    let (mut prints, gaps) = read_prints(&mut stream, 2000).await;
    assert_eq!(gaps, 0);
    tokio::time::sleep(Duration::from_secs(5)).await;

    let started = tokio::time::Instant::now();
    drop(stopped_stream);
    stopped.close().await.unwrap();
    let (rest, gaps) = read_prints(&mut stream, 3000).await;
    assert_eq!(gaps, 0);
    prints.extend(rest);
    assert_eq!(prints, (1..=5000).collect::<Vec<_>>());
    assert!(started.elapsed() < Duration::from_secs(1));

    tokio::time::sleep(Duration::from_millis(10)).await;
    ctrl.push("AAPL", trade("AAPL", 9000)).await;
    assert_eq!(read_prints(&mut stream, 1).await, (vec![9000], 0));
    let joined = dm
        .client_session()
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::default(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(
        joined,
        Subscribed::joined(LiveScope::backfill(Timestamp(0)))
    );
    let _ = reader.close().await;
}

/// A subscriber joining while the seam flush waits gets what is left of it and
/// then the live tail, gapless and in `seq` order.
#[tokio::test]
async fn a_join_during_the_flush_gets_the_rest_in_order() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 1)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (creator, mut stream) = backfilling_client(&dm).await;
    for ts in 100..5100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let joiner = dm.client_session();
    let outcome = joiner
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::default(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Subscribed::joined(LiveScope::backfill(Timestamp(0))).with_backfill_in_flight()
    );
    let mut joined_stream = joiner.take_events().await.unwrap();
    ctrl.push("AAPL", trade("AAPL", 9000)).await;
    let tail = tokio::spawn(async move { read_through(&mut joined_stream, 9000).await });

    let (prints, gaps) = read_through(&mut stream, 9000).await;
    assert_eq!((prints.len(), gaps), (5002, 0));
    let (joined_prints, gaps) = tail.await.unwrap();
    assert_eq!(gaps, 0);
    assert!(
        joined_prints.len() > 1,
        "the join came before the flush ended"
    );
    assert!(joined_prints.windows(2).all(|p| p[1].1 == p[0].1 + 1));
    assert_eq!(
        joined_prints[..],
        prints[prints.len() - joined_prints.len()..]
    );
    assert_replay_over(&dm).await;
    let _ = creator.close().await;
    let _ = joiner.close().await;
}

/// The replay waits on a stopped creator for 30 s, then it stops pacing: the
/// joiner gets the rest in order and the session reaches live.
#[tokio::test(start_paused = true)]
async fn a_join_during_the_flush_of_a_stopped_creator_reaches_live() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 1)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (creator, _unread) = backfilling_client(&dm).await;
    for ts in 100..5100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let joiner = dm.client_session();
    let outcome = joiner
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::default(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Subscribed::joined(LiveScope::backfill(Timestamp(0))).with_backfill_in_flight()
    );
    let mut stream = joiner.take_events().await.unwrap();
    ctrl.push("AAPL", trade("AAPL", 9000)).await;

    let started = tokio::time::Instant::now();
    let (prints, gaps) = read_through(&mut stream, 9000).await;
    assert_eq!(gaps, 0);
    assert!(prints.windows(2).all(|p| p[1].1 == p[0].1 + 1));
    let waited = started.elapsed();
    assert!(waited >= Duration::from_secs(29), "waited {waited:?}");
    assert!(waited < Duration::from_secs(31), "waited {waited:?}");

    assert_replay_over(&dm).await;
    ctrl.push("AAPL", trade("AAPL", 9001)).await;
    assert_eq!(read_prints(&mut stream, 1).await, (vec![9001], 0));
    drop(creator);
    drop(joiner);
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
    let backfill = LiveScope::backfill(Timestamp(0));
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
        .subscribe(inst("AAPL"), EventKind::Trade, LiveScope::default(), none)
        .await
        .unwrap();
    assert_eq!(joined, Subscribed::joined(backfill));
    let _ = creator.close().await;
}

/// A client's backfill older than the limit is refused whether it would create
/// the session or join it, and a `Session` is not limited.
#[tokio::test]
async fn a_client_backfill_past_the_limit_is_refused() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 1)).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .max_client_backfill(Duration::from_hours(1))
        .build()
        .unwrap();
    let none = PersistenceOptions::none();
    let too_old = LiveScope::backfill(Timestamp(0));
    let refused = |outcome: Result<Subscribed>| {
        assert!(
            matches!(outcome, Err(datamancer::Error::BackfillTooOld { .. })),
            "{outcome:?}"
        );
    };
    let client = dm.client_session();
    refused(
        client
            .subscribe(inst("AAPL"), EventKind::Trade, too_old, none)
            .await,
    );

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    let recent = LiveScope::backfill(Timestamp(now - 1_000_000_000));
    client
        .subscribe(inst("AAPL"), EventKind::Trade, recent, none)
        .await
        .expect("inside the limit");
    refused(
        dm.client_session()
            .subscribe(inst("AAPL"), EventKind::Trade, too_old, none)
            .await,
    );
    let _ = client.close().await;

    dm.session(
        inst("MSFT"),
        EventKind::Trade,
        datamancer::Scope::live_backfill(Timestamp(0)),
        none,
    )
    .await
    .expect("a session is not limited");
}

/// Live arrivals keep landing while a paced seam flush waits, so the provider
/// never parks on a full channel, and they reach the consumer after the flush.
#[tokio::test]
async fn a_paced_seam_flush_keeps_taking_live_arrivals() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 1)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    for ts in 100..4100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The flush now waits on an unread stream; more than a channel's worth
    // lands behind it.
    tokio::time::timeout(Duration::from_secs(2), async {
        for ts in 5000..8000 {
            ctrl.push("AAPL", trade("AAPL", ts)).await;
        }
    })
    .await
    .expect("the provider must not park");

    let joined = tokio::time::timeout(
        Duration::from_secs(1),
        dm.client_session().subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::default(),
            PersistenceOptions::none(),
        ),
    )
    .await
    .expect("a join must not wait on the flush")
    .unwrap();
    assert!(matches!(joined, Subscribed::Joined { .. }));

    let (prints, gaps) = read_prints(&mut stream, 7001).await;
    assert_eq!(gaps, 0);
    assert!(prints.is_sorted());
    let _ = client.close().await;
}

/// A seam overflow's `Gap` costs a slow consumer nothing after it: every
/// survivor arrives.
#[tokio::test]
async fn a_slow_consumer_gets_every_survivor_of_a_seam_overflow() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 3000)).await;
    let attached = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.gate_history(attached.clone()).await;
    ctrl.pause_history_after(2900, gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(64)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    attached.notify_one();
    for ts in 5000..5200 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();

    let (prints, gaps) = read_prints(&mut stream, 3064).await;
    assert_eq!(gaps, 1);
    assert_eq!(prints.last(), Some(&5199));
    let _ = client.close().await;
}

/// Two readers that keep reading, one a beat behind the other, are held back
/// by that beat alone: neither stops pacing, however long the replay lasts,
/// and neither loses anything.
#[tokio::test(start_paused = true)]
async fn two_steady_readers_a_beat_apart_both_stay_paced() {
    async fn read_steadily(mut stream: datamancer::EventStream, beat: Duration) -> Vec<i64> {
        tokio::time::sleep(beat).await;
        let mut prints = Vec::new();
        while prints.len() < 6000 {
            let ev = next_ev(&mut stream).await;
            assert!(
                !matches!(&ev, MarketEvent::Control(c) if matches!(c.kind, ControlKind::Gap { .. })),
                "a steady reader lost events: {ev:?}"
            );
            if let MarketEvent::Trade(t) = ev {
                prints.push(t.source_ts.0);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        prints
    }

    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 6000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (first, first_stream) = backfilling_client(&dm).await;
    let (second, second_stream) = backfilling_client(&dm).await;
    gate.notify_one();

    let first_reader = tokio::spawn(read_steadily(first_stream, Duration::ZERO));
    let second_reader = tokio::spawn(read_steadily(second_stream, Duration::from_millis(1)));
    let whole = (1..=6000).collect::<Vec<_>>();
    assert_eq!(first_reader.await.unwrap(), whole);
    assert_eq!(second_reader.await.unwrap(), whole);
    let _ = first.close().await;
    let _ = second.close().await;
}

/// A consumer that keeps reading, too slowly, can hold a shared replay back for
/// a bounded time in all, however often it frees a slot.
#[tokio::test(start_paused = true)]
async fn a_slow_reader_cannot_hold_a_co_subscriber_back_for_long() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 5000)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let backfill = LiveScope::backfill(Timestamp(0));
    let none = PersistenceOptions::none();
    let slow = dm.client_session();
    slow.subscribe(inst("AAPL"), EventKind::Trade, backfill, none)
        .await
        .unwrap();
    let mut slow_stream = slow.take_events().await.unwrap();
    let reader = dm.client_session();
    reader
        .subscribe(inst("AAPL"), EventKind::Trade, backfill, none)
        .await
        .unwrap();
    let mut stream = reader.take_events().await.unwrap();
    gate.notify_one();
    let slow_reader = tokio::spawn(async move {
        while slow_stream.next().await.is_some() {
            tokio::time::sleep(Duration::from_secs(29)).await;
        }
    });

    let started = tokio::time::Instant::now();
    let (prints, gaps) = read_prints(&mut stream, 5000).await;
    assert_eq!((prints.len(), gaps), (5000, 0));
    assert!(started.elapsed() < Duration::from_secs(40));
    slow_reader.abort();
    let _ = reader.close().await;
}

/// Taking a stream late hands over the buffered events without blocking the
/// client: a subscribe answers before the consumer reads, and everything still
/// arrives, in order.
#[tokio::test]
async fn a_late_take_keeps_the_client_answering() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 5000)).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let none = PersistenceOptions::none();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            none,
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut stream = client.take_events().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(
        Duration::from_secs(1),
        client.subscribe(inst("MSFT"), EventKind::Trade, LiveScope::default(), none),
    )
    .await
    .expect("subscribe must not wait on the consumer")
    .unwrap();

    let (prints, gaps) = read_prints(&mut stream, 5000).await;
    assert_eq!(gaps, 0);
    assert_eq!(prints, (1..=5000).collect::<Vec<_>>());
    let _ = client.close().await;
}

/// The resume buffer's occupancy counts what a late take left undelivered, and
/// falls as the consumer reads it.
#[tokio::test(start_paused = true)]
async fn a_late_take_reports_its_backlog_in_occupancy() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 3000)).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(3000)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let occupancy = |dm: &Datamancer| {
        dm.snapshot_live().client_sessions[0]
            .resume_buffer
            .occupancy
    };
    assert_eq!(occupancy(&dm), 3000, "detached ring holds the history");

    let mut stream = client.take_events().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        occupancy(&dm) > 0,
        "a backlog the consumer has not read is in the resume buffer"
    );

    let (prints, _) = read_prints(&mut stream, 3000).await;
    assert_eq!(prints.len(), 3000);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(occupancy(&dm), 0, "nothing left once read");
    let _ = client.close().await;
}

/// A client's own controls reach a reading consumer whatever the buffer's size.
#[tokio::test]
async fn an_unsubscribe_control_arrives_with_a_zero_resume_buffer() {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(0)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::default(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    client
        .unsubscribe(inst("AAPL"), EventKind::Trade)
        .await
        .unwrap();
    let ev = next_ev(&mut stream).await;
    assert!(
        matches!(
            &ev,
            MarketEvent::Control(c)
                if matches!(c.kind, ControlKind::SubscriptionChanged { active: false, .. })
        ),
        "{ev:?}"
    );
    let _ = client.close().await;
}

/// A client whose stream is full and unread still answers: its next subscribe
/// waits on nothing, and its close on the consumer for a bounded time.
#[tokio::test]
async fn a_client_with_an_unread_stream_still_answers() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 5000)).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let none = PersistenceOptions::none();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            none,
        )
        .await
        .unwrap();
    let _unread = client.take_events().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::time::timeout(
        Duration::from_secs(1),
        client.subscribe(inst("MSFT"), EventKind::Trade, LiveScope::default(), none),
    )
    .await
    .expect("subscribe must not wait on the stream")
    .unwrap();
    tokio::time::timeout(Duration::from_secs(4), client.close())
        .await
        .expect("close must stop waiting on the stream")
        .unwrap();
}

/// A sole consumer slower than the feed cannot keep a seam flush going: once
/// the flush has lost a whole ring, the session reaches its live loop.
#[tokio::test(start_paused = true)]
async fn a_flush_ends_when_the_feed_outruns_the_consumer() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 1)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(3000)
        .build()
        .unwrap();
    let backfill = LiveScope::backfill(Timestamp(0));
    let none = PersistenceOptions::none();
    let client = dm.client_session();
    client
        .subscribe(inst("AAPL"), EventKind::Trade, backfill, none)
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    for ts in 100..3100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for ts in 5000..12000 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    read_prints(&mut stream, 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let joined = dm
        .client_session()
        .subscribe(inst("AAPL"), EventKind::Trade, LiveScope::default(), none)
        .await
        .unwrap();
    assert_eq!(joined, Subscribed::joined(backfill));
}

/// A flush that has lost a whole ring ends with no help from the consumer: the
/// loss itself cuts a reader that never reads again loose.
#[tokio::test(start_paused = true)]
async fn a_flush_that_lost_a_ring_ends_for_a_stopped_reader() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 1)).await;
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(3000)
        .build()
        .unwrap();
    let (client, _unread) = backfilling_client(&dm).await;
    for ts in 100..3100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for ts in 5000..12000 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_replay_over(&dm).await;
    drop(client);
}

/// Past the whole-ring threshold, an arrival that evicts nothing costs a full
/// reader nothing: it stays paced and every survivor reaches it.
#[tokio::test(start_paused = true)]
async fn an_arrival_past_the_threshold_that_evicts_nothing_unpaces_nobody() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let attached = Arc::new(tokio::sync::Notify::new());
    let last_print = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 2000)).await;
    ctrl.gate_history(attached.clone()).await;
    ctrl.pause_history_after(1999, last_print.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(100)
        .build()
        .unwrap();
    let (stopped, _unread) = backfilling_client(&dm).await;
    let (reader, mut stream) = backfilling_client(&dm).await;
    attached.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    read_prints(&mut stream, 50).await;
    for ts in 5000..5100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    last_print.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The flush waits on the stopped reader. These lose a whole ring, which
    // stops it pacing; the flush then fills the reader and waits on it.
    for ts in 5100..5258 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    ctrl.push("AAPL", trade("AAPL", 9000)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (prints, _) = read_through(&mut stream, 9000).await;
    let survivors = &prints[prints.len() - 31..prints.len() - 1];
    assert!(survivors.windows(2).all(|p| p[1].0 == p[0].0 + 1));
    assert_eq!(survivors.last().map(|p| p.0), Some(5257));
    drop(stopped);
    drop(reader);
}

/// A tap log that fills the provider's channel at the first append and looks
/// at it `READ_AFTER` appends later.
#[derive(Default)]
struct ProbeTap {
    sink: std::sync::OnceLock<mpsc::Sender<MarketEvent>>,
    appended: AtomicUsize,
    read_meanwhile: AtomicBool,
    looked: AtomicBool,
}

impl ProbeTap {
    const READ_AFTER: usize = 500;
}

#[async_trait]
impl TapLog for ProbeTap {
    async fn append(&self, _ev: &MarketEvent) -> Result<()> {
        let sink = self.sink.get().expect("the sink is set");
        let appended = self.appended.fetch_add(1, Ordering::SeqCst);
        if appended == 0 {
            while sink.try_send(trade("AAPL", 9000)).is_ok() {}
        } else if appended == Self::READ_AFTER {
            self.read_meanwhile
                .store(sink.capacity() > 0, Ordering::SeqCst);
            self.looked.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
    fn as_replay_source(&self) -> Box<dyn ReplaySource> {
        unreachable!("the test replays nothing")
    }
}

/// A flush with room for everything still reads the provider as it goes.
#[tokio::test]
async fn a_flush_with_room_still_reads_arrivals() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(trades_from(1, 1)).await;
    ctrl.gate_history(gate.clone()).await;
    let tap = Arc::new(ProbeTap::default());
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .tap_log_arc(tap.clone())
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none().with_tap_log(true),
        )
        .await
        .unwrap();
    let _unread = client.take_events().await.unwrap();
    for ts in 100..1600 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    let sink = ctrl.shared.lock().await.sinks.get("AAPL").cloned();
    tap.sink.set(sink.expect("subscribed")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.notify_one();

    wait_until(|| tap.looked.load(Ordering::SeqCst)).await;
    assert!(tap.read_meanwhile.load(Ordering::SeqCst));
    drop(client);
}

/// A little overflow in a flush costs a reader that keeps up only what was
/// evicted: pacing holds, every survivor arrives, and the hole is the loss.
#[tokio::test]
async fn a_small_flush_overflow_keeps_a_reader_paced() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 2300)).await;
    let attached = Arc::new(tokio::sync::Notify::new());
    ctrl.gate_history(attached.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .resume_buffer_events(100)
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    attached.notify_one();
    for ts in 5000..5100 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    // Reading this much lets the history finish, and the flush begins full.
    tokio::time::sleep(Duration::from_millis(100)).await;
    read_prints(&mut stream, 270).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for ts in 5100..5140 {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut trades = Vec::new();
    let mut gaps = 0;
    while trades.len() < 2130 {
        let next = tokio::time::timeout(Duration::from_secs(5), stream.next());
        match next.await.expect("prints stopped").expect("stream ended") {
            MarketEvent::Trade(t) => trades.push((t.source_ts.0, t.seq.0)),
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::Gap { .. }) => gaps += 1,
            _ => {}
        }
    }
    assert_eq!(gaps, 1);
    let last_history = trades.iter().find(|t| t.0 == 2300).expect("history").1;
    for (ts, seq) in trades.iter().filter(|t| t.0 >= 5000) {
        let slot = u64::try_from(ts - 5000).unwrap() + 1;
        assert_eq!(
            *seq,
            last_history + slot,
            "a lost print leaves its slot empty"
        );
    }
    let _ = client.close().await;
}

/// A sole reader that never reads again stops pacing once the replay has
/// waited on it for 30 s: the backfill ends, and the reader gets what it
/// missed as a `Gap` ahead of the live tail.
#[tokio::test(start_paused = true)]
async fn a_stopped_sole_reader_stops_pacing_after_30s() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 5000)).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = backfilling_client(&dm).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_replay_over(&dm).await;

    let (_, gaps) = read_prints(&mut stream, 1000).await;
    assert_eq!(gaps, 0, "the first prints were delivered");
    ctrl.push("AAPL", trade("AAPL", 9000)).await;
    read_to_gap(&mut stream, 9000).await;
    let _ = client.close().await;
}

/// A cache that answers `lookup` or `store` only when told to.
struct GatedCache {
    gate: Arc<tokio::sync::Notify>,
    gate_store: bool,
    stored: Arc<AtomicUsize>,
}

#[async_trait]
impl HistoricalCache for GatedCache {
    async fn lookup(&self, _key: &CacheKey) -> Result<Option<CacheCoverage>> {
        if !self.gate_store {
            self.gate.notified().await;
        }
        Ok(None)
    }
    async fn store(&self, _key: &CacheKey, _events: &[MarketEvent]) -> Result<()> {
        if self.gate_store {
            self.gate.notified().await;
        }
        self.stored.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn as_replay_source(&self, _key: CacheKey) -> Box<dyn ReplaySource> {
        unreachable!("the gate holds the backfill before any replay")
    }
}

/// A backfill waiting on its cache still answers a joiner.
#[tokio::test]
async fn a_join_is_answered_while_the_backfill_waits_on_its_cache() {
    join_during_cache_wait(false).await;
    join_during_cache_wait(true).await;
}

async fn join_during_cache_wait(gate_store: bool) {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .historical_cache(Box::new(GatedCache {
            gate: gate.clone(),
            gate_store,
            stored: Arc::default(),
        }))
        .build()
        .unwrap();
    let creator = dm.client_session();
    creator
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let joiner = dm.client_session();
    let outcome = tokio::time::timeout(
        Duration::from_secs(1),
        joiner.subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::default(),
            PersistenceOptions::none(),
        ),
    )
    .await
    .expect("the join must not wait on the cache")
    .unwrap();
    assert!(matches!(outcome, Subscribed::Joined { .. }));
    gate.notify_one();
    let _ = joiner.close().await;
    let _ = creator.close().await;
}

/// A backfill waiting on its cache keeps reading the provider, so the shared
/// hub never parks, and the arrivals reach the seam ring.
#[tokio::test]
async fn live_arrivals_are_read_while_the_cache_answers() {
    arrivals_during_cache_wait(false).await;
    arrivals_during_cache_wait(true).await;
}

async fn arrivals_during_cache_wait(gate_store: bool) {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 1)).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .historical_cache(Box::new(GatedCache {
            gate: gate.clone(),
            gate_store,
            stored: Arc::default(),
        }))
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    tokio::time::timeout(Duration::from_secs(2), async {
        for ts in 100..2148 {
            ctrl.push("AAPL", trade("AAPL", ts)).await;
        }
    })
    .await
    .expect("the provider must not park");
    gate.notify_one();

    let (prints, gaps) = read_prints(&mut stream, 2049).await;
    assert_eq!(gaps, 0);
    let expected: Vec<i64> = std::iter::once(1).chain(100..2148).collect();
    assert_eq!(prints, expected);
    let _ = client.close().await;
}

/// A close during a held store releases the provider at once, with nothing
/// waiting on the cache, and the store still reaches its end.
#[tokio::test]
async fn a_close_during_a_held_store_leaves_the_provider_free() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.set_history(trades_from(1, 1)).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    let stored = Arc::new(AtomicUsize::new(0));
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .historical_cache(Box::new(GatedCache {
            gate: gate.clone(),
            gate_store: true,
            stored: stored.clone(),
        }))
        .build()
        .unwrap();
    let client = dm.client_session();
    client
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        for ts in 100..2148 {
            ctrl.push("AAPL", trade("AAPL", ts)).await;
        }
    })
    .await
    .expect("the provider must not park");
    wait_until(|| ctrl.unsubscribes() == 1).await;
    assert_eq!(stored.load(Ordering::SeqCst), 0);

    gate.notify_one();
    wait_until(|| stored.load(Ordering::SeqCst) == 1).await;
}

fn historical() -> Scope {
    Scope::Historical {
        from: Timestamp(0),
        to: Timestamp(10),
    }
}

/// A historical session answers a close while its cache plans the range.
#[tokio::test]
async fn a_historical_close_is_answered_while_the_cache_plans() {
    let (provider, _ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .historical_cache(Box::new(GatedCache {
            gate: Arc::default(),
            gate_store: false,
            stored: Arc::default(),
        }))
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            historical(),
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_secs(1), session.close())
        .await
        .expect("the close must not wait on the cache")
        .unwrap();
}

/// A historical session queued behind another's fetch answers a close.
#[tokio::test]
async fn a_close_queued_behind_the_fetch_slot_is_answered() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.gate_history(gate.clone()).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .historical_cache(Box::new(GatedCache {
            gate: Arc::default(),
            gate_store: true,
            stored: Arc::default(),
        }))
        .build()
        .unwrap();
    let cached = PersistenceOptions::cached();
    let holder = dm
        .session(inst("AAPL"), EventKind::Trade, historical(), cached)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let queued = dm
        .session(inst("AAPL"), EventKind::Trade, historical(), cached)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_secs(1), queued.close())
        .await
        .expect("the close must not wait on the slot")
        .unwrap();
    drop(holder);
}

/// The seam ring keeps the provider's controls when its prints overflow it: the
/// stream still carries the session's `SubscriptionChanged`, a joiner after
/// the backfill gets it replayed, and the controls cost no print its place.
#[tokio::test]
async fn a_seam_overflow_keeps_the_subscription_state_for_every_subscriber() {
    let (provider, ctrl) = FakeProvider::new("fake", true);
    let gate = Arc::new(tokio::sync::Notify::new());
    ctrl.set_history(vec![trade("AAPL", 1), trade("AAPL", 2)])
        .await;
    ctrl.gate_history(gate.clone()).await;
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
            LiveScope::backfill(Timestamp(0)),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = client.take_events().await.unwrap();

    // The connect and subscription controls lead the ring; four prints fill
    // it and a fifth evicts print 100, not a control.
    for ts in [100, 200, 300, 400, 500] {
        ctrl.push("AAPL", trade("AAPL", ts)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();

    let mut trades = Vec::new();
    let mut subscription_seq = None;
    while trades.len() < 6 {
        match next_ev(&mut stream).await {
            MarketEvent::Trade(t) => trades.push((t.source_ts.0, t.seq.0)),
            MarketEvent::Control(c) => {
                if matches!(
                    c.kind,
                    ControlKind::SubscriptionChanged { active: true, .. }
                ) {
                    subscription_seq = Some(c.seq.0);
                }
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(subscription_seq, Some(4), "{trades:?}");
    assert_eq!(
        trades,
        vec![(1, 0), (2, 1), (200, 5), (300, 6), (400, 7), (500, 8)],
        "the Gap takes slot 2, the connect 3, the subscription 4"
    );

    let joiner = dm.client_session();
    joiner
        .subscribe(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut replayed = joiner.take_events().await.unwrap();
    match next_ev(&mut replayed).await {
        MarketEvent::Control(c) => assert!(
            matches!(
                c.kind,
                ControlKind::SubscriptionChanged { active: true, .. }
            ),
            "{c:?}"
        ),
        other => panic!("expected the replayed subscription state, got {other:?}"),
    }
    let _ = joiner.close().await;
    let _ = client.close().await;
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
    let backfill = LiveScope::backfill(Timestamp(0));

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
        Subscribed::joined(backfill).with_backfill_in_flight(),
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
    let outcome = second
        .subscribe(
            inst("MSFT"),
            EventKind::Trade,
            backfill,
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    assert!(
        joined_as(outcome, live(), false),
        "a backfill request on a pure-live pair comes back pure-live: {outcome:?}"
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
    let backfill = LiveScope::backfill(Timestamp(0));

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
    assert_eq!(
        outcome,
        Subscribed::joined(backfill).with_backfill_in_flight()
    );
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
    assert!(
        joined_as(joined, live(), false),
        "the second subscriber joins the open session: {joined:?}"
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
    let backfill = LiveScope::backfill(future);

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
            Scope::Live(backfill),
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
    assert!(joined_as(outcome, live(), false), "{outcome:?}");
    let _ = other.close().await;
    let _ = client.close().await;
}

/// A retried backfill over the same range that fails the way the first did
/// is a second fetch failing, and its `ProviderError` must reach the client:
/// a `Subscription` error is always delivered, and without it the failure is
/// indistinguishable from an empty range.
#[tokio::test]
async fn a_retried_backfill_failing_the_same_way_is_delivered() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.shared.lock().await.history_error = Some("HTTP 403".to_string());
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    let mut stream = client.take_events().await.unwrap();
    for (attempt, sentinel) in [(1usize, 100i64), (2, 200)] {
        client
            .subscribe(
                inst("AAPL"),
                EventKind::Trade,
                LiveScope::backfill(Timestamp(0)),
                PersistenceOptions::none(),
            )
            .await
            .unwrap();
        wait_for_fetches(&ctrl, attempt).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        ctrl.push("AAPL", trade("AAPL", sentinel)).await;
        assert_eq!(
            errors_until(&mut stream, sentinel).await,
            1,
            "attempt {attempt}: the failed fetch's ProviderError reached the client"
        );
        client
            .unsubscribe(inst("AAPL"), EventKind::Trade)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = client.close().await;
}

/// One error, then one reconnect, each fanned out to both substreams; AAPL's
/// copies are read before MSFT's. MSFT's copy of the pre-reconnect error is
/// a copy of a fan-out the client delivered and must be suppressed.
#[tokio::test]
async fn a_pre_reconnect_error_copy_after_the_reconnect_is_dropped() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = two_symbol_client(&dm).await;
    let e0 = fanout("HTTP 403");
    ctrl.push("AAPL", fanout_error(&e0, 10)).await;
    ctrl.push("AAPL", disconnected(11)).await;
    ctrl.push("AAPL", connected(12)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    assert_eq!(errors_until(&mut stream, 100).await, 1);
    // MSFT's copies of the same error and the same reconnect.
    ctrl.push("MSFT", fanout_error(&e0, 10)).await;
    ctrl.push("MSFT", disconnected(11)).await;
    ctrl.push("MSFT", connected(12)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    assert_eq!(
        errors_until(&mut stream, 200).await,
        0,
        "the second substream's copy of one error is dropped"
    );
    let _ = client.close().await;
}

/// Two failures with the same text, each fanned out to both substreams
/// as two fan-outs; AAPL's channel is drained before MSFT's. MSFT's copies
/// carry fan-out ids the client already delivered on another instrument
/// (`datamancer_core::Seq` docs) and must be dropped.
#[tokio::test]
async fn same_text_errors_interleaved_copies_are_dropped() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = two_symbol_client(&dm).await;
    let [e0, e1] = [fanout("re-subscribe failed"), fanout("re-subscribe failed")];
    ctrl.push("AAPL", fanout_error(&e0, 10)).await;
    ctrl.push("AAPL", fanout_error(&e1, 20)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    let _ = errors_until(&mut stream, 100).await;
    ctrl.push("MSFT", fanout_error(&e0, 10)).await;
    ctrl.push("MSFT", fanout_error(&e1, 20)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    assert_eq!(
        errors_until(&mut stream, 200).await,
        0,
        "MSFT's copies of errors already delivered on AAPL are dropped"
    );
    let _ = client.close().await;
}

/// Two distinct errors, each fanned out to both substreams; AAPL's channel
/// is drained before MSFT's. MSFT's copies must be dropped.
#[tokio::test]
async fn distinct_errors_interleaved_copies_are_dropped() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = two_symbol_client(&dm).await;
    let [e0, e1] = [fanout("error A"), fanout("error B")];
    ctrl.push("AAPL", fanout_error(&e0, 10)).await;
    ctrl.push("AAPL", fanout_error(&e1, 20)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    assert_eq!(errors_until(&mut stream, 100).await, 2);
    ctrl.push("MSFT", fanout_error(&e0, 10)).await;
    ctrl.push("MSFT", fanout_error(&e1, 20)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    assert_eq!(
        errors_until(&mut stream, 200).await,
        0,
        "MSFT's copies of errors already delivered on AAPL are dropped"
    );
    let _ = client.close().await;
}

/// Two backfills on two instruments, both from 0, failing the same way:
/// each is its own fetch failure, a `Subscription` error, so both reach the
/// client.
#[tokio::test]
async fn adv_two_instruments_backfill_failures_both_delivered() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    ctrl.shared.lock().await.history_error = Some("HTTP 403".to_string());
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    let mut stream = client.take_events().await.unwrap();
    for (n, symbol) in [(1usize, "AAPL"), (2, "MSFT")] {
        client
            .subscribe(
                inst(symbol),
                EventKind::Trade,
                LiveScope::backfill(Timestamp(0)),
                PersistenceOptions::none(),
            )
            .await
            .unwrap();
        wait_for_fetches(&ctrl, n).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    let (mut errors, mut seen) = (0, 0);
    while seen < 2 {
        match next_ev(&mut stream).await {
            MarketEvent::Trade(t) if t.source_ts.0 == 100 || t.source_ts.0 == 200 => seen += 1,
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::ProviderError { .. }) => {
                errors += 1;
            }
            _ => {}
        }
    }
    assert_eq!(
        errors, 2,
        "each instrument's failed backfill keeps its ProviderError"
    );
    let _ = client.close().await;
}

/// 17 distinct fan-outs reach both substreams; AAPL's channel is drained
/// before MSFT's. However many are in flight, MSFT's copies are dropped.
#[tokio::test]
async fn adv_seventeen_interleaved_error_copies_are_dropped() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let (client, mut stream) = two_symbol_client(&dm).await;
    let ids: [ControlKind; 17] = std::array::from_fn(|i| fanout(&format!("error {i}")));
    for (i, id) in (0i64..).zip(&ids) {
        ctrl.push("AAPL", fanout_error(id, 10 + i)).await;
    }
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    assert_eq!(errors_until(&mut stream, 100).await, 17);
    for (i, id) in (0i64..).zip(&ids) {
        ctrl.push("MSFT", fanout_error(id, 10 + i)).await;
    }
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    assert_eq!(
        errors_until(&mut stream, 200).await,
        0,
        "MSFT's copies of errors already delivered on AAPL are dropped"
    );
    let _ = client.close().await;
}

/// A subscription made while a fan-out is in flight: the fan-out reaches the
/// client once whichever substream carries it first, before or after the
/// subscription, and the next fan-out reaches it once too.
#[tokio::test]
async fn a_subscription_made_while_a_fanout_is_in_flight_gets_it_once() {
    let (provider, ctrl) = FakeProvider::new("fake", false);
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let client = dm.client_session();
    let subscribe = |symbol: &'static str| {
        client.subscribe(
            inst(symbol),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
    };
    subscribe("AAPL").await.unwrap();
    let mut stream = client.take_events().await.unwrap();
    let [unread, delivered, next] = [fanout("unread"), fanout("delivered"), fanout("next")];
    // In flight on AAPL, unread, when MSFT subscribes; MSFT carries it too.
    ctrl.push("AAPL", fanout_error(&unread, 10)).await;
    subscribe("MSFT").await.unwrap();
    ctrl.push("MSFT", fanout_error(&unread, 10)).await;
    ctrl.push("AAPL", trade("AAPL", 100)).await;
    ctrl.push("MSFT", trade("MSFT", 200)).await;
    assert_eq!(errors_until(&mut stream, 200).await, 1);
    // Delivered from AAPL before GOOG subscribes; GOOG still carries a copy.
    ctrl.push("AAPL", fanout_error(&delivered, 20)).await;
    ctrl.push("MSFT", fanout_error(&delivered, 20)).await;
    ctrl.push("AAPL", trade("AAPL", 300)).await;
    assert_eq!(errors_until(&mut stream, 300).await, 1);
    ctrl.push("MSFT", trade("MSFT", 400)).await;
    assert_eq!(errors_until(&mut stream, 400).await, 0);
    subscribe("GOOG").await.unwrap();
    ctrl.push("GOOG", fanout_error(&delivered, 20)).await;
    ctrl.push("GOOG", trade("GOOG", 500)).await;
    assert_eq!(errors_until(&mut stream, 500).await, 0);
    // The next fan-out reaches all three substreams and the client once.
    for symbol in ["GOOG", "MSFT", "AAPL"] {
        ctrl.push(symbol, fanout_error(&next, 30)).await;
    }
    ctrl.push("AAPL", trade("AAPL", 600)).await;
    assert_eq!(errors_until(&mut stream, 600).await, 1);
    ctrl.push("MSFT", trade("MSFT", 700)).await;
    assert_eq!(errors_until(&mut stream, 700).await, 0);
    ctrl.push("GOOG", trade("GOOG", 800)).await;
    assert_eq!(errors_until(&mut stream, 800).await, 0);
    let _ = client.close().await;
}

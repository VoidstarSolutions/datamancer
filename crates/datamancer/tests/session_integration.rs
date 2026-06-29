//! End-to-end Session tests using a fake provider. Exercises historical and
//! live scopes against the new per-(instrument, kind) Session surface.

#![cfg(feature = "storage-surreal")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::storage::{SurrealTapLog, SurrealTapLogConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, ControlKind, Datamancer, EventKind, Instrument, LiveHandle,
    MarketEvent, PersistenceOptions, Price, Provider, ProviderId, ReplayRequest, Result, Scope,
    Seq, TapLog, Timestamp, Trade,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};

// ---------------------------------------------------------------------------
// Fake provider
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeProviderState {
    sink: Option<mpsc::Sender<MarketEvent>>,
    history: Vec<MarketEvent>,
    /// When set, `fetch_history` returns this as a `Provider` error after
    /// sending whatever `history` is queued — models a provider that fails the
    /// fetch (missing credentials, auth rejection, mid-stream transport fault).
    history_error: Option<String>,
}

struct FakeProvider {
    id: String,
    state: Arc<Mutex<FakeProviderState>>,
    closed: Arc<AtomicBool>,
}

impl FakeProvider {
    fn new(id: &str) -> (Arc<Self>, FakeController) {
        let state = Arc::new(Mutex::new(FakeProviderState::default()));
        let closed = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(Self {
            id: id.to_string(),
            state: state.clone(),
            closed: closed.clone(),
        });
        let controller = FakeController {
            state: state.clone(),
            closed,
        };
        (provider, controller)
    }
}

struct FakeController {
    state: Arc<Mutex<FakeProviderState>>,
    #[allow(dead_code)]
    closed: Arc<AtomicBool>,
}

impl FakeController {
    async fn push_live(&self, ev: MarketEvent) {
        let guard = self.state.lock().await;
        if let Some(sink) = guard.sink.as_ref() {
            let _ = sink.send(ev).await;
        }
    }

    async fn set_history(&self, events: Vec<MarketEvent>) {
        self.state.lock().await.history = events;
    }

    async fn set_history_error(&self, message: &str) {
        self.state.lock().await.history_error = Some(message.to_string());
    }
}

#[async_trait]
impl Provider for FakeProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn supports(&self, _instrument: &Instrument, _kind: EventKind) -> bool {
        true
    }
    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let mut guard = self.state.lock().await;
        guard.sink = Some(sink);
        Ok(Box::new(FakeLiveHandle {
            state: self.state.clone(),
            closed: self.closed.clone(),
        }))
    }
    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let (history, history_error) = {
            let guard = self.state.lock().await;
            (guard.history.clone(), guard.history_error.clone())
        };
        for ev in history {
            if sink.send(ev).await.is_err() {
                break;
            }
        }
        if let Some(message) = history_error {
            return Err(datamancer::Error::Provider {
                provider: self.id.clone(),
                message,
            });
        }
        Ok(())
    }
}

struct FakeLiveHandle {
    state: Arc<Mutex<FakeProviderState>>,
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl LiveHandle for FakeLiveHandle {
    async fn subscribe(&self, _instrument: Instrument, _kind: EventKind) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _instrument: Instrument, _kind: EventKind) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        let mut guard = self.state.lock().await;
        guard.sink = None;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Construct an `Instrument` matching the `FakeProvider("fake")` used
/// throughout this suite. Tests can name a symbol without re-spelling the
/// full qualifying tuple at every callsite.
fn inst(symbol: &str) -> Instrument {
    Instrument::new(ProviderId::from_static("fake"), AssetClass::Equity, symbol)
}

fn trade(symbol: &str, ts: i64, price: f64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(symbol),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(price),
        size: 1,
    })
}

fn bar(symbol: &str, ts: i64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: inst(symbol),
        interval: BarInterval::OneMinute,
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        open: Price::from_f64_round(close),
        high: Price::from_f64_round(close),
        low: Price::from_f64_round(close),
        close: Price::from_f64_round(close),
        volume: 1,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn live_session_assigns_monotonic_seq_and_passes_events_through() {
    let (provider, ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut events = session.take_events().await.unwrap();

    ctrl.push_live(trade("AAPL", 100, 1.0)).await;
    ctrl.push_live(trade("AAPL", 200, 2.0)).await;
    ctrl.push_live(trade("AAPL", 300, 3.0)).await;

    let mut got = Vec::new();
    for _ in 0..3 {
        let ev = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        got.push(ev);
    }
    let seqs: Vec<u64> = got
        .iter()
        .map(|e| match e {
            MarketEvent::Trade(t) => t.seq.0,
            _ => panic!("unexpected event"),
        })
        .collect();
    assert_eq!(seqs, vec![0, 1, 2]);
    let _ = session.close().await;
}

#[tokio::test]
async fn historical_session_streams_provider_fetch_in_order() {
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_history(vec![
        bar("AAPL", 100, 10.0),
        bar("AAPL", 200, 11.0),
        bar("AAPL", 300, 12.0),
    ])
    .await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    let mut stream = session.take_events().await.unwrap();
    let mut bars = Vec::new();
    for _ in 0..3 {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        match ev {
            MarketEvent::Bar(b) => bars.push(b),
            other => panic!("expected Bar, got {other:?}"),
        }
    }
    assert_eq!(bars[0].source_ts.0, 100);
    assert_eq!(bars[1].source_ts.0, 200);
    assert_eq!(bars[2].source_ts.0, 300);
    // seq is session-monotonic per (instrument, kind).
    assert_eq!(bars[0].seq.0, 0);
    assert_eq!(bars[1].seq.0, 1);
    assert_eq!(bars[2].seq.0, 2);
}

#[tokio::test]
async fn historical_session_surfaces_provider_fetch_error_as_control() {
    // A provider whose `fetch_history` returns `Err` (e.g. missing
    // credentials) must surface the failure in-band as a `ProviderError`
    // control rather than ending the stream silently. The non-cached
    // historical path spawns the fetch detached, so this is the seam that
    // would otherwise drop the error.
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_history_error("REST client not initialized (credentials missing?)")
        .await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    let mut stream = session.take_events().await.unwrap();
    let mut controls = Vec::new();
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
        if let MarketEvent::Control(c) = ev {
            controls.push(c.kind);
        }
    }

    let provider_error = controls.iter().find_map(|kind| match kind {
        ControlKind::ProviderError { message, .. } => Some(message.clone()),
        _ => None,
    });
    assert_eq!(
        provider_error.as_deref(),
        Some("REST client not initialized (credentials missing?)"),
        "expected a ProviderError control carrying the fetch failure; got {controls:?}"
    );
}

#[tokio::test]
async fn persist_true_without_persistence_layer_errors() {
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    match dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::cached(),
        )
        .await
    {
        Err(datamancer::Error::PersistenceRequired) => {}
        Err(other) => panic!("expected PersistenceRequired, got {other:?}"),
        Ok(_) => panic!("expected PersistenceRequired, got Ok"),
    }
}

#[tokio::test]
async fn second_live_open_shares_authoritative_session() {
    // Phase 2: a second live open for the same pair no longer conflicts — it
    // shares the one authoritative session. Both referrers observe the same
    // events with identical (seq, source_ts), carrying the Phase-1 source-stamp
    // guarantee through the share.
    let (provider, ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let first = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let second = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    let mut sa = first.take_events().await.unwrap();
    let mut sb = second.take_events().await.unwrap();

    ctrl.push_live(trade("AAPL", 100, 1.0)).await;
    ctrl.push_live(trade("AAPL", 200, 2.0)).await;

    for stream in [&mut sa, &mut sb] {
        let mut got = Vec::new();
        while got.len() < 2 {
            let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap();
            if let MarketEvent::Trade(t) = ev {
                got.push((t.source_ts.0, t.seq.0));
            }
        }
        // Identical (seq, source_ts) across both shared referrers.
        assert_eq!(got, vec![(100, 0), (200, 1)]);
    }
    let _ = first.close().await;
    let _ = second.close().await;
}

#[tokio::test]
async fn live_session_conflict_clears_when_first_is_dropped() {
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let first = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    drop(first);

    // After drop the registry slot is free and a new Live session opens.
    let _second = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn live_session_conflict_clears_when_first_is_closed() {
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let first = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    first.close().await.unwrap();

    let _second = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn historical_sessions_for_same_pair_are_concurrent() {
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_history(vec![bar("AAPL", 100, 10.0), bar("AAPL", 200, 11.0)])
        .await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    // Two concurrent Historical sessions for the same pair both succeed and
    // each independently receive the full fake history.
    let a = dm
        .session(
            inst("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let b = dm
        .session(
            inst("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    let mut sa = a.take_events().await.unwrap();
    let mut sb = b.take_events().await.unwrap();
    for stream in [&mut sa, &mut sb] {
        let mut tss = Vec::new();
        for _ in 0..2 {
            let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap();
            match ev {
                MarketEvent::Bar(b) => tss.push(b.source_ts.0),
                other => panic!("expected Bar, got {other:?}"),
            }
        }
        assert_eq!(tss, vec![100, 200]);
    }
}

#[tokio::test]
async fn historical_and_live_sessions_for_same_pair_coexist() {
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_history(vec![bar("AAPL", 100, 10.0)]).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    // Historical and Live for the same pair share no registry slot — different
    // EventKind for one (Bar vs Trade) and even same-kind would be allowed
    // since Historical doesn't participate in the registry.
    let _hist = dm
        .session(
            inst("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let _live = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    drop(ctrl);
}

#[tokio::test]
async fn live_stream_retake_resumes_with_contiguous_seq() {
    let (provider, ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    // First take: receive two events (guarantees the controller processed them).
    let mut first = session.take_events().await.unwrap();
    ctrl.push_live(trade("AAPL", 100, 1.0)).await;
    ctrl.push_live(trade("AAPL", 200, 2.0)).await;
    let mut seqs = Vec::new();
    for _ in 0..2 {
        let ev = tokio::time::timeout(Duration::from_secs(2), first.next())
            .await
            .unwrap()
            .unwrap();
        if let MarketEvent::Trade(t) = ev {
            seqs.push((t.source_ts.0, t.seq.0));
        }
    }
    drop(first);

    // Events arriving while detached are buffered (or delivered on re-attach
    // if the take command wins the race — either way order and seq hold).
    ctrl.push_live(trade("AAPL", 300, 3.0)).await;
    ctrl.push_live(trade("AAPL", 400, 4.0)).await;

    let mut second = session.take_events().await.unwrap();
    for _ in 0..2 {
        let ev = tokio::time::timeout(Duration::from_secs(2), second.next())
            .await
            .unwrap()
            .unwrap();
        match ev {
            MarketEvent::Trade(t) => seqs.push((t.source_ts.0, t.seq.0)),
            other => panic!("expected Trade (no Gap on a clean re-take), got {other:?}"),
        }
    }
    // Arrival order preserved, seq contiguous across the re-take, no Gap.
    assert_eq!(seqs, vec![(100, 0), (200, 1), (300, 2), (400, 3)]);
    let _ = session.close().await;
}

#[tokio::test]
async fn historical_session_with_no_consumer_terminates() {
    // Regression for the run_historical hang: if the consumer never calls
    // take_events, the controller used to wait on events_tx.closed() forever
    // because the receiver is still parked in events_holder. After the fix,
    // close() returns promptly even though the stream was never taken.
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_history(vec![bar("AAPL", 100, 10.0), bar("AAPL", 200, 11.0)])
        .await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    // Don't call take_events. Closing should return promptly.
    tokio::time::timeout(Duration::from_secs(2), session.close())
        .await
        .expect("close should return promptly when stream was never taken")
        .unwrap();
}

#[tokio::test]
async fn take_events_twice_concurrently_errors() {
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let _stream = session.take_events().await.unwrap();
    match session.take_events().await {
        Err(datamancer::Error::EventsAlreadyTaken) => {}
        Err(other) => panic!("expected EventsAlreadyTaken, got {other:?}"),
        Ok(_) => panic!("expected EventsAlreadyTaken, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// Tap-log helpers
// ---------------------------------------------------------------------------

async fn drain_n(stream: &mut datamancer::EventStream, n: usize) -> usize {
    let mut data = 0usize;
    while data < n {
        // Bound each poll like the rest of the suite, so a missing tee/forward
        // surfaces as a clean failure rather than hanging CI.
        let next = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("drain_n timed out waiting for events");
        match next {
            Some(MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_)) => {
                data += 1;
            }
            Some(_) => {}
            None => break,
        }
    }
    data
}

fn live_trade(symbol: &str, source_ts: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(symbol),
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(source_ts),
        seq: Seq(0),
        price: Price::from_f64_round(10.0),
        size: 1,
    })
}

// ---------------------------------------------------------------------------
// Tap-log tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn live_session_tees_data_events_to_tap_log() {
    let (provider, ctrl) = FakeProvider::new("fake");
    let log = std::sync::Arc::new(
        SurrealTapLog::open(SurrealTapLogConfig::Memory)
            .await
            .unwrap(),
    );
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .tap_log_arc(log.clone())
        .build()
        .unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none().with_tap_log(true),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.expect("take events");

    ctrl.push_live(live_trade("AAPL", 100)).await;
    ctrl.push_live(live_trade("AAPL", 200)).await;
    // Consuming from the stream implies forward() ran; forward() appends to the
    // tap log before sending downstream, so a flush now captures both.
    assert_eq!(drain_n(&mut stream, 2).await, 2);
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let mut replay = source
        .open(ReplayRequest {
            instruments: vec![inst("AAPL")],
            kinds: vec![EventKind::Trade],
            from: Timestamp(i64::MIN),
            to: Timestamp(i64::MAX),
        })
        .await
        .unwrap();
    let mut tss = Vec::new();
    while let Some(ev) = replay.next().await {
        if let MarketEvent::Trade(t) = ev {
            tss.push(t.source_ts.0);
        }
    }
    assert_eq!(tss, vec![100, 200]);
}

#[tokio::test]
async fn tap_log_disabled_captures_nothing() {
    let (provider, ctrl) = FakeProvider::new("fake");
    let log = std::sync::Arc::new(
        SurrealTapLog::open(SurrealTapLogConfig::Memory)
            .await
            .unwrap(),
    );
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .tap_log_arc(log.clone())
        .build()
        .unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(), // write_tap_log off
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.expect("take events");
    ctrl.push_live(live_trade("AAPL", 100)).await;
    assert_eq!(drain_n(&mut stream, 1).await, 1);
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let mut replay = source
        .open(ReplayRequest {
            instruments: vec![inst("AAPL")],
            kinds: vec![EventKind::Trade],
            from: Timestamp(i64::MIN),
            to: Timestamp(i64::MAX),
        })
        .await
        .unwrap();
    assert!(replay.next().await.is_none(), "nothing should be captured");
}

#[tokio::test]
async fn historical_take_events_stays_single_shot() {
    let (provider, ctrl) = FakeProvider::new("fake");
    ctrl.set_history(vec![bar("AAPL", 100, 10.0)]).await;
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let stream = session.take_events().await.unwrap();
    drop(stream);
    match session.take_events().await {
        Err(datamancer::Error::EventsAlreadyTaken) => {}
        Err(other) => panic!("expected EventsAlreadyTaken, got {other:?}"),
        Ok(_) => panic!("expected EventsAlreadyTaken, got Ok"),
    }
}

#[tokio::test]
async fn dropping_the_session_ends_a_held_stream() {
    let (provider, ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.unwrap();
    ctrl.push_live(trade("AAPL", 100, 1.0)).await;
    let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, MarketEvent::Trade(_)));

    // The Session handle is the lifecycle anchor: dropping it tears the
    // session down even though the stream is still held.
    drop(session);
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("controller should close the stream after Session drop");
        match ev {
            Some(MarketEvent::Control(c)) if c.kind == ControlKind::SessionClosing => {}
            Some(_) => {}
            None => break,
        }
    }
}

#[tokio::test]
async fn write_tap_log_without_a_log_is_rejected() {
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    match dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none().with_tap_log(true),
        )
        .await
    {
        Err(datamancer::Error::PersistenceRequired) => {}
        Err(other) => panic!("expected PersistenceRequired, got {other:?}"),
        Ok(_) => panic!("expected PersistenceRequired, got Ok"),
    }
}

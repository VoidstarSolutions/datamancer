//! End-to-end Session tests using a fake provider. Exercises historical and
//! live scopes against the new per-(instrument, kind) Session surface.
//!
//! The stitched-with-backfill seam is currently stubbed in the controller
//! (it emits a placeholder Gap rather than running the resume primitive),
//! so a corresponding test for that path is deferred until the resume
//! primitive lands alongside the registry.

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
        let history = self.state.lock().await.history.clone();
        for ev in history {
            if sink.send(ev).await.is_err() {
                break;
            }
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
    let mut session = dm
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
    let mut events = session.take_events().unwrap();

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

    let mut session = dm
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

    let mut stream = session.take_events().unwrap();
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
async fn live_with_backfill_emits_placeholder_seam_gap() {
    // The resume primitive isn't wired up yet; the controller emits a Gap
    // covering [backfill_from, now) so the placeholder is observable.
    // Replace this test with a real seam test once the resume primitive
    // lands (it should replay from persistence and only fall back to a Gap
    // when persistence can't cover the span).
    let (provider, ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(1_000)),
            },
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().unwrap();

    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    match first {
        MarketEvent::Control(c) => match c.kind {
            ControlKind::Gap {
                instrument, span, ..
            } => {
                assert_eq!(instrument.symbol(), "AAPL");
                assert_eq!(span.from_source_ts.0, 1_000);
            }
            other => panic!("expected Gap control, got {other:?}"),
        },
        other => panic!("expected Control, got {other:?}"),
    }

    // After the placeholder gap, live events flow normally.
    ctrl.push_live(trade("AAPL", 5_000, 1.0)).await;
    let next = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    match next {
        MarketEvent::Trade(t) => assert_eq!(t.source_ts.0, 5_000),
        other => panic!("expected live Trade, got {other:?}"),
    }

    let _ = session.close().await;
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
async fn live_session_conflict_rejects_second_live_session_for_same_pair() {
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    // First Live session reserves the (AAPL, Trade) registry slot.
    let _first = dm
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

    // Second Live session for the same pair is rejected.
    match dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(),
        )
        .await
    {
        Err(datamancer::Error::LiveSessionConflict { instrument, kind }) => {
            assert_eq!(instrument.symbol(), "AAPL");
            assert_eq!(kind, EventKind::Trade);
        }
        Err(other) => panic!("expected LiveSessionConflict, got {other:?}"),
        Ok(_) => panic!("expected LiveSessionConflict, got Ok"),
    }
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
    let mut a = dm
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
    let mut b = dm
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

    let mut sa = a.take_events().unwrap();
    let mut sb = b.take_events().unwrap();
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
async fn take_events_after_drop_returns_already_taken() {
    // Captures the documented single-shot semantics: dropping the EventStream
    // does not return the receiver to the slot, so re-take fails. When the
    // resume primitive lands this test should flip to assert successful re-take.
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let mut session = dm
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
    let stream = session.take_events().unwrap();
    drop(stream);
    match session.take_events() {
        Err(datamancer::Error::EventsAlreadyTaken) => {}
        Err(other) => panic!("expected EventsAlreadyTaken, got {other:?}"),
        Ok(_) => panic!("expected EventsAlreadyTaken, got Ok"),
    }
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
    let mut session = dm
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
    let _stream = session.take_events().unwrap();
    match session.take_events() {
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
        match stream.next().await {
            Some(MarketEvent::Trade(_) | MarketEvent::Quote(_) | MarketEvent::Bar(_)) => {
                data += 1;
            }
            Some(_) => {}
            None => break,
        }
    }
    data
}

fn equity(symbol: &str) -> Instrument {
    Instrument::new(ProviderId::from_static("fake"), AssetClass::Equity, symbol)
}

fn live_trade(symbol: &str, source_ts: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: equity(symbol),
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

    let mut session = dm
        .session(
            equity("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none().with_tap_log(true),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().expect("take events");

    ctrl.push_live(live_trade("AAPL", 100)).await;
    ctrl.push_live(live_trade("AAPL", 200)).await;
    // Consuming from the stream implies forward() ran; forward() appends to the
    // tap log before sending downstream, so a flush now captures both.
    assert_eq!(drain_n(&mut stream, 2).await, 2);
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let mut replay = source
        .open(ReplayRequest {
            instruments: vec![equity("AAPL")],
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

    let mut session = dm
        .session(
            equity("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::none(), // write_tap_log off
        )
        .await
        .unwrap();
    let mut stream = session.take_events().expect("take events");
    ctrl.push_live(live_trade("AAPL", 100)).await;
    assert_eq!(drain_n(&mut stream, 1).await, 1);
    log.flush().await.unwrap();

    let source = log.as_replay_source();
    let mut replay = source
        .open(ReplayRequest {
            instruments: vec![equity("AAPL")],
            kinds: vec![EventKind::Trade],
            from: Timestamp(i64::MIN),
            to: Timestamp(i64::MAX),
        })
        .await
        .unwrap();
    assert!(replay.next().await.is_none(), "nothing should be captured");
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
            equity("AAPL"),
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

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
use datamancer::{
    Bar, BarInterval, ControlKind, Datamancer, EventKind, Instrument, LiveHandle, MarketEvent,
    Price, Provider, Result, Scope, Seq, Timestamp, Trade,
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

fn trade(symbol: &str, ts: i64, price: f64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: Instrument::new(symbol),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(price),
        size: 1,
    })
}

fn bar(symbol: &str, ts: i64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: Instrument::new(symbol),
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
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();
    let mut session = dm
        .session(
            Instrument::new("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: None },
            false,
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
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();

    let mut session = dm
        .session(
            Instrument::new("AAPL"),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            false,
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
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();

    let mut session = dm
        .session(
            Instrument::new("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(1_000)),
            },
            false,
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
            ControlKind::Gap { instrument, span, .. } => {
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
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();
    match dm
        .session(
            Instrument::new("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: None },
            true, // persist
        )
        .await
    {
        Err(datamancer::Error::PersistenceRequired) => {}
        Err(other) => panic!("expected PersistenceRequired, got {other:?}"),
        Ok(_) => panic!("expected PersistenceRequired, got Ok"),
    }
}

#[tokio::test]
async fn take_events_twice_concurrently_errors() {
    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder().provider_arc(provider).build().unwrap();
    let mut session = dm
        .session(
            Instrument::new("AAPL"),
            EventKind::Trade,
            Scope::Live { backfill_from: None },
            false,
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

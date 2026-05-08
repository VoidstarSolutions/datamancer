//! End-to-end Session tests using a fake provider and the real Surreal
//! cache. Exercises live, replay, and stitched paths plus seam gap.

#![cfg(feature = "storage-surreal")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer::{
    Bar, BarInterval, CacheKey, ControlKind, Datamancer, EventKind, HistoricalCache, Instrument,
    LiveConfig, LiveHandle, MarketEvent, Price, Provider, ReplayConfig, ReplaySourceSpec, Result,
    Seq, StitchConfig, Subscription, Timestamp, Trade,
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
    async fn push(&self, ev: MarketEvent) {
        let guard = self.state.lock().await;
        if let Some(sink) = guard.sink.as_ref() {
            let _ = sink.send(ev).await;
        }
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
    async fn start_live(
        &self,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<Box<dyn LiveHandle>> {
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
        _sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        Ok(())
    }
}

struct FakeLiveHandle {
    state: Arc<Mutex<FakeProviderState>>,
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl LiveHandle for FakeLiveHandle {
    async fn subscribe(&self, _sub: Subscription) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _sub: Subscription) -> Result<()> {
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
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let mut session = dm
        .live(LiveConfig {
            initial_subscriptions: vec![Subscription::new("AAPL", [EventKind::Trade])],
            buffer_size: 64,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut events = session.take_events().unwrap();

    // Push three trades with strictly increasing source_ts.
    ctrl.push(trade("AAPL", 100, 1.0)).await;
    ctrl.push(trade("AAPL", 200, 2.0)).await;
    ctrl.push(trade("AAPL", 300, 3.0)).await;

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
async fn replay_from_surreal_cache_streams_in_order() {
    // Seed a Surreal cache with three bars for AAPL.
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let key = CacheKey {
        provider: "fake".to_string(),
        instrument: Instrument::new("AAPL"),
        kind: EventKind::Bar(BarInterval::OneMinute),
        from: Timestamp(0),
        to: Timestamp(1000),
    };
    let events = vec![bar("AAPL", 100, 10.0), bar("AAPL", 200, 11.0), bar("AAPL", 300, 12.0)];
    cache.store(&key, &events).await.unwrap();

    let (provider, _ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .historical_cache_arc(cache)
        .build()
        .unwrap();

    let mut session = dm
        .replay(ReplayConfig {
            source: ReplaySourceSpec::HistoricalCache,
            instruments: vec![Instrument::new("AAPL")],
            kinds: vec![EventKind::Bar(BarInterval::OneMinute)],
            from: Timestamp(0),
            to: Timestamp(1000),
        })
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
    // Replay sessions reject subscribe.
    assert!(
        session
            .subscribe(Subscription::new("MSFT", [EventKind::Trade]))
            .await
            .is_err()
    );
    let _ = session.close().await;
}

#[tokio::test]
async fn stitched_session_emits_seam_gap_then_live_events() {
    // Cache holds backfill events ending at ts=200; first live event is at
    // ts=500, so the controller should emit a Gap with span [200, 500).
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let key = CacheKey {
        provider: "fake".to_string(),
        instrument: Instrument::new("AAPL"),
        kind: EventKind::Trade,
        from: Timestamp(0),
        to: Timestamp(300),
    };
    cache
        .store(&key, &[trade("AAPL", 100, 1.0), trade("AAPL", 200, 2.0)])
        .await
        .unwrap();

    let (provider, ctrl) = FakeProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .historical_cache_arc(cache)
        .build()
        .unwrap();

    let mut session = dm
        .stitched(StitchConfig {
            backfill: ReplayConfig {
                source: ReplaySourceSpec::HistoricalCache,
                instruments: vec![Instrument::new("AAPL")],
                kinds: vec![EventKind::Trade],
                from: Timestamp(0),
                to: Timestamp(300),
            },
            live: LiveConfig {
                initial_subscriptions: vec![Subscription::new("AAPL", [EventKind::Trade])],
                buffer_size: 64,
                ..Default::default()
            },
        })
        .await
        .unwrap();

    let mut stream = session.take_events().unwrap();

    // The first two events should be the backfilled trades.
    let mut ts_seq = Vec::new();
    for _ in 0..2 {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        match ev {
            MarketEvent::Trade(t) => ts_seq.push(t.source_ts.0),
            other => panic!("expected Trade in backfill, got {other:?}"),
        }
    }
    assert_eq!(ts_seq, vec![100, 200]);

    // Now push a live trade at ts=500 — controller should emit Gap then the trade.
    ctrl.push(trade("AAPL", 500, 5.0)).await;

    // The next event should be the seam Gap (well-formed control entry).
    let next = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    match next {
        MarketEvent::Control(c) => match c.kind {
            ControlKind::Gap { provider, span, instrument } => {
                assert_eq!(provider, "fake");
                assert_eq!(instrument.symbol(), "AAPL");
                assert_eq!(span.from_source_ts.0, 200);
                assert_eq!(span.to_source_ts.0, 500);
            }
            other => panic!("expected Gap control, got {other:?}"),
        },
        other => panic!("expected Control, got {other:?}"),
    }

    // Then the live trade.
    let next = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    match next {
        MarketEvent::Trade(t) => assert_eq!(t.source_ts.0, 500),
        other => panic!("expected live Trade, got {other:?}"),
    }

    let _ = session.close().await;
}

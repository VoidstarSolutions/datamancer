//! Integration tests for the read-through historical cache path.
//!
//! Uses an in-memory [`SurrealCache`] and a synthetic provider that records the
//! ranges it was asked to fetch (and can be told to fail mid-fetch), so the
//! tests assert exactly which gaps hit the provider.

#![cfg(feature = "storage-surreal")]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, CacheKey, ControlKind, Datamancer, EventKind, HistoricalCache,
    Instrument, LiveHandle, MarketEvent, PersistenceOptions, Price, Provider, ProviderId, Result,
    Scope, Seq, Timestamp,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::mpsc;

// --- synthetic provider -----------------------------------------------------

type FetchLog = Arc<Mutex<Vec<(i64, i64)>>>;

/// Serves a fixed, source_ts-sorted dataset for whatever sub-range is
/// requested, recording each requested `[from, to)`. With `fail_at = Some(ts)`
/// it returns an error upon reaching the first event whose `source_ts >= ts`
/// (that event and everything after it is NOT sent).
struct RecordingProvider {
    id: String,
    data: Vec<MarketEvent>,
    fetched: FetchLog,
    fail_at: Option<i64>,
}

impl RecordingProvider {
    fn new(id: &str, data: Vec<MarketEvent>) -> (Self, FetchLog) {
        let fetched = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                id: id.to_string(),
                data,
                fetched: fetched.clone(),
                fail_at: None,
            },
            fetched,
        )
    }

    #[allow(dead_code)]
    fn with_fail_at(mut self, ts: i64) -> Self {
        self.fail_at = Some(ts);
        self
    }
}

#[async_trait]
impl Provider for RecordingProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn supports(&self, _instrument: &Instrument, _kind: EventKind) -> bool {
        true
    }
    async fn start_live(&self, _sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        Ok(Box::new(NoopLive))
    }
    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.fetched
            .lock()
            .unwrap()
            .push((request.from.0, request.to.0));
        for ev in &self.data {
            let ts = match ev {
                MarketEvent::Bar(b) => b.source_ts.0,
                MarketEvent::Trade(t) => t.source_ts.0,
                _ => continue,
            };
            if ts < request.from.0 || ts >= request.to.0 {
                continue;
            }
            if let Some(fail) = self.fail_at
                && ts >= fail
            {
                return Err(datamancer::Error::Provider {
                    provider: self.id.clone(),
                    message: "synthetic mid-fetch failure".to_string(),
                });
            }
            if sink.send(ev.clone()).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

struct NoopLive;
#[async_trait]
impl LiveHandle for NoopLive {
    async fn subscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

// --- helpers ----------------------------------------------------------------

fn inst() -> Instrument {
    Instrument::new(ProviderId::from_static("rec"), AssetClass::Equity, "AAPL")
}

fn bar(ts: i64, close: f64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        instrument: inst(),
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

fn key(from: i64, to: i64) -> CacheKey {
    CacheKey {
        instrument: inst(),
        kind: EventKind::Bar(BarInterval::OneMinute),
        from: Timestamp(from),
        to: Timestamp(to),
    }
}

/// Drain a historical session to completion, returning bar `source_ts`/`seq` pairs
/// (in arrival order) and any Gap control spans seen.
async fn drain(session: &mut datamancer::Session) -> (Vec<(i64, u64)>, Vec<(i64, i64)>) {
    let mut stream = session.take_events().unwrap();
    let mut bars = Vec::new();
    let mut gaps = Vec::new();
    while let Some(ev) = stream.next().await {
        match ev {
            MarketEvent::Bar(b) => bars.push((b.source_ts.0, b.seq.0)),
            MarketEvent::Control(c) => match c.kind {
                ControlKind::Gap { span, .. } => {
                    gaps.push((span.from_source_ts.0, span.to_source_ts.0));
                }
                ControlKind::SessionClosing => break,
                _ => {}
            },
            _ => {}
        }
    }
    (bars, gaps)
}

// --- tests ------------------------------------------------------------------

#[tokio::test]
async fn cold_fetch_populates_cache_and_streams_in_order() {
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let mut session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();

    let (bars, gaps) = drain(&mut session).await;
    assert_eq!(
        bars,
        vec![(100, 0), (200, 1), (300, 2)],
        "ordered, monotonic seq"
    );
    assert!(gaps.is_empty());
    // Whole range was one gap -> provider asked exactly once for [0,1000).
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 1000)]);
    // Coverage now recorded.
    assert!(cache.lookup(&key(0, 1000)).await.unwrap().is_some());
}

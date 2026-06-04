//! Integration tests for the resume primitive: detached buffering with honest
//! overflow gaps, recording through silence, and the historical→live backfill
//! seam.

#![cfg(feature = "storage-surreal")]

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig, SurrealTapLog, SurrealTapLogConfig};
use datamancer::{
    AssetClass, CacheKey, ControlKind, Datamancer, EventKind, GapSpan, HistoricalCache, Instrument,
    LiveHandle, MarketEvent, PersistenceOptions, Price, Provider, ProviderId, ReplayRequest,
    Result, Scope, Seq, TapLog, Timestamp, Trade,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::{Mutex, Notify, mpsc};

// --- synthetic provider ------------------------------------------------------

#[derive(Default)]
struct FakeState {
    sink: Option<mpsc::Sender<MarketEvent>>,
    history: Vec<MarketEvent>,
}

/// Live + historical fake. Records the `[from, to)` ranges `fetch_history`
/// is asked for; an optional gate holds the fetch open until released (so
/// tests can push live events mid-backfill); `fail_at` aborts the fetch upon
/// reaching the first event with `source_ts >= fail_at`.
struct FakeProvider {
    id: String,
    state: Arc<Mutex<FakeState>>,
    fetched: Arc<StdMutex<Vec<(i64, i64)>>>,
    gate: Option<Arc<Notify>>,
    fail_at: Option<i64>,
}

struct FakeHandles {
    state: Arc<Mutex<FakeState>>,
    fetched: Arc<StdMutex<Vec<(i64, i64)>>>,
}

impl FakeProvider {
    fn new(id: &str) -> (Self, FakeHandles) {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let fetched = Arc::new(StdMutex::new(Vec::new()));
        (
            Self {
                id: id.to_string(),
                state: state.clone(),
                fetched: fetched.clone(),
                gate: None,
                fail_at: None,
            },
            FakeHandles { state, fetched },
        )
    }

    /// Hold the fetch open until the gate fires. Releases exactly one fetch
    /// (single `Notify` permit), so do not build multi-fetch barriers on it.
    fn gated(mut self, gate: Arc<Notify>) -> Self {
        self.gate = Some(gate);
        self
    }

    fn with_fail_at(mut self, ts: i64) -> Self {
        self.fail_at = Some(ts);
        self
    }
}

impl FakeHandles {
    async fn push_live(&self, ev: MarketEvent) {
        let guard = self.state.lock().await;
        if let Some(sink) = guard.sink.as_ref() {
            let _ = sink.send(ev).await;
        }
    }

    async fn set_history(&self, events: Vec<MarketEvent>) {
        self.state.lock().await.history = events;
    }

    fn fetched(&self) -> Vec<(i64, i64)> {
        self.fetched.lock().unwrap().clone()
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
        self.state.lock().await.sink = Some(sink);
        Ok(Box::new(FakeLiveHandle {
            state: self.state.clone(),
        }))
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
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        let history = self.state.lock().await.history.clone();
        for ev in history {
            let MarketEvent::Trade(t) = &ev else { continue };
            let ts = t.source_ts.0;
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
            if sink.send(ev).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

struct FakeLiveHandle {
    state: Arc<Mutex<FakeState>>,
}

#[async_trait]
impl LiveHandle for FakeLiveHandle {
    async fn subscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _i: Instrument, _k: EventKind) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        self.state.lock().await.sink = None;
        Ok(())
    }
}

// --- helpers ------------------------------------------------------------------

fn inst(symbol: &str) -> Instrument {
    Instrument::new(ProviderId::from_static("fake"), AssetClass::Equity, symbol)
}

fn trade(ts: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst("AAPL"),
        source_ts: Timestamp(ts),
        rx_ts: Timestamp(ts),
        seq: Seq(0),
        price: Price::from_f64_round(1.0),
        size: 1,
    })
}

/// Replay the tap log and return the captured trade `source_ts` in seq order.
async fn tapped(log: &SurrealTapLog) -> Vec<i64> {
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
    tss
}

/// Poll the tap log until it has captured `n` trades (bounded wait). This
/// doubles as a barrier proving the controller processed those events.
async fn wait_for_tapped(log: &SurrealTapLog, n: usize) -> Vec<i64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let tss = tapped(log).await;
        if tss.len() >= n {
            return tss;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {n} tapped events, have {}",
            tss.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// --- tests --------------------------------------------------------------------

#[tokio::test]
async fn overflow_reports_one_gap_and_tap_log_captures_everything() {
    let (provider, handles) = FakeProvider::new("fake");
    let log = Arc::new(
        SurrealTapLog::open(SurrealTapLogConfig::Memory)
            .await
            .unwrap(),
    );
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .tap_log_arc(log.clone())
        .resume_buffer_events(4)
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

    // Never take the stream: the session is recording-only. Push 10 events
    // through a ring bounded at 4 — six (ts 100..600) must be evicted.
    for ts in (1..=10).map(|i| i * 100) {
        handles.push_live(trade(ts)).await;
    }

    // Spec test 3 (recording through silence): the tap log captures ALL ten
    // events — overflow affects delivery, never the tee. Also our barrier:
    // once the log holds ev10, the controller has ring-buffered everything.
    let captured = wait_for_tapped(&log, 10).await;
    assert_eq!(
        captured,
        vec![100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    );

    // Spec test 2 (overflow honesty): attach now. First a single Gap covering
    // exactly the evicted span [100, 601), then the four survivors, with seq
    // contiguous from 0 (evicted events were never stamped).
    let mut stream = session.take_events().await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    match first {
        MarketEvent::Control(c) => {
            assert_eq!(c.seq.0, 0);
            match c.kind {
                ControlKind::Gap { span, .. } => {
                    assert_eq!(span.from_source_ts.0, 100);
                    assert_eq!(span.to_source_ts.0, 601);
                }
                other => panic!("expected Gap, got {other:?}"),
            }
        }
        other => panic!("expected Control, got {other:?}"),
    }
    let mut survivors = Vec::new();
    for _ in 0..4 {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        match ev {
            MarketEvent::Trade(t) => survivors.push((t.source_ts.0, t.seq.0)),
            other => panic!("expected Trade, got {other:?}"),
        }
    }
    assert_eq!(survivors, vec![(700, 1), (800, 2), (900, 3), (1000, 4)]);
    let _ = session.close().await;
}

fn key(from: i64, to: i64) -> CacheKey {
    CacheKey {
        instrument: inst("AAPL"),
        kind: EventKind::Trade,
        from: Timestamp(from),
        to: Timestamp(to),
    }
}

/// Drain `n` data events (collecting any Gap spans seen on the way) from a
/// stream, returning (`source_ts`, seq) pairs in arrival order.
async fn drain_data(
    stream: &mut datamancer::EventStream,
    n: usize,
) -> (Vec<(i64, u64)>, Vec<(i64, i64)>) {
    let mut data = Vec::new();
    let mut gaps = Vec::new();
    while data.len() < n {
        let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out draining stream")
            .expect("stream ended early");
        match ev {
            MarketEvent::Trade(t) => data.push((t.source_ts.0, t.seq.0)),
            MarketEvent::Control(c) => {
                if let ControlKind::Gap { span, .. } = c.kind {
                    gaps.push((span.from_source_ts.0, span.to_source_ts.0));
                }
            }
            _ => {}
        }
    }
    (data, gaps)
}

#[tokio::test]
async fn stitched_session_splices_cache_provider_and_live_in_order() {
    // Cache holds [0, 500) with trades at 100, 300. The provider serves the
    // rest of the backfill (600, 900). Live trades are pushed while the
    // gated fetch is held open, proving the pending-live buffering.
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    cache
        .store(&key(0, 500), &[trade(100), trade(300)])
        .await
        .unwrap();

    let gate = Arc::new(Notify::new());
    let (provider, handles) = FakeProvider::new("fake");
    let provider = provider.gated(gate.clone());
    handles.set_history(vec![trade(600), trade(900)]).await;

    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.unwrap();

    // The fetch is gated open: these arrive live, mid-backfill, and must be
    // buffered to splice in AFTER the backfill output.
    handles.push_live(trade(5_000)).await;
    handles.push_live(trade(6_000)).await;
    gate.notify_one();

    let (data, gaps) = drain_data(&mut stream, 6).await;
    // Backfill in source_ts order (cache then provider gap), then the live
    // tail in arrival order; seq contiguous across both seams; no Gap.
    assert_eq!(
        data,
        vec![
            (100, 0),
            (300, 1),
            (600, 2),
            (900, 3),
            (5_000, 4),
            (6_000, 5)
        ]
    );
    assert!(gaps.is_empty(), "healthy seam emits no synthetic control");

    // Provider was asked only for the uncovered [500, B) — B is the
    // wall-clock live edge, far above the test timestamps.
    let fetched = handles.fetched();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].0, 500);
    assert!(fetched[0].1 > 900, "fetch bound is the live edge B");

    // Edge-conservative claim: coverage stops at last_event_ts + 1 (901),
    // so [901, 1000) is still reported as a gap (spec test 7).
    let remaining = cache.gaps(&key(0, 1000)).await.unwrap();
    assert_eq!(
        remaining,
        vec![GapSpan {
            from_source_ts: Timestamp(901),
            to_source_ts: Timestamp(1000),
        }]
    );
    let _ = session.close().await;
}

#[tokio::test]
async fn failed_backfill_gaps_to_the_live_edge_and_live_continues() {
    let (provider, handles) = FakeProvider::new("fake");
    let provider = provider.with_fail_at(900);
    handles.set_history(vec![trade(600), trade(900)]).await;

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
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.unwrap();
    handles.push_live(trade(7_000)).await;

    // 600 arrives, the fetch fails at 900, the remainder gaps through to the
    // live edge B, and the live tail still flows.
    let (data, gaps) = drain_data(&mut stream, 2).await;
    assert_eq!(
        data.iter().map(|d| d.0).collect::<Vec<_>>(),
        vec![600, 7_000]
    );
    assert_eq!(gaps.len(), 1, "exactly one gap for the failed remainder");
    assert_eq!(gaps[0].0, 601, "gap starts at the confirmed prefix end");
    assert!(gaps[0].1 > 900, "gap runs through to the live edge B");

    // Coverage claims only the confirmed prefix [0, 601).
    let remaining = cache.gaps(&key(0, 1000)).await.unwrap();
    assert_eq!(
        remaining,
        vec![GapSpan {
            from_source_ts: Timestamp(601),
            to_source_ts: Timestamp(1000),
        }]
    );
    let _ = session.close().await;
}

#[tokio::test]
async fn tap_log_captures_only_the_live_tail_of_a_stitched_session() {
    let gate = Arc::new(Notify::new());
    let (provider, handles) = FakeProvider::new("fake");
    let provider = provider.gated(gate.clone());
    handles.set_history(vec![trade(600), trade(900)]).await;

    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    let log = Arc::new(
        SurrealTapLog::open(SurrealTapLogConfig::Memory)
            .await
            .unwrap(),
    );
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache)
        .tap_log_arc(log.clone())
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::cached().with_tap_log(true),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.unwrap();
    handles.push_live(trade(5_000)).await; // live, mid-backfill: tapped
    gate.notify_one();

    let (data, _gaps) = drain_data(&mut stream, 3).await;
    assert_eq!(
        data.iter().map(|d| d.0).collect::<Vec<_>>(),
        vec![600, 900, 5_000]
    );

    // Only the live tail is tapped — backfill events land in the cache, not
    // the tap log (live-tail-only decision; no seq rebase ever needed).
    assert_eq!(wait_for_tapped(&log, 1).await, vec![5_000]);
    let _ = session.close().await;
}

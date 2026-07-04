//! Integration tests for the resume primitive: detached buffering with honest
//! overflow gaps, recording through silence, and the historical→live backfill
//! seam.

#![cfg(feature = "storage-turso")]

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::storage::{TursoCache, TursoCacheConfig, TursoTapLog, TursoTapLogConfig};
use datamancer::{
    Adjustment, AssetClass, CacheKey, ControlKind, Datamancer, EventKind, GapSpan, HistoricalCache,
    Instrument, LiveHandle, MarketEvent, PersistenceOptions, Price, Provider, ProviderId,
    ReplayRequest, Result, Scope, Seq, TapLog, Timestamp, Trade,
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
        size: datamancer::Quantity::from_units(1),
    })
}

/// Replay the tap log and return the captured trade `source_ts` in seq order.
async fn tapped(log: &TursoTapLog) -> Vec<i64> {
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
async fn wait_for_tapped(log: &TursoTapLog, n: usize) -> Vec<i64> {
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
    let log = Arc::new(TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap());
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

    // Spec test 2 (overflow honesty): attach now. Under source-stamping the
    // ten events are stamped seq 0..9 at push; seq 0..5 (source 100..600) are
    // evicted, leaving a real seq hole. On re-attach the eviction Gap sits at
    // the first-evicted slot (seq 0 = dropped_first_seq) covering exactly the
    // evicted span [100, 601), then the four survivors keep their push-time
    // seq 6..9 (the hole seq 1..5 is never re-delivered).
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
    assert_eq!(survivors, vec![(700, 6), (800, 7), (900, 8), (1000, 9)]);
    let _ = session.close().await;
}

fn key(from: i64, to: i64) -> CacheKey {
    CacheKey {
        instrument: inst("AAPL"),
        kind: EventKind::Trade,
        from: Timestamp(from),
        to: Timestamp(to),
        adjustment: Adjustment::default(),
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
    let cache = Arc::new(TursoCache::open(TursoCacheConfig::Memory).await.unwrap());
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

    let cache = Arc::new(TursoCache::open(TursoCacheConfig::Memory).await.unwrap());
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

    let cache = Arc::new(TursoCache::open(TursoCacheConfig::Memory).await.unwrap());
    let log = Arc::new(TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap());
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

/// Replay the tap log and return captured trades as `(source_ts, seq)` pairs in
/// seq order.
async fn tapped_with_seq(log: &TursoTapLog) -> Vec<(i64, u64)> {
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
    let mut out = Vec::new();
    while let Some(ev) = replay.next().await {
        if let MarketEvent::Trade(t) = ev {
            out.push((t.source_ts.0, t.seq.0));
        }
    }
    out
}

#[tokio::test]
async fn tap_log_replay_reproduces_the_source_seq() {
    // Convergence guard: seq is stamped at the source before the tap-log tee,
    // so the persisted (and replayed) seq is byte-identical to the delivered
    // stream's seq — the tap log no longer mints its own.
    let (provider, handles) = FakeProvider::new("fake");
    let log = Arc::new(TursoTapLog::open(TursoTapLogConfig::Memory).await.unwrap());
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
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
    let mut stream = session.take_events().await.unwrap();

    for ts in [100, 200, 300] {
        handles.push_live(trade(ts)).await;
    }

    let mut delivered = Vec::new();
    for _ in 0..3 {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        match ev {
            MarketEvent::Trade(t) => delivered.push((t.source_ts.0, t.seq.0)),
            other => panic!("expected Trade, got {other:?}"),
        }
    }
    // Source-stamped from 0 in delivery order.
    assert_eq!(delivered, vec![(100, 0), (200, 1), (300, 2)]);

    // Tap-log replay reproduces the same (source_ts, seq) verbatim.
    let replayed = tapped_with_seq(&log).await;
    assert_eq!(replayed, delivered);
    let _ = session.close().await;
}

/// Drain a historical session to completion, returning data events as
/// `(seq, source_ts)` pairs in delivery order.
async fn drain_historical(session: &datamancer::Session) -> Vec<(u64, i64)> {
    let mut stream = session.take_events().await.unwrap();
    let mut out = Vec::new();
    // Stop at SessionClosing: the historical controller emits it then waits for
    // the consumer to drop the stream, so reading until `None` would deadlock.
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out draining historical stream")
            .expect("stream ended before SessionClosing");
        match ev {
            MarketEvent::Trade(t) => out.push((t.seq.0, t.source_ts.0)),
            MarketEvent::Control(c) if matches!(c.kind, ControlKind::SessionClosing) => break,
            _ => {}
        }
    }
    out
}

#[tokio::test]
async fn historical_seq_is_deterministic_across_independent_sessions() {
    // Two independent historical sessions over the same instrument+range,
    // backed by identical input, produce identical (seq, source_ts) sequences.
    // Historical scope has no live-registry participation, so both run
    // concurrently. Proves seq is now a function of source order, not of
    // per-consumer poll timing.
    let (provider, handles) = FakeProvider::new("fake");
    handles
        .set_history(vec![trade(100), trade(200), trade(300), trade(400)])
        .await;
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .build()
        .unwrap();

    let scope = Scope::Historical {
        from: Timestamp(0),
        to: Timestamp(1_000),
    };
    let session_a = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            scope,
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let session_b = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            scope,
            PersistenceOptions::none(),
        )
        .await
        .unwrap();

    let a = drain_historical(&session_a).await;
    let b = drain_historical(&session_b).await;

    assert_eq!(a, vec![(0, 100), (1, 200), (2, 300), (3, 400)]);
    assert_eq!(a, b, "seq is a deterministic function of source order");
}

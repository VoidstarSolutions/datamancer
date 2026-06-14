//! Integration tests for the read-through historical cache path.
//!
//! Uses an in-memory [`SurrealCache`] and a synthetic provider that records the
//! ranges it was asked to fetch (and can be told to fail mid-fetch), so the
//! tests assert exactly which gaps hit the provider.

#![cfg(feature = "storage-surreal")]

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer::{
    Adjustment, AssetClass, Bar, BarInterval, CacheKey, ControlKind, Datamancer, EventKind,
    HistoricalCache, Instrument, LiveHandle, MarketEvent, PersistenceOptions, Price, Provider,
    ProviderId, Result, Scope, Seq, Timestamp,
};
use datamancer_core::HistoryRequest;
use futures::StreamExt;
use tokio::sync::{mpsc, watch};

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
        // Match the session's default mode so these direct cache probes line up
        // with what the session stores.
        adjustment: Adjustment::default(),
    }
}

/// Drain a historical session to completion, returning bar `source_ts`/`seq` pairs
/// (in arrival order) and any Gap control spans seen.
async fn drain(session: &datamancer::Session) -> (Vec<(i64, u64)>, Vec<(i64, i64)>) {
    let mut stream = session.take_events().await.unwrap();
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

    let session = dm
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

    let (bars, gaps) = drain(&session).await;
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

#[tokio::test]
async fn fully_cached_serves_without_touching_provider() {
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    // Pre-populate the whole range.
    cache
        .store(&key(0, 1000), &[bar(100, 1.0), bar(900, 2.0)])
        .await
        .unwrap();

    // Provider has no data and should never be asked.
    let (provider, fetched) = RecordingProvider::new("rec", vec![]);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let session = dm
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

    let (bars, gaps) = drain(&session).await;
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 900]);
    // seq is reassigned on the pure cache-replay branch (stored bars carry
    // Seq(0)); pin it so a stored-seq passthrough regression is caught here too.
    assert_eq!(bars.iter().map(|b| b.1).collect::<Vec<_>>(), vec![0, 1]);
    assert!(gaps.is_empty());
    assert!(
        fetched.lock().unwrap().is_empty(),
        "provider must not be asked"
    );
}

#[tokio::test]
async fn partial_overlap_fetches_only_the_gaps_and_merges_in_order() {
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    // Pre-cache the middle [300, 600).
    cache
        .store(&key(300, 600), &[bar(350, 5.0), bar(550, 6.0)])
        .await
        .unwrap();

    // Provider serves the two flanking gaps.
    let data = vec![bar(100, 1.0), bar(250, 2.0), bar(700, 7.0), bar(900, 9.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let session = dm
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

    let (bars, gaps) = drain(&session).await;
    // Cached (350,550) spliced with fetched (100,250,700,900), ordered.
    assert_eq!(
        bars.iter().map(|b| b.0).collect::<Vec<_>>(),
        vec![100, 250, 350, 550, 700, 900]
    );
    // seq is contiguous across the covered+gap boundaries.
    assert_eq!(
        bars.iter().map(|b| b.1).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5]
    );
    assert!(gaps.is_empty());
    // Provider asked ONLY for the two gaps.
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 300), (600, 1000)]);
    // The whole range is now covered.
    assert!(cache.gaps(&key(0, 1000)).await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_gap_fetch_claims_only_prefix_emits_gap_and_re_request_resumes() {
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );

    // First provider: has 100,200,300,400 but fails on reaching ts >= 300.
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0), bar(400, 4.0)];
    let (provider, fetched1) = RecordingProvider::new("rec", data.clone());
    let provider = provider.with_fail_at(300);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let session = dm
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

    let (bars, gaps) = drain(&session).await;
    // Only 100 and 200 were forwarded before the failure at 300.
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 200]);
    // A Gap was emitted for the unfetched remainder [201, 1000).
    assert_eq!(gaps, vec![(201, 1000)]);
    assert_eq!(*fetched1.lock().unwrap(), vec![(0, 1000)]);
    // Coverage claims only the confirmed prefix [0, 201).
    let remaining = cache.gaps(&key(0, 1000)).await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|g| (g.from_source_ts.0, g.to_source_ts.0))
            .collect::<Vec<_>>(),
        vec![(201, 1000)]
    );
    drop(session);

    // Second run with a healthy provider: only the remaining gap is fetched,
    // and the merged stream is complete and ordered.
    let (provider2, fetched2) = RecordingProvider::new("rec", data);
    let dm2 = Datamancer::builder()
        .provider_arc(Arc::new(provider2))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let session2 = dm2
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
    let (bars2, gaps2) = drain(&session2).await;
    assert_eq!(
        bars2.iter().map(|b| b.0).collect::<Vec<_>>(),
        vec![100, 200, 300, 400]
    );
    assert!(gaps2.is_empty());
    // Provider only asked for the previously-missing span.
    assert_eq!(*fetched2.lock().unwrap(), vec![(201, 1000)]);
}

#[tokio::test]
async fn read_only_fetches_gaps_but_does_not_persist() {
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    let data = vec![bar(100, 1.0), bar(200, 2.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::read_only(),
        )
        .await
        .unwrap();

    let (bars, _gaps) = drain(&session).await;
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 200]);
    // The gap was fetched...
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 1000)]);
    // ...but nothing was persisted: the whole range is still a gap.
    assert!(cache.lookup(&key(0, 1000)).await.unwrap().is_none());
}

#[tokio::test]
async fn refresh_refetches_whole_range_despite_coverage() {
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    // Pre-cache the whole range with STALE data.
    cache.store(&key(0, 1000), &[bar(500, 99.0)]).await.unwrap();

    // Provider serves FRESH data across the whole range.
    let data = vec![bar(100, 1.0), bar(900, 9.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    let session = dm
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1000),
            },
            PersistenceOptions::refresh(),
        )
        .await
        .unwrap();

    let (bars, gaps) = drain(&session).await;
    // Served from the provider (fresh), not the stale cached 500/99.0.
    assert_eq!(bars.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100, 900]);
    // Whole range was re-fetched despite existing coverage.
    assert_eq!(*fetched.lock().unwrap(), vec![(0, 1000)]);
    // Provider succeeded, so no gap is surfaced.
    assert!(gaps.is_empty());
    // write_cache=true: the refreshed data is persisted (full refresh cycle).
    assert!(cache.lookup(&key(0, 1000)).await.unwrap().is_some());
}

// --- gated provider (forces genuine fetch overlap) --------------------------

/// Counts `fetch_history` calls and blocks inside each fetch until released,
/// so a test can guarantee multiple sessions are contending before the winner
/// finishes. Serves the same dataset filtered to the requested range.
struct GatedProvider {
    id: String,
    data: Vec<MarketEvent>,
    calls: Arc<AtomicUsize>,
    started: Arc<tokio::sync::Notify>,
    release: watch::Receiver<bool>,
}

#[async_trait]
impl Provider for GatedProvider {
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        let mut rx = self.release.clone();
        while !*rx.borrow() {
            rx.changed().await.ok();
        }
        for ev in &self.data {
            let ts = match ev {
                MarketEvent::Bar(b) => b.source_ts.0,
                _ => continue,
            };
            if ts < request.from.0 || ts >= request.to.0 {
                continue;
            }
            if sink.send(ev.clone()).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_identical_requests_fetch_once() {
    const N: usize = 8;
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0)];
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = watch::channel(false);

    let provider = GatedProvider {
        id: "rec".to_string(),
        data,
        calls: calls.clone(),
        started: started.clone(),
        release: release_rx,
    };
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let dm = Arc::new(dm);

    let mut handles = Vec::new();
    for _ in 0..N {
        let dm = dm.clone();
        handles.push(tokio::spawn(async move {
            let session = dm
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
            drain(&session).await.0
        }));
    }

    started.notified().await;
    release_tx.send(true).unwrap();

    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }

    assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one provider fetch");
    for bars in &results {
        assert_eq!(
            bars,
            &vec![(100, 0), (200, 1), (300, 2)],
            "every consumer gets the full range"
        );
    }
}

#[tokio::test]
async fn failed_fetch_releases_slot_for_next_session() {
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());

    // Session A: provider fails at ts >= 200, so only [.., 200) of the data
    // is delivered/stored; the remainder is reported as a Gap.
    let data = vec![bar(100, 1.0), bar(200, 2.0), bar(300, 3.0)];
    let (failing, _f1) = RecordingProvider::new("rec", data.clone());
    let failing = failing.with_fail_at(200);
    let dm_a = Datamancer::builder()
        .provider_arc(Arc::new(failing))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let session_a = dm_a
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let (bars_a, gaps_a) = drain(&session_a).await;
    assert_eq!(bars_a.iter().map(|b| b.0).collect::<Vec<_>>(), vec![100]);
    assert!(!gaps_a.is_empty(), "A reports the unfetched remainder as a gap");

    // Session B: a healthy provider on the SAME cache. The slot must have
    // been released by A's failure, and B must fetch the still-uncovered
    // remainder and deliver the full range.
    let (healthy, f2) = RecordingProvider::new("rec", data);
    let dm_b = Datamancer::builder()
        .provider_arc(Arc::new(healthy))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();
    let session_b = dm_b
        .session(
            inst(),
            EventKind::Bar(BarInterval::OneMinute),
            Scope::Historical { from: Timestamp(0), to: Timestamp(1000) },
            PersistenceOptions::cached(),
        )
        .await
        .unwrap();
    let (bars_b, _gaps_b) = drain(&session_b).await;
    assert_eq!(
        bars_b.iter().map(|b| b.0).collect::<Vec<_>>(),
        vec![100, 200, 300],
        "B sees the full range after re-tiling the remainder"
    );
    assert!(
        !f2.lock().unwrap().is_empty(),
        "B fetched the remainder rather than serving a permanently-masked gap"
    );
}

#[tokio::test]
async fn distinct_ranges_each_fetch() {
    let data = vec![bar(100, 1.0), bar(1100, 2.0)];
    let (provider, fetched) = RecordingProvider::new("rec", data);
    let cache = Arc::new(SurrealCache::open(SurrealCacheConfig::Memory).await.unwrap());
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .historical_cache_arc(cache.clone())
        .build()
        .unwrap();

    for (from, to) in [(0_i64, 1000_i64), (1000, 2000)] {
        let session = dm
            .session(
                inst(),
                EventKind::Bar(BarInterval::OneMinute),
                Scope::Historical { from: Timestamp(from), to: Timestamp(to) },
                PersistenceOptions::cached(),
            )
            .await
            .unwrap();
        let _ = drain(&session).await;
    }

    let fetched = fetched.lock().unwrap().clone();
    assert!(
        fetched.contains(&(0, 1000)) && fetched.contains(&(1000, 2000)),
        "distinct cache keys each fetch their own range: {fetched:?}"
    );
}

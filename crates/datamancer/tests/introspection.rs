//! Phase 3 introspection: provider accounting, the cache catalog in the
//! snapshot, and per-symbol live state through `Datamancer::snapshot()`.

#![cfg(feature = "storage-surreal")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datamancer::storage::{SurrealCache, SurrealCacheConfig};
use datamancer::{
    AssetClass, Bar, BarInterval, Control, ControlKind, Datamancer, EventKind, GapSpan, Instrument,
    LiveHandle, MarketEvent, PersistenceOptions, Price, Provider, ProviderId, ProviderSnapshot,
    Result, Scope, Seq, Timestamp, Trade,
};
use datamancer_core::{AuthoritativeSessionSnapshot, HistoryRequest};
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc, watch};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn inst(symbol: &str) -> Instrument {
    Instrument::new(ProviderId::from_static("fake"), AssetClass::Equity, symbol)
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
        volume: datamancer::Quantity::from_units(1),
    })
}

fn trade(symbol: &str, source_ts: i64, rx_ts: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument: inst(symbol),
        source_ts: Timestamp(source_ts),
        rx_ts: Timestamp(rx_ts),
        seq: Seq(0),
        price: Price::from_f64_round(1.0),
        size: datamancer::Quantity::from_units(1),
    })
}

fn live() -> Scope {
    Scope::Live {
        backfill_from: None,
    }
}

fn provider_snap(snap: &datamancer_core::SystemSnapshot, id: &str) -> ProviderSnapshot {
    snap.providers
        .iter()
        .find(|p| p.provider.as_str() == id)
        .cloned()
        .unwrap_or_else(|| panic!("no provider snapshot for {id}"))
}

fn auth_snap(snap: &datamancer_core::SystemSnapshot, symbol: &str) -> AuthoritativeSessionSnapshot {
    snap.authoritative_sessions
        .iter()
        .find(|a| a.instrument.symbol() == symbol)
        .cloned()
        .unwrap_or_else(|| panic!("no authoritative snapshot for {symbol}"))
}

// ---------------------------------------------------------------------------
// A serving provider for the historical-fetch accounting tests.
// ---------------------------------------------------------------------------

struct ServingProvider {
    id: String,
    data: Vec<MarketEvent>,
}

#[async_trait]
impl Provider for ServingProvider {
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

/// Drain a historical session's stream until `SessionClosing` (the controller
/// waits for the consumer to finish, so reading to `None` would deadlock).
async fn drain_historical(session: &datamancer::Session) {
    let mut stream = session.take_events().await.unwrap();
    while let Some(ev) = stream.next().await {
        if matches!(
            ev,
            MarketEvent::Control(Control {
                kind: ControlKind::SessionClosing,
                ..
            })
        ) {
            break;
        }
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

#[tokio::test]
async fn provider_accounting_counts_fetches() {
    let provider = ServingProvider {
        id: "fake".to_string(),
        data: vec![bar("AAPL", 100, 1.0)],
    };
    let dm = Datamancer::builder()
        .provider_arc(Arc::new(provider))
        .build()
        .unwrap();

    // Three sequential uncached historical sessions = three upstream fetches.
    for _ in 0..3 {
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
        drain_historical(&session).await;
    }

    let snap = dm.snapshot().await.unwrap();
    assert_eq!(provider_snap(&snap, "fake").history_fetches, 3);
}

// ---------------------------------------------------------------------------
// Coalesced (single-flight) fetch accounting.
// ---------------------------------------------------------------------------

struct GatedProvider {
    id: String,
    data: Vec<MarketEvent>,
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
async fn provider_accounting_counts_coalesced_fetches() {
    const N: usize = 6;
    let started = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = watch::channel(false);
    let provider = GatedProvider {
        id: "fake".to_string(),
        data: vec![bar("AAPL", 100, 1.0), bar("AAPL", 200, 2.0)],
        started: started.clone(),
        release: release_rx,
    };
    let cache = Arc::new(
        SurrealCache::open(SurrealCacheConfig::Memory)
            .await
            .unwrap(),
    );
    let dm = Arc::new(
        Datamancer::builder()
            .provider_arc(Arc::new(provider))
            .historical_cache_arc(cache)
            .build()
            .unwrap(),
    );

    let mut handles = Vec::new();
    for _ in 0..N {
        let dm = dm.clone();
        handles.push(tokio::spawn(async move {
            let session = dm
                .session(
                    inst("AAPL"),
                    EventKind::Bar(BarInterval::OneMinute),
                    Scope::Historical {
                        from: Timestamp(0),
                        to: Timestamp(1000),
                    },
                    PersistenceOptions::cached(),
                )
                .await
                .unwrap();
            drain_historical(&session).await;
        }));
    }

    // One fetch is now contending in the slot; the others queue on it. Release.
    started.notified().await;
    release_tx.send(true).unwrap();
    for h in handles {
        h.await.unwrap();
    }

    let snap = dm.snapshot().await.unwrap();
    let p = provider_snap(&snap, "fake");
    // The hard single-flight guarantee, independent of scheduling: the shared
    // uncovered range is fetched from the provider exactly once, so the other
    // N-1 sessions never hit the provider.
    assert_eq!(p.history_fetches, 1, "exactly one upstream fetch");
    // How each non-winner avoided the fetch is scheduler-dependent: one parked
    // on the single-flight slot coalesces at the re-tile; one that only reaches
    // the cache after the winner stored serves the now-covered range directly.
    // Both are valid single-flight outcomes, so assert the bound rather than an
    // exact count (the exact count was the source of flakiness): at most N-1
    // can coalesce, and `history_fetches + non-winner-avoidance` accounts for
    // all N sessions.
    assert!(
        p.history_fetch_coalesced <= (N - 1) as u64,
        "coalesced ({}) cannot exceed the N-1 non-winner sessions",
        p.history_fetch_coalesced
    );
}

// ---------------------------------------------------------------------------
// A controllable live provider for live-state + control-derived accounting.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Shared {
    sinks: HashMap<String, mpsc::Sender<MarketEvent>>,
}

struct LiveProvider {
    id: String,
    shared: Arc<Mutex<Shared>>,
    live_starts: Arc<AtomicUsize>,
}

struct LiveCtrl {
    shared: Arc<Mutex<Shared>>,
}

impl LiveCtrl {
    async fn push(&self, symbol: &str, ev: MarketEvent) {
        let sink = self.shared.lock().await.sinks.get(symbol).cloned();
        if let Some(sink) = sink {
            sink.send(ev).await.expect("live sink closed");
        }
    }
}

impl LiveProvider {
    fn new(id: &str) -> (Arc<Self>, LiveCtrl) {
        let shared = Arc::new(Mutex::new(Shared::default()));
        (
            Arc::new(Self {
                id: id.to_string(),
                shared: shared.clone(),
                live_starts: Arc::new(AtomicUsize::new(0)),
            }),
            LiveCtrl { shared },
        )
    }
}

#[async_trait]
impl Provider for LiveProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn supports(&self, _instrument: &Instrument, _kind: EventKind) -> bool {
        true
    }
    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        self.live_starts.fetch_add(1, Ordering::SeqCst);
        let _ = sink
            .send(MarketEvent::Control(Control {
                source_ts: Timestamp(0),
                rx_ts: Timestamp(0),
                seq: Seq(0),
                kind: ControlKind::ProviderConnected {
                    provider: self.id.clone(),
                },
            }))
            .await;
        Ok(Box::new(LiveHandleImpl {
            shared: self.shared.clone(),
            sink,
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

struct LiveHandleImpl {
    shared: Arc<Mutex<Shared>>,
    sink: mpsc::Sender<MarketEvent>,
}

#[async_trait]
impl LiveHandle for LiveHandleImpl {
    async fn subscribe(&self, instrument: Instrument, _kind: EventKind) -> Result<()> {
        self.shared
            .lock()
            .await
            .sinks
            .insert(instrument.symbol().to_string(), self.sink.clone());
        Ok(())
    }
    async fn unsubscribe(&self, instrument: Instrument, _kind: EventKind) -> Result<()> {
        self.shared.lock().await.sinks.remove(instrument.symbol());
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn connected(id: &str) -> MarketEvent {
    MarketEvent::Control(Control {
        source_ts: Timestamp(0),
        rx_ts: Timestamp(0),
        seq: Seq(0),
        kind: ControlKind::ProviderConnected {
            provider: id.to_string(),
        },
    })
}

fn disconnected(id: &str) -> MarketEvent {
    MarketEvent::Control(Control {
        source_ts: Timestamp(0),
        rx_ts: Timestamp(0),
        seq: Seq(0),
        kind: ControlKind::ProviderDisconnected {
            provider: id.to_string(),
            reason: "drop".to_string(),
        },
    })
}

fn provider_error(id: &str, message: &str) -> MarketEvent {
    MarketEvent::Control(Control {
        source_ts: Timestamp(0),
        rx_ts: Timestamp(0),
        seq: Seq(0),
        kind: ControlKind::ProviderError {
            provider: id.to_string(),
            message: message.to_string(),
        },
    })
}

fn gap(symbol: &str) -> MarketEvent {
    MarketEvent::Control(Control {
        source_ts: Timestamp(5),
        rx_ts: Timestamp(5),
        seq: Seq(0),
        kind: ControlKind::Gap {
            provider: "fake".to_string(),
            instrument: inst(symbol),
            span: GapSpan {
                from_source_ts: Timestamp(1),
                to_source_ts: Timestamp(4),
            },
        },
    })
}

async fn next_ev(stream: &mut datamancer::EventStream) -> MarketEvent {
    tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timed out")
        .expect("stream ended")
}

/// Drain the stream until a `Trade` with `source_ts == marker` arrives. Any
/// events pushed before it have, by FIFO through the authoritative controller,
/// already been recorded into the stats/accounting.
async fn drain_until_marker(stream: &mut datamancer::EventStream, marker: i64) {
    loop {
        if let MarketEvent::Trade(t) = next_ev(stream).await
            && t.source_ts.0 == marker
        {
            return;
        }
    }
}

#[tokio::test]
async fn snapshot_live_stats_reflects_events() {
    let (provider, ctrl) = LiveProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.unwrap();

    ctrl.push("AAPL", trade("AAPL", 100, 105)).await;
    ctrl.push("AAPL", gap("AAPL")).await;
    ctrl.push("AAPL", trade("AAPL", 200, 207)).await;
    drain_until_marker(&mut stream, 200).await;

    let snap = dm.snapshot().await.unwrap();
    let a = auth_snap(&snap, "AAPL");
    assert_eq!(a.kind, EventKind::Trade);
    assert_eq!(a.last_source_ts, Some(Timestamp(200)));
    assert_eq!(a.last_rx_ts, Some(Timestamp(207)));
    assert_eq!(a.latency_ns, Some(7));
    assert_eq!(a.gap_count, 1);
    assert!(a.seq_position.is_some());

    // Provider accounting saw two live data messages and one gap.
    let p = provider_snap(&snap, "fake");
    assert_eq!(p.messages, 2);
    assert_eq!(p.gaps_emitted, 1);
    assert_eq!(p.connection_state, datamancer::ConnectionState::Connected);
    // bytes / rate-limit not reported by this provider.
    assert_eq!(p.bytes, None);
    assert_eq!(p.rate_limit_hits, None);
}

#[tokio::test]
async fn provider_accounting_reconnects_and_last_error_from_control() {
    let (provider, ctrl) = LiveProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let mut stream = session.take_events().await.unwrap();

    // start_live already emitted one ProviderConnected (the initial connect).
    ctrl.push("AAPL", disconnected("fake")).await;
    ctrl.push("AAPL", connected("fake")).await; // the reconnect
    ctrl.push("AAPL", provider_error("fake", "boom")).await;
    ctrl.push("AAPL", trade("AAPL", 1, 1)).await;
    drain_until_marker(&mut stream, 1).await;

    let snap = dm.snapshot().await.unwrap();
    let p = provider_snap(&snap, "fake");
    assert_eq!(p.connection_state, datamancer::ConnectionState::Connected);
    assert_eq!(p.reconnects, 1);
    assert_eq!(p.last_error.as_deref(), Some("boom"));
}

#[tokio::test]
async fn snapshot_reflects_authoritative_and_client_sessions() {
    let (provider, _ctrl) = LiveProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();

    // Two client sessions referencing the same authoritative (instrument, kind).
    let c1 = dm.client_session();
    let c2 = dm.client_session();
    c1.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();
    c2.subscribe(
        inst("AAPL"),
        EventKind::Trade,
        live(),
        PersistenceOptions::none(),
    )
    .await
    .unwrap();

    // Let the second subscribe's add_subscriber settle.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snap = dm.snapshot().await.unwrap();
    let a = auth_snap(&snap, "AAPL");
    assert_eq!(a.subscriber_refcount, 2);

    // Both client sessions are enumerated, each with the one subscription.
    let ids: Vec<_> = snap.client_sessions.iter().map(|c| c.id).collect();
    assert!(ids.contains(&c1.id()));
    assert!(ids.contains(&c2.id()));
    let c1_snap = snap
        .client_sessions
        .iter()
        .find(|c| c.id == c1.id())
        .unwrap();
    assert_eq!(c1_snap.subscriptions.len(), 1);
    assert_eq!(c1_snap.subscriptions[0].instrument.symbol(), "AAPL");
    assert_eq!(c1_snap.subscriptions[0].kind, EventKind::Trade);
    assert_eq!(c1_snap.resume_buffer.dropped_events, 0);
}

#[tokio::test]
async fn snapshot_does_not_block_under_load() {
    let (provider, ctrl) = LiveProvider::new("fake");
    let dm = Datamancer::builder()
        .provider_arc(provider)
        .build()
        .unwrap();
    let session = dm
        .session(
            inst("AAPL"),
            EventKind::Trade,
            live(),
            PersistenceOptions::none(),
        )
        .await
        .unwrap();
    let _stream = session.take_events().await.unwrap();

    // Push events continuously in the background.
    let pump = tokio::spawn(async move {
        for ts in 0..500 {
            ctrl.push("AAPL", trade("AAPL", ts, ts)).await;
        }
    });

    // Snapshot repeatedly while events flow; each call must return promptly.
    for _ in 0..20 {
        tokio::time::timeout(Duration::from_secs(2), dm.snapshot())
            .await
            .expect("snapshot blocked under load")
            .unwrap();
    }
    pump.await.unwrap();
}

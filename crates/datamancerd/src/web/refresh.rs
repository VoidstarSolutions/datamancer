//! The two independent snapshot-refresh tasks that publish into the
//! [`WebState`](crate::web::state::WebState) swaps.
//!
//! Two tasks, two cadences, two `ArcSwap`s: a **fast live-state** refresh
//! (`live_state_cadence_ms`) drives the SSE stream and the live JSON endpoints,
//! and a **slow cache-catalog** refresh (`cache_catalog_cadence_ms`) backs
//! `/api/cache`. Splitting them keeps a (potentially slow) catalog walk off the
//! live path, and lets the daemon decouple the live refresh interval from the
//! browser's consumption rate.
//!
//! Both swaps are **warmed before the HTTP listener binds** (see
//! [`Refreshers::warm`]) so a handler never serves an empty snapshot.

use std::sync::Arc;

use arc_swap::ArcSwap;
use datamancer::{Datamancer, SystemSnapshot};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::web::state::WebState;

/// Handles to the two refresh tasks plus the shared state they publish into.
pub struct Refreshers {
    /// The handle the web router is built from.
    pub state: WebState,
    live: Arc<ArcSwap<SystemSnapshot>>,
    cache: Arc<ArcSwap<SystemSnapshot>>,
    live_tx: watch::Sender<u64>,
    tasks: Vec<JoinHandle<()>>,
}

impl Refreshers {
    /// Warm both swaps with an initial snapshot, then build the shared state.
    /// Call this **before** binding the listener.
    ///
    /// # Errors
    ///
    /// Propagates the first `Datamancer::snapshot()` failure (so the daemon does
    /// not bind a web surface that cannot produce a snapshot).
    pub async fn warm(dm: &Datamancer) -> datamancer::Result<Self> {
        let initial = dm.snapshot().await?;
        let live = Arc::new(ArcSwap::from_pointee(initial.clone()));
        let cache = Arc::new(ArcSwap::from_pointee(initial));
        let (live_tx, live_rx) = watch::channel(0);
        let state = WebState::new(live.clone(), cache.clone(), live_rx);
        Ok(Self {
            state,
            live,
            cache,
            live_tx,
            tasks: Vec::new(),
        })
    }

    /// Spawn the two periodic refresh tasks on the current (shared) runtime.
    pub fn spawn(&mut self, dm: Datamancer, live_cadence_ms: u64, cache_cadence_ms: u64) {
        self.tasks.push(spawn_live(
            dm.clone(),
            self.live.clone(),
            self.live_tx.clone(),
            live_cadence_ms,
        ));
        self.tasks
            .push(spawn_cache(dm, self.cache.clone(), cache_cadence_ms));
    }

    /// Abort both refresh tasks (called on daemon shutdown).
    pub fn abort(&self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn spawn_live(
    dm: Datamancer,
    live: Arc<ArcSwap<SystemSnapshot>>,
    tx: watch::Sender<u64>,
    cadence_ms: u64,
) -> JoinHandle<()> {
    let mut ticker = interval(cadence_ms);
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            match dm.snapshot().await {
                Ok(snap) => {
                    #[cfg(feature = "metrics")]
                    crate::web::metrics::update_from_snapshot(&snap);
                    live.store(Arc::new(snap));
                    tx.send_modify(|v| *v = v.wrapping_add(1));
                }
                Err(e) => tracing::warn!(error = %e, "web live-state refresh failed"),
            }
        }
    })
}

fn spawn_cache(
    dm: Datamancer,
    cache: Arc<ArcSwap<SystemSnapshot>>,
    cadence_ms: u64,
) -> JoinHandle<()> {
    let mut ticker = interval(cadence_ms);
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            match dm.snapshot().await {
                Ok(snap) => cache.store(Arc::new(snap)),
                Err(e) => tracing::warn!(error = %e, "web cache-catalog refresh failed"),
            }
        }
    })
}

fn interval(cadence_ms: u64) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(cadence_ms.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use datamancer::storage::{SurrealCache, SurrealCacheConfig};
    use datamancer::{
        Adjustment, AssetClass, Bar, BarInterval, CacheKey, CacheSnapshot, EventKind,
        HistoricalCache as _, Instrument, Price, ProviderId, Seq, Timestamp,
    };
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    fn inst() -> Instrument {
        Instrument::new(ProviderId::from_static("rec"), AssetClass::Equity, "AAPL")
    }

    fn bar(ts: i64) -> datamancer::MarketEvent {
        datamancer::MarketEvent::Bar(Bar {
            instrument: inst(),
            interval: BarInterval::OneMinute,
            source_ts: Timestamp(ts),
            rx_ts: Timestamp(ts),
            seq: Seq(0),
            open: Price::from_f64_round(1.0),
            high: Price::from_f64_round(1.0),
            low: Price::from_f64_round(1.0),
            close: Price::from_f64_round(1.0),
            volume: 1,
        })
    }

    fn key() -> CacheKey {
        CacheKey {
            instrument: inst(),
            kind: EventKind::Bar(BarInterval::OneMinute),
            from: Timestamp(0),
            to: Timestamp(1000),
            adjustment: Adjustment::default(),
        }
    }

    #[tokio::test]
    async fn web_cache_catalog_reflects_stored_ranges() {
        let cache = std::sync::Arc::new(
            SurrealCache::open(SurrealCacheConfig::Memory)
                .await
                .unwrap(),
        );
        cache.store(&key(), &[bar(100), bar(900)]).await.unwrap();

        let dm = Datamancer::builder()
            .historical_cache_arc(cache)
            .build()
            .unwrap();

        // `warm` populates the cache swap with the freshly-enumerated catalog.
        let refreshers = Refreshers::warm(&dm).await.unwrap();
        let app = crate::web::router(refreshers.state.clone(), None);

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/cache")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let catalog: CacheSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert!(
            catalog.entries.iter().any(|e| {
                e.symbol == "AAPL"
                    && e.kind == EventKind::Bar(BarInterval::OneMinute)
                    && e.adjustment == Adjustment::default()
            }),
            "catalog must list the seeded (instrument, kind, adjustment): {:?}",
            catalog.entries
        );
    }
}

//! Routing honours the live/history surface split.
//!
//! `Datamancer::route` picks a provider that serves *every* surface a scope
//! needs. The load-bearing case is a backfilling live session: it needs history
//! and live from a **single** provider, because satisfying the halves from
//! different upstreams would splice two sources into one `(instrument, seq)`
//! substream at the historical→live seam.

use std::sync::Arc;

use async_trait::async_trait;
use datamancer::{
    AssetClass, Datamancer, Error, EventKind, Instrument, LiveHandle, MarketEvent,
    PersistenceOptions, Provider, ProviderId, Result, Scope, Surface, Timestamp,
};
use datamancer_core::HistoryRequest;
use tokio::sync::mpsc;

/// A provider that serves exactly the surfaces it is told to.
struct SurfaceFake {
    id: &'static str,
    surfaces: &'static [Surface],
}

struct NoopHandle;

#[async_trait]
impl LiveHandle for NoopHandle {
    async fn subscribe(&self, _instrument: Instrument, _kind: EventKind) -> Result<()> {
        Ok(())
    }
    async fn unsubscribe(&self, _instrument: Instrument, _kind: EventKind) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Provider for SurfaceFake {
    fn id(&self) -> &str {
        self.id
    }

    fn supports(&self, _instrument: &Instrument, _kind: EventKind, surface: Surface) -> bool {
        self.surfaces.contains(&surface)
    }

    async fn start_live(&self, _sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        Ok(Box::new(NoopHandle))
    }

    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        Ok(())
    }
}

fn instrument() -> Instrument {
    Instrument::new(ProviderId::from_static("fake"), AssetClass::Equity, "AAPL")
}

fn dm(providers: Vec<Arc<dyn Provider>>) -> Datamancer {
    let mut b = Datamancer::builder();
    for p in providers {
        b = b.provider_arc(p);
    }
    b.build().expect("build")
}

fn live_only(id: &'static str) -> Arc<dyn Provider> {
    Arc::new(SurfaceFake {
        id,
        surfaces: &[Surface::Live],
    })
}

fn history_only(id: &'static str) -> Arc<dyn Provider> {
    Arc::new(SurfaceFake {
        id,
        surfaces: &[Surface::History],
    })
}

/// Asserts the error names the surface that was missing, not merely that the
/// request failed.
fn assert_missing_surface(err: Error, want: Surface) {
    match err {
        Error::UnsupportedEventKind { surface, .. } => assert_eq!(surface, want),
        other => panic!("expected UnsupportedEventKind, got {other:?}"),
    }
}

#[tokio::test]
async fn live_only_provider_serves_a_plain_live_session() {
    let got = dm(vec![live_only("live")])
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::default(),
        )
        .await;
    assert!(got.is_ok(), "plain live needs only the live surface");
}

#[tokio::test]
async fn live_only_provider_is_rejected_for_history() {
    let err = dm(vec![live_only("live")])
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Historical {
                from: Timestamp(0),
                to: Timestamp(1_000),
            },
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("history surface is unserved");
    assert_missing_surface(err, Surface::History);
}

#[tokio::test]
async fn history_only_provider_is_rejected_for_live() {
    let err = dm(vec![history_only("hist")])
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("live surface is unserved");
    assert_missing_surface(err, Surface::Live);
}

/// A backfilling live session needs both halves. Under the pre-split flat
/// predicate this opened successfully and failed later, inside the backfill.
#[tokio::test]
async fn backfill_requires_both_surfaces() {
    let err = dm(vec![live_only("live")])
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("backfill needs a history surface");
    assert_missing_surface(err, Surface::History);
}

/// The determinism guard: two providers *between* them cover both surfaces, but
/// neither covers both alone. Routing must refuse rather than seam a backfill
/// from one upstream into a live tail from another — that would splice two
/// sources into a single `(instrument, seq)` substream.
#[tokio::test]
async fn backfill_refuses_to_split_surfaces_across_providers() {
    let err = dm(vec![live_only("live"), history_only("hist")])
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("no single provider serves both surfaces");
    // Both surfaces are individually served, so the error falls back to naming
    // the first requested surface rather than claiming one is unreachable.
    assert_missing_surface(err, Surface::Live);
}

/// A provider serving both surfaces satisfies the backfilling session that the
/// single-surface fakes above cannot.
#[tokio::test]
async fn provider_serving_both_surfaces_satisfies_backfill() {
    let both: Arc<dyn Provider> = Arc::new(SurfaceFake {
        id: "both",
        surfaces: &[Surface::Live, Surface::History],
    });
    let got = dm(vec![both])
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: Some(Timestamp(0)),
            },
            PersistenceOptions::default(),
        )
        .await;
    assert!(got.is_ok(), "one provider covering both surfaces routes");
}

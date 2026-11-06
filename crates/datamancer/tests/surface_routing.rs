//! Routing honours the live/history surface split.
//!
//! `Datamancer::route` answers with the one provider an instrument names, which
//! must serve *every* surface a scope needs. A backfilling live session needs
//! history and live from that single provider.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use datamancer::{
    AssetClass, Datamancer, Error, EventKind, Instrument, LiveHandle, LiveScope, MarketEvent,
    PersistenceOptions, Provider, ProviderId, Result, Scope, Subscribed, Surface, Timestamp,
};
use datamancer_core::HistoryRequest;
use tokio::sync::mpsc;

/// A provider that serves exactly the surfaces it is told to, and can be
/// parked disabled the way a settings source does.
struct SurfaceFake {
    id: &'static str,
    surfaces: &'static [Surface],
    enabled: AtomicBool,
}

/// A live handle that emits nothing but keeps the session's sink open, so
/// the controller stays up (a dropped sink reads as a closed upstream).
struct NoopHandle {
    _sink: mpsc::Sender<MarketEvent>,
}

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

    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    fn supports(&self, _instrument: &Instrument, _kind: EventKind, surface: Surface) -> bool {
        self.surfaces.contains(&surface)
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        Ok(Box::new(NoopHandle { _sink: sink }))
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
        enabled: AtomicBool::new(true),
    })
}

fn history_only(id: &'static str) -> Arc<dyn Provider> {
    Arc::new(SurfaceFake {
        id,
        surfaces: &[Surface::History],
        enabled: AtomicBool::new(true),
    })
}

/// A provider that serves both surfaces and can be disabled while running.
fn switchable(id: &'static str) -> Arc<SurfaceFake> {
    Arc::new(SurfaceFake {
        id,
        surfaces: &[Surface::Live, Surface::History],
        enabled: AtomicBool::new(true),
    })
}

/// A provider that serves both surfaces but is parked disabled.
fn disabled(id: &'static str) -> Arc<SurfaceFake> {
    let fake = switchable(id);
    fake.enabled.store(false, Ordering::SeqCst);
    fake
}

/// Asserts the error is [`Error::ProviderDisabled`] naming `want`.
fn assert_disabled(err: &Error, want: &str) {
    assert!(
        matches!(err, Error::ProviderDisabled { provider } if provider == want),
        "{err:?}"
    );
}

const LIVE: Scope = Scope::Live {
    backfill_from: None,
};

/// An instrument whose id no test provider owns.
fn unowned() -> Instrument {
    Instrument::new(
        ProviderId::from_static("nobody"),
        AssetClass::Equity,
        "AAPL",
    )
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
    let got = dm(vec![live_only("fake")])
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
    let err = dm(vec![live_only("fake")])
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
    let err = dm(vec![history_only("fake")])
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
    let err = dm(vec![live_only("fake")])
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

/// Two providers between them cover both surfaces, but the instrument names
/// one: a backfill is never seamed from one upstream into a live tail from
/// another.
#[tokio::test]
async fn backfill_refuses_to_split_surfaces_across_providers() {
    let err = dm(vec![live_only("fake"), history_only("hist")])
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
        .expect("the named provider has no history surface");
    assert_missing_surface(err, Surface::History);
}

/// A provider serving both surfaces satisfies the backfilling session that the
/// single-surface fakes above cannot.
#[tokio::test]
async fn provider_serving_both_surfaces_satisfies_backfill() {
    let both: Arc<dyn Provider> = Arc::new(SurfaceFake {
        id: "fake",
        surfaces: &[Surface::Live, Surface::History],
        enabled: AtomicBool::new(true),
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

// ---------------------------------------------------------------------------
// Routing by the instrument's own provider id
// ---------------------------------------------------------------------------

/// A live-only provider that counts its `start_live` calls, so a test can
/// see which of two overlapping providers a session routed to.
struct RecordingFake {
    id: &'static str,
    starts: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl Provider for RecordingFake {
    fn id(&self) -> &str {
        self.id
    }

    fn supports(&self, _instrument: &Instrument, _kind: EventKind, surface: Surface) -> bool {
        surface == Surface::Live
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        self.starts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(NoopHandle { _sink: sink }))
    }

    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        Ok(())
    }
}

fn recording(id: &'static str) -> Arc<RecordingFake> {
    Arc::new(RecordingFake {
        id,
        starts: std::sync::atomic::AtomicUsize::new(0),
    })
}

fn starts(p: &RecordingFake) -> usize {
    p.starts.load(std::sync::atomic::Ordering::SeqCst)
}

/// Two providers claim the same (asset-class, kind) space; an instrument that
/// names the second by id routes there, not to whichever registered first.
#[tokio::test]
async fn instrument_provider_id_picks_among_overlapping_providers() {
    let first = recording("first");
    let second = recording("second");
    let dm = dm(vec![first.clone(), second.clone()]);
    let session = dm
        .session(
            Instrument::new(
                ProviderId::from_static("second"),
                AssetClass::Equity,
                "AAPL",
            ),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::default(),
        )
        .await
        .expect("routes to the named provider");
    assert_eq!(
        starts(&second),
        1,
        "the instrument's own provider serves it"
    );
    assert_eq!(
        starts(&first),
        0,
        "registration order does not win over the id"
    );
    let _ = session.close().await;
}

/// An id no registered provider owns is refused, live and historical alike,
/// with no other provider standing in.
#[tokio::test]
async fn id_no_provider_owns_is_an_unknown_provider() {
    let first = recording("first");
    let dm = dm(vec![first.clone()]);
    let historical = Scope::Historical {
        from: Timestamp(1),
        to: Timestamp(2),
    };
    for scope in [LIVE, historical] {
        let err = dm
            .session(
                unowned(),
                EventKind::Trade,
                scope,
                PersistenceOptions::default(),
            )
            .await
            .err()
            .expect("nobody owns the id");
        assert!(
            matches!(&err, Error::UnknownProvider(id) if id == "nobody"),
            "{err:?}"
        );
    }
    assert_eq!(starts(&first), 0);
}

/// A subscribe naming an id no provider owns answers the same, even while a
/// disabled provider is registered.
#[tokio::test]
async fn subscribe_to_an_id_no_provider_owns_is_an_unknown_provider() {
    let dm = dm(vec![disabled("parked")]);
    let client = dm.client_session();
    let err = client
        .subscribe(
            unowned(),
            EventKind::Trade,
            LiveScope::Live,
            PersistenceOptions::default(),
        )
        .await
        .expect_err("nobody owns the id");
    assert!(
        matches!(&err, Error::UnknownProvider(id) if id == "nobody"),
        "{err:?}"
    );
    let _ = client.close().await;
}

/// The provider an instrument names answers alone: when it cannot serve the
/// pair, that is the error.
#[tokio::test]
async fn named_provider_that_cannot_serve_is_an_error_not_a_fallthrough() {
    let other = recording("other");
    let dm = dm(vec![other.clone(), history_only("named")]);
    let err = dm
        .session(
            Instrument::new(ProviderId::from_static("named"), AssetClass::Equity, "AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("the named provider has no live surface");
    assert_missing_surface(err, Surface::Live);
    assert_eq!(
        starts(&other),
        0,
        "another provider must not serve a pair addressed to `named`"
    );
}

/// A named provider that is parked disabled is reported as such, not
/// silently replaced by an enabled one claiming the same asset class.
#[tokio::test]
async fn disabled_named_provider_is_an_error_not_a_fallthrough() {
    let other = recording("other");
    let dm = dm(vec![other.clone(), disabled("named")]);
    let err = dm
        .session(
            Instrument::new(ProviderId::from_static("named"), AssetClass::Equity, "AAPL"),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("the named provider is disabled");
    assert_disabled(&err, "named");
    assert_eq!(starts(&other), 0);
}

/// Without a cache to read, a disabled provider leaves a historical open
/// nothing to serve.
#[tokio::test]
async fn disabled_provider_refuses_a_historical_open_that_reads_no_cache() {
    let dm = dm(vec![disabled("fake")]);
    let historical = Scope::Historical {
        from: Timestamp(1),
        to: Timestamp(2),
    };
    let Err(err) = dm
        .session(
            instrument(),
            EventKind::Trade,
            historical,
            PersistenceOptions::default(),
        )
        .await
    else {
        panic!("nothing can serve the range");
    };
    assert_disabled(&err, "fake");
}

/// A subscriber joining a session whose provider has since been disabled gets
/// the creator's answer, and re-enabling lets it join.
#[tokio::test]
async fn joiner_on_a_disabled_provider_is_refused_like_a_creator() {
    let fake = switchable("fake");
    let dm = dm(vec![fake.clone()]);
    let holder = dm
        .session(
            instrument(),
            EventKind::Trade,
            LIVE,
            PersistenceOptions::default(),
        )
        .await
        .expect("opens while enabled");

    fake.enabled.store(false, Ordering::SeqCst);
    let client = dm.client_session();
    let err = client
        .subscribe(
            instrument(),
            EventKind::Trade,
            LiveScope::Live,
            PersistenceOptions::default(),
        )
        .await
        .expect_err("the session's provider is disabled");
    assert_disabled(&err, "fake");

    fake.enabled.store(true, Ordering::SeqCst);
    let outcome = client
        .subscribe(
            instrument(),
            EventKind::Trade,
            LiveScope::Live,
            PersistenceOptions::default(),
        )
        .await
        .expect("joins once enabled");
    assert_eq!(
        outcome,
        Subscribed::Joined {
            session: LiveScope::Live
        }
    );
    let _ = client.close().await;
    let _ = holder.close().await;
}

/// A pair another consumer holds pure-live is joined as is: a `live_backfill`
/// request on it needs no history surface from the provider, because no
/// backfill runs for a joiner. Routing happens only when a session is
/// created.
#[tokio::test]
async fn backfill_request_joins_a_pair_held_pure_live_by_a_live_only_provider() {
    let dm = dm(vec![live_only("fake")]);
    let holder = dm
        .session(
            instrument(),
            EventKind::Trade,
            Scope::Live {
                backfill_from: None,
            },
            PersistenceOptions::default(),
        )
        .await
        .expect("pure-live on a live-only provider");
    let client = dm.client_session();
    let outcome = client
        .subscribe(
            instrument(),
            EventKind::Trade,
            LiveScope::LiveBackfill { from: Timestamp(1) },
            PersistenceOptions::default(),
        )
        .await
        .expect("joins the pure-live session instead of routing for history");
    assert_eq!(
        outcome,
        Subscribed::Joined {
            session: LiveScope::Live
        }
    );
    let _ = client.close().await;
    let _ = holder.close().await;
}

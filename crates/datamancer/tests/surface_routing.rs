//! Routing honors the live/history surface split, and admission answers in
//! one order on every path.
//!
//! An open is served by the one provider an instrument names, which must serve
//! *every* surface a scope needs. A backfilling live session needs history and
//! live from that single provider.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use common::joined_as;
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

const LIVE: Scope = Scope::live();

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
            Scope::live(),
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
            Scope::live(),
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
            Scope::live_backfill(Timestamp(0)),
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("backfill needs a history surface");
    assert_missing_surface(err, Surface::History);
}

/// Joining fetches nothing, so a backfill request that finds a session open
/// joins it even where the provider has no history to create one from.
#[tokio::test]
async fn a_backfill_request_joins_an_open_session_without_a_history_surface() {
    let dm = dm(vec![live_only("fake")]);
    let backfill = LiveScope::backfill(Timestamp(0));
    let none = PersistenceOptions::none();

    let refused = dm
        .client_session()
        .subscribe(instrument(), EventKind::Trade, backfill, none)
        .await
        .expect_err("nothing to join, so the creator needs history");
    assert_missing_surface(refused, Surface::History);

    let creator = dm.client_session();
    creator
        .subscribe(instrument(), EventKind::Trade, LiveScope::default(), none)
        .await
        .expect("live needs only the live surface");
    let joined = dm
        .client_session()
        .subscribe(instrument(), EventKind::Trade, backfill, none)
        .await
        .expect("a joiner needs no history surface");
    assert!(joined_as(joined, LiveScope::default(), false), "{joined:?}");
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
            Scope::live_backfill(Timestamp(0)),
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
            Scope::live_backfill(Timestamp(0)),
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
/// names the second by id is served by the second, and the first never starts.
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
            Scope::live(),
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

/// A subscribe naming an id no provider owns answers `UnknownProvider`, even
/// while a disabled provider is registered.
#[tokio::test]
async fn subscribe_to_an_id_no_provider_owns_is_an_unknown_provider() {
    let dm = dm(vec![disabled("parked")]);
    let client = dm.client_session();
    let err = client
        .subscribe(
            unowned(),
            EventKind::Trade,
            LiveScope::default(),
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
            Scope::live(),
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

/// A named provider that is parked disabled answers `ProviderDisabled`, and an
/// enabled provider claiming the same asset class never starts.
#[tokio::test]
async fn disabled_named_provider_is_an_error_not_a_fallthrough() {
    let other = recording("other");
    let dm = dm(vec![other.clone(), disabled("named")]);
    let err = dm
        .session(
            Instrument::new(ProviderId::from_static("named"), AssetClass::Equity, "AAPL"),
            EventKind::Trade,
            Scope::live(),
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
/// `ProviderDisabled`, as a creator would, and re-enabling lets it join.
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
            LiveScope::default(),
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
            LiveScope::default(),
            PersistenceOptions::default(),
        )
        .await
        .expect("joins once enabled");
    assert!(
        joined_as(outcome, LiveScope::default(), false),
        "{outcome:?}"
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
            Scope::live(),
            PersistenceOptions::default(),
        )
        .await
        .expect("pure-live on a live-only provider");
    let client = dm.client_session();
    let outcome = client
        .subscribe(
            instrument(),
            EventKind::Trade,
            LiveScope::backfill(Timestamp(1)),
            PersistenceOptions::default(),
        )
        .await
        .expect("joins the pure-live session instead of routing for history");
    assert!(
        joined_as(outcome, LiveScope::default(), false),
        "{outcome:?}"
    );
    let _ = client.close().await;
    let _ = holder.close().await;
}

// ---------------------------------------------------------------------------
// Admission order
// ---------------------------------------------------------------------------

/// A disabled provider that serves no surface for the pair: enabling it would
/// not let it serve, so the surface refusal answers first, live and
/// historical alike.
#[tokio::test]
async fn a_disabled_pair_answers_the_surface_refusal_first_on_both_paths() {
    let alpaca: Arc<dyn Provider> = Arc::new(SurfaceFake {
        id: "alpaca",
        surfaces: &[],
        enabled: AtomicBool::new(false),
    });
    let dm = dm(vec![alpaca]);
    let pair = Instrument::new(
        ProviderId::from_static("alpaca"),
        AssetClass::Crypto,
        "BTC/USD",
    );
    let live = dm
        .session(
            pair.clone(),
            EventKind::Trade,
            LIVE,
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("no live surface");
    assert_missing_surface(live, Surface::Live);
    let history = dm
        .session(
            pair,
            EventKind::Trade,
            Scope::Historical {
                from: Timestamp(1),
                to: Timestamp(2),
            },
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("no history surface");
    assert_missing_surface(history, Surface::History);
}

/// A future backfill start is refused whether or not the provider is enabled,
/// so it answers before the disabled refusal.
#[tokio::test]
async fn a_future_start_on_a_disabled_provider_answers_the_start_refusal() {
    let dm = dm(vec![disabled("fake")]);
    let future = Timestamp(i64::MAX);
    let err = dm
        .session(
            instrument(),
            EventKind::Trade,
            Scope::live_backfill(future),
            PersistenceOptions::default(),
        )
        .await
        .err()
        .expect("the start is not in the past");
    assert!(
        matches!(err, Error::BackfillFromNotInPast { from, .. } if from == future),
        "{err:?}"
    );
}

/// The two halves of a hold placed on the next [`GatedFake::enabled`] call:
/// `entered` fires once that call has read the flag, and the call returns what
/// it read when `release` is sent.
type Gate = (
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

/// A provider serving both surfaces whose next `enabled` read can be held, so
/// a test can let a second opener claim the pair between one opener's
/// admission and its registry re-check.
struct GatedFake {
    enabled: AtomicBool,
    gate: std::sync::Mutex<Option<Gate>>,
}

impl GatedFake {
    /// Hold the next `enabled` call after it reads the flag.
    fn hold_next_enabled(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *self.gate.lock().expect("gate") = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }
}

#[async_trait]
impl Provider for GatedFake {
    fn id(&self) -> &'static str {
        "fake"
    }

    fn enabled(&self) -> bool {
        let enabled = self.enabled.load(Ordering::SeqCst);
        let gate = self.gate.lock().expect("gate").take();
        if let Some((entered, release)) = gate {
            let _ = entered.send(());
            let _ = release.recv();
        }
        enabled
    }

    fn supports(&self, _instrument: &Instrument, _kind: EventKind, _surface: Surface) -> bool {
        true
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

fn gated() -> Arc<GatedFake> {
    Arc::new(GatedFake {
        enabled: AtomicBool::new(true),
        gate: std::sync::Mutex::new(None),
    })
}

/// Stage a lost race: a client subscribe with `scope` passes its create
/// admission and is held there; a second opener then creates the pair, and
/// `meanwhile` runs before the held subscribe re-checks the registry and
/// joins the winner. Returns the held subscribe's answer.
async fn lose_the_race(
    dm: &Datamancer,
    fake: &GatedFake,
    scope: LiveScope,
    meanwhile: impl std::future::Future<Output = ()>,
) -> Result<Subscribed> {
    let (entered, release) = fake.hold_next_enabled();
    let client = dm.client_session();
    let mut loser = tokio::spawn(async move {
        let got = client
            .subscribe(
                instrument(),
                EventKind::Trade,
                scope,
                PersistenceOptions::none(),
            )
            .await;
        let _ = client.close().await;
        got
    });
    tokio::select! {
        reached = entered => reached.expect("the loser reaches admission"),
        got = &mut loser => panic!("loser answered at create admission: {got:?}"),
    }
    let winner = dm
        .session(
            instrument(),
            EventKind::Trade,
            LIVE,
            PersistenceOptions::none(),
        )
        .await
        .expect("the winner creates the pair");
    meanwhile.await;
    release.send(()).expect("release the loser");
    let got = loser.await.expect("loser task");
    let _ = winner.close().await;
    got
}

/// A provider disabled after an opener passed its create admission, but
/// before it found the pair taken and joined, refuses that join as it would a
/// joiner that found the pair open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_race_join_on_a_disabled_provider_is_refused() {
    let fake = gated();
    let dm = dm(vec![fake.clone()]);
    let disable = async {
        fake.enabled.store(false, Ordering::SeqCst);
    };
    let err = lose_the_race(&dm, &fake, LiveScope::live(), disable)
        .await
        .expect_err("the session's provider is disabled");
    assert_disabled(&err, "fake");
}

/// A client's backfill that falls past the limit while it was being admitted
/// is refused when it joins after losing the race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backfill_limit_is_checked_on_a_lost_race_join() {
    const LIMIT: std::time::Duration = std::time::Duration::from_secs(3);
    let fake = gated();
    let dm = Datamancer::builder()
        .provider_arc(fake.clone())
        .max_client_backfill(LIMIT)
        .build()
        .expect("build");
    let from = Timestamp(wall_clock_nanos() - 1_000_000_000);
    let age_past_the_limit = async {
        let limit = i64::try_from(LIMIT.as_nanos()).expect("limit nanos");
        while wall_clock_nanos() <= from.0 + limit {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };
    let err = lose_the_race(&dm, &fake, LiveScope::backfill(from), age_past_the_limit)
        .await
        .expect_err("the backfill is now older than the limit");
    assert!(
        matches!(err, Error::BackfillTooOld { from: got, .. } if got == from),
        "{err:?}"
    );
}

/// Nanoseconds since the Unix epoch, on the clock the client limit reads.
fn wall_clock_nanos() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    i64::try_from(now).expect("nanos")
}

//! Alpaca-backed [`Provider`].
//!
//! Wraps [`oxidized_alpaca`]'s streaming stock client for live trades, quotes,
//! and minute/daily bars, and its REST market-data client for bounded
//! historical fetches. Decoded events are pushed verbatim into the supplied
//! [`MarketEvent`] sink with a placeholder [`Seq`]; the session merger assigns
//! the final session-monotonic sequence.
//!
//! # Stream host
//!
//! Alpaca's market-data websocket is account-type independent: paper and
//! live keys both authenticate on `stream.data.alpaca.markets`. The
//! `stream.data.sandbox.alpaca.markets` host that `oxidized_alpaca` selects
//! for [`AccountType::Paper`] is the Broker-API sandbox, which rejects
//! ordinary paper keys with `402 auth failed`. The streaming client is
//! therefore always built with [`STREAM_ACCOUNT_TYPE`] (a URL choice only —
//! see [`stream_url`]); `account_type` keeps selecting the trading/REST host
//! and, for the env credential source, the credential pair.
//!
//! # Single shared connection
//!
//! Alpaca allows one market-data websocket per key pair; a second connect is
//! refused with `406 connection limit exceeded`. The session layer calls
//! [`Provider::start_live`] once per authoritative `(instrument, kind)`
//! session, so those calls must not each open a socket. This provider lazily
//! spawns a single hub task on the first `start_live`; every subsequent
//! `start_live` returns a [`LiveHandle`] that talks to that hub over a
//! command channel. The hub owns the upstream socket, applies the aggregate
//! subscription set to it, and routes incoming events to the right
//! per-session sink by `(instrument, kind)`. The session-registry rule (at
//! most one live session per pair) keeps that routing a function.
//!
//! # Reconnect
//!
//! On websocket failure the hub tears down the client, sleeps with
//! exponential backoff per the configured [`ReconnectPolicy`], and
//! reconnects — re-applying the aggregate subscription set. A
//! `ProviderDisconnected` / `ProviderConnected` control pair is broadcast to
//! every active sink so consumers see the gap window in the event stream. A
//! rejected credential pair (`402`) parks instead of retrying until the
//! credentials rotate; a `406` or any other server error is transient and
//! backs off (see `classify_stream_failure` for the upstream caveat).

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datamancer_core::{
    Adjustment, AssetClass, BarInterval, Control, ControlKind, DisconnectCause, Error, EventKind,
    HistoryRequest, Instrument, InstrumentCapabilities, InstrumentEntry, LiveHandle, MarketEvent,
    OrderType, Price, Provider, ProviderId, Quantity, Result, Seq, Surface, TimeInForce, Timestamp,
    Trade,
};
use datamancer_core::{Bar, Quote};
use oxidized_alpaca::{
    AccountType, MarketDataClient, RestFeed, StreamingFeed, TradingClient,
    restful::{
        market_data::TimeFrame,
        market_data::stock::Adjustment as AlpacaAdjustment,
        market_data::stock::bars::StockBarsRequest,
        market_data::stock::quotes::{StockQuote, StockQuotesRequest},
        market_data::stock::trades::StockTradesRequest,
        trading::assets::{Asset, AssetClass as AlpacaAssetClass, Status as AlpacaAssetStatus},
    },
    streaming::{
        StockStreamMessage, StockSubscriptionList, StreamErrorCode, StreamingStockClient,
        messages::stock::{StockBar, StockQuoteEvent, StockTradeEvent},
    },
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::credentials::{AlpacaCredentials, CredentialsSource, Resolved};
use super::runtime::{Park, SettingsSource};
use crate::session::ReconnectPolicy;

/// Stable provider identifier for the Alpaca-backed provider.
pub const PROVIDER_ID: &str = "alpaca";

/// Construct an `Instrument` rooted at this provider. The streaming decoder
/// only sees symbols, not the asset-class metadata Alpaca tracks on the REST
/// `/v2/assets` surface; until that catalog is wired, decoded events default
/// to [`AssetClass::Equity`]. Catalog-driven construction (ETFs, etc.) will
/// override this at the boundary where the rich `Instrument` is built.
fn provider_instrument(symbol: impl Into<String>) -> Instrument {
    Instrument::new(
        ProviderId::from_static(PROVIDER_ID),
        AssetClass::Equity,
        symbol,
    )
}

/// Order types Alpaca accepts for a fractional equity order (provider policy;
/// not advertised per-asset — sourced from Alpaca's fractional-trading docs).
const FRACTIONAL_ORDER_TYPES: [OrderType; 4] = [
    OrderType::Market,
    OrderType::Limit,
    OrderType::Stop,
    OrderType::StopLimit,
];
/// Times-in-force Alpaca accepts for a fractional equity order (`day` only).
const FRACTIONAL_TIF: [TimeInForce; 1] = [TimeInForce::Day];

/// Build capabilities for one Alpaca equity `Asset`. The notional flag and the
/// fractional order-type/TIF vocabulary only apply to a *fractional* order —
/// notional orders **are** fractional dollar orders, and the `allowed_*` fields
/// describe orders valid fractionally — so a non-fractionable asset reports
/// `supports_notional_orders: Some(false)` and leaves the fractional `allowed_*`
/// fields `None` (we don't model whole-share order rules, so unknown is honest).
fn equity_capabilities(asset: &Asset) -> InstrumentCapabilities {
    let mut caps = InstrumentCapabilities::default();
    caps.fractionable = Some(asset.fractionable);
    caps.supports_notional_orders = Some(asset.fractionable);
    if asset.fractionable {
        caps.allowed_order_types = Some(FRACTIONAL_ORDER_TYPES.to_vec());
        caps.allowed_tif = Some(FRACTIONAL_TIF.to_vec());
    }
    // Sizing increments (min_qty/qty_increment/price_increment/min_notional)
    // stay None: oxidized_alpaca 0.0.10 Asset carries no sizing fields.
    caps
}

/// Build a catalog entry for one Alpaca equity `Asset`: the instrument stamped
/// with the authoritative [`AssetClass::Equity`] plus its capabilities. Shared
/// by bulk listing and the on-demand `capabilities` lookup so both stamp the
/// same class (and the on-demand path corrects the daemon's placeholder).
fn equity_entry(asset: &Asset) -> InstrumentEntry {
    let instrument = Instrument::new(
        ProviderId::from_static(PROVIDER_ID),
        AssetClass::Equity,
        asset.symbol.clone(),
    );
    let mut entry = InstrumentEntry::bare(instrument);
    entry.capabilities = Some(equity_capabilities(asset));
    entry
}

/// Map one Alpaca `Asset` to a catalog entry **iff** it is eligible for this
/// equities provider: tradable and [`AlpacaAssetClass::UsEquity`]. Everything
/// else — non-tradable rows, options, crypto-perp, plain crypto (handled by the
/// dedicated crypto provider) — maps to `None`, so neither bulk listing nor the
/// on-demand `capabilities` lookup ever stamps an ineligible asset as an
/// authoritative equity.
///
/// Alpaca returns ETFs under [`AlpacaAssetClass::UsEquity`] with no explicit ETF
/// flag on the row itself, so for now they land as [`AssetClass::Equity`]; a
/// future revision can read the `attributes` vector (e.g. `"etp"`) to promote
/// them to [`AssetClass::Etf`].
fn asset_to_entry(asset: &Asset) -> Option<InstrumentEntry> {
    (asset.tradable && matches!(asset.class, AlpacaAssetClass::UsEquity))
        .then(|| equity_entry(asset))
}

/// Translate Alpaca's `/v2/assets` rows into the datamancer instrument catalog.
/// Pure function — no client, no I/O — so it can be exercised against canned
/// JSON fixtures without credentials. Eligibility is per [`asset_to_entry`].
fn assets_to_entries(assets: &[Asset]) -> Vec<InstrumentEntry> {
    assets.iter().filter_map(asset_to_entry).collect()
}

/// Which Alpaca streaming endpoint to use for live data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlpacaStreamFeed {
    /// IEX feed (free tier).
    Iex,
    /// Full SIP feed (paid subscription).
    Sip,
    /// 15-minute delayed SIP feed.
    DelayedSip,
    /// Test feed — synthetic messages, available outside market hours.
    Test,
}

/// The account type handed to `oxidized_alpaca`'s streaming constructors.
/// There it is a **URL selector only**: `StreamingFeed::url(Paper)` is the
/// Broker-API sandbox host, which rejects ordinary paper keys, while the
/// production data host authenticates paper and live keys alike. The
/// configured [`AlpacaSettings::account_type`] still selects the trading/REST
/// host and the env credential pair; it never reaches the stream URL choice.
const STREAM_ACCOUNT_TYPE: AccountType = AccountType::Live;

/// Map the provider's feed selector onto `oxidized_alpaca`'s.
fn streaming_feed(feed: AlpacaStreamFeed) -> StreamingFeed {
    match feed {
        AlpacaStreamFeed::Iex => StreamingFeed::IEX,
        AlpacaStreamFeed::Sip => StreamingFeed::SIP,
        AlpacaStreamFeed::DelayedSip => StreamingFeed::DelayedSip,
        AlpacaStreamFeed::Test => StreamingFeed::Test,
    }
}

/// The websocket URL the streaming task connects to for `feed`, whatever the
/// configured account type — the production market-data host, never the
/// sandbox. Pure so the host choice is testable without a socket; the connect
/// path reaches the same URL through `new_with_credentials(STREAM_ACCOUNT_TYPE, ..)`.
fn stream_url(feed: AlpacaStreamFeed) -> &'static str {
    streaming_feed(feed).url(STREAM_ACCOUNT_TYPE)
}

/// Which Alpaca REST feed historical fetches (`fetch_history`) query.
///
/// Alpaca's history endpoints default to SIP when no feed is named, and a
/// free data plan refuses SIP for the most recent fifteen minutes
/// (`403 subscription does not permit querying recent SIP data`), so a
/// backfill window ending at "now" fails outright. The provider therefore
/// always names a feed; see [`AlpacaSettings::history_feed`] for the
/// default rule.
///
/// Only the two feeds the history endpoints (bars, trades, quotes) accept
/// are offered: `delayed_sip` is a stream/snapshot feed and those endpoints
/// answer it with `400 invalid feed` (verified against the live API on
/// 2026-11-04), so it is deliberately absent here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlpacaHistoryFeed {
    /// IEX only: served on every plan up to "now", but IEX-only volume and
    /// prices that can differ from the consolidated tape.
    Iex,
    /// Full consolidated SIP: requires the unlimited data plan for recent
    /// data.
    Sip,
}

/// Runtime settings for [`AlpacaProvider`] — the hot-reconfigurable subset
/// of its configuration, delivered through a [`SettingsSource`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlpacaSettings {
    /// Paper or live account; selects the trading/REST endpoints and, for
    /// the legacy `Env` credential source, which env credential pair is
    /// loaded. The market-data stream host does not depend on it (see
    /// [`STREAM_ACCOUNT_TYPE`]).
    pub account_type: AccountType,
    /// The REST feed historical fetches query. `None` (the default) follows
    /// the stream feed: `Sip` for a SIP stream, otherwise `Iex` — the only
    /// feed a free plan may query up to "now". Set it explicitly to override
    /// (for example `Sip` once the account has the unlimited plan).
    pub history_feed: Option<AlpacaHistoryFeed>,
}

impl Default for AlpacaSettings {
    /// Paper account, history feed following the stream feed.
    fn default() -> Self {
        Self {
            account_type: AccountType::Paper,
            history_feed: None,
        }
    }
}

/// Configuration for [`AlpacaProvider`].
#[derive(Clone, Debug)]
pub struct AlpacaProviderConfig {
    /// Runtime settings source. `Static` (the default) is always enabled;
    /// `Watch` is the daemon's enable/disable + hot-settings seam (`None` =
    /// disabled: the streaming task parks and REST calls fail unavailable).
    pub settings: SettingsSource<AlpacaSettings>,
    /// Which streaming endpoint to connect to.
    pub stream_feed: AlpacaStreamFeed,
    /// Reconnect/retry policy for the live websocket.
    pub reconnect: ReconnectPolicy,
    /// Where this provider's credentials come from. This — not the
    /// `DatamancerBuilder` — is the library's credential-source API (spec
    /// decision 9); the builder consumes providers already constructed.
    pub credentials: CredentialsSource,
}

impl Default for AlpacaProviderConfig {
    fn default() -> Self {
        Self {
            settings: SettingsSource::Static(AlpacaSettings::default()),
            stream_feed: AlpacaStreamFeed::Iex,
            reconnect: ReconnectPolicy::default(),
            credentials: CredentialsSource::Env,
        }
    }
}

/// REST clients rebuilt whenever the credential source changes (watch
/// bump) — cheap relative to REST call frequency, and `has_changed` makes
/// the common path a no-op.
#[derive(Clone)]
struct RestClients {
    /// Market-data REST client. `None` when disabled or without credentials.
    market_data: Option<MarketDataClient>,
    /// Trading API client, used for the reference-data surface (asset
    /// catalog). `None` when disabled or without credentials.
    trading: Option<TradingClient>,
    /// Whether the settings read these clients were built from found the
    /// provider enabled.
    enabled: bool,
}

impl RestClients {
    /// The market-data client, or the refusal this snapshot decides.
    fn market_data(self) -> Result<MarketDataClient> {
        self.market_data
            .ok_or_else(|| super::runtime::no_rest_client(PROVIDER_ID, self.enabled))
    }

    /// The trading client, or the refusal this snapshot decides.
    fn trading(self) -> Result<TradingClient> {
        self.trading
            .ok_or_else(|| super::runtime::no_rest_client(PROVIDER_ID, self.enabled))
    }
}

fn build_rest(cfg: &AlpacaProviderConfig) -> RestClients {
    let Some(settings) = cfg.settings.current() else {
        return RestClients {
            market_data: None,
            trading: None,
            enabled: false,
        };
    };
    let (market_data, trading) = match cfg.credentials.current() {
        Resolved::Env => (
            MarketDataClient::new(settings.account_type).ok(),
            TradingClient::new(settings.account_type).ok(),
        ),
        Resolved::Creds(c) => {
            let key = c.to_api_key();
            (
                MarketDataClient::new_with_credentials(settings.account_type, key.clone()).ok(),
                TradingClient::new_with_credentials(settings.account_type, key).ok(),
            )
        }
        Resolved::Missing => (None, None),
    };
    RestClients {
        market_data,
        trading,
        enabled: true,
    }
}

/// [`RestClients`] plus the cached credential-watch receiver used to detect
/// when they need rebuilding. One `std::sync::Mutex` (never held across an
/// await) guards both so the check-and-rebuild is atomic.
struct RestState {
    clients: RestClients,
    /// `Some` only for [`CredentialsSource::Watch`]; `has_changed` on this
    /// cached receiver is the rebuild trigger.
    cred_rx: Option<watch::Receiver<Option<AlpacaCredentials>>>,
    /// `Some` only for [`SettingsSource::Watch`]; `has_changed` on this
    /// cached receiver is the rebuild trigger.
    settings_rx: Option<watch::Receiver<Option<AlpacaSettings>>>,
}

/// Alpaca-backed [`Provider`].
pub struct AlpacaProvider {
    cfg: AlpacaProviderConfig,
    rest: std::sync::Mutex<RestState>,
    /// The one hub task behind every live session (see the module docs);
    /// spawned lazily on the first `start_live`.
    hub: std::sync::Mutex<HubSlot>,
    #[cfg(test)]
    hub_spawns: std::sync::atomic::AtomicUsize,
}

enum HubSlot {
    Idle,
    Active {
        cmd_tx: mpsc::Sender<HubCommand>,
        /// The hub exits on its own once the provider and every handle are
        /// gone (its command channel closes); the join handle is kept so a
        /// test can observe that.
        #[allow(dead_code, reason = "observed by the lifecycle test only")]
        task: JoinHandle<()>,
    },
}

impl AlpacaProvider {
    /// Construct without eagerly initializing the REST clients. Use this
    /// when only live streaming is needed and credentials are loaded later,
    /// or in tests where the env vars are not set.
    #[must_use]
    pub fn new(cfg: AlpacaProviderConfig) -> Self {
        // Ordering invariant — receiver before build: a rotation after
        // capture triggers rebuild; before capture is included in the build.
        // (Capturing after building would mark an in-between rotation seen
        // while the cached clients are stale.) Applies to both the
        // credentials and the settings receiver.
        let cred_rx = cfg.credentials.watch();
        let settings_rx = cfg.settings.watch();
        let rest = std::sync::Mutex::new(RestState {
            clients: build_rest(&cfg),
            cred_rx,
            settings_rx,
        });
        Self {
            cfg,
            rest,
            hub: std::sync::Mutex::new(HubSlot::Idle),
            #[cfg(test)]
            hub_spawns: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Construct with an explicit market-data REST client. Useful in tests.
    /// The trading client is still resolved from the configured credential
    /// source.
    #[must_use]
    pub fn with_rest(cfg: AlpacaProviderConfig, rest: MarketDataClient) -> Self {
        // Ordering invariant — receiver before build: a rotation after
        // capture triggers rebuild; before capture is included in the build.
        let cred_rx = cfg.credentials.watch();
        let settings_rx = cfg.settings.watch();
        let mut clients = build_rest(&cfg);
        clients.market_data = Some(rest);
        let rest = std::sync::Mutex::new(RestState {
            clients,
            cred_rx,
            settings_rx,
        });
        Self {
            cfg,
            rest,
            hub: std::sync::Mutex::new(HubSlot::Idle),
            #[cfg(test)]
            hub_spawns: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Lazily spawn the hub task and return its command channel. Every
    /// `start_live` goes through here, so the provider never holds more
    /// than one upstream socket.
    fn ensure_hub(&self) -> mpsc::Sender<HubCommand> {
        // A std mutex, never held across an await (the spawn is synchronous):
        // keeps the provider `UnwindSafe`/`RefUnwindSafe` like its REST state.
        let mut slot = self.hub.lock().expect("hub slot poisoned");
        match &*slot {
            HubSlot::Active { cmd_tx, .. } => cmd_tx.clone(),
            HubSlot::Idle => {
                let (cmd_tx, cmd_rx) = mpsc::channel::<HubCommand>(64);
                let cfg = self.cfg.clone();
                let task = tokio::spawn(run_hub_task(cfg, cmd_rx));
                #[cfg(test)]
                self.hub_spawns
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                *slot = HubSlot::Active {
                    cmd_tx: cmd_tx.clone(),
                    task,
                };
                cmd_tx
            }
        }
    }

    /// How many hub tasks this provider has spawned (test observability).
    #[cfg(test)]
    fn hub_spawns(&self) -> usize {
        self.hub_spawns.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Take the hub task out of the provider, dropping the provider's own
    /// command sender: once every handle is gone too, the task exits and the
    /// returned join handle resolves (test observability).
    #[cfg(test)]
    fn take_hub_task(&self) -> Option<JoinHandle<()>> {
        let mut slot = self.hub.lock().expect("hub slot poisoned");
        match std::mem::replace(&mut *slot, HubSlot::Idle) {
            HubSlot::Active { task, .. } => Some(task),
            HubSlot::Idle => None,
        }
    }

    /// Current REST clients, rebuilt first if the credential or settings
    /// source changed since the last call. Cloning out of the mutex keeps
    /// the guard from crossing an await (the clients are cheaply cloneable
    /// handles).
    fn rest_clients(&self) -> RestClients {
        let mut state = self.rest.lock().expect("REST client state poisoned");
        let changed = super::runtime::watch_changed(&mut state.cred_rx)
            | super::runtime::watch_changed(&mut state.settings_rx);
        if changed {
            state.clients = build_rest(&self.cfg);
        }
        state.clients.clone()
    }
}

#[async_trait]
impl Provider for AlpacaProvider {
    fn id(&self) -> &str {
        PROVIDER_ID
    }

    fn enabled(&self) -> bool {
        self.cfg.settings.current().is_some()
    }

    fn supports(&self, instrument: &Instrument, kind: EventKind, surface: Surface) -> bool {
        if !matches!(
            instrument.asset_class(),
            AssetClass::Equity | AssetClass::Etf
        ) {
            return false;
        }
        match surface {
            // The equity websocket streams trades, quotes, and only minute and
            // daily bars, all decoded as equities.
            Surface::Live => {
                instrument.asset_class() == AssetClass::Equity
                    && match kind {
                        EventKind::Trade
                        | EventKind::Quote
                        | EventKind::Bar(BarInterval::OneMinute | BarInterval::OneDay) => true,
                        EventKind::Bar(_) => false,
                    }
            }
            // The REST bar surface is wider than the streamed one: `stock_bars`
            // accepts every interval `fetch_history_via` maps except one-second,
            // which has no endpoint. Keep this in step with that match — an
            // over-promise here routes a request that then fails at network time.
            Surface::History => match kind {
                // No REST second-bar endpoint; every other interval is served.
                EventKind::Bar(BarInterval::OneSecond) => false,
                EventKind::Trade | EventKind::Quote | EventKind::Bar(_) => true,
            },
        }
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let cmd_tx = self.ensure_hub();
        Ok(Box::new(SharedLiveHandle {
            cmd_tx,
            sink,
            subscribed: Mutex::new(None),
        }))
    }

    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let rest = self.rest_clients().market_data()?;
        // The REST client above is `None` whenever the settings are, so a
        // missing value here is unreachable; fall back to the default rule
        // rather than fail a fetch the client would otherwise serve.
        let explicit = self.cfg.settings.current().and_then(|s| s.history_feed);
        let feed = history_feed(explicit, self.cfg.stream_feed);
        fetch_history_via(&rest, request, sink, feed).await
    }

    async fn latest(
        &self,
        instrument: &Instrument,
        kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        // Seed from the same feed the live subscription streams, so the first
        // painted value comes from the feed it is seeding — not Alpaca's
        // snapshot default ("sip if unlimited, else iex"), which can diverge from
        // an IEX stream. `Test` has no snapshot equivalent (it would silently hit
        // the real production endpoint), so it no-ops like a provider with no
        // snapshot surface.
        let Some(feed) = snapshot_feed(self.cfg.stream_feed) else {
            return Ok(None);
        };
        let rest = self.rest_clients().market_data()?;
        let snap = rest
            .stock_snapshot(instrument.symbol())
            .feed(feed)
            .execute()
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("stock_snapshot: {e}"),
            })?;
        Ok(snapshot_to_event(&snap, instrument, kind, wall_clock_ts()))
    }

    async fn list_instruments(&self) -> Result<Vec<InstrumentEntry>> {
        let trading = self.rest_clients().trading()?;
        // Filter at the API edge: status=Active + class=UsEquity is what
        // Alpaca's `/v2/assets` understands; we still re-filter on
        // `tradable` because inactive-but-listed rows occasionally slip
        // through the status filter on the equity surface.
        let assets = trading
            .list_assets()
            .status(AlpacaAssetStatus::Active)
            .asset_class(AlpacaAssetClass::UsEquity)
            .execute()
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("list_assets: {e}"),
            })?;
        Ok(assets_to_entries(&assets))
    }

    async fn capabilities(&self, instrument: &Instrument) -> Result<Option<InstrumentEntry>> {
        let trading = self.rest_clients().trading()?;
        let asset = trading
            .get_asset(instrument.symbol())
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("get_asset({}): {e}", instrument.symbol()),
            })?;
        // Same eligibility gate as bulk listing: an ineligible asset (crypto,
        // option, non-tradable) resolves to `None`, so the caller keeps its
        // placeholder instrument rather than a mislabeled authoritative equity.
        Ok(asset_to_entry(&asset))
    }
}

// ---------------------------------------------------------------------------
// Hub command channel and live handle
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum HubCommand {
    Subscribe {
        instrument: Instrument,
        kind: EventKind,
        sink: mpsc::Sender<MarketEvent>,
        ack: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        instrument: Instrument,
        kind: EventKind,
        /// The asking session's sink: only the route it owns is removed.
        sink: mpsc::Sender<MarketEvent>,
        ack: oneshot::Sender<Result<()>>,
    },
}

/// One session's handle onto the shared hub: it carries the session's sink
/// to the hub on subscribe and cleans its route up on close. The socket
/// itself belongs to the hub, never to a handle.
struct SharedLiveHandle {
    cmd_tx: mpsc::Sender<HubCommand>,
    /// Per-session sink the hub will route events to once subscribed.
    sink: mpsc::Sender<MarketEvent>,
    /// Tracks the pair this handle is subscribed to so `close` can clean it
    /// up. With a per-session handle this is at most one pair.
    subscribed: Mutex<Option<(Instrument, EventKind)>>,
}

#[async_trait]
impl LiveHandle for SharedLiveHandle {
    async fn subscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(HubCommand::Subscribe {
                instrument: instrument.clone(),
                kind,
                sink: self.sink.clone(),
                ack: tx,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        let res = rx.await.map_err(|_| Error::SessionClosed)?;
        if res.is_ok() {
            *self.subscribed.lock().await = Some((instrument, kind));
        }
        res
    }

    async fn unsubscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(HubCommand::Unsubscribe {
                instrument: instrument.clone(),
                kind,
                sink: self.sink.clone(),
                ack: tx,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        let res = rx.await.map_err(|_| Error::SessionClosed)?;
        if res.is_ok() {
            let mut slot = self.subscribed.lock().await;
            if slot.as_ref() == Some(&(instrument, kind)) {
                *slot = None;
            }
        }
        res
    }

    async fn close(self: Box<Self>) -> Result<()> {
        // If the session never explicitly unsubscribed, do it now so the hub
        // drops the route. The hub itself is shared; its lifetime is tied to
        // the provider, not any one session.
        let pair = self.subscribed.lock().await.take();
        if let Some((instrument, kind)) = pair {
            let (tx, rx) = oneshot::channel();
            let _ = self
                .cmd_tx
                .send(HubCommand::Unsubscribe {
                    instrument,
                    kind,
                    sink: self.sink.clone(),
                    ack: tx,
                })
                .await;
            let _ = rx.await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Hub task
// ---------------------------------------------------------------------------

/// How a streaming failure is handled. A rejected credential pair parks the
/// hub until a rotation (retrying cannot help, provisioning can); everything
/// else — a dropped socket, `406 connection limit exceeded`, `407 slow
/// client` — is transient and backs off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamFailure {
    Unauthenticated,
    Transient,
}

/// Classify a connect-time or mid-stream error from `oxidized_alpaca`.
///
/// `406 connection limit exceeded` is transient: another connection on the
/// same key pair holds the slot, which clears when it closes. Only
/// `402 auth failed` means the credentials themselves are rejected.
/// Limitation to know about: at **connect** time `oxidized_alpaca` 0.0.10
/// (and 0.0.11) collapses every non-`authenticated` auth response into the
/// payload-less `Error::StreamingAuth` and only logs the envelope, so a 406
/// there is indistinguishable from a 402 and lands in the park. The hub
/// itself never opens a second connection, so a connect-time rejection is
/// treated as the credential case it is in practice; once upstream carries
/// the `StreamError` on that path, it classifies through the same arm as
/// the mid-stream case below with no further change here.
fn classify_stream_failure(err: &oxidized_alpaca::Error) -> StreamFailure {
    match err {
        oxidized_alpaca::Error::StreamingAuth => StreamFailure::Unauthenticated,
        oxidized_alpaca::Error::StreamingError(e) => match e.code {
            StreamErrorCode::AuthFailed => StreamFailure::Unauthenticated,
            _ => StreamFailure::Transient,
        },
        _ => StreamFailure::Transient,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "single-pass connect / authenticate / subscribe / dispatch / reconnect state machine; extraction would obscure the linear lifecycle"
)]
async fn run_hub_task(cfg: AlpacaProviderConfig, mut cmd_rx: mpsc::Receiver<HubCommand>) {
    // Per-session sinks keyed by the pair they own; the session registry's
    // at-most-one-live-session-per-pair rule keeps this a function.
    let mut routes: HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>> = HashMap::new();
    // Authoritative aggregate subscription set, applied to every fresh socket.
    let mut subs = StockSubscriptionList::new();
    let mut backoff = cfg.reconnect.initial_backoff_ms;

    'outer: loop {
        let mut settings_rx = cfg.settings.watch();
        let Some(settings) = cfg.settings.current() else {
            // Disabled: park until the settings watch delivers a value. Only a
            // Watch source can resolve to None, so the receiver is always
            // present here; exit defensively if it isn't (never busy-loop).
            let Some(rx) = settings_rx.as_mut() else {
                return;
            };
            if !wait_for_provisioning(rx, &mut cmd_rx, Park::Disabled).await {
                return;
            }
            continue 'outer;
        };

        let feed = streaming_feed(cfg.stream_feed);
        // Fresh receiver per connect attempt: `watch()` marks the current
        // value as seen on the clone (tokio's `Receiver::clone` would
        // otherwise inherit the stored receiver's stale seen-version), so
        // the hot-reconnect arm below only fires on rotations that land
        // *after* this resolution.
        let mut cred_rx = cfg.credentials.watch();
        // Every path connects through `new_with_credentials` so the account
        // type it sees is `STREAM_ACCOUNT_TYPE` (the production data host);
        // the configured account type only picks which env pair `Env` reads.
        tracing::info!(
            provider = PROVIDER_ID,
            url = stream_url(cfg.stream_feed),
            account_type = ?settings.account_type,
            "connecting to the market-data stream"
        );
        let connect_result = match cfg.credentials.current() {
            Resolved::Env => match AlpacaCredentials::from_env(settings.account_type) {
                Ok(c) => {
                    StreamingStockClient::new_with_credentials(
                        STREAM_ACCOUNT_TYPE,
                        feed,
                        c.to_api_key(),
                    )
                    .await
                }
                Err(missing) => Err(missing),
            },
            Resolved::Creds(c) => {
                StreamingStockClient::new_with_credentials(
                    STREAM_ACCOUNT_TYPE,
                    feed,
                    c.to_api_key(),
                )
                .await
            }
            Resolved::Missing => {
                // No credentials yet: wait for provisioning instead of
                // hammering bad auth, then retry the outer loop. Only a
                // Watch source can resolve to Missing, so the receiver is
                // always present here; exit defensively if it isn't (never
                // busy-loop).
                let Some(rx) = cred_rx.as_mut() else { return };
                if !wait_for_provisioning(rx, &mut cmd_rx, Park::MissingCredentials).await {
                    return;
                }
                continue 'outer;
            }
        };

        let mut client = match connect_result {
            Ok(client) => {
                backoff = cfg.reconnect.initial_backoff_ms;
                broadcast_control(
                    &routes,
                    ControlKind::ProviderConnected {
                        provider: PROVIDER_ID.to_string(),
                    },
                )
                .await;
                client
            }
            Err(err) => {
                let failure = classify_stream_failure(&err);
                let unauthenticated = failure == StreamFailure::Unauthenticated;
                tracing::warn!(
                    provider = PROVIDER_ID,
                    ?failure,
                    error = %error_chain(&err),
                    "market-data stream connect failed"
                );
                broadcast_control(
                    &routes,
                    ControlKind::ProviderDisconnected {
                        provider: PROVIDER_ID.to_string(),
                        reason: format!("connect failed: {}", error_chain(&err)),
                        cause: if unauthenticated {
                            DisconnectCause::Unauthenticated
                        } else {
                            DisconnectCause::Error
                        },
                    },
                )
                .await;
                if unauthenticated && let Some(rx) = cred_rx.as_mut() {
                    // Rejected credentials: retrying cannot help. Park until
                    // a rotation (set-credentials hot-apply) or disable —
                    // mirrors the Missing-credentials park above. Static
                    // sources can't rotate, so they fall through to backoff.
                    if !wait_for_provisioning(rx, &mut cmd_rx, Park::RejectedCredentials).await {
                        return;
                    }
                    continue 'outer;
                }
                if !sleep_with_jitter(&mut backoff, &cfg.reconnect, &mut cmd_rx).await {
                    return;
                }
                continue 'outer;
            }
        };

        // Re-apply the aggregate subscription set on a fresh socket.
        if !is_empty(&subs)
            && let Err(err) = client.add_subscriptions(&subs).await
        {
            broadcast_error(
                &routes,
                format!("re-subscribe failed: {}", error_chain(&err)),
            )
            .await;
        }

        // Event loop: select between commands, incoming messages, and the
        // credential/settings watches.
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        // The provider and every handle are gone; no command
                        // can ever arrive again. Close the websocket and exit.
                        let _ = client.shut_down().await;
                        return;
                    };
                    handle_route_command(&mut routes, &mut subs, &mut client, cmd).await;
                }
                next = client.next_message() => {
                    match next {
                        Ok(msg) => {
                            for ev in translate_stock_message(msg) {
                                dispatch_event(&routes, ev).await;
                            }
                        }
                        Err(err) => {
                            // oxidized_alpaca 0.0.10: `Error::StreamingError`
                            // carries the server's envelope mid-stream, so a
                            // `402` parks and a `406`/`407` backs off; a
                            // mid-stream `StreamingAuth` is connect-only
                            // upstream today and handled the same way.
                            let failure = classify_stream_failure(&err);
                            let unauthenticated = failure == StreamFailure::Unauthenticated;
                            tracing::warn!(
                                provider = PROVIDER_ID,
                                ?failure,
                                error = %error_chain(&err),
                                "market-data stream failed"
                            );
                            broadcast_control(
                                &routes,
                                ControlKind::ProviderDisconnected {
                                    provider: PROVIDER_ID.to_string(),
                                    reason: format!("websocket: {}", error_chain(&err)),
                                    cause: if unauthenticated {
                                        DisconnectCause::Unauthenticated
                                    } else {
                                        DisconnectCause::Error
                                    },
                                },
                            )
                            .await;
                            // Drop the client and reconnect.
                            drop(client);
                            if unauthenticated && let Some(rx) = cred_rx.as_mut() {
                                // Rejected credentials: retrying cannot help.
                                // Park until a rotation or disable, exactly
                                // as the connect-time arm does. Static
                                // sources can't rotate, so they fall through
                                // to backoff.
                                if !wait_for_provisioning(rx, &mut cmd_rx, Park::RejectedCredentials)
                                    .await
                                {
                                    return;
                                }
                                continue 'outer;
                            }
                            if !sleep_with_jitter(&mut backoff, &cfg.reconnect, &mut cmd_rx).await {
                                return;
                            }
                            continue 'outer;
                        }
                    }
                }
                changed = async {
                    match cred_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        // Unreachable: the arm is guarded on `is_some()`.
                        None => std::future::pending().await,
                    }
                }, if cred_rx.is_some() => {
                    if changed.is_ok() {
                        tracing::info!(
                            provider = PROVIDER_ID,
                            "credentials changed; reconnecting"
                        );
                        // Same in-band control the websocket error path
                        // broadcasts, then reconnect immediately with the
                        // new credentials — reset the backoff, this is a
                        // deliberate rotation, not a failure.
                        broadcast_control(
                            &routes,
                            ControlKind::ProviderDisconnected {
                                provider: PROVIDER_ID.to_string(),
                                reason: "credentials rotated".to_string(),
                                cause: DisconnectCause::Error,
                            },
                        )
                        .await;
                        let _ = client.shut_down().await;
                        backoff = cfg.reconnect.initial_backoff_ms;
                        continue 'outer;
                    }
                    // Watch sender dropped: no further rotations can
                    // arrive. Disable this arm (a closed receiver would
                    // otherwise resolve immediately and spin the select).
                    cred_rx = None;
                }
                changed = async {
                    match settings_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        // Unreachable: the arm is guarded on `is_some()`.
                        None => std::future::pending().await,
                    }
                }, if settings_rx.is_some() => {
                    if changed.is_ok() {
                        // A disable is deliberate, not a fault: classify it so
                        // health does not read the dropped streams as Stale.
                        let (reason, cause) = if cfg.settings.current().is_none() {
                            ("provider disabled", DisconnectCause::Disabled)
                        } else {
                            ("settings changed", DisconnectCause::Error)
                        };
                        tracing::info!(provider = PROVIDER_ID, reason, "settings changed; reconnecting");
                        broadcast_control(
                            &routes,
                            ControlKind::ProviderDisconnected {
                                provider: PROVIDER_ID.to_string(),
                                reason: reason.to_string(),
                                cause,
                            },
                        )
                        .await;
                        let _ = client.shut_down().await;
                        backoff = cfg.reconnect.initial_backoff_ms;
                        continue 'outer;
                    }
                    settings_rx = None;
                }
            }
        }
    }
}

/// Waits for a `Watch` source (settings or credentials) to deliver a new
/// value, servicing the command channel meanwhile (drop of the provider and
/// every handle exits; subscribe/unsubscribe fail fast with `park`'s error).
/// Returns `false` if the hub task should exit.
async fn wait_for_provisioning<T>(
    rx: &mut watch::Receiver<T>,
    cmd_rx: &mut mpsc::Receiver<HubCommand>,
    park: Park,
) -> bool {
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_ok() {
                    // The outer loop re-resolves; if the new value is still
                    // disabled/missing it lands back here rather than spinning.
                    return true;
                }
                // Sender dropped with no value: it can never arrive. Keep
                // servicing commands so hub shutdown (all handles dropped)
                // still works.
                loop {
                    match cmd_rx.recv().await {
                        Some(
                            HubCommand::Subscribe { ack, .. }
                            | HubCommand::Unsubscribe { ack, .. },
                        ) => {
                            let _ = ack.send(Err(park.error(PROVIDER_ID)));
                        }
                        None => return false,
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(
                        HubCommand::Subscribe { ack, .. }
                        | HubCommand::Unsubscribe { ack, .. },
                    ) => {
                        let _ = ack.send(Err(park.error(PROVIDER_ID)));
                    }
                    None => return false,
                }
            }
        }
    }
}

/// Sleeps with exponential backoff, servicing the command channel meanwhile.
/// Returns `false` if the hub task should exit (provider and handles gone).
async fn sleep_with_jitter(
    backoff_ms: &mut u64,
    policy: &ReconnectPolicy,
    cmd_rx: &mut mpsc::Receiver<HubCommand>,
) -> bool {
    // Full jitter: pick a sleep uniformly in [0, backoff_ms]. With many
    // providers reconnecting after a shared upstream blip this avoids
    // synchronized retry storms; without it every one retries on the same
    // exponential ladder.
    let delay_ms = if policy.jitter {
        fastrand::u64(0..=*backoff_ms)
    } else {
        *backoff_ms
    };
    let delay = Duration::from_millis(delay_ms);
    *backoff_ms = (*backoff_ms * 2).min(policy.max_backoff_ms);

    tokio::select! {
        () = tokio::time::sleep(delay) => true,
        cmd = cmd_rx.recv() => {
            match cmd {
                // While disconnected, fail subscribe/unsubscribe immediately so
                // sessions see a clear error rather than stalling.
                Some(HubCommand::Subscribe { ack, .. } | HubCommand::Unsubscribe { ack, .. }) => {
                    let _ = ack.send(Err(Error::Provider {
                        provider: PROVIDER_ID.to_string(),
                        message: "provider is reconnecting".to_string(),
                    }));
                    true
                }
                // SessionClosing is emitted by Controller::shutdown for each
                // session; the hub serves all of them and shouldn't double-emit.
                None => false,
            }
        }
    }
}

/// Route one translated event to the sink owning its `(instrument, kind)`;
/// a control (no pair) fans out to every active sink so each session sees
/// provider-level state changes.
async fn dispatch_event(
    routes: &HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    ev: MarketEvent,
) {
    if let Some(key) = event_route_key(&ev) {
        if let Some(sink) = routes.get(&key) {
            let _ = sink.send(ev).await;
        }
        return;
    }
    for sink in routes.values() {
        let _ = sink.send(ev.clone()).await;
    }
}

fn event_route_key(ev: &MarketEvent) -> Option<(Instrument, EventKind)> {
    match ev {
        MarketEvent::Trade(t) => Some((t.instrument.clone(), EventKind::Trade)),
        MarketEvent::Quote(q) => Some((q.instrument.clone(), EventKind::Quote)),
        MarketEvent::Bar(b) => Some((b.instrument.clone(), EventKind::Bar(b.interval))),
        _ => None,
    }
}

/// The upstream calls a session's subscribe or unsubscribe makes on the
/// connected socket.
trait RouteUpstream {
    /// Apply the aggregate subscription set `subs`.
    async fn add(&mut self, subs: &StockSubscriptionList) -> Result<()>;
    /// Remove the pairs in `removal`.
    async fn remove(&mut self, removal: &StockSubscriptionList) -> Result<()>;
}

impl RouteUpstream for StreamingStockClient {
    async fn add(&mut self, subs: &StockSubscriptionList) -> Result<()> {
        self.add_subscriptions(subs)
            .await
            .map(|_| ())
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("add_subscriptions: {}", error_chain(&e)),
            })
    }

    async fn remove(&mut self, removal: &StockSubscriptionList) -> Result<()> {
        self.remove_subscriptions(removal)
            .await
            .map(|_| ())
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("remove_subscriptions: {}", error_chain(&e)),
            })
    }
}

/// Apply one session's subscribe or unsubscribe on the connected socket and
/// answer it.
async fn handle_route_command(
    routes: &mut HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    subs: &mut StockSubscriptionList,
    upstream: &mut impl RouteUpstream,
    cmd: HubCommand,
) {
    match cmd {
        HubCommand::Subscribe {
            instrument,
            kind,
            sink,
            ack,
        } => {
            let res = subscribe_route(routes, subs, upstream, instrument, kind, sink).await;
            let _ = ack.send(res);
        }
        HubCommand::Unsubscribe {
            instrument,
            kind,
            sink,
            ack,
        } => {
            let res = unsubscribe_route(routes, subs, upstream, instrument, kind, &sink).await;
            let _ = ack.send(res);
        }
    }
}

/// Route `(instrument, kind)` to `sink` and subscribe it upstream. If the
/// upstream refuses, a route this one replaced (a session torn down whose
/// unsubscribe has not arrived yet) is restored, its pair still streaming,
/// so that session's unsubscribe still removes it; with no such route the
/// pair is dropped again, so a reconnect does not apply a subscription the
/// server rejected.
async fn subscribe_route(
    routes: &mut HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    subs: &mut StockSubscriptionList,
    upstream: &mut impl RouteUpstream,
    instrument: Instrument,
    kind: EventKind,
    sink: mpsc::Sender<MarketEvent>,
) -> Result<()> {
    let key = (instrument, kind);
    let replaced = routes.insert(key.clone(), sink);
    apply_pair_to_list(subs, &key.0, kind, true);
    let res = upstream.add(subs).await;
    if res.is_err() {
        if let Some(previous) = replaced {
            routes.insert(key, previous);
        } else {
            routes.remove(&key);
            apply_pair_to_list(subs, &key.0, kind, false);
        }
        return res;
    }
    // We are in the connected loop, so the provider is up.
    // `ProviderConnected` is otherwise only broadcast on the connect *edge*,
    // which a route that subscribes afterward misses — leaving its connection
    // state Unknown even as data flows. Tell the freshly-subscribed route the
    // provider is up.
    let route = routes.get(&key);
    broadcast_control_to(
        route,
        ControlKind::ProviderConnected {
            provider: PROVIDER_ID.to_string(),
        },
    )
    .await;
    broadcast_control_to(
        route,
        ControlKind::SubscriptionChanged {
            provider: PROVIDER_ID.to_string(),
            instrument: key.0.clone(),
            kind,
            active: true,
        },
    )
    .await;
    res
}

/// Remove `sink`'s route for `(instrument, kind)`: drop the pair from
/// `subs`, remove it upstream, then drop the route and tell its sink the
/// subscription ended. A route that another session's sink holds, or no
/// route at all, is left alone, upstream included, and the call answers
/// `Ok`: a session torn down after a successor took its pair cannot cancel
/// the successor. A failed upstream removal keeps the pair in `subs`, so a
/// reconnect applies it again.
async fn unsubscribe_route(
    routes: &mut HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    subs: &mut StockSubscriptionList,
    upstream: &mut impl RouteUpstream,
    instrument: Instrument,
    kind: EventKind,
    sink: &mpsc::Sender<MarketEvent>,
) -> Result<()> {
    let key = (instrument, kind);
    if !routes
        .get(&key)
        .is_some_and(|route| route.same_channel(sink))
    {
        return Ok(());
    }
    let (instrument, kind) = key;
    apply_pair_to_list(subs, &instrument, kind, false);
    let mut removal = StockSubscriptionList::new();
    apply_pair_to_list(&mut removal, &instrument, kind, true);
    let res = upstream.remove(&removal).await;
    if res.is_ok() {
        // Drop the route after the upstream ack so any tail events still in
        // flight are routable.
        let removed = routes.remove(&(instrument.clone(), kind));
        broadcast_control_to(
            removed.as_ref(),
            ControlKind::SubscriptionChanged {
                provider: PROVIDER_ID.to_string(),
                instrument,
                kind,
                active: false,
            },
        )
        .await;
    } else {
        // The pair is still streaming upstream, so a reconnect must apply it.
        apply_pair_to_list(subs, &instrument, kind, true);
    }
    res
}

fn is_empty(list: &StockSubscriptionList) -> bool {
    list.bars.as_ref().is_none_or(Vec::is_empty)
        && list.daily_bars.as_ref().is_none_or(Vec::is_empty)
        && list.quotes.as_ref().is_none_or(Vec::is_empty)
        && list.trades.as_ref().is_none_or(Vec::is_empty)
}

fn apply_pair_to_list(
    list: &mut StockSubscriptionList,
    instrument: &Instrument,
    kind: EventKind,
    add: bool,
) {
    let symbol = instrument.symbol().to_string();
    match kind {
        EventKind::Trade => mutate_field(&mut list.trades, &symbol, add),
        EventKind::Quote => mutate_field(&mut list.quotes, &symbol, add),
        EventKind::Bar(BarInterval::OneMinute) => mutate_field(&mut list.bars, &symbol, add),
        EventKind::Bar(BarInterval::OneDay) => mutate_field(&mut list.daily_bars, &symbol, add),
        // Unsupported intervals were rejected by `supports`; ignore here.
        EventKind::Bar(_) => {}
    }
}

fn mutate_field(field: &mut Option<Vec<String>>, symbol: &str, add: bool) {
    let list = field.get_or_insert_with(Vec::new);
    if add {
        if !list.iter().any(|s| s == symbol) {
            list.push(symbol.to_string());
        }
    } else {
        list.retain(|s| s != symbol);
    }
    if list.is_empty() {
        *field = None;
    }
}

fn control_event(kind: ControlKind) -> MarketEvent {
    let now = wall_clock_ts();
    MarketEvent::Control(Control {
        source_ts: now,
        rx_ts: now,
        seq: Seq(0),
        kind,
    })
}

/// Fan a control out to every active sink.
async fn broadcast_control(
    routes: &HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    kind: ControlKind,
) {
    let ev = control_event(kind);
    for sink in routes.values() {
        let _ = sink.send(ev.clone()).await;
    }
}

/// Fan one provider error out to every active sink, every copy carrying the
/// same fresh fan-out id.
async fn broadcast_error(
    routes: &HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    message: String,
) {
    broadcast_control(routes, ControlKind::fanout_error(PROVIDER_ID, message)).await;
}

/// Send a control to one sink, if present.
async fn broadcast_control_to(sink: Option<&mpsc::Sender<MarketEvent>>, kind: ControlKind) {
    if let Some(s) = sink {
        let _ = s.send(control_event(kind)).await;
    }
}

fn wall_clock_ts() -> Timestamp {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "i64 nanos since epoch representable until year 2262"
        )]
        let n = d.as_nanos() as i64;
        n
    });
    Timestamp(nanos)
}

/// Walk a `std::error::Error` source chain to a single string, so a wrapped
/// websocket or I/O cause lands in the control's reason and the log line
/// rather than only the outer variant's text.
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![err.to_string()];
    let mut src = err.source();
    while let Some(e) = src {
        parts.push(e.to_string());
        src = e.source();
    }
    parts.join(" -> ")
}

// ---------------------------------------------------------------------------
// Translation: oxidized_alpaca -> MarketEvent
// ---------------------------------------------------------------------------

/// Translate one stock streaming message into 0..N `MarketEvent`s.
///
/// Public-in-crate so the unit tests at `tests/alpaca_translation.rs` can
/// exercise the boundary directly without spinning up the streaming task.
pub(crate) fn translate_stock_message(msg: StockStreamMessage) -> Vec<MarketEvent> {
    let rx = wall_clock_ts();
    match msg {
        StockStreamMessage::Trade(t) => vec![MarketEvent::Trade(translate_trade(&t, rx))],
        StockStreamMessage::Quote(q) => vec![MarketEvent::Quote(translate_quote(&q, rx))],
        StockStreamMessage::Bar(b) => vec![MarketEvent::Bar(translate_bar(
            &b,
            BarInterval::OneMinute,
            rx,
        ))],
        StockStreamMessage::DailyBar(b) => {
            vec![MarketEvent::Bar(translate_bar(&b, BarInterval::OneDay, rx))]
        }
        StockStreamMessage::UpdatedBar(b) => {
            vec![MarketEvent::Bar(translate_bar(
                &b,
                BarInterval::OneMinute,
                rx,
            ))]
        }
        StockStreamMessage::Error(err) => vec![MarketEvent::Control(Control {
            source_ts: rx,
            rx_ts: rx,
            seq: Seq(0),
            kind: ControlKind::fanout_error(
                PROVIDER_ID,
                format!("{:?}: {}", err.code, err.message),
            ),
        })],
        // Control envelopes, subscription confirmations, and the auxiliary
        // stock variants are not part of the canonical event surface.
        _ => Vec::new(),
    }
}

fn translate_trade(t: &StockTradeEvent, rx: Timestamp) -> Trade {
    Trade {
        instrument: provider_instrument(&t.symbol),
        source_ts: chrono_to_ts(t.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        price: Price::from_f64_round(t.price),
        size: Quantity::from_f64_round(t.size),
    }
}

fn translate_quote(q: &StockQuoteEvent, rx: Timestamp) -> Quote {
    Quote {
        instrument: provider_instrument(&q.symbol),
        source_ts: chrono_to_ts(q.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        bid: Price::from_f64_round(q.bid_price),
        bid_size: Quantity::from_f64_round(q.bid_size),
        ask: Price::from_f64_round(q.ask_price),
        ask_size: Quantity::from_f64_round(q.ask_size),
    }
}

/// Map a historical REST quote onto the canonical [`Quote`], mirroring
/// [`translate_quote`]'s live mapping field for field so a backfilled quote is
/// indistinguishable from a streamed one. The REST payload is keyed by the
/// request path and carries no symbol, so `instrument` is supplied by the
/// caller. `seq` is a placeholder; the authoritative controller re-stamps on
/// delivery.
fn rest_quote_to_quote(q: &StockQuote, instrument: &Instrument, rx: Timestamp) -> Quote {
    Quote {
        instrument: instrument.clone(),
        source_ts: chrono_to_ts(q.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        bid: Price::from_f64_round(q.bid_price),
        bid_size: Quantity::from_units(u64::from(q.bid_size)),
        ask: Price::from_f64_round(q.ask_price),
        ask_size: Quantity::from_units(u64::from(q.ask_size)),
    }
}

fn translate_bar(b: &StockBar, interval: BarInterval, rx: Timestamp) -> Bar {
    Bar {
        instrument: provider_instrument(&b.symbol),
        interval,
        source_ts: chrono_to_ts(b.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        open: Price::from_f64_round(b.open),
        high: Price::from_f64_round(b.high),
        low: Price::from_f64_round(b.low),
        close: Price::from_f64_round(b.close),
        volume: Quantity::from_units(b.volume.max(0).cast_unsigned()),
    }
}

/// The REST snapshot feed matching a live stream feed, so a live-seed snapshot
/// is sourced from the same feed it seeds. `Test` has no snapshot equivalent
/// (there is no synthetic snapshot endpoint), so it maps to `None` and the seed
/// gracefully no-ops rather than silently querying the real production feed.
fn snapshot_feed(stream_feed: AlpacaStreamFeed) -> Option<RestFeed> {
    match stream_feed {
        AlpacaStreamFeed::Iex => Some(RestFeed::IEX),
        AlpacaStreamFeed::Sip => Some(RestFeed::SIP),
        AlpacaStreamFeed::DelayedSip => Some(RestFeed::DelayedSip),
        AlpacaStreamFeed::Test => None,
    }
}

/// The REST feed a historical fetch queries: the explicit setting when one is
/// given, otherwise derived from the stream feed — `SIP` for a SIP stream
/// (the account can query it), `IEX` for everything else (the only feed a
/// free plan may query up to "now"; the history endpoints reject
/// `delayed_sip`). Never unset: Alpaca's own default is SIP, which a free
/// plan refuses for the most recent fifteen minutes.
fn history_feed(explicit: Option<AlpacaHistoryFeed>, stream_feed: AlpacaStreamFeed) -> RestFeed {
    match explicit {
        Some(AlpacaHistoryFeed::Iex) => RestFeed::IEX,
        Some(AlpacaHistoryFeed::Sip) => RestFeed::SIP,
        None => match stream_feed {
            AlpacaStreamFeed::Sip => RestFeed::SIP,
            AlpacaStreamFeed::Iex | AlpacaStreamFeed::DelayedSip | AlpacaStreamFeed::Test => {
                RestFeed::IEX
            }
        },
    }
}

/// Map a stock snapshot onto the canonical event for `kind`, or `None` when the
/// snapshot lacks that datum (or the bar interval has no snapshot field). `seq`
/// is a placeholder; the authoritative controller re-stamps on delivery.
fn snapshot_to_event(
    snap: &oxidized_alpaca::restful::market_data::stock::snapshots::StockSnapshot,
    instrument: &Instrument,
    kind: EventKind,
    rx: Timestamp,
) -> Option<MarketEvent> {
    match kind {
        EventKind::Trade => snap.latest_trade.as_ref().map(|t| {
            MarketEvent::Trade(Trade {
                instrument: instrument.clone(),
                source_ts: chrono_to_ts(t.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                price: Price::from_f64_round(t.price),
                size: Quantity::from_units(u64::from(t.size)),
            })
        }),
        EventKind::Quote => snap.latest_quote.as_ref().map(|q| {
            MarketEvent::Quote(Quote {
                instrument: instrument.clone(),
                source_ts: chrono_to_ts(q.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                bid: Price::from_f64_round(q.bid_price),
                bid_size: Quantity::from_units(u64::from(q.bid_size)),
                ask: Price::from_f64_round(q.ask_price),
                ask_size: Quantity::from_units(u64::from(q.ask_size)),
            })
        }),
        EventKind::Bar(interval) => {
            let bar = match interval {
                BarInterval::OneMinute => snap.minute_bar.as_ref(),
                BarInterval::OneDay => snap.daily_bar.as_ref(),
                _ => None,
            }?;
            Some(MarketEvent::Bar(Bar {
                instrument: instrument.clone(),
                interval,
                source_ts: chrono_to_ts(bar.time),
                rx_ts: rx,
                seq: Seq(0),
                open: Price::from_f64_round(bar.open),
                high: Price::from_f64_round(bar.high),
                low: Price::from_f64_round(bar.low),
                close: Price::from_f64_round(bar.close),
                volume: Quantity::from_units(bar.volume),
            }))
        }
    }
}

fn chrono_to_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp(dt.timestamp_nanos_opt().unwrap_or(0))
}

fn ts_to_chrono(ts: Timestamp) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(ts.0)
}

// ---------------------------------------------------------------------------
// Historical fetch
// ---------------------------------------------------------------------------

/// Map datamancer's adjustment mode onto oxidized-alpaca's. The two enums are
/// 1:1; this keeps the provider boundary the only place that knows the Alpaca
/// type. Applied only to the historical `stock_bars` REST path — live ticks and
/// trades are always raw.
fn map_adjustment(adjustment: Adjustment) -> AlpacaAdjustment {
    match adjustment {
        Adjustment::Raw => AlpacaAdjustment::Raw,
        Adjustment::Split => AlpacaAdjustment::Split,
        Adjustment::Dividend => AlpacaAdjustment::Dividend,
        Adjustment::SpinOff => AlpacaAdjustment::SpinOff,
        Adjustment::All => AlpacaAdjustment::All,
    }
}

/// The historical-trades request for one window. Always names `feed`: left
/// unset, Alpaca defaults to SIP, which a free plan refuses for a window
/// reaching into the last fifteen minutes.
fn trades_request<'a>(
    rest: &'a MarketDataClient,
    symbol: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    feed: RestFeed,
) -> StockTradesRequest<'a> {
    rest.stock_trades(symbol).start(from).end(to).feed(feed)
}

/// The historical-quotes request for one window; see [`trades_request`].
fn quotes_request<'a>(
    rest: &'a MarketDataClient,
    symbol: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    feed: RestFeed,
) -> StockQuotesRequest<'a> {
    rest.stock_quotes(symbol).start(from).end(to).feed(feed)
}

/// The historical-bars request for one window; see [`trades_request`].
fn bars_request<'a>(
    rest: &'a MarketDataClient,
    symbol: &str,
    timeframe: TimeFrame,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    adjustment: AlpacaAdjustment,
    feed: RestFeed,
) -> StockBarsRequest<'a> {
    rest.stock_bars(symbol, timeframe)
        .start(from)
        .end(to)
        .adjustment(adjustment)
        .feed(feed)
}

async fn fetch_history_via(
    rest: &MarketDataClient,
    request: HistoryRequest,
    sink: mpsc::Sender<MarketEvent>,
    feed: RestFeed,
) -> Result<()> {
    let symbol = request.instrument.symbol();
    let from = ts_to_chrono(request.from);
    let to = ts_to_chrono(request.to);
    let rx = wall_clock_ts();
    match request.kind {
        EventKind::Trade => {
            let trades = trades_request(rest, symbol, from, to, feed)
                .execute()
                .await
                .map_err(|e| Error::Provider {
                    provider: PROVIDER_ID.to_string(),
                    message: format!("stock_trades: {e}"),
                })?;
            for t in trades {
                let trade = Trade {
                    instrument: request.instrument.clone(),
                    source_ts: chrono_to_ts(t.timestamp),
                    rx_ts: rx,
                    seq: Seq(0),
                    price: Price::from_f64_round(t.price),
                    size: Quantity::from_units(u64::from(t.size)),
                };
                if sink.send(MarketEvent::Trade(trade)).await.is_err() {
                    return Ok(());
                }
            }
        }
        EventKind::Quote => {
            let quotes = quotes_request(rest, symbol, from, to, feed)
                .execute()
                .await
                .map_err(|e| Error::Provider {
                    provider: PROVIDER_ID.to_string(),
                    message: format!("stock_quotes: {e}"),
                })?;
            for q in &quotes {
                let quote = rest_quote_to_quote(q, &request.instrument, rx);
                if sink.send(MarketEvent::Quote(quote)).await.is_err() {
                    return Ok(());
                }
            }
        }
        EventKind::Bar(interval) => {
            let timeframe = match interval {
                BarInterval::OneSecond => {
                    return Err(Error::UnsupportedEventKind {
                        kind: EventKind::Bar(interval),
                        instrument: request.instrument.clone(),
                        surface: Surface::History,
                    });
                }
                BarInterval::OneMinute => TimeFrame::ONE_MINUTE,
                BarInterval::FiveMinute => TimeFrame::FIVE_MINUTES,
                BarInterval::FifteenMinute => TimeFrame::FIFTEEN_MINUTES,
                BarInterval::OneHour => TimeFrame::ONE_HOUR,
                BarInterval::OneDay => TimeFrame::ONE_DAY,
            };
            let bars = bars_request(
                rest,
                symbol,
                timeframe,
                from,
                to,
                map_adjustment(request.adjustment),
                feed,
            )
            .execute()
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("stock_bars: {e}"),
            })?;
            for b in bars {
                let bar = Bar {
                    instrument: request.instrument.clone(),
                    interval,
                    source_ts: chrono_to_ts(b.time),
                    rx_ts: rx,
                    seq: Seq(0),
                    open: Price::from_f64_round(b.open),
                    high: Price::from_f64_round(b.high),
                    low: Price::from_f64_round(b.low),
                    close: Price::from_f64_round(b.close),
                    volume: Quantity::from_units(b.volume),
                };
                if sink.send(MarketEvent::Bar(bar)).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datamancer_core::{ErrorOrigin, FanoutId};

    const ALL_STREAM_FEEDS: [AlpacaStreamFeed; 4] = [
        AlpacaStreamFeed::Iex,
        AlpacaStreamFeed::Sip,
        AlpacaStreamFeed::DelayedSip,
        AlpacaStreamFeed::Test,
    ];

    /// The equities stream must connect to the production market-data host
    /// for every feed, whatever the account type: the sandbox host that
    /// `oxidized_alpaca` picks for `Paper` only accepts Broker-API sandbox
    /// keys and answers ordinary paper keys with `402 auth failed`.
    #[test]
    fn stream_url_never_targets_the_sandbox_host() {
        for feed in ALL_STREAM_FEEDS {
            let url = stream_url(feed);
            assert!(
                url.starts_with("wss://stream.data.alpaca.markets/"),
                "{feed:?} -> {url}"
            );
            assert!(!url.contains("sandbox"), "{feed:?} -> {url}");
        }
    }

    /// Pin the premise the fix rests on: upstream's `Paper` URL *is* the
    /// sandbox host for the real feeds (Test has no sandbox variant), so
    /// `STREAM_ACCOUNT_TYPE` must stay `Live` until `oxidized_alpaca` stops
    /// routing paper accounts there. If this fails, upstream changed and the
    /// override can be revisited.
    #[test]
    fn upstream_paper_url_is_the_sandbox_host() {
        for feed in [
            AlpacaStreamFeed::Iex,
            AlpacaStreamFeed::Sip,
            AlpacaStreamFeed::DelayedSip,
        ] {
            let paper = streaming_feed(feed).url(AccountType::Paper);
            assert!(paper.contains("sandbox"), "{feed:?} -> {paper}");
            assert_ne!(paper, stream_url(feed));
        }
        assert_eq!(STREAM_ACCOUNT_TYPE, AccountType::Live);
    }

    #[test]
    fn stream_url_keeps_the_feed_path() {
        assert_eq!(
            stream_url(AlpacaStreamFeed::Iex),
            "wss://stream.data.alpaca.markets/v2/iex"
        );
        assert_eq!(
            stream_url(AlpacaStreamFeed::Sip),
            "wss://stream.data.alpaca.markets/v2/sip"
        );
        assert_eq!(
            stream_url(AlpacaStreamFeed::DelayedSip),
            "wss://stream.data.alpaca.markets/v2/delayed_sip"
        );
        assert_eq!(
            stream_url(AlpacaStreamFeed::Test),
            "wss://stream.data.alpaca.markets/v2/test"
        );
    }

    #[test]
    fn snapshot_feed_mirrors_stream_feed() {
        assert_eq!(snapshot_feed(AlpacaStreamFeed::Iex), Some(RestFeed::IEX));
        assert_eq!(snapshot_feed(AlpacaStreamFeed::Sip), Some(RestFeed::SIP));
        assert_eq!(
            snapshot_feed(AlpacaStreamFeed::DelayedSip),
            Some(RestFeed::DelayedSip)
        );
        // Test has no snapshot equivalent -> seed no-ops instead of hitting prod.
        assert_eq!(snapshot_feed(AlpacaStreamFeed::Test), None);
    }

    #[test]
    fn snapshot_maps_kind_to_event() {
        use oxidized_alpaca::restful::market_data::stock::snapshots::StockSnapshot;

        // Deserialize a snapshot fixture (Alpaca's wire field names).
        let json = r#"{
            "latestTrade": {"t":"2024-01-02T15:00:00Z","x":"V","p":187.5,"s":10,"c":[],"z":"C"},
            "latestQuote": {"t":"2024-01-02T15:00:00Z","bx":"V","bp":187.4,"bs":3,"ax":"V","ap":187.6,"as":4,"c":[],"z":"C"},
            "minuteBar": {"t":"2024-01-02T15:00:00Z","o":187.0,"c":187.5,"h":188.0,"l":186.5,"v":1000,"n":50,"vw":187.3},
            "dailyBar": {"t":"2024-01-02T00:00:00Z","o":185.0,"c":187.5,"h":189.0,"l":184.0,"v":900000,"n":5000,"vw":186.9},
            "prevDailyBar": null
        }"#;
        let snap: StockSnapshot = serde_json::from_str(json).unwrap();
        let inst = provider_instrument("AAPL");
        let rx = Timestamp(42);

        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Trade, rx),
            Some(MarketEvent::Trade(_))
        ));
        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Quote, rx),
            Some(MarketEvent::Quote(_))
        ));
        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneMinute), rx),
            Some(MarketEvent::Bar(_))
        ));
        assert!(matches!(
            snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneDay), rx),
            Some(MarketEvent::Bar(_))
        ));
        // Unsupported interval -> None.
        assert!(
            snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::FiveMinute), rx).is_none()
        );
    }

    #[test]
    fn translates_trade_message() {
        let json = r#"{"T":"t","S":"AAPL","i":12345,"x":"V","p":150.10,"s":100,"c":["@"],"t":"2024-01-02T15:30:00.123456789Z","z":"C"}"#;
        let msg: StockStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_stock_message(msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Trade(t) => {
                assert_eq!(t.instrument.symbol(), "AAPL");
                assert_eq!(t.size, Quantity::from_units(100));
                assert_eq!(t.price, Price::from_f64_round(150.10));
                assert_eq!(t.source_ts.0, 1_704_209_400_123_456_789);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn translates_quote_message() {
        let json = r#"{"T":"q","S":"MSFT","ax":"V","ap":420.10,"as":3,"bx":"V","bp":420.05,"bs":2,"c":["R"],"t":"2024-01-02T15:30:00Z","z":"C"}"#;
        let msg: StockStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_stock_message(msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Quote(q) => {
                assert_eq!(q.instrument.symbol(), "MSFT");
                assert_eq!(q.bid, Price::from_f64_round(420.05));
                assert_eq!(q.ask, Price::from_f64_round(420.10));
                assert_eq!(q.bid_size, Quantity::from_units(2));
                assert_eq!(q.ask_size, Quantity::from_units(3));
            }
            other => panic!("expected Quote, got {other:?}"),
        }
    }

    /// The REST quote payload carries no symbol (it is keyed by the request
    /// path), so the instrument must come from the `HistoryRequest` rather than
    /// the wire — unlike the streaming quote, which is self-describing.
    #[test]
    fn translates_rest_quote() {
        let json = r#"{"t":"2024-01-02T15:30:00Z","bx":"V","bp":420.05,"bs":2,"ax":"V","ap":420.10,"as":3,"c":["R"],"z":"C"}"#;
        let rest: StockQuote = serde_json::from_str(json).unwrap();
        let instrument = provider_instrument("MSFT");
        let rx = Timestamp(1_700_000_000_000_000_000);

        let q = rest_quote_to_quote(&rest, &instrument, rx);

        assert_eq!(q.instrument, instrument);
        assert_eq!(q.bid, Price::from_f64_round(420.05));
        assert_eq!(q.ask, Price::from_f64_round(420.10));
        assert_eq!(q.bid_size, Quantity::from_units(2));
        assert_eq!(q.ask_size, Quantity::from_units(3));
        assert_eq!(q.rx_ts, rx);
        // Placeholder; the authoritative controller re-stamps on delivery.
        assert_eq!(q.seq, Seq(0));
    }

    /// The historical and live paths must agree on the mapped result, so a
    /// consumer cannot tell a backfilled quote from a streamed one.
    #[test]
    fn rest_and_streaming_quotes_agree() {
        let rest: StockQuote = serde_json::from_str(
            r#"{"t":"2024-01-02T15:30:00Z","bx":"V","bp":420.05,"bs":2,"ax":"V","ap":420.10,"as":3,"c":["R"],"z":"C"}"#,
        )
        .unwrap();
        let live: StockQuoteEvent = serde_json::from_str(
            r#"{"T":"q","S":"MSFT","ax":"V","ap":420.10,"as":3,"bx":"V","bp":420.05,"bs":2,"c":["R"],"t":"2024-01-02T15:30:00Z","z":"C"}"#,
        )
        .unwrap();
        let rx = Timestamp(1_700_000_000_000_000_000);

        let from_rest = rest_quote_to_quote(&rest, &provider_instrument("MSFT"), rx);
        let from_live = translate_quote(&live, rx);

        assert_eq!(from_rest, from_live);
    }

    #[test]
    fn translates_minute_bar() {
        let json = r#"{"T":"b","S":"AAPL","o":150.0,"h":151.0,"l":149.5,"c":150.5,"v":12345,"vw":150.25,"n":42,"t":"2024-01-02T15:30:00Z"}"#;
        let msg: StockStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_stock_message(msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Bar(b) => {
                assert_eq!(b.instrument.symbol(), "AAPL");
                assert_eq!(b.interval, BarInterval::OneMinute);
                assert_eq!(b.volume, Quantity::from_units(12345));
            }
            other => panic!("expected Bar, got {other:?}"),
        }
    }

    #[test]
    fn translates_daily_bar() {
        let json = r#"{"T":"d","S":"AAPL","o":150.0,"h":151.0,"l":149.5,"c":150.5,"v":12345,"t":"2024-01-02T00:00:00Z"}"#;
        let msg: StockStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_stock_message(msg);
        match &events[0] {
            MarketEvent::Bar(b) => assert_eq!(b.interval, BarInterval::OneDay),
            other => panic!("expected Bar, got {other:?}"),
        }
    }

    #[test]
    fn translates_error_to_provider_error_control() {
        let json = r#"{"T":"error","code":400,"msg":"invalid"}"#;
        let msg: StockStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_stock_message(msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Control(Control {
                kind:
                    ControlKind::ProviderError {
                        provider,
                        message,
                        origin: ErrorOrigin::Fanout(_),
                        ..
                    },
                ..
            }) => {
                assert_eq!(provider, PROVIDER_ID);
                assert!(message.contains("invalid"), "msg={message:?}");
            }
            other => panic!("expected ProviderError control, got {other:?}"),
        }
    }

    #[test]
    fn subscription_list_apply_add_remove() {
        let mut list = StockSubscriptionList::new();
        let aapl = provider_instrument("AAPL");
        apply_pair_to_list(&mut list, &aapl, EventKind::Trade, true);
        apply_pair_to_list(
            &mut list,
            &aapl,
            EventKind::Bar(BarInterval::OneMinute),
            true,
        );
        assert_eq!(list.trades.as_deref(), Some(&["AAPL".to_string()][..]));
        assert_eq!(list.bars.as_deref(), Some(&["AAPL".to_string()][..]));
        apply_pair_to_list(&mut list, &aapl, EventKind::Trade, false);
        apply_pair_to_list(
            &mut list,
            &aapl,
            EventKind::Bar(BarInterval::OneMinute),
            false,
        );
        assert!(list.trades.is_none());
        assert!(list.bars.is_none());
    }

    #[test]
    fn maps_core_adjustment_to_alpaca_adjustment() {
        use datamancer_core::Adjustment as Core;
        use oxidized_alpaca::restful::market_data::stock::Adjustment as Alpaca;
        assert_eq!(map_adjustment(Core::Raw), Alpaca::Raw);
        assert_eq!(map_adjustment(Core::Split), Alpaca::Split);
        assert_eq!(map_adjustment(Core::Dividend), Alpaca::Dividend);
        assert_eq!(map_adjustment(Core::SpinOff), Alpaca::SpinOff);
        assert_eq!(map_adjustment(Core::All), Alpaca::All);
    }

    #[test]
    fn an_etf_is_history_only_because_live_decodes_as_equity() {
        let p = AlpacaProvider::new(AlpacaProviderConfig::default());
        let etf = Instrument::new(ProviderId::from_static(PROVIDER_ID), AssetClass::Etf, "SPY");
        assert!(!p.supports(&etf, EventKind::Trade, Surface::Live));
        assert!(p.supports(&etf, EventKind::Trade, Surface::History));
    }

    #[test]
    fn provider_supports_live_kinds() {
        let p = AlpacaProvider::new(AlpacaProviderConfig::default());
        let inst = provider_instrument("AAPL");
        assert!(p.supports(&inst, EventKind::Trade, Surface::Live));
        assert!(p.supports(&inst, EventKind::Quote, Surface::Live));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneMinute), Surface::Live));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneDay), Surface::Live));
        // The websocket streams only minute and daily bars.
        assert!(!p.supports(
            &inst,
            EventKind::Bar(BarInterval::FiveMinute),
            Surface::Live
        ));
        assert!(!p.supports(&inst, EventKind::Bar(BarInterval::OneSecond), Surface::Live));
    }

    /// The REST bar surface is strictly wider than the streamed one: `stock_bars`
    /// serves five intervals where the websocket streams two. Under the old flat
    /// predicate this asymmetry was unrepresentable, and historical 5m/15m/1h
    /// requests were rejected for a provider that could serve them.
    #[test]
    fn provider_supports_history_kinds() {
        let p = AlpacaProvider::new(AlpacaProviderConfig::default());
        let inst = provider_instrument("AAPL");
        assert!(p.supports(&inst, EventKind::Trade, Surface::History));
        assert!(p.supports(&inst, EventKind::Quote, Surface::History));
        for interval in [
            BarInterval::OneMinute,
            BarInterval::FiveMinute,
            BarInterval::FifteenMinute,
            BarInterval::OneHour,
            BarInterval::OneDay,
        ] {
            assert!(
                p.supports(&inst, EventKind::Bar(interval), Surface::History),
                "history should serve {interval:?}"
            );
        }
        // No REST second-bar endpoint.
        assert!(!p.supports(
            &inst,
            EventKind::Bar(BarInterval::OneSecond),
            Surface::History
        ));
    }

    #[test]
    fn provider_rejects_foreign_asset_class_on_both_surfaces() {
        let p = AlpacaProvider::new(AlpacaProviderConfig::default());
        let crypto = Instrument::new(
            ProviderId::from_static(PROVIDER_ID),
            AssetClass::Crypto,
            "BTC/USD",
        );
        for surface in [Surface::Live, Surface::History] {
            assert!(!p.supports(&crypto, EventKind::Trade, surface));
        }
    }

    #[tokio::test]
    async fn disabled_provider_parks_and_fails_subscribes_fast() {
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let p = AlpacaProvider::new(AlpacaProviderConfig {
            settings: SettingsSource::Watch(rx),
            ..Default::default()
        });
        let (sink, _events) = tokio::sync::mpsc::channel(8);
        let handle = p.start_live(sink).await.expect("start_live");
        let err = handle
            .subscribe(provider_instrument("AAPL"), EventKind::Trade)
            .await
            .expect_err("disabled provider must fail subscribes fast");
        assert!(matches!(err, Error::ProviderDisabled { .. }), "{err:?}");
        let err = p
            .list_instruments()
            .await
            .expect_err("a disabled provider has no REST client");
        assert!(matches!(err, Error::ProviderDisabled { .. }), "{err:?}");
        // `start_live` already returns `Box<dyn LiveHandle>`; `close` takes
        // `self: Box<Self>`, so call it directly on the box.
        handle.close().await.expect("close while parked");
    }

    #[tokio::test]
    async fn missing_rest_credentials_are_unauthenticated() {
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let p = AlpacaProvider::new(AlpacaProviderConfig {
            credentials: CredentialsSource::Watch(rx),
            ..Default::default()
        });
        let err = p
            .list_instruments()
            .await
            .expect_err("no credentials, no trading client");
        assert!(matches!(err, Error::Unauthenticated { .. }), "{err:?}");
        let (sink, _events) = tokio::sync::mpsc::channel(8);
        let request = HistoryRequest {
            instrument: provider_instrument("AAPL"),
            kind: EventKind::Trade,
            from: Timestamp(0),
            to: Timestamp(1),
            adjustment: datamancer_core::Adjustment::default(),
        };
        let err = p
            .fetch_history(request, sink)
            .await
            .expect_err("no credentials, no market-data client");
        assert!(matches!(err, Error::Unauthenticated { .. }), "{err:?}");
    }

    #[test]
    fn a_rest_refusal_is_decided_from_one_snapshot() {
        let (settings_tx, settings_rx) = tokio::sync::watch::channel(None);
        let (_creds_tx, creds_rx) = tokio::sync::watch::channel(None);
        let p = AlpacaProvider::new(AlpacaProviderConfig {
            settings: SettingsSource::Watch(settings_rx),
            credentials: CredentialsSource::Watch(creds_rx),
            ..Default::default()
        });

        let disabled = p.rest_clients();
        settings_tx.send(Some(AlpacaSettings::default())).unwrap();
        let err = disabled.trading().expect_err("taken while disabled");
        assert!(matches!(err, Error::ProviderDisabled { .. }), "{err:?}");

        let enabled = p.rest_clients();
        settings_tx.send(None).unwrap();
        let err = enabled
            .market_data()
            .expect_err("taken without credentials");
        assert!(matches!(err, Error::Unauthenticated { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn settings_source_default_is_enabled_paper() {
        let cfg = AlpacaProviderConfig::default();
        assert_eq!(
            cfg.settings.current(),
            Some(AlpacaSettings {
                account_type: AccountType::Paper,
                history_feed: None,
            })
        );
        assert_eq!(cfg.settings.current(), Some(AlpacaSettings::default()));
    }

    /// With no explicit setting the history feed follows the stream feed:
    /// SIP history for a SIP stream, IEX for everything else — the only feed
    /// a free plan may query up to "now" (the history endpoints reject
    /// `delayed_sip`, so a delayed-SIP stream also backfills from IEX).
    #[test]
    fn history_feed_follows_the_stream_feed_by_default() {
        assert_eq!(history_feed(None, AlpacaStreamFeed::Sip), RestFeed::SIP);
        for stream in [
            AlpacaStreamFeed::Iex,
            AlpacaStreamFeed::DelayedSip,
            AlpacaStreamFeed::Test,
        ] {
            assert_eq!(history_feed(None, stream), RestFeed::IEX, "{stream:?}");
        }
    }

    /// The derived feed is never one the history endpoints refuse outright:
    /// `delayed_sip` is a stream/snapshot feed only (`400 invalid feed` on
    /// bars, trades and quotes).
    #[test]
    fn history_feed_never_derives_delayed_sip() {
        for stream in ALL_STREAM_FEEDS {
            assert_ne!(
                history_feed(None, stream),
                RestFeed::DelayedSip,
                "{stream:?}"
            );
        }
    }

    #[test]
    fn explicit_history_feed_overrides_the_stream_feed() {
        for stream in ALL_STREAM_FEEDS {
            assert_eq!(
                history_feed(Some(AlpacaHistoryFeed::Iex), stream),
                RestFeed::IEX
            );
            assert_eq!(
                history_feed(Some(AlpacaHistoryFeed::Sip), stream),
                RestFeed::SIP
            );
        }
    }

    /// A REST client that never talks to the network: enough to build and
    /// serialize request builders.
    fn offline_rest_client() -> MarketDataClient {
        MarketDataClient::new_with_credentials(
            AccountType::Paper,
            oxidized_alpaca::ApiKey::new("key-id", "secret"),
        )
        .expect("an offline REST client builds without network access")
    }

    /// Every historical request names its feed on the wire. The builders
    /// serialize to the query string (`serde_urlencoded` upstream), so the
    /// JSON view is the same field set; an absent `feed` key here would mean
    /// Alpaca falls back to SIP and a free plan 403s a window ending at now.
    #[test]
    fn history_requests_always_name_a_feed() {
        let rest = offline_rest_client();
        let from = DateTime::<Utc>::from_timestamp(1_704_200_000, 0).unwrap();
        let to = DateTime::<Utc>::from_timestamp(1_704_300_000, 0).unwrap();

        let bars = serde_json::to_value(bars_request(
            &rest,
            "AAPL",
            TimeFrame::ONE_DAY,
            from,
            to,
            AlpacaAdjustment::All,
            RestFeed::IEX,
        ))
        .unwrap();
        assert_eq!(bars["feed"], "iex", "{bars}");
        assert_eq!(bars["adjustment"], "all", "{bars}");

        let trades =
            serde_json::to_value(trades_request(&rest, "AAPL", from, to, RestFeed::SIP)).unwrap();
        assert_eq!(trades["feed"], "sip", "{trades}");

        let quotes =
            serde_json::to_value(quotes_request(&rest, "AAPL", from, to, RestFeed::IEX)).unwrap();
        assert_eq!(quotes["feed"], "iex", "{quotes}");
    }

    #[test]
    fn assets_to_entries_filters_and_maps() {
        // Fixture mirrors Alpaca's `/v2/assets` JSON, including a row that
        // must be filtered (non-tradable) and an asset class outside v0.
        let json = r#"[
            {
                "id":"1","class":"us_equity","exchange":"NASDAQ","symbol":"AAPL",
                "name":"Apple Inc.","status":"active","tradable":true,
                "marginable":true,"shortable":true,"easy_to_borrow":true,
                "fractionable":true,"attributes":[]
            },
            {
                "id":"2","class":"us_equity","exchange":"NYSE","symbol":"GE",
                "name":"General Electric","status":"active","tradable":false,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":false,"attributes":[]
            },
            {
                "id":"3","class":"us_option","exchange":"AMEX","symbol":"AAPL250117C00150000",
                "name":"AAPL 2025 Call","status":"active","tradable":true,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":false,"attributes":[]
            },
            {
                "id":"4","class":"us_equity","exchange":"NYSEARCA","symbol":"SPY",
                "name":"SPDR S&P 500 ETF","status":"active","tradable":true,
                "marginable":true,"shortable":true,"easy_to_borrow":true,
                "fractionable":true,"attributes":["etp"]
            }
        ]"#;
        let assets: Vec<Asset> = serde_json::from_str(json).expect("parse fixture");
        let entries = assets_to_entries(&assets);
        // AAPL and SPY pass; GE filtered (not tradable); option skipped.
        let symbols: Vec<&str> = entries.iter().map(|e| e.instrument.symbol()).collect();
        assert_eq!(symbols, vec!["AAPL", "SPY"]);
        for e in &entries {
            assert_eq!(e.instrument.provider().as_str(), PROVIDER_ID);
            assert_eq!(e.instrument.asset_class(), AssetClass::Equity);
        }
    }

    #[test]
    fn assets_to_entries_maps_fractional_flag_and_policy() {
        let json = r#"[
            {
                "id":"1","class":"us_equity","exchange":"NASDAQ","symbol":"AAPL",
                "name":"Apple Inc.","status":"active","tradable":true,
                "marginable":true,"shortable":true,"easy_to_borrow":true,
                "fractionable":true,"attributes":[]
            },
            {
                "id":"2","class":"us_equity","exchange":"NYSE","symbol":"GE",
                "name":"General Electric","status":"active","tradable":true,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":false,"attributes":[]
            }
        ]"#;
        let assets: Vec<Asset> = serde_json::from_str(json).expect("parse fixture");
        let entries = assets_to_entries(&assets);
        let frac = entries
            .iter()
            .find(|e| e.instrument.symbol() == "AAPL")
            .unwrap();
        let caps = frac.capabilities.as_ref().unwrap();
        assert_eq!(caps.fractionable, Some(true));
        assert_eq!(caps.supports_notional_orders, Some(true));
        assert_eq!(
            caps.allowed_order_types.as_deref(),
            Some(&FRACTIONAL_ORDER_TYPES[..])
        );
        assert_eq!(caps.allowed_tif.as_deref(), Some(&[TimeInForce::Day][..]));
        assert!(caps.min_qty.is_none()); // not advertised

        let non_frac = entries
            .iter()
            .find(|e| e.instrument.symbol() == "GE")
            .unwrap();
        let non_frac_caps = non_frac.capabilities.as_ref().unwrap();
        assert_eq!(non_frac_caps.fractionable, Some(false));
        // Non-fractionable: no notional support, and the fractional-only
        // order-type/TIF vocabulary is left unset (unknown, not asserted).
        assert_eq!(non_frac_caps.supports_notional_orders, Some(false));
        assert!(non_frac_caps.allowed_order_types.is_none());
        assert!(non_frac_caps.allowed_tif.is_none());
    }

    #[test]
    fn asset_to_entry_rejects_ineligible_assets() {
        // A crypto, option, or non-tradable asset resolved through the equities
        // provider must NOT become an authoritative equity entry — the on-demand
        // `capabilities` lookup relies on this gate to avoid mislabeling.
        let json = r#"[
            {
                "id":"1","class":"crypto","exchange":"CRYPTO","symbol":"BTC/USD",
                "name":"Bitcoin","status":"active","tradable":true,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":true,"attributes":[]
            },
            {
                "id":"2","class":"us_option","exchange":"AMEX","symbol":"AAPL250117C00150000",
                "name":"AAPL 2025 Call","status":"active","tradable":true,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":false,"attributes":[]
            },
            {
                "id":"3","class":"us_equity","exchange":"NYSE","symbol":"GE",
                "name":"General Electric","status":"active","tradable":false,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":false,"attributes":[]
            },
            {
                "id":"4","class":"us_equity","exchange":"NASDAQ","symbol":"AAPL",
                "name":"Apple Inc.","status":"active","tradable":true,
                "marginable":true,"shortable":true,"easy_to_borrow":true,
                "fractionable":true,"attributes":[]
            }
        ]"#;
        let assets: Vec<Asset> = serde_json::from_str(json).expect("parse fixture");
        assert!(asset_to_entry(&assets[0]).is_none(), "crypto rejected");
        assert!(asset_to_entry(&assets[1]).is_none(), "option rejected");
        assert!(
            asset_to_entry(&assets[2]).is_none(),
            "non-tradable rejected"
        );
        let ok = asset_to_entry(&assets[3]).expect("eligible equity");
        assert_eq!(ok.instrument.symbol(), "AAPL");
        assert_eq!(ok.instrument.asset_class(), AssetClass::Equity);
    }

    /// Two live sessions on one provider share one hub (and so one socket):
    /// the second `start_live` spawns nothing, both handles are served by the
    /// same task, and the task exits once the provider's sender and every
    /// handle are gone. Parked (disabled) so no socket is opened.
    #[tokio::test]
    async fn two_start_live_calls_share_one_hub() {
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let p = AlpacaProvider::new(AlpacaProviderConfig {
            settings: SettingsSource::Watch(rx),
            ..Default::default()
        });
        let (sink_a, _events_a) = mpsc::channel(8);
        let (sink_b, _events_b) = mpsc::channel(8);
        let a = p.start_live(sink_a).await.expect("start_live a");
        let b = p.start_live(sink_b).await.expect("start_live b");
        assert_eq!(p.hub_spawns(), 1, "one hub for two sessions");

        for (handle, symbol) in [(&a, "AAPL"), (&b, "SPY")] {
            let err = handle
                .subscribe(provider_instrument(symbol), EventKind::Trade)
                .await
                .expect_err("parked hub fails subscribes fast");
            assert!(matches!(err, Error::ProviderDisabled { .. }), "{err:?}");
        }

        let task = p.take_hub_task().expect("the hub was spawned");
        a.close().await.expect("close a");
        b.close().await.expect("close b");
        // Provider sender and both handles gone: the command channel closes
        // and the parked hub exits.
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("hub exits once nothing can command it")
            .expect("hub task did not panic");
    }

    /// Data routes to the one sink owning its pair and nowhere else; a pair
    /// nobody subscribed to is dropped rather than misrouted; controls fan
    /// out to every sink.
    #[tokio::test]
    async fn dispatch_routes_data_by_pair_and_fans_out_controls() {
        let now = Timestamp(1);
        let trade = |symbol: &str| {
            MarketEvent::Trade(Trade {
                instrument: provider_instrument(symbol),
                source_ts: now,
                rx_ts: now,
                seq: Seq(0),
                price: Price::from_f64_round(1.0),
                size: Quantity::from_units(1),
            })
        };
        let mut routes = HashMap::new();
        let (aapl_tx, mut aapl_rx) = mpsc::channel(8);
        let (spy_tx, mut spy_rx) = mpsc::channel(8);
        routes.insert((provider_instrument("AAPL"), EventKind::Trade), aapl_tx);
        routes.insert((provider_instrument("SPY"), EventKind::Quote), spy_tx);

        dispatch_event(&routes, trade("AAPL")).await;
        assert!(
            matches!(aapl_rx.try_recv(), Ok(MarketEvent::Trade(t)) if t.instrument.symbol() == "AAPL")
        );
        assert!(
            spy_rx.try_recv().is_err(),
            "SPY quote sink must not see an AAPL trade"
        );

        // SPY trades have no subscriber (only SPY quotes do): dropped.
        dispatch_event(&routes, trade("SPY")).await;
        assert!(aapl_rx.try_recv().is_err());
        assert!(spy_rx.try_recv().is_err());

        broadcast_control(
            &routes,
            ControlKind::ProviderConnected {
                provider: PROVIDER_ID.to_string(),
            },
        )
        .await;
        for rx in [&mut aapl_rx, &mut spy_rx] {
            assert!(matches!(
                rx.try_recv(),
                Ok(MarketEvent::Control(Control {
                    kind: ControlKind::ProviderConnected { .. },
                    ..
                }))
            ));
        }
    }

    fn fanout_ids(rx: &mut mpsc::Receiver<MarketEvent>) -> Vec<FanoutId> {
        let mut ids = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            match ev {
                MarketEvent::Control(Control {
                    kind:
                        ControlKind::ProviderError {
                            origin: ErrorOrigin::Fanout(id),
                            ..
                        },
                    ..
                }) => ids.push(id),
                other => panic!("expected a fan-out ProviderError, got {other:?}"),
            }
        }
        ids
    }

    type Routes = HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>;

    fn two_routes() -> (
        Routes,
        mpsc::Receiver<MarketEvent>,
        mpsc::Receiver<MarketEvent>,
    ) {
        let (a_tx, a_rx) = mpsc::channel(8);
        let (b_tx, b_rx) = mpsc::channel(8);
        let mut routes = HashMap::new();
        routes.insert((provider_instrument("AAPL"), EventKind::Trade), a_tx);
        routes.insert((provider_instrument("SPY"), EventKind::Quote), b_tx);
        (routes, a_rx, b_rx)
    }

    #[tokio::test]
    async fn every_copy_of_a_broadcast_error_shares_one_increasing_id() {
        let (routes, mut a_rx, mut b_rx) = two_routes();
        broadcast_error(&routes, "first".to_string()).await;
        broadcast_error(&routes, "second".to_string()).await;
        let a = fanout_ids(&mut a_rx);
        assert_eq!(a, fanout_ids(&mut b_rx));
        assert_eq!(a.len(), 2);
        assert!(a[0] < a[1]);
    }

    #[tokio::test]
    async fn every_copy_of_a_stream_error_shares_one_increasing_id() {
        let (routes, mut a_rx, mut b_rx) = two_routes();
        for _ in 0..2 {
            let msg: StockStreamMessage =
                serde_json::from_str(r#"{"T":"error","code":400,"msg":"invalid"}"#).unwrap();
            for ev in translate_stock_message(msg) {
                dispatch_event(&routes, ev).await;
            }
        }
        let a = fanout_ids(&mut a_rx);
        assert_eq!(a, fanout_ids(&mut b_rx));
        assert_eq!(a.len(), 2);
        assert!(a[0] < a[1]);
    }

    #[test]
    fn event_route_key_matches_pair() {
        let now = Timestamp(1);
        let bar = MarketEvent::Bar(Bar {
            instrument: provider_instrument("AAPL"),
            interval: BarInterval::OneDay,
            source_ts: now,
            rx_ts: now,
            seq: Seq(0),
            open: Price::ZERO,
            high: Price::ZERO,
            low: Price::ZERO,
            close: Price::ZERO,
            volume: Quantity::ZERO,
        });
        assert_eq!(
            event_route_key(&bar),
            Some((
                provider_instrument("AAPL"),
                EventKind::Bar(BarInterval::OneDay)
            ))
        );
        assert_eq!(
            event_route_key(&control_event(ControlKind::ProviderConnected {
                provider: PROVIDER_ID.to_string(),
            })),
            None
        );
    }

    /// `402 auth failed` is the only server error that parks the hub; a
    /// `406 connection limit exceeded` (another connection holds the slot),
    /// `407 slow client` and subscribe-level rejections back off instead.
    /// The payload-less connect-time `StreamingAuth` parks (see the
    /// classifier's docs for the upstream limitation).
    #[test]
    fn stream_failures_park_on_402_and_back_off_on_406() {
        use oxidized_alpaca::streaming::StreamError;
        let server_error = |code: u16| {
            let e: StreamError =
                serde_json::from_str(&format!(r#"{{"code":{code},"msg":"server says {code}"}}"#))
                    .expect("documented code");
            oxidized_alpaca::Error::StreamingError(e)
        };
        assert_eq!(
            classify_stream_failure(&server_error(402)),
            StreamFailure::Unauthenticated
        );
        for code in [406_u16, 407, 405, 409, 400] {
            assert_eq!(
                classify_stream_failure(&server_error(code)),
                StreamFailure::Transient,
                "code {code}"
            );
        }
        assert_eq!(
            classify_stream_failure(&oxidized_alpaca::Error::StreamingAuth),
            StreamFailure::Unauthenticated
        );
        assert_eq!(
            classify_stream_failure(&oxidized_alpaca::Error::UnexpectedConnectionMessage(
                "boom".to_string()
            )),
            StreamFailure::Transient
        );
    }

    /// Records the upstream calls a route command makes; refuses `add` while
    /// `refuse_add` is set.
    #[derive(Default)]
    struct FakeUpstream {
        adds: usize,
        removals: Vec<StockSubscriptionList>,
        refuse_add: bool,
    }

    impl RouteUpstream for FakeUpstream {
        async fn add(&mut self, _subs: &StockSubscriptionList) -> Result<()> {
            self.adds += 1;
            if self.refuse_add {
                return Err(Error::Provider {
                    provider: PROVIDER_ID.to_string(),
                    message: "refused".to_string(),
                });
            }
            Ok(())
        }

        async fn remove(&mut self, removal: &StockSubscriptionList) -> Result<()> {
            self.removals.push(removal.clone());
            Ok(())
        }
    }

    /// A connected hub's route state, served one command at a time.
    struct FakeHub {
        cmd_tx: mpsc::Sender<HubCommand>,
        cmd_rx: mpsc::Receiver<HubCommand>,
        routes: HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
        subs: StockSubscriptionList,
        upstream: FakeUpstream,
    }

    impl FakeHub {
        fn new() -> Self {
            let (cmd_tx, cmd_rx) = mpsc::channel(8);
            Self {
                cmd_tx,
                cmd_rx,
                routes: HashMap::new(),
                subs: StockSubscriptionList::new(),
                upstream: FakeUpstream::default(),
            }
        }

        /// A session's handle onto this hub and the receiver of its sink.
        fn handle(&self) -> (Box<SharedLiveHandle>, mpsc::Receiver<MarketEvent>) {
            let (sink, rx) = mpsc::channel(8);
            let handle = SharedLiveHandle {
                cmd_tx: self.cmd_tx.clone(),
                sink,
                subscribed: Mutex::new(None),
            };
            (Box::new(handle), rx)
        }

        async fn serve_one(&mut self) {
            let cmd = self.cmd_rx.recv().await.expect("a command");
            handle_route_command(&mut self.routes, &mut self.subs, &mut self.upstream, cmd).await;
        }

        /// Route one trade for `instrument` as the socket would.
        async fn deliver_trade(&self, instrument: &Instrument) {
            let now = Timestamp(1);
            dispatch_event(
                &self.routes,
                MarketEvent::Trade(Trade {
                    instrument: instrument.clone(),
                    source_ts: now,
                    rx_ts: now,
                    seq: Seq(0),
                    price: Price::from_f64_round(1.0),
                    size: Quantity::from_units(1),
                }),
            )
            .await;
        }
    }

    /// Whether `rx` holds a trade, past any controls.
    fn has_trade(rx: &mut mpsc::Receiver<MarketEvent>) -> bool {
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, MarketEvent::Trade(_)) {
                return true;
            }
        }
        false
    }

    /// A session torn down after another session took its pair: its late
    /// unsubscribe leaves the successor's route, its data and the upstream
    /// subscription alone, and the successor's own unsubscribe still removes
    /// all three.
    #[tokio::test]
    async fn a_late_unsubscribe_keeps_the_successors_route() {
        let instrument = provider_instrument("AAPL");
        let mut hub = FakeHub::new();
        let (s1, _s1_rx) = hub.handle();
        let (s2, mut s2_rx) = hub.handle();
        let (subscribed, ()) = tokio::join!(
            s1.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        subscribed.expect("s1 subscribes");
        let (subscribed, ()) = tokio::join!(
            s2.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        subscribed.expect("s2 subscribes");

        let (closed, ()) = tokio::join!(s1.close(), hub.serve_one());
        closed.expect("s1 closes");
        assert!(
            hub.upstream.removals.is_empty(),
            "the upstream subscription was removed"
        );
        assert_eq!(hub.subs.trades, Some(vec!["AAPL".to_string()]));
        hub.deliver_trade(&instrument).await;
        assert!(has_trade(&mut s2_rx), "the successor lost its route");

        let (unsubscribed, ()) = tokio::join!(
            s2.unsubscribe(instrument, EventKind::Trade),
            hub.serve_one()
        );
        unsubscribed.expect("s2 unsubscribes");
        assert_eq!(hub.upstream.removals.len(), 1);
        assert!(hub.routes.is_empty());
        assert_eq!(hub.subs.trades, None);
        assert!(matches!(
            s2_rx.try_recv(),
            Ok(MarketEvent::Control(Control {
                kind: ControlKind::SubscriptionChanged { active: false, .. },
                ..
            }))
        ));
    }

    /// A successor's subscribe that the upstream refuses restores the route
    /// it replaced: the old session keeps its data, and its late unsubscribe
    /// still removes the pair upstream.
    #[tokio::test]
    async fn a_refused_successor_restores_the_route_it_replaced() {
        let instrument = provider_instrument("AAPL");
        let mut hub = FakeHub::new();
        let (s1, mut s1_rx) = hub.handle();
        let (s2, _s2_rx) = hub.handle();
        let (subscribed, ()) = tokio::join!(
            s1.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        subscribed.expect("s1 subscribes");

        hub.upstream.refuse_add = true;
        let (refused, ()) = tokio::join!(
            s2.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        assert!(refused.is_err(), "the upstream refused s2");
        assert_eq!(hub.subs.trades, Some(vec!["AAPL".to_string()]));
        hub.deliver_trade(&instrument).await;
        assert!(has_trade(&mut s1_rx), "s1 lost its route");

        let (closed, ()) = tokio::join!(s1.close(), hub.serve_one());
        closed.expect("s1 closes");
        assert_eq!(
            hub.upstream.removals.len(),
            1,
            "the pair was left subscribed upstream"
        );
        assert!(hub.routes.is_empty());
        assert_eq!(hub.subs.trades, None);
    }

    /// A first subscribe the upstream refuses leaves no route, and no pair to
    /// re-apply on the next connect.
    #[tokio::test]
    async fn a_refused_first_subscribe_leaves_nothing_behind() {
        let instrument = provider_instrument("AAPL");
        let mut hub = FakeHub::new();
        let (session, _rx) = hub.handle();
        hub.upstream.refuse_add = true;
        let (refused, ()) = tokio::join!(
            session.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        assert!(refused.is_err(), "the upstream refused the subscribe");
        assert!(hub.routes.is_empty());
        assert_eq!(hub.subs.trades, None);
    }
}

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
//! rejected credential pair (`402`) parks until the credentials rotate when
//! they come from a watched source, and backs off like any other failure
//! otherwise; a `406` or any other server error is transient and backs off
//! (see `classify_stream_failure` for the upstream caveat).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datamancer_core::{
    Adjustment, AssetClass, BarInterval, Control, ControlKind, Error, EventKind, HistoryRequest,
    Instrument, InstrumentCapabilities, InstrumentEntry, LiveHandle, MarketEvent, OrderType, Price,
    Provider, ProviderId, Quantity, Result, Seq, Surface, TimeInForce, Timestamp, Trade,
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
use tokio::sync::{mpsc, watch};

use super::credentials::{AlpacaCredentials, CredentialsSource, Resolved};
use super::hub::{
    Connector, HubSlot, PresentCredentials, SharedLiveHandle, StreamFailure, SubscriptionSet,
    Upstream, wall_clock_ts,
};
use super::runtime::SettingsSource;
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
    hub: HubSlot<StockConnector>,
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
            hub: HubSlot::idle(),
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
            hub: HubSlot::idle(),
        }
    }

    /// How many hub tasks this provider has spawned (test observability).
    #[cfg(test)]
    fn hub_spawns(&self) -> usize {
        self.hub.spawns()
    }

    /// Take the hub task out of the provider, dropping the provider's own
    /// command sender: once every handle is gone too, the task exits and the
    /// returned join handle resolves (test observability).
    #[cfg(test)]
    fn take_hub_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.hub.take_task()
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
        // Every `start_live` goes through the one slot, so the provider never
        // holds more than one upstream socket.
        let cmd_tx = self.hub.ensure(|| StockConnector {
            cfg: self.cfg.clone(),
        });
        Ok(Box::new(SharedLiveHandle::new(cmd_tx, sink)))
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

/// Classify a connect-time, subscribe or mid-stream error from
/// `oxidized_alpaca`.
///
/// `406 connection limit exceeded` is transient: another connection on the
/// same key pair holds the slot, which clears when it closes. Only
/// `402 auth failed` means the credentials themselves are rejected, whether
/// it arrives mid-stream (`Error::StreamingError`) or as the server's
/// refusal of a subscribe (`Error::StreamingSubscribe`). Any other code on
/// either variant, `401 not authenticated` included, is transient.
///
/// Limitation to know about: at **connect** time `oxidized_alpaca` 0.0.10
/// (and 0.0.11) collapses every non-`authenticated` auth response into the
/// payload-less `Error::StreamingAuth` and only logs the envelope, so a 406
/// there is indistinguishable from a 402 and lands in the park. The hub
/// itself never opens a second connection, so a connect-time rejection is
/// treated as the credential case it is in practice; once upstream carries
/// the `StreamError` on that path, it classifies through the same arm as
/// the mid-stream case below with no further change here.
pub(super) fn classify_stream_failure(err: &oxidized_alpaca::Error) -> StreamFailure {
    match err {
        oxidized_alpaca::Error::StreamingAuth => StreamFailure::Unauthenticated,
        oxidized_alpaca::Error::StreamingError(e)
        | oxidized_alpaca::Error::StreamingSubscribe(e) => match e.code {
            StreamErrorCode::AuthFailed => StreamFailure::Unauthenticated,
            _ => StreamFailure::Transient,
        },
        _ => StreamFailure::Transient,
    }
}

// ---------------------------------------------------------------------------
// Hub connector
// ---------------------------------------------------------------------------

/// How the hub opens and classifies the stock market-data stream.
struct StockConnector {
    cfg: AlpacaProviderConfig,
}

impl Connector for StockConnector {
    const PROVIDER_ID: &'static str = PROVIDER_ID;
    type Settings = AlpacaSettings;
    type Failure = oxidized_alpaca::Error;
    type Upstream = StreamingStockClient;

    fn settings(&self) -> &SettingsSource<AlpacaSettings> {
        &self.cfg.settings
    }

    fn credentials(&self) -> &CredentialsSource {
        &self.cfg.credentials
    }

    fn reconnect(&self) -> &ReconnectPolicy {
        &self.cfg.reconnect
    }

    async fn connect(
        &self,
        settings: &AlpacaSettings,
        credentials: PresentCredentials,
    ) -> std::result::Result<StreamingStockClient, oxidized_alpaca::Error> {
        let feed = streaming_feed(self.cfg.stream_feed);
        // Every path connects through `new_with_credentials` so the account
        // type it sees is `STREAM_ACCOUNT_TYPE` (the production data host);
        // the configured account type only picks which env pair `Env` reads.
        tracing::info!(
            provider = PROVIDER_ID,
            url = stream_url(self.cfg.stream_feed),
            account_type = ?settings.account_type,
            "connecting to the market-data stream"
        );
        let credentials = match credentials {
            PresentCredentials::Env => AlpacaCredentials::from_env(settings.account_type)?,
            PresentCredentials::Creds(c) => c,
        };
        StreamingStockClient::new_with_credentials(
            STREAM_ACCOUNT_TYPE,
            feed,
            credentials.to_api_key(),
        )
        .await
    }

    fn classify(failure: &oxidized_alpaca::Error) -> StreamFailure {
        classify_stream_failure(failure)
    }
}

impl Upstream for StreamingStockClient {
    type Subs = StockSubscriptionList;
    type Failure = oxidized_alpaca::Error;

    async fn add(
        &mut self,
        subs: &StockSubscriptionList,
    ) -> std::result::Result<(), oxidized_alpaca::Error> {
        self.add_subscriptions(subs).await.map(|_| ())
    }

    async fn remove(
        &mut self,
        removal: &StockSubscriptionList,
    ) -> std::result::Result<(), oxidized_alpaca::Error> {
        self.remove_subscriptions(removal).await.map(|_| ())
    }

    async fn next_events(
        &mut self,
    ) -> std::result::Result<Vec<MarketEvent>, oxidized_alpaca::Error> {
        self.next_message().await.map(translate_stock_message)
    }

    async fn shut_down(self) {
        let _ = StreamingStockClient::shut_down(self).await;
    }
}

impl SubscriptionSet for StockSubscriptionList {
    fn empty() -> Self {
        Self::new()
    }

    fn is_empty(&self) -> bool {
        self.bars.as_ref().is_none_or(Vec::is_empty)
            && self.daily_bars.as_ref().is_none_or(Vec::is_empty)
            && self.quotes.as_ref().is_none_or(Vec::is_empty)
            && self.trades.as_ref().is_none_or(Vec::is_empty)
    }

    fn apply(&mut self, instrument: &Instrument, kind: EventKind, add: bool) {
        apply_pair_to_list(self, instrument, kind, add);
    }
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
pub(super) mod tests {
    use super::*;
    use crate::providers::hub::dispatch_event;
    use crate::providers::hub::tests::{fanout_ids, two_routes};
    use datamancer_core::ErrorOrigin;
    use std::time::Duration;

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
    fn stock_set_is_empty_until_a_pair_is_applied() {
        let mut set = <StockSubscriptionList as SubscriptionSet>::empty();
        assert!(SubscriptionSet::is_empty(&set));
        set.apply(&provider_instrument("AAPL"), EventKind::Trade, true);
        assert!(!SubscriptionSet::is_empty(&set));
        set.apply(&provider_instrument("AAPL"), EventKind::Trade, false);
        assert!(SubscriptionSet::is_empty(&set));
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

    /// A `start_live` that panics while it holds the hub slot (here: polled
    /// once outside a tokio runtime, so the spawn panics) must not take the
    /// provider down for every later caller: a later `start_live` inside a
    /// runtime still succeeds.
    #[test]
    fn a_panicking_start_live_does_not_poison_later_start_lives() {
        use std::future::Future;
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::task::{Context, Poll, Waker};
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let p = AlpacaProvider::new(AlpacaProviderConfig {
            settings: SettingsSource::Watch(rx),
            ..Default::default()
        });
        // Polls `start_live` once by hand; true when it completes with a handle.
        let poll_once = || {
            catch_unwind(AssertUnwindSafe(|| {
                let (sink, _events) = tokio::sync::mpsc::channel(8);
                let mut fut = std::pin::pin!(p.start_live(sink));
                let mut cx = Context::from_waker(Waker::noop());
                matches!(fut.as_mut().poll(&mut cx), Poll::Ready(Ok(_)))
            }))
        };
        assert!(poll_once().is_err(), "a spawn outside a runtime panics");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let later = rt.block_on(async { poll_once() });
        assert!(
            matches!(later, Ok(true)),
            "a later start_live in a runtime must succeed, got {:?}",
            later.map_err(|e| e
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| e.downcast_ref::<String>().cloned()))
        );
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

    #[tokio::test]
    async fn every_copy_of_a_stream_error_shares_one_distinct_id() {
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
        assert_ne!(a[0], a[1]);
    }

    /// A server error envelope carrying `code`, wrapped the way the library
    /// reports it: `oxidized_alpaca::Error::StreamingError` mid-stream, or
    /// `oxidized_alpaca::Error::StreamingSubscribe` when a subscribe is
    /// refused.
    pub(in crate::providers) fn server_error(
        wrap: fn(oxidized_alpaca::streaming::StreamError) -> oxidized_alpaca::Error,
        code: StreamErrorCode,
    ) -> oxidized_alpaca::Error {
        let e: oxidized_alpaca::streaming::StreamError =
            serde_json::from_value(serde_json::json!({ "code": code, "msg": "server says" }))
                .expect("documented code");
        wrap(e)
    }

    /// `402 auth failed` is the only server error read as a rejected
    /// credential; a `406 connection limit exceeded` (another connection
    /// holds the slot), `407 slow client` and the other server codes are
    /// transient. The payload-less connect-time `StreamingAuth` is a
    /// rejected credential too (see the classifier's docs for the upstream
    /// limitation).
    #[test]
    fn stream_failures_park_on_402_and_back_off_on_406() {
        let mid_stream = oxidized_alpaca::Error::StreamingError;
        assert_eq!(
            classify_stream_failure(&server_error(mid_stream, StreamErrorCode::AuthFailed)),
            StreamFailure::Unauthenticated
        );
        for code in [
            StreamErrorCode::ConnectionLimitExceeded,
            StreamErrorCode::SlowClient,
            StreamErrorCode::SymbolLimitExceeded,
            StreamErrorCode::InsufficientSubscription,
            StreamErrorCode::InvalidSyntax,
        ] {
            assert_eq!(
                classify_stream_failure(&server_error(mid_stream, code)),
                StreamFailure::Transient,
                "code {code:?}"
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

    /// A subscribe the server refuses with `402 auth failed` reports a
    /// rejected credential; any other refusal, `401 not authenticated`
    /// included, is transient.
    #[test]
    fn stock_subscribe_refused_with_402_is_unauthenticated() {
        let refused = oxidized_alpaca::Error::StreamingSubscribe;
        assert_eq!(
            StockConnector::classify(&server_error(refused, StreamErrorCode::AuthFailed)),
            StreamFailure::Unauthenticated
        );
        for code in [
            StreamErrorCode::ConnectionLimitExceeded,
            StreamErrorCode::NotAuthenticated,
        ] {
            assert_eq!(
                StockConnector::classify(&server_error(refused, code)),
                StreamFailure::Transient,
                "code {code:?}"
            );
        }
    }
}

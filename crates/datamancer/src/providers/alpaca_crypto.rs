//! Alpaca-backed crypto [`Provider`].
//!
//! Wraps [`oxidized_alpaca`]'s [`StreamingCryptoClient`] for live crypto
//! trades, quotes, and minute/daily bars. Symbols use Alpaca's pair format,
//! e.g. `BTC/USD`. Historical fetch is not implemented — Alpaca exposes
//! crypto history through a separate REST surface and this provider focuses
//! on the streaming side.
//!
//! # Single shared connection
//!
//! Alpaca's crypto websocket allows only one concurrent connection per
//! credential pair. Calling [`Provider::start_live`] multiple times on the
//! same provider therefore must not open multiple sockets. This provider
//! lazily spawns a single hub task on first `start_live`; every subsequent
//! `start_live` returns a [`LiveHandle`] that talks to that hub via a
//! command channel. The hub maintains the upstream socket, applies the
//! aggregate subscription set to it, and routes incoming events to the
//! correct per-session sink based on `(instrument, kind)`.
//!
//! The session-registry rule (at most one live session per pair) makes the
//! routing unambiguous: each `(instrument, kind)` key maps to exactly one
//! sink at any time.
//!
//! # Reconnect
//!
//! On websocket failure the hub tears down the client, sleeps with
//! exponential backoff per the configured [`ReconnectPolicy`], and
//! reconnects, re-applying the active subscription set. A
//! `ProviderDisconnected` / `ProviderConnected` control pair is broadcast
//! to all active sinks so consumers see the gap window.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datamancer_core::{
    AssetClass, Bar, BarInterval, Control, ControlKind, Error, EventKind, HistoryRequest,
    Instrument, InstrumentCapabilities, InstrumentEntry, LiveHandle, MarketEvent, OrderType, Price,
    Provider, ProviderId, Quantity, Quote, Result, Seq, Surface, TimeInForce, Timestamp, Trade,
};
use oxidized_alpaca::{
    AccountType, CryptoFeed, MarketDataClient, TradingClient,
    restful::trading::assets::{
        Asset, AssetClass as AlpacaAssetClass, Status as AlpacaAssetStatus,
    },
    streaming::{
        CryptoStreamMessage, CryptoSubscriptionList, StreamingCryptoClient,
        messages::crypto::{CryptoBarEvent, CryptoQuoteEvent, CryptoTradeEvent},
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

/// Stable provider identifier for the Alpaca crypto provider.
pub const PROVIDER_ID: &str = "alpaca-crypto";

/// Construct an `Instrument` rooted at this provider. Crypto pairs always
/// land as [`AssetClass::Crypto`]; the decoder doesn't need a catalog
/// roundtrip to disambiguate.
fn provider_instrument(symbol: impl Into<String>) -> Instrument {
    Instrument::new(
        ProviderId::from_static(PROVIDER_ID),
        AssetClass::Crypto,
        symbol,
    )
}

// Crypto order policy is NOT the equity fractional policy: Alpaca crypto
// accepts market/limit/stop-limit (no plain stop) with GTC/IOC time-in-force
// (no day/opg/cls). Verified against https://docs.alpaca.markets/docs/crypto-orders.
const CRYPTO_ORDER_TYPES: [OrderType; 3] =
    [OrderType::Market, OrderType::Limit, OrderType::StopLimit];
const CRYPTO_TIF: [TimeInForce; 2] = [TimeInForce::Gtc, TimeInForce::Ioc];

/// Build capabilities for one Alpaca crypto `Asset`. Order types and
/// time-in-force follow crypto's own policy (see
/// [`CRYPTO_ORDER_TYPES`]/[`CRYPTO_TIF`]), which differs from equities. As on
/// the equity side, notional and the fractional `allowed_*` vocabulary only
/// apply to fractional orders, so they are gated on `fractionable` — crypto
/// rows report it trivially true, so in practice this is always populated, but
/// the gate keeps the contract honest if Alpaca ever reports otherwise.
fn crypto_capabilities(asset: &Asset) -> InstrumentCapabilities {
    let mut caps = InstrumentCapabilities::default();
    caps.fractionable = Some(asset.fractionable);
    caps.supports_notional_orders = Some(asset.fractionable);
    if asset.fractionable {
        caps.allowed_order_types = Some(CRYPTO_ORDER_TYPES.to_vec());
        caps.allowed_tif = Some(CRYPTO_TIF.to_vec());
    }
    // Sizing increments (min_qty/qty_increment/price_increment/min_notional)
    // stay None: oxidized_alpaca 0.0.10 Asset carries no sizing fields.
    caps
}

/// Build a catalog entry for one Alpaca crypto `Asset`: the instrument stamped
/// with the authoritative [`AssetClass::Crypto`] plus its capabilities. Shared
/// by bulk listing and the on-demand `capabilities` lookup so both stamp the
/// same class (and the on-demand path corrects the daemon's placeholder).
fn crypto_entry(asset: &Asset) -> InstrumentEntry {
    let instrument = Instrument::new(
        ProviderId::from_static(PROVIDER_ID),
        AssetClass::Crypto,
        asset.symbol.clone(),
    );
    let mut entry = InstrumentEntry::bare(instrument);
    entry.capabilities = Some(crypto_capabilities(asset));
    entry
}

/// Map one Alpaca `Asset` to a catalog entry **iff** it is eligible for this
/// crypto provider: tradable and [`AlpacaAssetClass::Crypto`]. Non-tradable
/// rows and non-crypto classes map to `None`, so neither bulk listing nor the
/// on-demand `capabilities` lookup ever stamps an ineligible asset as an
/// authoritative crypto instrument. (Alpaca's `Crypto` request-side filter
/// should already exclude these, but the guard is cheap and defends against
/// API drift — and the `capabilities` path has no such request-side filter.)
fn asset_to_entry(asset: &Asset) -> Option<InstrumentEntry> {
    (asset.tradable && matches!(asset.class, AlpacaAssetClass::Crypto)).then(|| crypto_entry(asset))
}

/// Translate Alpaca's `/v2/assets` crypto rows into the datamancer catalog.
/// Pure function — no client, no I/O — so it can be exercised against canned
/// JSON fixtures without credentials. Eligibility is per [`asset_to_entry`].
fn assets_to_entries(assets: &[Asset]) -> Vec<InstrumentEntry> {
    assets.iter().filter_map(asset_to_entry).collect()
}

/// Which Alpaca crypto venue to stream from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlpacaCryptoVenue {
    /// Alpaca's aggregated US crypto feed.
    Us,
    /// Kraken-backed US feed.
    UsKraken,
    /// Kraken-backed EU feed.
    EuKraken,
}

/// Map a crypto venue onto the market-data snapshot API's location
/// parameter (distinct from the streaming `CryptoFeed` enum used for the
/// live websocket).
fn venue_location(
    venue: AlpacaCryptoVenue,
) -> oxidized_alpaca::restful::market_data::crypto::CryptoLocation {
    use oxidized_alpaca::restful::market_data::crypto::CryptoLocation;
    match venue {
        AlpacaCryptoVenue::Us => CryptoLocation::Us,
        AlpacaCryptoVenue::UsKraken => CryptoLocation::Us1,
        AlpacaCryptoVenue::EuKraken => CryptoLocation::Eu1,
    }
}

/// Runtime settings for [`AlpacaCryptoProvider`] — the hot-reconfigurable
/// subset of its configuration, delivered through a [`SettingsSource`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlpacaCryptoSettings {
    /// Paper or live account; selects which credential pair is loaded from
    /// the environment by `oxidized_alpaca`.
    pub account_type: AccountType,
    /// Which crypto venue to connect to.
    pub venue: AlpacaCryptoVenue,
}

/// Configuration for [`AlpacaCryptoProvider`].
#[derive(Clone, Debug)]
pub struct AlpacaCryptoProviderConfig {
    /// Runtime settings source. `Static` (the default) is always enabled;
    /// `Watch` is the daemon's enable/disable + hot-settings seam (`None` =
    /// disabled: the hub task parks and REST calls fail unavailable).
    pub settings: SettingsSource<AlpacaCryptoSettings>,
    /// Reconnect/retry policy for the live websocket.
    pub reconnect: ReconnectPolicy,
    /// Where this provider's credentials come from. This — not the
    /// `DatamancerBuilder` — is the library's credential-source API (spec
    /// decision 9); the builder consumes providers already constructed.
    pub credentials: CredentialsSource,
}

impl Default for AlpacaCryptoProviderConfig {
    fn default() -> Self {
        Self {
            settings: SettingsSource::Static(AlpacaCryptoSettings {
                account_type: AccountType::Paper,
                venue: AlpacaCryptoVenue::Us,
            }),
            reconnect: ReconnectPolicy::default(),
            credentials: CredentialsSource::Env,
        }
    }
}

/// The crypto provider's REST surface, rebuilt whenever the credential or
/// settings source changes (watch bump) — cheap relative to REST call
/// frequency, and `has_changed` makes the common path a no-op. One
/// `std::sync::Mutex` (never held across an await) guards the
/// check-and-rebuild.
struct RestState {
    /// Trading API client, used for the reference-data surface (crypto
    /// asset catalog). `None` when disabled or without credentials.
    trading: Option<TradingClient>,
    /// Market-data API client, used for the snapshot/latest surface. `None`
    /// when disabled or without credentials.
    market_data: Option<MarketDataClient>,
    /// Whether the settings read the clients were built from found the
    /// provider enabled.
    enabled: bool,
    /// `Some` only for [`CredentialsSource::Watch`]; `has_changed` on this
    /// cached receiver is the rebuild trigger.
    cred_rx: Option<watch::Receiver<Option<AlpacaCredentials>>>,
    /// `Some` only for [`SettingsSource::Watch`]; `has_changed` on this
    /// cached receiver is the rebuild trigger.
    settings_rx: Option<watch::Receiver<Option<AlpacaCryptoSettings>>>,
}

impl RestState {
    /// Rebuild both clients from one settings read.
    fn rebuild(&mut self, cfg: &AlpacaCryptoProviderConfig) {
        let Some(settings) = cfg.settings.current() else {
            self.trading = None;
            self.market_data = None;
            self.enabled = false;
            return;
        };
        (self.trading, self.market_data) = match cfg.credentials.current() {
            Resolved::Env => (
                TradingClient::new(settings.account_type).ok(),
                MarketDataClient::new(settings.account_type).ok(),
            ),
            Resolved::Creds(c) => {
                let key = c.to_api_key();
                (
                    TradingClient::new_with_credentials(settings.account_type, key.clone()).ok(),
                    MarketDataClient::new_with_credentials(settings.account_type, key).ok(),
                )
            }
            Resolved::Missing => (None, None),
        };
        self.enabled = true;
    }

    /// The trading client, or the refusal this state decides.
    fn trading(&self) -> Result<TradingClient> {
        self.trading
            .clone()
            .ok_or_else(|| super::runtime::no_rest_client(PROVIDER_ID, self.enabled))
    }

    /// The market-data client, or the refusal this state decides.
    fn market_data(&self) -> Result<MarketDataClient> {
        self.market_data
            .clone()
            .ok_or_else(|| super::runtime::no_rest_client(PROVIDER_ID, self.enabled))
    }
}

/// Alpaca-backed crypto [`Provider`].
pub struct AlpacaCryptoProvider {
    cfg: AlpacaCryptoProviderConfig,
    /// The one hub task behind every live session (see the module docs);
    /// spawned lazily on the first `start_live`.
    hub: HubSlot<CryptoConnector>,
    rest: std::sync::Mutex<RestState>,
}

impl AlpacaCryptoProvider {
    #[must_use]
    pub fn new(cfg: AlpacaCryptoProviderConfig) -> Self {
        // Ordering invariant — receiver before build: a rotation after
        // capture triggers rebuild; before capture is included in the build.
        // (Capturing after building would mark an in-between rotation seen
        // while the cached client is stale.) Applies to both the
        // credentials and the settings receiver.
        let cred_rx = cfg.credentials.watch();
        let settings_rx = cfg.settings.watch();
        let mut state = RestState {
            trading: None,
            market_data: None,
            enabled: false,
            cred_rx,
            settings_rx,
        };
        state.rebuild(&cfg);
        let rest = std::sync::Mutex::new(state);
        Self {
            cfg,
            hub: HubSlot::idle(),
            rest,
        }
    }

    /// Lock the REST state, rebuilding **both** clients first if the credential
    /// or settings source changed since the last check. Returns the locked guard
    /// so the caller can clone out the one field it needs; cloning out of the
    /// mutex keeps the guard from crossing an await (each client is a cheaply
    /// cloneable handle). Both clients are rebuilt together off the same `changed`
    /// detection so they stay in lockstep on a credential/settings watch bump.
    fn refresh_rest(&self) -> std::sync::MutexGuard<'_, RestState> {
        let mut state = self.rest.lock().expect("REST client state poisoned");
        let changed = super::runtime::watch_changed(&mut state.cred_rx)
            | super::runtime::watch_changed(&mut state.settings_rx);
        if changed {
            state.rebuild(&self.cfg);
        }
        state
    }
}

#[async_trait]
impl Provider for AlpacaCryptoProvider {
    fn id(&self) -> &str {
        PROVIDER_ID
    }

    fn enabled(&self) -> bool {
        self.cfg.settings.current().is_some()
    }

    fn supports(&self, instrument: &Instrument, kind: EventKind, surface: Surface) -> bool {
        if instrument.asset_class() != AssetClass::Crypto {
            return false;
        }
        match surface {
            Surface::Live => match kind {
                EventKind::Trade
                | EventKind::Quote
                | EventKind::Bar(BarInterval::OneMinute | BarInterval::OneDay) => true,
                EventKind::Bar(_) => false,
            },
            // No historical surface is wired yet: `oxidized_alpaca` exposes only
            // `crypto_latest_*`, not the historical crypto trade/quote
            // endpoints, and `fetch_history` below errors unconditionally.
            // Declaring the empty set keeps that honest — the request is refused
            // at routing rather than after a network round trip. Widen this in
            // lockstep with `fetch_history`, never ahead of it.
            Surface::History => false,
        }
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let cmd_tx = self.hub.ensure(|| CryptoConnector {
            cfg: self.cfg.clone(),
        });
        Ok(Box::new(SharedLiveHandle::new(cmd_tx, sink)))
    }

    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        Err(Error::Provider {
            provider: PROVIDER_ID.to_string(),
            message: "crypto historical fetch is not wired up in this provider".to_string(),
        })
    }

    async fn latest(
        &self,
        instrument: &Instrument,
        kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        let rest = self.refresh_rest().market_data()?;
        let venue = self
            .cfg
            .settings
            .current()
            .map_or(AlpacaCryptoVenue::Us, |s| s.venue);
        let symbol = instrument.symbol();
        let mut snaps = rest
            .crypto_snapshots(&[symbol], venue_location(venue))
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("crypto_snapshots: {e}"),
            })?;
        let Some(snap) = snaps.remove(symbol) else {
            return Ok(None);
        };
        Ok(crypto_snapshot_to_event(
            &snap,
            instrument,
            kind,
            wall_clock_ts(),
        ))
    }

    async fn list_instruments(&self) -> Result<Vec<InstrumentEntry>> {
        let trading = self.refresh_rest().trading()?;
        let assets = trading
            .list_assets()
            .status(AlpacaAssetStatus::Active)
            .asset_class(AlpacaAssetClass::Crypto)
            .execute()
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("list_assets: {e}"),
            })?;
        Ok(assets_to_entries(&assets))
    }

    async fn capabilities(&self, instrument: &Instrument) -> Result<Option<InstrumentEntry>> {
        let trading = self.refresh_rest().trading()?;
        let asset = trading
            .get_asset(instrument.symbol())
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("get_asset({}): {e}", instrument.symbol()),
            })?;
        // Same eligibility gate as bulk listing: an ineligible asset (equity,
        // non-tradable) resolves to `None`, so the caller keeps its placeholder
        // instrument rather than a mislabeled authoritative crypto instrument.
        Ok(asset_to_entry(&asset))
    }
}

// ---------------------------------------------------------------------------
// Hub connector
// ---------------------------------------------------------------------------

/// How the hub opens and classifies the crypto market-data stream.
struct CryptoConnector {
    cfg: AlpacaCryptoProviderConfig,
}

impl Connector for CryptoConnector {
    const PROVIDER_ID: &'static str = PROVIDER_ID;
    type Settings = AlpacaCryptoSettings;
    type Failure = oxidized_alpaca::Error;
    type Upstream = StreamingCryptoClient;

    fn settings(&self) -> &SettingsSource<AlpacaCryptoSettings> {
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
        settings: &AlpacaCryptoSettings,
        credentials: PresentCredentials,
    ) -> std::result::Result<StreamingCryptoClient, oxidized_alpaca::Error> {
        let feed = match settings.venue {
            AlpacaCryptoVenue::Us => CryptoFeed::Us,
            AlpacaCryptoVenue::UsKraken => CryptoFeed::UsKraken,
            AlpacaCryptoVenue::EuKraken => CryptoFeed::EuKraken,
        };
        match credentials {
            PresentCredentials::Env => {
                StreamingCryptoClient::new(settings.account_type, feed).await
            }
            PresentCredentials::Creds(c) => {
                StreamingCryptoClient::new_with_credentials(
                    settings.account_type,
                    feed,
                    c.to_api_key(),
                )
                .await
            }
        }
    }

    /// `oxidized_alpaca` 0.0.10 returns `Error::StreamingAuth` when the
    /// market-data connect handshake's auth response is not `Authenticated`;
    /// only that parks. Every other failure, a mid-stream server error
    /// envelope included, backs off.
    fn classify(failure: &oxidized_alpaca::Error) -> StreamFailure {
        if matches!(failure, oxidized_alpaca::Error::StreamingAuth) {
            StreamFailure::Unauthenticated
        } else {
            StreamFailure::Transient
        }
    }
}

impl Upstream for StreamingCryptoClient {
    type Subs = CryptoSubscriptionList;
    type Failure = oxidized_alpaca::Error;

    async fn add(
        &mut self,
        subs: &CryptoSubscriptionList,
    ) -> std::result::Result<(), oxidized_alpaca::Error> {
        self.add_subscriptions(subs).await.map(|_| ())
    }

    async fn remove(
        &mut self,
        removal: &CryptoSubscriptionList,
    ) -> std::result::Result<(), oxidized_alpaca::Error> {
        self.remove_subscriptions(removal).await.map(|_| ())
    }

    async fn next_events(
        &mut self,
    ) -> std::result::Result<Vec<MarketEvent>, oxidized_alpaca::Error> {
        self.next_message().await.map(translate_crypto_message)
    }

    async fn shut_down(self) {
        let _ = StreamingCryptoClient::shut_down(self).await;
    }
}

impl SubscriptionSet for CryptoSubscriptionList {
    fn empty() -> Self {
        Self::new()
    }

    fn is_empty(&self) -> bool {
        self.bars.as_ref().is_none_or(Vec::is_empty)
            && self.daily_bars.as_ref().is_none_or(Vec::is_empty)
            && self.updated_bars.as_ref().is_none_or(Vec::is_empty)
            && self.quotes.as_ref().is_none_or(Vec::is_empty)
            && self.trades.as_ref().is_none_or(Vec::is_empty)
            && self.orderbooks.as_ref().is_none_or(Vec::is_empty)
    }

    fn apply(&mut self, instrument: &Instrument, kind: EventKind, add: bool) {
        apply_pair_to_list(self, instrument, kind, add);
    }
}

fn apply_pair_to_list(
    list: &mut CryptoSubscriptionList,
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
// Translation: oxidized_alpaca crypto -> MarketEvent
// ---------------------------------------------------------------------------

pub(crate) fn translate_crypto_message(msg: CryptoStreamMessage) -> Vec<MarketEvent> {
    let rx = wall_clock_ts();
    match msg {
        CryptoStreamMessage::Trade(t) => vec![MarketEvent::Trade(translate_trade(&t, rx))],
        CryptoStreamMessage::Quote(q) => vec![MarketEvent::Quote(translate_quote(&q, rx))],
        CryptoStreamMessage::Bar(b) | CryptoStreamMessage::UpdatedBar(b) => {
            vec![MarketEvent::Bar(translate_bar(
                &b,
                BarInterval::OneMinute,
                rx,
            ))]
        }
        CryptoStreamMessage::DailyBar(b) => {
            vec![MarketEvent::Bar(translate_bar(&b, BarInterval::OneDay, rx))]
        }
        CryptoStreamMessage::Error(err) => vec![MarketEvent::Control(Control {
            source_ts: rx,
            rx_ts: rx,
            seq: Seq(0),
            kind: ControlKind::fanout_error(
                PROVIDER_ID,
                format!("{:?}: {}", err.code, err.message),
            ),
        })],
        // Subscription confirmations and orderbook updates aren't part of the
        // canonical MarketEvent surface yet; drop them here.
        _ => Vec::new(),
    }
}

fn translate_trade(t: &CryptoTradeEvent, rx: Timestamp) -> Trade {
    Trade {
        instrument: provider_instrument(&t.symbol),
        source_ts: chrono_to_ts(t.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        price: Price::from_f64_round(t.price),
        size: Quantity::from_f64_round(t.size),
    }
}

fn translate_quote(q: &CryptoQuoteEvent, rx: Timestamp) -> Quote {
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

fn translate_bar(b: &CryptoBarEvent, interval: BarInterval, rx: Timestamp) -> Bar {
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
        volume: Quantity::from_f64_round(b.volume),
    }
}

fn chrono_to_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp(dt.timestamp_nanos_opt().unwrap_or(0))
}

/// Map a crypto snapshot onto the canonical event for `kind`, or `None` when the
/// snapshot lacks that datum. `seq` is a placeholder; the controller re-stamps.
fn crypto_snapshot_to_event(
    snap: &oxidized_alpaca::restful::market_data::crypto::CryptoSnapshot,
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
                size: Quantity::from_f64_round(t.size),
            })
        }),
        EventKind::Quote => snap.latest_quote.as_ref().map(|q| {
            MarketEvent::Quote(Quote {
                instrument: instrument.clone(),
                source_ts: chrono_to_ts(q.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                bid: Price::from_f64_round(q.bid_price),
                bid_size: Quantity::from_f64_round(q.bid_size),
                ask: Price::from_f64_round(q.ask_price),
                ask_size: Quantity::from_f64_round(q.ask_size),
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
                source_ts: chrono_to_ts(bar.timestamp),
                rx_ts: rx,
                seq: Seq(0),
                open: Price::from_f64_round(bar.open),
                high: Price::from_f64_round(bar.high),
                low: Price::from_f64_round(bar.low),
                close: Price::from_f64_round(bar.close),
                volume: Quantity::from_f64_round(bar.volume),
            }))
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::alpaca::tests::server_error;
    use crate::providers::hub::dispatch_event;
    use crate::providers::hub::tests::{fanout_ids, two_routes};
    use oxidized_alpaca::streaming::StreamErrorCode;

    #[test]
    fn translates_crypto_trade() {
        // 0.004 BTC is the canonical case the fixed-point Quantity change
        // exists for: under the old `f64_to_u64_saturating(size.round())`
        // boundary it collapsed to `size: 0`; it must now survive exactly.
        let json = r#"{"T":"t","S":"BTC/USD","i":12345,"p":50050.0,"s":0.004,"tks":"B","t":"2024-01-02T15:30:00Z"}"#;
        let msg: CryptoStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_crypto_message(msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Trade(t) => {
                assert_eq!(t.instrument.symbol(), "BTC/USD");
                assert_eq!(t.price, Price::from_f64_round(50050.0));
                assert_eq!(t.size, Quantity::from_raw(4_000_000));
                assert_ne!(t.size, Quantity::ZERO);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn translates_crypto_quote() {
        let json = r#"{"T":"q","S":"BTC/USD","bp":50000.0,"bs":1.0,"ap":50100.0,"as":2.0,"t":"2024-01-02T15:30:00Z"}"#;
        let msg: CryptoStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_crypto_message(msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Quote(q) => {
                assert_eq!(q.instrument.symbol(), "BTC/USD");
                assert_eq!(q.bid, Price::from_f64_round(50000.0));
                assert_eq!(q.ask, Price::from_f64_round(50100.0));
            }
            other => panic!("expected Quote, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn every_copy_of_a_stream_error_shares_one_distinct_id() {
        let (routes, mut a_rx, mut b_rx) = two_routes();
        for _ in 0..2 {
            let msg: CryptoStreamMessage =
                serde_json::from_str(r#"{"T":"error","code":400,"msg":"invalid"}"#).unwrap();
            for ev in translate_crypto_message(msg) {
                dispatch_event(&routes, ev).await;
            }
        }
        let a = fanout_ids(&mut a_rx);
        assert_eq!(a, fanout_ids(&mut b_rx));
        assert_eq!(a.len(), 2);
        assert_ne!(a[0], a[1]);
    }

    #[test]
    fn provider_supports_live_kinds() {
        let p = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig::default());
        let inst = provider_instrument("BTC/USD");
        assert!(p.supports(&inst, EventKind::Trade, Surface::Live));
        assert!(p.supports(&inst, EventKind::Quote, Surface::Live));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneMinute), Surface::Live));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneDay), Surface::Live));
        assert!(!p.supports(
            &inst,
            EventKind::Bar(BarInterval::FiveMinute),
            Surface::Live
        ));
    }

    /// Crypto has no historical surface at all until the upstream
    /// `oxidized_alpaca` endpoints land. Declaring the empty set honestly turns
    /// what was a string `Error::Provider` raised at network time into a typed
    /// `UnsupportedEventKind` caught at routing.
    #[test]
    fn provider_supports_no_history_kinds() {
        let p = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig::default());
        let inst = provider_instrument("BTC/USD");
        for kind in EventKind::enumerate() {
            assert!(
                !p.supports(&inst, kind, Surface::History),
                "crypto history is not wired; {kind:?} must not be advertised"
            );
        }
    }

    #[tokio::test]
    async fn missing_rest_credentials_are_unauthenticated() {
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let p = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
            credentials: CredentialsSource::Watch(rx),
            ..Default::default()
        });
        let err = p
            .list_instruments()
            .await
            .expect_err("no credentials, no trading client");
        assert!(matches!(err, Error::Unauthenticated { .. }), "{err:?}");
        let err = p
            .latest(&provider_instrument("BTC/USD"), EventKind::Trade)
            .await
            .expect_err("no credentials, no market-data client");
        assert!(matches!(err, Error::Unauthenticated { .. }), "{err:?}");
    }

    #[test]
    fn a_rest_refusal_is_decided_from_one_snapshot() {
        let (settings_tx, settings_rx) = tokio::sync::watch::channel(None);
        let (_creds_tx, creds_rx) = tokio::sync::watch::channel(None);
        let p = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
            settings: SettingsSource::Watch(settings_rx),
            credentials: CredentialsSource::Watch(creds_rx),
            ..Default::default()
        });

        let disabled = p.refresh_rest();
        settings_tx
            .send(Some(AlpacaCryptoSettings {
                account_type: AccountType::Paper,
                venue: AlpacaCryptoVenue::Us,
            }))
            .unwrap();
        let err = disabled.trading().expect_err("taken while disabled");
        assert!(matches!(err, Error::ProviderDisabled { .. }), "{err:?}");
        drop(disabled);

        let enabled = p.refresh_rest();
        settings_tx.send(None).unwrap();
        let err = enabled
            .market_data()
            .expect_err("taken without credentials");
        assert!(matches!(err, Error::Unauthenticated { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn disabled_provider_parks_and_fails_subscribes_fast() {
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let p = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
            settings: SettingsSource::Watch(rx),
            ..Default::default()
        });
        let (sink, _events) = tokio::sync::mpsc::channel(8);
        let handle = p.start_live(sink).await.expect("start_live");
        let err = handle
            .subscribe(provider_instrument("BTC/USD"), EventKind::Trade)
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
    async fn settings_source_default_is_enabled_paper() {
        let cfg = AlpacaCryptoProviderConfig::default();
        assert_eq!(
            cfg.settings.current(),
            Some(AlpacaCryptoSettings {
                account_type: AccountType::Paper,
                venue: AlpacaCryptoVenue::Us,
            })
        );
    }

    #[test]
    fn assets_to_entries_filters_and_maps() {
        let json = r#"[
            {
                "id":"1","class":"crypto","exchange":"CRYPTO","symbol":"BTC/USD",
                "name":"Bitcoin","status":"active","tradable":true,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":true,"attributes":[]
            },
            {
                "id":"2","class":"crypto","exchange":"CRYPTO","symbol":"DOGE/USD",
                "name":"Dogecoin","status":"active","tradable":false,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":true,"attributes":[]
            },
            {
                "id":"3","class":"us_equity","exchange":"NASDAQ","symbol":"AAPL",
                "name":"Apple Inc.","status":"active","tradable":true,
                "marginable":true,"shortable":true,"easy_to_borrow":true,
                "fractionable":true,"attributes":[]
            }
        ]"#;
        let assets: Vec<Asset> = serde_json::from_str(json).expect("parse fixture");
        let entries = assets_to_entries(&assets);
        // BTC/USD passes; DOGE/USD filtered (not tradable); AAPL filtered
        // (wrong class — guards against API drift).
        let symbols: Vec<&str> = entries.iter().map(|e| e.instrument.symbol()).collect();
        assert_eq!(symbols, vec!["BTC/USD"]);
        for e in &entries {
            assert_eq!(e.instrument.provider().as_str(), PROVIDER_ID);
            assert_eq!(e.instrument.asset_class(), AssetClass::Crypto);
        }
    }

    #[test]
    fn assets_to_entries_maps_fractional_flag_and_policy() {
        let json = r#"[
            {
                "id":"1","class":"crypto","exchange":"CRYPTO","symbol":"BTC/USD",
                "name":"Bitcoin","status":"active","tradable":true,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":true,"attributes":[]
            }
        ]"#;
        let assets: Vec<Asset> = serde_json::from_str(json).expect("parse fixture");
        let entries = assets_to_entries(&assets);
        let frac = entries
            .iter()
            .find(|e| e.instrument.symbol() == "BTC/USD")
            .unwrap();
        assert_eq!(frac.instrument.asset_class(), AssetClass::Crypto);
        let caps = frac.capabilities.as_ref().unwrap();
        assert_eq!(caps.fractionable, Some(true));
        assert_eq!(caps.supports_notional_orders, Some(true));
        // Crypto policy, NOT the equity fractional policy: market/limit/stop-limit
        // (no plain stop), GTC/IOC (no day).
        assert_eq!(
            caps.allowed_order_types.as_deref(),
            Some(&[OrderType::Market, OrderType::Limit, OrderType::StopLimit][..])
        );
        assert_eq!(
            caps.allowed_tif.as_deref(),
            Some(&[TimeInForce::Gtc, TimeInForce::Ioc][..])
        );
        assert!(caps.min_qty.is_none()); // not advertised
    }

    #[test]
    fn asset_to_entry_rejects_ineligible_assets() {
        // An equity or non-tradable asset resolved through the crypto provider
        // must NOT become an authoritative crypto entry — the on-demand
        // `capabilities` lookup relies on this gate to avoid mislabeling.
        let json = r#"[
            {
                "id":"1","class":"us_equity","exchange":"NASDAQ","symbol":"AAPL",
                "name":"Apple Inc.","status":"active","tradable":true,
                "marginable":true,"shortable":true,"easy_to_borrow":true,
                "fractionable":true,"attributes":[]
            },
            {
                "id":"2","class":"crypto","exchange":"CRYPTO","symbol":"DEAD/USD",
                "name":"Delisted","status":"active","tradable":false,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":true,"attributes":[]
            },
            {
                "id":"3","class":"crypto","exchange":"CRYPTO","symbol":"BTC/USD",
                "name":"Bitcoin","status":"active","tradable":true,
                "marginable":false,"shortable":false,"easy_to_borrow":false,
                "fractionable":true,"attributes":[]
            }
        ]"#;
        let assets: Vec<Asset> = serde_json::from_str(json).expect("parse fixture");
        assert!(asset_to_entry(&assets[0]).is_none(), "equity rejected");
        assert!(
            asset_to_entry(&assets[1]).is_none(),
            "non-tradable rejected"
        );
        let ok = asset_to_entry(&assets[2]).expect("eligible crypto");
        assert_eq!(ok.instrument.symbol(), "BTC/USD");
        assert_eq!(ok.instrument.asset_class(), AssetClass::Crypto);
    }

    #[test]
    fn crypto_set_counts_updated_bars_and_orderbooks() {
        let mut set = <CryptoSubscriptionList as SubscriptionSet>::empty();
        assert!(SubscriptionSet::is_empty(&set));
        set.updated_bars = Some(vec!["BTC/USD".to_string()]);
        assert!(!SubscriptionSet::is_empty(&set));
        set.updated_bars = None;
        set.orderbooks = Some(vec!["BTC/USD".to_string()]);
        assert!(!SubscriptionSet::is_empty(&set));
    }

    #[test]
    fn crypto_classifies_only_streaming_auth_as_unauthenticated() {
        assert_eq!(
            CryptoConnector::classify(&oxidized_alpaca::Error::StreamingAuth),
            StreamFailure::Unauthenticated
        );
        assert_eq!(
            CryptoConnector::classify(&server_error(
                oxidized_alpaca::Error::StreamingError,
                StreamErrorCode::AuthFailed
            )),
            StreamFailure::Transient
        );
    }

    #[test]
    fn crypto_snapshot_maps_kind_to_event() {
        use oxidized_alpaca::restful::market_data::crypto::CryptoSnapshot;

        let json = r#"{
            "latestTrade": {"t":"2024-01-02T15:00:00Z","p":42000.0,"s":0.5,"i":1,"tks":"B"},
            "latestQuote": {"t":"2024-01-02T15:00:00Z","bp":41999.0,"bs":1.2,"ap":42001.0,"as":0.8},
            "minuteBar": {"t":"2024-01-02T15:00:00Z","o":42000.0,"h":42100.0,"l":41900.0,"c":42050.0,"v":12.5,"n":10,"vw":42010.0},
            "dailyBar": {"t":"2024-01-02T00:00:00Z","o":41000.0,"h":42500.0,"l":40800.0,"c":42050.0,"v":900.0,"n":500,"vw":41800.0},
            "prevDailyBar": null
        }"#;
        let snap: CryptoSnapshot = serde_json::from_str(json).unwrap();
        let inst = provider_instrument("BTC/USD");
        let rx = Timestamp(42);

        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Trade, rx),
            Some(MarketEvent::Trade(_))
        ));
        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Quote, rx),
            Some(MarketEvent::Quote(_))
        ));
        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneMinute), rx),
            Some(MarketEvent::Bar(_))
        ));
        assert!(matches!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::OneDay), rx),
            Some(MarketEvent::Bar(_))
        ));
        assert!(
            crypto_snapshot_to_event(&snap, &inst, EventKind::Bar(BarInterval::FiveMinute), rx)
                .is_none()
        );
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
        let p = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
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
}

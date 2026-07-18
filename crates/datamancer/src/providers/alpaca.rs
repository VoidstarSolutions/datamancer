//! Alpaca-backed [`Provider`].
//!
//! Wraps [`oxidized_alpaca`]'s streaming stock client for live trades, quotes,
//! and minute/daily bars, and its REST market-data client for bounded
//! historical fetches. Decoded events are pushed verbatim into the supplied
//! [`MarketEvent`] sink with a placeholder [`Seq`]; the session merger assigns
//! the final session-monotonic sequence.
//!
//! # Reconnect
//!
//! On websocket failure the streaming task tears down the client, sleeps with
//! exponential backoff per the configured [`ReconnectPolicy`], and reconnects
//! — re-applying the active subscription set. A `ProviderDisconnected` /
//! `ProviderConnected` control pair is emitted in-band so consumers see the
//! gap window in the event stream.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datamancer_core::{
    Adjustment, AssetClass, BarInterval, Control, ControlKind, DisconnectCause, Error, EventKind,
    HistoryRequest, Instrument, InstrumentCapabilities, InstrumentEntry, LiveHandle, MarketEvent,
    OrderType, Price, Provider, ProviderId, Quantity, Result, Seq, TimeInForce, Timestamp, Trade,
};
use datamancer_core::{Bar, Quote};
use oxidized_alpaca::{
    AccountType, MarketDataClient, RestFeed, StreamingFeed, TradingClient,
    restful::{
        market_data::TimeFrame,
        market_data::stock::Adjustment as AlpacaAdjustment,
        trading::assets::{Asset, AssetClass as AlpacaAssetClass, Status as AlpacaAssetStatus},
    },
    streaming::{
        StockStreamMessage, StockSubscriptionList, StreamingStockClient,
        messages::stock::{StockBar, StockQuoteEvent, StockTradeEvent},
    },
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::credentials::{AlpacaCredentials, CredentialsSource, Resolved};
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

/// Build capabilities for one Alpaca equity `Asset`.
fn equity_capabilities(asset: &Asset) -> InstrumentCapabilities {
    let mut caps = InstrumentCapabilities::default();
    caps.fractionable = Some(asset.fractionable);
    caps.supports_notional_orders = Some(true);
    caps.allowed_order_types = Some(FRACTIONAL_ORDER_TYPES.to_vec());
    caps.allowed_tif = Some(FRACTIONAL_TIF.to_vec());
    // Sizing increments (min_qty/qty_increment/price_increment/min_notional)
    // stay None: oxidized_alpaca 0.0.10 Asset carries no sizing fields.
    caps
}

/// Translate Alpaca's `/v2/assets` rows into the datamancer instrument
/// catalog. Pure function — no client, no I/O — so it can be exercised
/// against canned JSON fixtures without credentials.
///
/// Filters to `tradable = true` and skips asset classes outside our v0
/// taxonomy. Alpaca returns ETFs under [`AlpacaAssetClass::UsEquity`] with
/// no explicit ETF flag on the row itself, so for now they land as
/// [`AssetClass::Equity`]; a future revision can read the `attributes`
/// vector (e.g. `"etp"`) to promote them to [`AssetClass::Etf`].
fn assets_to_entries(assets: &[Asset]) -> Vec<InstrumentEntry> {
    assets
        .iter()
        .filter(|a| a.tradable)
        .filter_map(|a| {
            let asset_class = match a.class {
                AlpacaAssetClass::UsEquity => AssetClass::Equity,
                // Options and crypto-perp aren't part of v0's taxonomy;
                // the dedicated crypto provider handles plain crypto.
                _ => return None,
            };
            let instrument = Instrument::new(
                ProviderId::from_static(PROVIDER_ID),
                asset_class,
                a.symbol.clone(),
            );
            let mut entry = InstrumentEntry::bare(instrument);
            entry.capabilities = Some(equity_capabilities(a));
            Some(entry)
        })
        .collect()
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

/// Runtime settings for [`AlpacaProvider`] — the hot-reconfigurable subset
/// of its configuration, delivered through a [`SettingsSource`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlpacaSettings {
    /// Paper or live account; selects endpoints and, for the legacy `Env`
    /// credential source, which env credential pair is loaded.
    pub account_type: AccountType,
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
            settings: SettingsSource::Static(AlpacaSettings {
                account_type: AccountType::Paper,
            }),
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
    /// Market-data REST client. `None` when credentials aren't available —
    /// `fetch_history` will surface a Provider error in that case.
    market_data: Option<MarketDataClient>,
    /// Trading API client, used for the reference-data surface (asset
    /// catalog). `None` when credentials aren't available —
    /// `list_instruments` will surface a Provider error in that case.
    trading: Option<TradingClient>,
}

fn build_rest(cfg: &AlpacaProviderConfig) -> RestClients {
    let Some(settings) = cfg.settings.current() else {
        return RestClients {
            market_data: None,
            trading: None,
        };
    };
    match cfg.credentials.current() {
        Resolved::Env => RestClients {
            market_data: MarketDataClient::new(settings.account_type).ok(),
            trading: TradingClient::new(settings.account_type).ok(),
        },
        Resolved::Creds(c) => {
            let key = c.to_api_key();
            RestClients {
                market_data: MarketDataClient::new_with_credentials(
                    settings.account_type,
                    key.clone(),
                )
                .ok(),
                trading: TradingClient::new_with_credentials(settings.account_type, key).ok(),
            }
        }
        Resolved::Missing => RestClients {
            market_data: None,
            trading: None,
        },
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
        Self { cfg, rest }
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
        Self { cfg, rest }
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

    fn supports(&self, instrument: &Instrument, kind: EventKind) -> bool {
        if !matches!(
            instrument.asset_class(),
            AssetClass::Equity | AssetClass::Etf
        ) {
            return false;
        }
        match kind {
            EventKind::Trade
            | EventKind::Quote
            | EventKind::Bar(BarInterval::OneMinute | BarInterval::OneDay) => true,
            EventKind::Bar(_) => false,
        }
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let (cmd_tx, cmd_rx) = mpsc::channel::<LiveCommand>(32);
        let cfg = self.cfg.clone();
        let task = tokio::spawn(run_streaming_task(cfg, sink, cmd_rx));
        Ok(Box::new(AlpacaLiveHandle {
            cmd_tx,
            task: Mutex::new(Some(task)),
            active: Mutex::new(BTreeSet::new()),
        }))
    }

    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let rest = self
            .rest_clients()
            .market_data
            .ok_or_else(|| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: "REST client not initialized (Alpaca credentials missing?)".to_string(),
            })?;
        fetch_history_via(&rest, request, sink).await
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
        let rest = self
            .rest_clients()
            .market_data
            .ok_or_else(|| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: "REST client not initialized (Alpaca credentials missing?)".to_string(),
            })?;
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
        let trading = self.rest_clients().trading.ok_or_else(|| Error::Provider {
            provider: PROVIDER_ID.to_string(),
            message: "Trading client not initialized (Alpaca credentials missing?)".to_string(),
        })?;
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

    async fn capabilities(
        &self,
        instrument: &Instrument,
    ) -> Result<Option<InstrumentCapabilities>> {
        let trading = self.rest_clients().trading.ok_or_else(|| Error::Provider {
            provider: PROVIDER_ID.to_string(),
            message: "Trading client not initialized (Alpaca credentials missing?)".to_string(),
        })?;
        let asset = trading
            .get_asset(instrument.symbol())
            .await
            .map_err(|e| Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("get_asset: {e}"),
            })?;
        Ok(Some(equity_capabilities(&asset)))
    }
}

// ---------------------------------------------------------------------------
// Live handle + command channel
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum LiveCommand {
    Subscribe(Instrument, EventKind, oneshot::Sender<Result<()>>),
    Unsubscribe(Instrument, EventKind, oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<()>),
}

struct AlpacaLiveHandle {
    cmd_tx: mpsc::Sender<LiveCommand>,
    task: Mutex<Option<JoinHandle<()>>>,
    /// Mirror of subscriptions sent through this handle, retained so that
    /// subscribe-after-reconnect logic in the streaming task can ask us for
    /// the list to re-apply.
    #[allow(dead_code)]
    active: Mutex<BTreeSet<(Instrument, EventKind)>>,
}

#[async_trait]
impl LiveHandle for AlpacaLiveHandle {
    async fn subscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(LiveCommand::Subscribe(instrument.clone(), kind, tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        let res = rx.await.map_err(|_| Error::SessionClosed)?;
        if res.is_ok() {
            self.active.lock().await.insert((instrument, kind));
        }
        res
    }

    async fn unsubscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(LiveCommand::Unsubscribe(instrument.clone(), kind, tx))
            .await
            .map_err(|_| Error::SessionClosed)?;
        let res = rx.await.map_err(|_| Error::SessionClosed)?;
        if res.is_ok() {
            self.active.lock().await.remove(&(instrument, kind));
        }
        res
    }

    async fn close(self: Box<Self>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(LiveCommand::Close(tx)).await;
        let _ = rx.await;
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Streaming task
// ---------------------------------------------------------------------------

#[allow(
    clippy::too_many_lines,
    reason = "single-pass connect / authenticate / subscribe / dispatch / reconnect state machine; extraction would obscure the linear lifecycle"
)]
async fn run_streaming_task(
    cfg: AlpacaProviderConfig,
    sink: mpsc::Sender<MarketEvent>,
    mut cmd_rx: mpsc::Receiver<LiveCommand>,
) {
    // Authoritative subscription state, applied to every fresh client.
    let active: Arc<Mutex<StockSubscriptionList>> =
        Arc::new(Mutex::new(StockSubscriptionList::new()));
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
            if !wait_for_provisioning(rx, &mut cmd_rx, "provider disabled").await {
                return;
            }
            continue 'outer;
        };

        let feed = match cfg.stream_feed {
            AlpacaStreamFeed::Iex => StreamingFeed::IEX,
            AlpacaStreamFeed::Sip => StreamingFeed::SIP,
            AlpacaStreamFeed::DelayedSip => StreamingFeed::DelayedSip,
            AlpacaStreamFeed::Test => StreamingFeed::Test,
        };
        // Fresh receiver per connect attempt: `watch()` marks the current
        // value as seen on the clone (tokio's `Receiver::clone` would
        // otherwise inherit the stored receiver's stale seen-version), so
        // the hot-reconnect arm below only fires on rotations that land
        // *after* this resolution.
        let mut cred_rx = cfg.credentials.watch();
        let connect_result = match cfg.credentials.current() {
            Resolved::Env => StreamingStockClient::new(settings.account_type, feed).await,
            Resolved::Creds(c) => {
                StreamingStockClient::new_with_credentials(
                    settings.account_type,
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
                if !wait_for_provisioning(rx, &mut cmd_rx, "waiting for credentials").await {
                    return;
                }
                continue 'outer;
            }
        };

        let mut client = match connect_result {
            Ok(client) => {
                backoff = cfg.reconnect.initial_backoff_ms;
                emit_control(
                    &sink,
                    ControlKind::ProviderConnected {
                        provider: PROVIDER_ID.to_string(),
                    },
                )
                .await;
                client
            }
            Err(err) => {
                // oxidized_alpaca 0.0.10 returns `Error::StreamingAuth` when
                // the market-data connect handshake's auth response is not
                // `Authenticated` (fixed upstream in
                // fix(streaming): return StreamingAuth on rejected
                // market-data credentials); this classification consumes it.
                let unauthenticated = matches!(err, oxidized_alpaca::Error::StreamingAuth);
                emit_control(
                    &sink,
                    ControlKind::ProviderDisconnected {
                        provider: PROVIDER_ID.to_string(),
                        reason: format!("connect failed: {err}"),
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
                    if !wait_for_provisioning(
                        rx,
                        &mut cmd_rx,
                        "waiting for new credentials after auth rejection",
                    )
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
        };

        // Re-apply persistent subscription set, if any.
        {
            let snapshot = active.lock().await.clone();
            if !is_empty(&snapshot)
                && let Err(err) = client.add_subscriptions(&snapshot).await
            {
                emit_control(
                    &sink,
                    ControlKind::ProviderError {
                        provider: PROVIDER_ID.to_string(),
                        message: format!("re-subscribe failed: {err}"),
                    },
                )
                .await;
            }
        }

        // Event loop: select between commands and incoming messages.
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(LiveCommand::Subscribe(instrument, kind, ack)) => {
                            let mut list = active.lock().await;
                            apply_pair_to_list(&mut list, &instrument, kind, true);
                            let snapshot = list.clone();
                            drop(list);
                            let res = client
                                .add_subscriptions(&snapshot)
                                .await
                                .map(|_| ())
                                .map_err(|e| Error::Provider {
                                    provider: PROVIDER_ID.to_string(),
                                    message: format!("add_subscriptions: {e}"),
                                });
                            if res.is_ok() {
                                emit_control(
                                    &sink,
                                    ControlKind::SubscriptionChanged {
                                        provider: PROVIDER_ID.to_string(),
                                        instrument,
                                        kind,
                                        active: true,
                                    },
                                )
                                .await;
                            } else {
                                // Roll back so reconnect doesn't keep trying to apply a
                                // subscription the server rejected.
                                let mut list = active.lock().await;
                                apply_pair_to_list(&mut list, &instrument, kind, false);
                            }
                            let _ = ack.send(res);
                        }
                        Some(LiveCommand::Unsubscribe(instrument, kind, ack)) => {
                            let mut list = active.lock().await;
                            apply_pair_to_list(&mut list, &instrument, kind, false);
                            drop(list);
                            // Build a list containing only the pair being removed; that's
                            // what the Alpaca API expects in remove_subscriptions.
                            let mut removal = StockSubscriptionList::new();
                            apply_pair_to_list(&mut removal, &instrument, kind, true);
                            let res = client
                                .remove_subscriptions(&removal)
                                .await
                                .map(|_| ())
                                .map_err(|e| Error::Provider {
                                    provider: PROVIDER_ID.to_string(),
                                    message: format!("remove_subscriptions: {e}"),
                                });
                            if res.is_ok() {
                                emit_control(
                                    &sink,
                                    ControlKind::SubscriptionChanged {
                                        provider: PROVIDER_ID.to_string(),
                                        instrument,
                                        kind,
                                        active: false,
                                    },
                                )
                                .await;
                            } else {
                                // Roll back so the upstream server's view stays consistent
                                // with ours: the pair is still streaming, so we should still
                                // include it on reconnect.
                                let mut list = active.lock().await;
                                apply_pair_to_list(&mut list, &instrument, kind, true);
                            }
                            let _ = ack.send(res);
                        }
                        Some(LiveCommand::Close(ack)) => {
                            let _ = client.shut_down().await;
                            // SessionClosing is emitted by Controller::shutdown;
                            // don't double-emit here.
                            let _ = ack.send(());
                            return;
                        }
                        None => {
                            // The handle was dropped. Close the websocket and exit.
                            let _ = client.shut_down().await;
                            return;
                        }
                    }
                }
                next = client.next_message() => {
                    match next {
                        Ok(msg) => {
                            for ev in translate_stock_message(msg) {
                                if sink.send(ev).await.is_err() {
                                    // Consumer dropped; shut down.
                                    let _ = client.shut_down().await;
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            // oxidized_alpaca 0.0.10: `Error::StreamingError`
                            // (mid-stream server error envelope). A mid-stream
                            // `StreamingAuth` is connect-only upstream today;
                            // handled defensively, mirroring the connect arm.
                            let unauthenticated =
                                matches!(err, oxidized_alpaca::Error::StreamingAuth);
                            emit_control(
                                &sink,
                                ControlKind::ProviderDisconnected {
                                    provider: PROVIDER_ID.to_string(),
                                    reason: format!("websocket: {err}"),
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
                                if !wait_for_provisioning(
                                    rx,
                                    &mut cmd_rx,
                                    "waiting for new credentials after auth rejection",
                                )
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
                        // emits, then reconnect immediately with the new
                        // credentials — reset the backoff, this is a
                        // deliberate rotation, not a failure.
                        emit_control(
                            &sink,
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
                        let reason = if cfg.settings.current().is_none() {
                            "provider disabled"
                        } else {
                            "settings changed"
                        };
                        tracing::info!(provider = PROVIDER_ID, reason, "settings changed; reconnecting");
                        emit_control(
                            &sink,
                            ControlKind::ProviderDisconnected {
                                provider: PROVIDER_ID.to_string(),
                                reason: reason.to_string(),
                                cause: DisconnectCause::Error,
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
/// value, servicing the command channel meanwhile (close exits,
/// subscribe/unsubscribe fail fast with `reason`). Returns `false` if the
/// task should exit.
async fn wait_for_provisioning<T>(
    rx: &mut watch::Receiver<T>,
    cmd_rx: &mut mpsc::Receiver<LiveCommand>,
    reason: &'static str,
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
                // servicing commands so close still works.
                loop {
                    match cmd_rx.recv().await {
                        Some(LiveCommand::Close(ack)) => {
                            let _ = ack.send(());
                            return false;
                        }
                        Some(
                            LiveCommand::Subscribe(_, _, ack)
                            | LiveCommand::Unsubscribe(_, _, ack),
                        ) => {
                            let _ = ack.send(Err(Error::Provider {
                                provider: PROVIDER_ID.to_string(),
                                message: reason.to_string(),
                            }));
                        }
                        None => return false,
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(LiveCommand::Close(ack)) => {
                        let _ = ack.send(());
                        return false;
                    }
                    Some(
                        LiveCommand::Subscribe(_, _, ack)
                        | LiveCommand::Unsubscribe(_, _, ack),
                    ) => {
                        let _ = ack.send(Err(Error::Provider {
                            provider: PROVIDER_ID.to_string(),
                            message: reason.to_string(),
                        }));
                    }
                    None => return false,
                }
            }
        }
    }
}

/// Sleeps with exponential backoff. Returns `false` if a Close command came in
/// during the sleep (caller should exit the task).
async fn sleep_with_jitter(
    backoff_ms: &mut u64,
    policy: &ReconnectPolicy,
    cmd_rx: &mut mpsc::Receiver<LiveCommand>,
) -> bool {
    // Full jitter: pick a sleep uniformly in [0, backoff_ms]. With many
    // sessions reconnecting after a shared upstream blip this avoids
    // synchronized retry storms; without it every session retries on the
    // same exponential ladder.
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
                Some(LiveCommand::Close(ack)) => {
                    // SessionClosing is emitted by Controller::shutdown;
                    // don't double-emit here.
                    let _ = ack.send(());
                    false
                }
                Some(LiveCommand::Subscribe(_, _, ack) | LiveCommand::Unsubscribe(_, _, ack)) => {
                    let _ = ack.send(Err(Error::Provider {
                        provider: PROVIDER_ID.to_string(),
                        message: "provider is reconnecting".to_string(),
                    }));
                    true
                }
                None => false,
            }
        }
    }
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

async fn emit_control(sink: &mpsc::Sender<MarketEvent>, kind: ControlKind) {
    let now = wall_clock_ts();
    let ev = MarketEvent::Control(Control {
        source_ts: now,
        rx_ts: now,
        seq: Seq(0),
        kind,
    });
    let _ = sink.send(ev).await;
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
            kind: ControlKind::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("{:?}: {}", err.code, err.message),
            },
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

async fn fetch_history_via(
    rest: &MarketDataClient,
    request: HistoryRequest,
    sink: mpsc::Sender<MarketEvent>,
) -> Result<()> {
    let symbol = request.instrument.symbol();
    let from = ts_to_chrono(request.from);
    let to = ts_to_chrono(request.to);
    let rx = wall_clock_ts();
    match request.kind {
        EventKind::Trade => {
            let trades = rest
                .stock_trades(symbol)
                .start(from)
                .end(to)
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
            // The REST stock_quotes builder mirrors stock_trades; oxidized-alpaca
            // does expose it through `MarketDataClient::stock_quotes`.
            // Implementation parity guarded by feature presence.
            return Err(Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: "historical quotes not yet wired through fetch_history".to_string(),
            });
        }
        EventKind::Bar(interval) => {
            let timeframe = match interval {
                BarInterval::OneSecond => {
                    return Err(Error::UnsupportedEventKind {
                        kind: EventKind::Bar(interval),
                        instrument: request.instrument.clone(),
                    });
                }
                BarInterval::OneMinute => TimeFrame::ONE_MINUTE,
                BarInterval::FiveMinute => TimeFrame::FIVE_MINUTES,
                BarInterval::FifteenMinute => TimeFrame::FIFTEEN_MINUTES,
                BarInterval::OneHour => TimeFrame::ONE_HOUR,
                BarInterval::OneDay => TimeFrame::ONE_DAY,
            };
            let bars = rest
                .stock_bars(symbol, timeframe)
                .start(from)
                .end(to)
                .adjustment(map_adjustment(request.adjustment))
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
                kind: ControlKind::ProviderError { provider, message },
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
    fn provider_supports_kinds() {
        let p = AlpacaProvider::new(AlpacaProviderConfig::default());
        let inst = provider_instrument("AAPL");
        assert!(p.supports(&inst, EventKind::Trade));
        assert!(p.supports(&inst, EventKind::Quote));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneMinute)));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneDay)));
        assert!(!p.supports(&inst, EventKind::Bar(BarInterval::FiveMinute)));
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
        let msg = format!("{err}");
        assert!(msg.contains("provider disabled"), "msg={msg:?}");
        // `start_live` already returns `Box<dyn LiveHandle>`; `close` takes
        // `self: Box<Self>`, so call it directly on the box.
        handle.close().await.expect("close while parked");
    }

    #[tokio::test]
    async fn settings_source_default_is_enabled_paper() {
        let cfg = AlpacaProviderConfig::default();
        assert_eq!(
            cfg.settings.current(),
            Some(AlpacaSettings {
                account_type: AccountType::Paper
            })
        );
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
        assert_eq!(caps.allowed_tif.as_deref(), Some(&[TimeInForce::Day][..]));
        assert!(caps.min_qty.is_none()); // not advertised

        let non_frac = entries
            .iter()
            .find(|e| e.instrument.symbol() == "GE")
            .unwrap();
        let non_frac_caps = non_frac.capabilities.as_ref().unwrap();
        assert_eq!(non_frac_caps.fractionable, Some(false));
    }
}

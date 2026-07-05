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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datamancer_core::{
    AssetClass, Bar, BarInterval, Control, ControlKind, Error, EventKind, HistoryRequest,
    Instrument, LiveHandle, MarketEvent, Price, Provider, ProviderId, Quantity, Quote, Result, Seq,
    Timestamp, Trade,
};
use oxidized_alpaca::{
    AccountType, CryptoFeed, TradingClient,
    restful::trading::assets::{
        Asset, AssetClass as AlpacaAssetClass, Status as AlpacaAssetStatus,
    },
    streaming::{
        CryptoStreamMessage, CryptoSubscriptionList, StreamingCryptoClient,
        messages::crypto::{CryptoBarEvent, CryptoQuoteEvent, CryptoTradeEvent},
    },
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::credentials::{AlpacaCredentials, CredentialsSource, Resolved};
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

/// Translate Alpaca's `/v2/assets` crypto rows into the datamancer
/// catalog. Pure function — no client, no I/O — so it can be exercised
/// against canned JSON fixtures without credentials. Skips non-tradable
/// rows and asset classes outside crypto (Alpaca's `Crypto` filter on the
/// request side should already handle this, but the guard is cheap and
/// defends against API drift).
fn crypto_assets_to_instruments(assets: &[Asset]) -> Vec<Instrument> {
    assets
        .iter()
        .filter(|a| a.tradable && matches!(a.class, AlpacaAssetClass::Crypto))
        .map(|a| {
            Instrument::new(
                ProviderId::from_static(PROVIDER_ID),
                AssetClass::Crypto,
                a.symbol.clone(),
            )
        })
        .collect()
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

/// Configuration for [`AlpacaCryptoProvider`].
#[derive(Clone, Debug)]
pub struct AlpacaCryptoProviderConfig {
    /// Paper or live account; selects which credential pair is loaded from
    /// the environment by `oxidized_alpaca`.
    pub account_type: AccountType,
    /// Which crypto venue to connect to.
    pub venue: AlpacaCryptoVenue,
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
            account_type: AccountType::Paper,
            venue: AlpacaCryptoVenue::Us,
            reconnect: ReconnectPolicy::default(),
            credentials: CredentialsSource::Env,
        }
    }
}

/// The crypto provider's REST surface, rebuilt whenever the credential
/// source changes (watch bump) — cheap relative to REST call frequency, and
/// `has_changed` makes the common path a no-op. One `std::sync::Mutex`
/// (never held across an await) guards the check-and-rebuild.
struct RestState {
    /// Trading API client, used for the reference-data surface (crypto
    /// asset catalog). `None` when credentials aren't available —
    /// `list_instruments` then surfaces a Provider error.
    trading: Option<TradingClient>,
    /// `Some` only for [`CredentialsSource::Watch`]; `has_changed` on this
    /// cached receiver is the rebuild trigger.
    cred_rx: Option<watch::Receiver<Option<AlpacaCredentials>>>,
}

fn build_trading(cfg: &AlpacaCryptoProviderConfig) -> Option<TradingClient> {
    match cfg.credentials.current() {
        Resolved::Env => TradingClient::new(cfg.account_type).ok(),
        Resolved::Creds(c) => {
            TradingClient::new_with_credentials(cfg.account_type, c.to_api_key()).ok()
        }
        Resolved::Missing => None,
    }
}

/// Alpaca-backed crypto [`Provider`].
pub struct AlpacaCryptoProvider {
    cfg: AlpacaCryptoProviderConfig,
    hub: Arc<Mutex<HubSlot>>,
    rest: std::sync::Mutex<RestState>,
}

enum HubSlot {
    Idle,
    Active {
        cmd_tx: mpsc::Sender<HubCommand>,
        #[allow(dead_code)]
        task: JoinHandle<()>,
    },
}

impl AlpacaCryptoProvider {
    #[must_use]
    pub fn new(cfg: AlpacaCryptoProviderConfig) -> Self {
        let rest = std::sync::Mutex::new(RestState {
            trading: build_trading(&cfg),
            cred_rx: cfg.credentials.watch(),
        });
        Self {
            cfg,
            hub: Arc::new(Mutex::new(HubSlot::Idle)),
            rest,
        }
    }

    /// Current trading client, rebuilt first if the credential source
    /// changed since the last call. Cloning out of the mutex keeps the
    /// guard from crossing an await (the client is a cheaply cloneable
    /// handle).
    fn trading_client(&self) -> Option<TradingClient> {
        let mut state = self.rest.lock().expect("REST client state poisoned");
        if let Some(rx) = state.cred_rx.as_mut()
            && rx.has_changed().unwrap_or(false)
        {
            let _ = rx.borrow_and_update();
            state.trading = build_trading(&self.cfg);
        }
        state.trading.clone()
    }

    /// Lazily spawn the hub task and return its command channel.
    async fn ensure_hub(&self) -> mpsc::Sender<HubCommand> {
        let mut slot = self.hub.lock().await;
        match &*slot {
            HubSlot::Active { cmd_tx, .. } => cmd_tx.clone(),
            HubSlot::Idle => {
                let (cmd_tx, cmd_rx) = mpsc::channel::<HubCommand>(64);
                let cfg = self.cfg.clone();
                let task = tokio::spawn(run_hub_task(cfg, cmd_rx));
                *slot = HubSlot::Active {
                    cmd_tx: cmd_tx.clone(),
                    task,
                };
                cmd_tx
            }
        }
    }
}

#[async_trait]
impl Provider for AlpacaCryptoProvider {
    fn id(&self) -> &str {
        PROVIDER_ID
    }

    fn supports(&self, _instrument: &Instrument, kind: EventKind) -> bool {
        match kind {
            EventKind::Trade
            | EventKind::Quote
            | EventKind::Bar(BarInterval::OneMinute | BarInterval::OneDay) => true,
            EventKind::Bar(_) => false,
        }
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let cmd_tx = self.ensure_hub().await;
        Ok(Box::new(SharedLiveHandle {
            cmd_tx,
            sink,
            subscribed: Mutex::new(None),
        }))
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

    async fn list_instruments(&self) -> Result<Vec<Instrument>> {
        let trading = self.trading_client().ok_or_else(|| Error::Provider {
            provider: PROVIDER_ID.to_string(),
            message: "Trading client not initialized (Alpaca credentials missing?)".to_string(),
        })?;
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
        Ok(crypto_assets_to_instruments(&assets))
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
        ack: oneshot::Sender<Result<()>>,
    },
}

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

#[allow(
    clippy::too_many_lines,
    reason = "single-pass connect / authenticate / subscribe / dispatch / reconnect state machine; extraction would obscure the linear lifecycle"
)]
async fn run_hub_task(cfg: AlpacaCryptoProviderConfig, mut cmd_rx: mpsc::Receiver<HubCommand>) {
    let mut routes: HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>> = HashMap::new();
    let mut subs = CryptoSubscriptionList::new();
    let mut backoff = cfg.reconnect.initial_backoff_ms;

    'outer: loop {
        let feed = match cfg.venue {
            AlpacaCryptoVenue::Us => CryptoFeed::Us,
            AlpacaCryptoVenue::UsKraken => CryptoFeed::UsKraken,
            AlpacaCryptoVenue::EuKraken => CryptoFeed::EuKraken,
        };
        // Fresh receiver per connect attempt: the clone marks the current
        // value as seen, so the hot-reconnect arm below only fires on
        // rotations that land *after* this resolution.
        let mut cred_rx = cfg.credentials.watch();
        let connect_result = match cfg.credentials.current() {
            Resolved::Env => StreamingCryptoClient::new(cfg.account_type, feed).await,
            Resolved::Creds(c) => {
                StreamingCryptoClient::new_with_credentials(cfg.account_type, feed, c.to_api_key())
                    .await
            }
            Resolved::Missing => {
                // No credentials yet: wait for provisioning instead of
                // hammering bad auth, then retry the outer loop. Only a
                // Watch source can resolve to Missing, so the receiver is
                // always present here; exit defensively if it isn't (never
                // busy-loop).
                let Some(rx) = cred_rx.as_mut() else { return };
                if !wait_for_credentials(rx, &mut cmd_rx).await {
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
                broadcast_control(
                    &routes,
                    ControlKind::ProviderDisconnected {
                        provider: PROVIDER_ID.to_string(),
                        reason: format!("connect failed: {}", error_chain(&err)),
                    },
                )
                .await;
                if !sleep_with_jitter(&mut backoff, &cfg.reconnect, &mut cmd_rx).await {
                    return;
                }
                continue 'outer;
            }
        };

        // Re-apply persistent subscription set on a fresh socket.
        if !is_empty(&subs)
            && let Err(err) = client.add_subscriptions(&subs).await
        {
            broadcast_control(
                &routes,
                ControlKind::ProviderError {
                    provider: PROVIDER_ID.to_string(),
                    message: format!("re-subscribe failed: {}", error_chain(&err)),
                },
            )
            .await;
        }

        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(HubCommand::Subscribe { instrument, kind, sink, ack }) => {
                            routes.insert((instrument.clone(), kind), sink);
                            apply_pair_to_list(&mut subs, &instrument, kind, true);
                            let res = client
                                .add_subscriptions(&subs)
                                .await
                                .map(|_| ())
                                .map_err(|e| Error::Provider {
                                    provider: PROVIDER_ID.to_string(),
                                    message: format!("add_subscriptions: {}", error_chain(&e)),
                                });
                            if res.is_err() {
                                // Roll back local state so a retry can succeed.
                                routes.remove(&(instrument.clone(), kind));
                                apply_pair_to_list(&mut subs, &instrument, kind, false);
                            } else {
                                // We are in the connected loop, so the provider is
                                // up. `ProviderConnected` is otherwise only
                                // broadcast on the connect *edge*, which a route
                                // that subscribes afterward misses — leaving its
                                // connection state Unknown even as data flows. Tell
                                // the freshly-subscribed route the provider is up.
                                broadcast_control_to(
                                    routes.get(&(instrument.clone(), kind)),
                                    ControlKind::ProviderConnected {
                                        provider: PROVIDER_ID.to_string(),
                                    },
                                )
                                .await;
                                broadcast_control_to(
                                    routes.get(&(instrument.clone(), kind)),
                                    ControlKind::SubscriptionChanged {
                                        provider: PROVIDER_ID.to_string(),
                                        instrument,
                                        kind,
                                        active: true,
                                    },
                                )
                                .await;
                            }
                            let _ = ack.send(res);
                        }
                        Some(HubCommand::Unsubscribe { instrument, kind, ack }) => {
                            apply_pair_to_list(&mut subs, &instrument, kind, false);
                            let mut removal = CryptoSubscriptionList::new();
                            apply_pair_to_list(&mut removal, &instrument, kind, true);
                            let res = client
                                .remove_subscriptions(&removal)
                                .await
                                .map(|_| ())
                                .map_err(|e| Error::Provider {
                                    provider: PROVIDER_ID.to_string(),
                                    message: format!("remove_subscriptions: {}", error_chain(&e)),
                                });
                            if res.is_ok() {
                                // Drop the route after the upstream ack so any
                                // tail events still in flight are routable.
                                let removed = routes.remove(&(instrument.clone(), kind));
                                if let Some(sink) = removed.as_ref() {
                                    let _ = sink
                                        .send(MarketEvent::Control(Control {
                                            source_ts: wall_clock_ts(),
                                            rx_ts: wall_clock_ts(),
                                            seq: Seq(0),
                                            kind: ControlKind::SubscriptionChanged {
                                                provider: PROVIDER_ID.to_string(),
                                                instrument: instrument.clone(),
                                                kind,
                                                active: false,
                                            },
                                        }))
                                        .await;
                                }
                            } else {
                                // Roll back the local subs so reconnect re-applies
                                // this pair (the upstream view of it is still active).
                                apply_pair_to_list(&mut subs, &instrument, kind, true);
                            }
                            let _ = ack.send(res);
                        }
                        None => {
                            // No outstanding handles can ever issue commands
                            // again; close the websocket and exit.
                            let _ = client.shut_down().await;
                            return;
                        }
                    }
                }
                next = client.next_message() => {
                    match next {
                        Ok(msg) => {
                            for ev in translate_crypto_message(msg) {
                                dispatch_event(&routes, ev).await;
                            }
                        }
                        Err(err) => {
                            broadcast_control(
                                &routes,
                                ControlKind::ProviderDisconnected {
                                    provider: PROVIDER_ID.to_string(),
                                    reason: format!("websocket: {}", error_chain(&err)),
                                },
                            )
                            .await;
                            drop(client);
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
            }
        }
    }
}

/// Waits for a `Watch` credential source to deliver a value, servicing the
/// command channel meanwhile (same semantics as [`sleep_with_jitter`]:
/// subscribe/unsubscribe fail fast, channel closure exits). Returns `false`
/// if the hub task should exit.
async fn wait_for_credentials(
    cred_rx: &mut watch::Receiver<Option<AlpacaCredentials>>,
    cmd_rx: &mut mpsc::Receiver<HubCommand>,
) -> bool {
    loop {
        tokio::select! {
            changed = cred_rx.changed() => {
                if changed.is_ok() {
                    // The outer loop re-resolves; if the new value is still
                    // `None` it lands back here rather than spinning.
                    return true;
                }
                // Sender dropped with no credentials: they can never
                // arrive. Keep servicing commands so hub shutdown (all
                // handles dropped) still works.
                loop {
                    match cmd_rx.recv().await {
                        Some(
                            HubCommand::Subscribe { ack, .. }
                            | HubCommand::Unsubscribe { ack, .. },
                        ) => {
                            let _ = ack.send(Err(Error::Provider {
                                provider: PROVIDER_ID.to_string(),
                                message: "no credentials provisioned".to_string(),
                            }));
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
                        let _ = ack.send(Err(Error::Provider {
                            provider: PROVIDER_ID.to_string(),
                            message: "waiting for credentials".to_string(),
                        }));
                    }
                    None => return false,
                }
            }
        }
    }
}

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
    // Control / non-routable event: fan out to every active sink so each
    // session sees provider-level state changes.
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

async fn sleep_with_jitter(
    backoff_ms: &mut u64,
    policy: &ReconnectPolicy,
    cmd_rx: &mut mpsc::Receiver<HubCommand>,
) -> bool {
    // Full jitter: pick a sleep uniformly in [0, backoff_ms]. See the stock
    // provider's sleep_with_jitter for the rationale.
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
                None => {
                    // SessionClosing is emitted by Controller::shutdown for each
                    // session; the hub serves all of them and shouldn't double-emit.
                    false
                }
            }
        }
    }
}

fn is_empty(list: &CryptoSubscriptionList) -> bool {
    list.bars.as_ref().is_none_or(Vec::is_empty)
        && list.daily_bars.as_ref().is_none_or(Vec::is_empty)
        && list.updated_bars.as_ref().is_none_or(Vec::is_empty)
        && list.quotes.as_ref().is_none_or(Vec::is_empty)
        && list.trades.as_ref().is_none_or(Vec::is_empty)
        && list.orderbooks.as_ref().is_none_or(Vec::is_empty)
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

async fn broadcast_control(
    routes: &HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    kind: ControlKind,
) {
    let now = wall_clock_ts();
    let ev = MarketEvent::Control(Control {
        source_ts: now,
        rx_ts: now,
        seq: Seq(0),
        kind,
    });
    for sink in routes.values() {
        let _ = sink.send(ev.clone()).await;
    }
}

async fn broadcast_control_to(sink: Option<&mpsc::Sender<MarketEvent>>, kind: ControlKind) {
    if let Some(s) = sink {
        let now = wall_clock_ts();
        let _ = s
            .send(MarketEvent::Control(Control {
                source_ts: now,
                rx_ts: now,
                seq: Seq(0),
                kind,
            }))
            .await;
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
            kind: ControlKind::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("{:?}: {}", err.code, err.message),
            },
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

/// Walk a `std::error::Error` source chain to a single string. oxidized-alpaca
/// 0.0.5 has a `{}, 0` thiserror format for its `WebsocketError` variant that
/// prints the literal `0` instead of the wrapped error, so we extract the
/// real cause via the source chain.
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![err.to_string()];
    let mut src = err.source();
    while let Some(e) = src {
        parts.push(e.to_string());
        src = e.source();
    }
    parts.join(" -> ")
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn provider_supports_kinds() {
        let p = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig::default());
        let inst = provider_instrument("BTC/USD");
        assert!(p.supports(&inst, EventKind::Trade));
        assert!(p.supports(&inst, EventKind::Quote));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneMinute)));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneDay)));
        assert!(!p.supports(&inst, EventKind::Bar(BarInterval::FiveMinute)));
    }

    #[test]
    fn event_route_key_matches_pair() {
        let now = Timestamp(1);
        let trade = MarketEvent::Trade(Trade {
            instrument: provider_instrument("BTC/USD"),
            source_ts: now,
            rx_ts: now,
            seq: Seq(0),
            price: Price::from_f64_round(1.0),
            size: Quantity::from_units(1),
        });
        assert_eq!(
            event_route_key(&trade),
            Some((provider_instrument("BTC/USD"), EventKind::Trade))
        );

        let bar = MarketEvent::Bar(Bar {
            instrument: provider_instrument("ETH/USD"),
            interval: BarInterval::OneMinute,
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
                provider_instrument("ETH/USD"),
                EventKind::Bar(BarInterval::OneMinute)
            ))
        );
    }

    #[test]
    fn crypto_assets_to_instruments_filters_and_maps() {
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
        let instruments = crypto_assets_to_instruments(&assets);
        // BTC/USD passes; DOGE/USD filtered (not tradable); AAPL filtered
        // (wrong class — guards against API drift).
        let symbols: Vec<&str> = instruments.iter().map(Instrument::symbol).collect();
        assert_eq!(symbols, vec!["BTC/USD"]);
        for i in &instruments {
            assert_eq!(i.provider().as_str(), PROVIDER_ID);
            assert_eq!(i.asset_class(), AssetClass::Crypto);
        }
    }
}

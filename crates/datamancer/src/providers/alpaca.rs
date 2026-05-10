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
    BarInterval, Control, ControlKind, Error, EventKind, GapSpan, HistoryRequest, Instrument,
    LiveHandle, MarketEvent, Price, Provider, Result, Seq, Timestamp, Trade,
};
use datamancer_core::{Bar, Quote};
use oxidized_alpaca::{
    AccountType, MarketDataClient,
    restful::market_data::TimeFrame,
    streaming::{
        StockBar, StockQuote, StockStreamMessage, StockSubscriptionList, StockTrade,
        StreamingStockClient,
    },
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::session::ReconnectPolicy;

/// Stable provider identifier for the Alpaca-backed provider.
pub const PROVIDER_ID: &str = "alpaca";

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


/// Configuration for [`AlpacaProvider`].
#[derive(Clone, Debug)]
pub struct AlpacaProviderConfig {
    /// Paper or live account; selects which credential pair is loaded from
    /// the environment by `oxidized_alpaca`.
    pub account_type: AccountType,
    /// Which streaming endpoint to connect to.
    pub stream_feed: AlpacaStreamFeed,
    /// Reconnect/retry policy for the live websocket.
    pub reconnect: ReconnectPolicy,
}

impl Default for AlpacaProviderConfig {
    fn default() -> Self {
        Self {
            account_type: AccountType::Paper,
            stream_feed: AlpacaStreamFeed::Iex,
            reconnect: ReconnectPolicy::default(),
        }
    }
}

/// Alpaca-backed [`Provider`].
pub struct AlpacaProvider {
    cfg: AlpacaProviderConfig,
    rest: Option<MarketDataClient>,
}

impl AlpacaProvider {
    /// Construct without eagerly initializing the REST client. Use this when
    /// only live streaming is needed and credentials are loaded later, or in
    /// tests where the env vars are not set.
    pub fn new(cfg: AlpacaProviderConfig) -> Self {
        let rest = MarketDataClient::new(cfg.account_type).ok();
        Self { cfg, rest }
    }

    /// Construct with an explicit REST client. Useful in tests.
    pub fn with_rest(cfg: AlpacaProviderConfig, rest: MarketDataClient) -> Self {
        Self {
            cfg,
            rest: Some(rest),
        }
    }
}

#[async_trait]
impl Provider for AlpacaProvider {
    fn id(&self) -> &str {
        PROVIDER_ID
    }

    fn supports(&self, _instrument: &Instrument, kind: EventKind) -> bool {
        match kind {
            EventKind::Trade | EventKind::Quote => true,
            EventKind::Bar(BarInterval::OneMinute | BarInterval::OneDay) => true,
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
        let rest = self.rest.as_ref().ok_or_else(|| Error::Provider {
            provider: PROVIDER_ID.to_string(),
            message: "REST client not initialized (Alpaca credentials missing?)".to_string(),
        })?;
        fetch_history_via(rest, request, sink).await
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

async fn run_streaming_task(
    cfg: AlpacaProviderConfig,
    sink: mpsc::Sender<MarketEvent>,
    mut cmd_rx: mpsc::Receiver<LiveCommand>,
) {
    // Authoritative subscription state, applied to every fresh client.
    let active: Arc<Mutex<StockSubscriptionList>> = Arc::new(Mutex::new(StockSubscriptionList::new()));
    let mut backoff = cfg.reconnect.initial_backoff_ms;

    'outer: loop {
        let connect_result = match cfg.stream_feed {
            AlpacaStreamFeed::Iex => StreamingStockClient::new_iex_client(cfg.account_type).await,
            AlpacaStreamFeed::Sip => StreamingStockClient::new_sip_client(cfg.account_type).await,
            AlpacaStreamFeed::DelayedSip => {
                StreamingStockClient::new_delayed_sip_client(cfg.account_type).await
            }
            AlpacaStreamFeed::Test => StreamingStockClient::new_test_client(cfg.account_type).await,
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
                emit_control(
                    &sink,
                    ControlKind::ProviderDisconnected {
                        provider: PROVIDER_ID.to_string(),
                        reason: format!("connect failed: {err}"),
                    },
                )
                .await;
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
                            emit_control(
                                &sink,
                                ControlKind::ProviderDisconnected {
                                    provider: PROVIDER_ID.to_string(),
                                    reason: format!("websocket: {err}"),
                                },
                            )
                            .await;
                            // Drop the client and reconnect.
                            drop(client);
                            if !sleep_with_jitter(&mut backoff, &cfg.reconnect, &mut cmd_rx).await {
                                return;
                            }
                            continue 'outer;
                        }
                    }
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
    let delay = Duration::from_millis(*backoff_ms);
    *backoff_ms = (*backoff_ms * 2).min(policy.max_backoff_ms);

    tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        cmd = cmd_rx.recv() => {
            match cmd {
                Some(LiveCommand::Close(ack)) => {
                    // SessionClosing is emitted by Controller::shutdown;
                    // don't double-emit here.
                    let _ = ack.send(());
                    false
                }
                Some(LiveCommand::Subscribe(_, _, ack))
                | Some(LiveCommand::Unsubscribe(_, _, ack)) => {
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
    list.bars.as_ref().is_none_or(|v| v.is_empty())
        && list.daily_bars.as_ref().is_none_or(|v| v.is_empty())
        && list.quotes.as_ref().is_none_or(|v| v.is_empty())
        && list.trades.as_ref().is_none_or(|v| v.is_empty())
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
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
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
        StockStreamMessage::Bar(b) => vec![MarketEvent::Bar(translate_bar(&b, BarInterval::OneMinute, rx))],
        StockStreamMessage::DailyBar(b) => {
            vec![MarketEvent::Bar(translate_bar(&b, BarInterval::OneDay, rx))]
        }
        StockStreamMessage::UpdatedBar(b) => {
            vec![MarketEvent::Bar(translate_bar(&b, BarInterval::OneMinute, rx))]
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

fn translate_trade(t: &StockTrade, rx: Timestamp) -> Trade {
    Trade {
        instrument: Instrument::new(&t.symbol),
        source_ts: chrono_to_ts(t.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        price: Price::from_f64_round(t.price),
        size: t.size as u64,
    }
}

fn translate_quote(q: &StockQuote, rx: Timestamp) -> Quote {
    Quote {
        instrument: Instrument::new(&q.symbol),
        source_ts: chrono_to_ts(q.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        bid: Price::from_f64_round(q.bid_price),
        bid_size: q.bid_size as u64,
        ask: Price::from_f64_round(q.ask_price),
        ask_size: q.ask_size as u64,
    }
}

fn translate_bar(b: &StockBar, interval: BarInterval, rx: Timestamp) -> Bar {
    Bar {
        instrument: Instrument::new(&b.symbol),
        interval,
        source_ts: chrono_to_ts(b.timestamp),
        rx_ts: rx,
        seq: Seq(0),
        open: Price::from_f64_round(b.open),
        high: Price::from_f64_round(b.high),
        low: Price::from_f64_round(b.low),
        close: Price::from_f64_round(b.close),
        volume: b.volume.max(0) as u64,
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
                    size: t.size as u64,
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
                BarInterval::OneMinute => TimeFrame::OneMinute,
                BarInterval::FiveMinute => TimeFrame::FiveMinute,
                BarInterval::FifteenMinute => TimeFrame::FifteenMinute,
                BarInterval::OneHour => TimeFrame::OneHour,
                BarInterval::OneDay => TimeFrame::OneDay,
            };
            let bars = rest
                .stock_bars(symbol, timeframe)
                .start(from)
                .end(to)
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
                    volume: b.volume as u64,
                };
                if sink.send(MarketEvent::Bar(bar)).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn _gap_span_from(_a: Timestamp, _b: Timestamp) -> GapSpan {
    GapSpan {
        from_source_ts: _a,
        to_source_ts: _b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_trade_message() {
        let json = r#"{"T":"t","S":"AAPL","i":12345,"x":"V","p":150.10,"s":100,"c":["@"],"t":"2024-01-02T15:30:00.123456789Z","z":"C"}"#;
        let msg: StockStreamMessage = serde_json::from_str(json).unwrap();
        let events = translate_stock_message(msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Trade(t) => {
                assert_eq!(t.instrument.symbol(), "AAPL");
                assert_eq!(t.size, 100);
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
                assert_eq!(q.bid_size, 2);
                assert_eq!(q.ask_size, 3);
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
                assert_eq!(b.volume, 12345);
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
        let aapl = Instrument::new("AAPL");
        apply_pair_to_list(&mut list, &aapl, EventKind::Trade, true);
        apply_pair_to_list(&mut list, &aapl, EventKind::Bar(BarInterval::OneMinute), true);
        assert_eq!(list.trades.as_deref(), Some(&["AAPL".to_string()][..]));
        assert_eq!(list.bars.as_deref(), Some(&["AAPL".to_string()][..]));
        apply_pair_to_list(&mut list, &aapl, EventKind::Trade, false);
        apply_pair_to_list(&mut list, &aapl, EventKind::Bar(BarInterval::OneMinute), false);
        assert!(list.trades.is_none());
        assert!(list.bars.is_none());
    }

    #[test]
    fn provider_supports_kinds() {
        let p = AlpacaProvider::new(AlpacaProviderConfig::default());
        let inst = Instrument::new("AAPL");
        assert!(p.supports(&inst, EventKind::Trade));
        assert!(p.supports(&inst, EventKind::Quote));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneMinute)));
        assert!(p.supports(&inst, EventKind::Bar(BarInterval::OneDay)));
        assert!(!p.supports(&inst, EventKind::Bar(BarInterval::FiveMinute)));
    }
}

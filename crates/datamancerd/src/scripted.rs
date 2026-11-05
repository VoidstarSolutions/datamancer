//! A deterministic, hermetic test provider: serves scripted trades on both
//! surfaces so the daemon's end-to-end suites can exercise the backfill seam,
//! the latest-value seed and the control replies without a network or
//! credentials. Feature `scripted-provider`, never on by default; it is a
//! test fixture for this binary, not a market-data source and not library
//! API.
//!
//! Behaviour, all knobs on [`ScriptedSettings`]:
//! - `fetch_history` waits `history_delay`, then serves `history_prints` trades
//!   over the requested range `[from, to)`, the first at `from`, the last at
//!   `to - 1`, the rest evenly between (a single print sits at `from`), prices
//!   `1.0, 2.0, …`, or fails if the symbol is `failing_symbol`.
//! - `LiveHandle::subscribe` returns after `connect_delay`; from then on, one
//!   trade every `live_interval`, stamped with the wall clock and priced
//!   `100.0, 101.0, …`, until unsubscribe or close.
//! - `latest` returns one trade priced [`LATEST_PRICE`] at `source_ts` 1 when
//!   `latest` is set.
//!
//! Trades only, on both surfaces, for any symbol that names this provider
//! (`supports` answers only for instruments carrying [`PROVIDER_ID`]); `enabled`
//! follows the settings source like every other provider (a `Watch(None)`
//! source parks it).

use std::time::Duration;

use async_trait::async_trait;
use datamancer_core::{
    Error, EventKind, HistoryRequest, Instrument, LiveHandle, MarketEvent, Price, Provider,
    Quantity, Result, Seq, Surface, Timestamp, Trade,
};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use datamancer::providers::SettingsSource;

/// The provider id, as `Provider::id` reports it and as `[provider.scripted]`
/// names it.
pub const PROVIDER_ID: &str = "scripted";

/// The price every `latest` trade carries.
pub const LATEST_PRICE: f64 = 999.0;

/// Runtime settings for [`ScriptedProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptedSettings {
    /// Trades served per `fetch_history` call.
    pub history_prints: u32,
    /// Wait before serving a history request.
    pub history_delay: Duration,
    /// Interval between live trades after a subscribe.
    pub live_interval: Duration,
    /// Whether `latest` returns a value (`true`) or `None`.
    pub latest: bool,
    /// The symbol whose history requests fail.
    pub failing_symbol: Option<String>,
    /// How long `LiveHandle::subscribe` takes to return; no live trade before it.
    pub connect_delay: Duration,
}

impl Default for ScriptedSettings {
    fn default() -> Self {
        Self {
            history_prints: 5,
            history_delay: Duration::from_millis(300),
            live_interval: Duration::from_millis(50),
            latest: true,
            failing_symbol: None,
            connect_delay: Duration::ZERO,
        }
    }
}

/// See the module docs.
pub struct ScriptedProvider {
    settings: SettingsSource<ScriptedSettings>,
}

impl ScriptedProvider {
    /// Build a scripted provider; a `Watch(None)` source parks it.
    #[must_use]
    pub fn new(settings: SettingsSource<ScriptedSettings>) -> Self {
        Self { settings }
    }

    fn settings(&self) -> Result<ScriptedSettings> {
        self.settings
            .current()
            .ok_or_else(|| Error::ProviderDisabled {
                provider: PROVIDER_ID.to_string(),
            })
    }
}

fn trade(instrument: Instrument, source_ts: Timestamp, price: f64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        instrument,
        source_ts,
        rx_ts: source_ts,
        seq: Seq(0),
        price: Price::from_f64_round(price),
        size: Quantity::from_units(1),
    })
}

fn wall_clock() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
    Timestamp(nanos)
}

#[async_trait]
impl Provider for ScriptedProvider {
    fn id(&self) -> &str {
        PROVIDER_ID
    }

    fn enabled(&self) -> bool {
        self.settings.current().is_some()
    }

    fn supports(&self, instrument: &Instrument, kind: EventKind, _surface: Surface) -> bool {
        instrument.provider().as_str() == PROVIDER_ID && kind == EventKind::Trade
    }

    async fn start_live(&self, sink: mpsc::Sender<MarketEvent>) -> Result<Box<dyn LiveHandle>> {
        let settings = self.settings()?;
        Ok(Box::new(ScriptedLiveHandle {
            sink,
            interval: settings.live_interval,
            connect_delay: settings.connect_delay,
            tasks: Mutex::new(Vec::new()),
        }))
    }

    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let settings = self.settings()?;
        tokio::time::sleep(settings.history_delay).await;
        let symbol = request.instrument.symbol();
        if settings.failing_symbol.as_deref() == Some(symbol) {
            return Err(Error::Provider {
                provider: PROVIDER_ID.to_string(),
                message: format!("scripted history failure for {symbol}"),
            });
        }
        let last = request.to.0.saturating_sub(1).max(request.from.0);
        let span = i128::from(last) - i128::from(request.from.0);
        let n = i64::from(settings.history_prints);
        for i in 0..n {
            let offset = if n > 1 {
                span * i128::from(i) / i128::from(n - 1)
            } else {
                0
            };
            let ts = Timestamp(request.from.0 + i64::try_from(offset).unwrap_or(0));
            #[allow(
                clippy::cast_precision_loss,
                reason = "a handful of scripted prints; exact for any count a test uses"
            )]
            let price = 1.0 + i as f64;
            if sink
                .send(trade(request.instrument.clone(), ts, price))
                .await
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }

    async fn latest(
        &self,
        instrument: &Instrument,
        _kind: EventKind,
    ) -> Result<Option<MarketEvent>> {
        let settings = self.settings()?;
        Ok(settings
            .latest
            .then(|| trade(instrument.clone(), Timestamp(1), LATEST_PRICE)))
    }
}

struct ScriptedLiveHandle {
    sink: mpsc::Sender<MarketEvent>,
    interval: Duration,
    connect_delay: Duration,
    /// One emitter task per subscribed pair.
    tasks: Mutex<Vec<(Instrument, EventKind, JoinHandle<()>)>>,
}

#[async_trait]
impl LiveHandle for ScriptedLiveHandle {
    async fn subscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        tokio::time::sleep(self.connect_delay).await;
        let sink = self.sink.clone();
        let interval = self.interval;
        let task_instrument = instrument.clone();
        let task = tokio::spawn(async move {
            let mut emitted: u64 = 0;
            loop {
                tokio::time::sleep(interval).await;
                let n = emitted;
                emitted += 1;
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "a test fixture's print counter; exact far beyond any test's run"
                )]
                let price = 100.0 + n as f64;
                if sink
                    .send(trade(task_instrument.clone(), wall_clock(), price))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        self.tasks.lock().await.push((instrument, kind, task));
        Ok(())
    }

    async fn unsubscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let mut tasks = self.tasks.lock().await;
        tasks.retain(|(i, k, task)| {
            let keep = !(*i == instrument && *k == kind);
            if !keep {
                task.abort();
            }
            keep
        });
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        for (_, _, task) in self.tasks.lock().await.drain(..) {
            task.abort();
        }
        Ok(())
    }
}

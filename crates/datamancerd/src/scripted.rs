//! A scripted, hermetic test provider: serves scripted trades on both
//! surfaces so the daemon's end-to-end suites can exercise the backfill seam,
//! the latest-value seed and the control replies without a network or
//! credentials. Feature `scripted-provider`, off by default.
//!
//! Behavior, all knobs on [`ScriptedSettings`]:
//! - `fetch_history` waits `history_delay_ms`, then serves `history_prints`
//!   trades spaced evenly over the requested range, prices `1.0, 2.0, …`.
//! - `LiveHandle::subscribe` emits `ProviderConnected`, then one trade every
//!   `live_interval_ms`, stamped with the wall clock and priced
//!   `100.0, 101.0, …`, until unsubscribe or close. The interval is re-read
//!   from the settings source at every tick; when the source goes to `None`,
//!   the pair gets `ProviderDisconnected` and its trades stop, and when it
//!   comes back, `ProviderConnected` and its trades resume. A subscribe while
//!   the source is `None` is refused with `ProviderDisabled`.
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
    Control, ControlKind, DisconnectCause, Error, EventKind, HistoryRequest, Instrument,
    LiveHandle, MarketEvent, Price, Provider, Quantity, Result, Seq, Surface, Timestamp, Trade,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;

use datamancer::providers::SettingsSource;

/// The provider id, as `Provider::id` reports it and as `[provider.scripted]`
/// names it.
pub const PROVIDER_ID: &str = "scripted";

/// The price every `latest` trade carries.
pub const LATEST_PRICE: f64 = 999.0;

/// The `[provider.scripted]` section and the provider's runtime settings.
/// Every knob is optional and defaults as [`Default`] gives it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScriptedSettings {
    /// Trades served per `fetch_history` call.
    pub history_prints: u32,
    /// Milliseconds a history request waits before serving.
    pub history_delay_ms: u64,
    /// Milliseconds between live trades after a subscribe; greater than 0.
    pub live_interval_ms: u64,
    /// Whether `latest` returns a value (`true`) or `None`.
    pub latest: bool,
}

impl Default for ScriptedSettings {
    fn default() -> Self {
        Self {
            history_prints: 5,
            history_delay_ms: 300,
            live_interval_ms: 50,
            latest: true,
        }
    }
}

/// The scripted test provider, serving trades as its [`ScriptedSettings`]
/// select.
pub struct ScriptedProvider {
    settings: SettingsSource<ScriptedSettings>,
}

impl ScriptedProvider {
    /// Build a scripted provider that follows `settings`.
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
        self.settings()?;
        Ok(Box::new(ScriptedLiveHandle {
            sink,
            settings: self.settings.clone(),
            tasks: Mutex::new(Vec::new()),
        }))
    }

    async fn fetch_history(
        &self,
        request: HistoryRequest,
        sink: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let settings = self.settings()?;
        tokio::time::sleep(Duration::from_millis(settings.history_delay_ms)).await;
        let n = i64::from(settings.history_prints);
        let step = request.to.0.saturating_sub(request.from.0) / n.max(1);
        for i in 0..n {
            let ts = Timestamp(request.from.0 + step * i);
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
    settings: SettingsSource<ScriptedSettings>,
    /// One emitter task per subscribed pair.
    tasks: Mutex<Vec<(Instrument, EventKind, JoinHandle<()>)>>,
}

fn control(kind: ControlKind) -> MarketEvent {
    let now = wall_clock();
    MarketEvent::Control(Control {
        source_ts: now,
        rx_ts: now,
        seq: Seq(0),
        kind,
    })
}

/// Resolves when `rx` sees a change; never, with no receiver. A closed
/// channel clears `rx`, so the last value stands.
async fn settings_changed(rx: &mut Option<watch::Receiver<Option<ScriptedSettings>>>) {
    match rx.as_mut() {
        Some(r) => {
            if r.changed().await.is_err() {
                *rx = None;
            }
        }
        None => std::future::pending().await,
    }
}

/// Emit one trade per `live_interval_ms`, re-read at each tick and on each
/// settings change, until the sink closes. A disabled source sends
/// `ProviderDisconnected` and parks the pair; re-enabled, it sends
/// `ProviderConnected` and resumes.
async fn emit(
    sink: mpsc::Sender<MarketEvent>,
    settings: SettingsSource<ScriptedSettings>,
    instrument: Instrument,
) {
    let mut rx = match &settings {
        SettingsSource::Watch(rx) => {
            let mut rx = rx.clone();
            rx.mark_unchanged();
            Some(rx)
        }
        SettingsSource::Static(_) => None,
    };
    let mut emitted: u64 = 0;
    loop {
        let Some(current) = settings.current() else {
            if sink
                .send(control(ControlKind::ProviderDisconnected {
                    provider: PROVIDER_ID.to_string(),
                    reason: "provider disabled".to_string(),
                    cause: DisconnectCause::Disabled,
                }))
                .await
                .is_err()
            {
                return;
            }
            while settings.current().is_none() {
                settings_changed(&mut rx).await;
            }
            if sink
                .send(control(ControlKind::ProviderConnected {
                    provider: PROVIDER_ID.to_string(),
                }))
                .await
                .is_err()
            {
                return;
            }
            continue;
        };
        // Settings first, so no trade goes out between a remove and its
        // `ProviderDisconnected`.
        tokio::select! {
            biased;
            () = settings_changed(&mut rx) => continue,
            () = tokio::time::sleep(Duration::from_millis(current.live_interval_ms)) => {}
        }
        #[allow(
            clippy::cast_precision_loss,
            reason = "a test fixture's print counter; exact far beyond any test's run"
        )]
        let price = 100.0 + emitted as f64;
        emitted += 1;
        if sink
            .send(trade(instrument.clone(), wall_clock(), price))
            .await
            .is_err()
        {
            return;
        }
    }
}

#[async_trait]
impl LiveHandle for ScriptedLiveHandle {
    async fn subscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        if self.settings.current().is_none() {
            return Err(Error::ProviderDisabled {
                provider: PROVIDER_ID.to_string(),
            });
        }
        let _ = self
            .sink
            .send(control(ControlKind::ProviderConnected {
                provider: PROVIDER_ID.to_string(),
            }))
            .await;
        let task = tokio::spawn(emit(
            self.sink.clone(),
            self.settings.clone(),
            instrument.clone(),
        ));
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

#[cfg(test)]
mod tests {
    use datamancer::providers::SettingsSource;
    use datamancer_core::{AssetClass, Error, EventKind, Instrument, Provider as _, ProviderId};
    use tokio::sync::{mpsc, watch};

    use super::{PROVIDER_ID, ScriptedProvider, ScriptedSettings};

    /// A subscribe on a live handle whose settings source has gone to `None`
    /// is refused with `ProviderDisabled`.
    #[tokio::test]
    async fn subscribe_while_disabled_is_refused() {
        let (tx, rx) = watch::channel(Some(ScriptedSettings::default()));
        let provider = ScriptedProvider::new(SettingsSource::Watch(rx));
        let (sink, _events) = mpsc::channel(16);
        let live = provider.start_live(sink).await.expect("start_live");

        tx.send_replace(None);
        let instrument = Instrument::new(
            ProviderId::from_static(PROVIDER_ID),
            AssetClass::Equity,
            "AAPL",
        );
        let result = live.subscribe(instrument, EventKind::Trade).await;
        assert!(
            matches!(result, Err(Error::ProviderDisabled { ref provider }) if provider == PROVIDER_ID),
            "expected ProviderDisabled, got {result:?}"
        );
        live.close().await.expect("close");
    }
}

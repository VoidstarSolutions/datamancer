//! The subscription vocabulary shared by every control surface: the wire-level
//! `Cfg` selectors and the `(instrument, kind)` + scope/persistence spec that
//! `subscribe`/`unsubscribe` carry.

use datamancer_core::{AssetClass, EventKind};
use serde::{Deserialize, Serialize};

/// Asset class selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClassCfg {
    Equity,
    Crypto,
}

impl From<AssetClassCfg> for AssetClass {
    fn from(value: AssetClassCfg) -> Self {
        match value {
            AssetClassCfg::Equity => AssetClass::Equity,
            AssetClassCfg::Crypto => AssetClass::Crypto,
        }
    }
}

/// Event-kind selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKindCfg {
    Trade,
    Quote,
    Bar1s,
    Bar1m,
    Bar5m,
    Bar15m,
    Bar1h,
    Bar1d,
}

impl From<EventKindCfg> for EventKind {
    fn from(value: EventKindCfg) -> Self {
        use datamancer_core::BarInterval;
        match value {
            EventKindCfg::Trade => EventKind::Trade,
            EventKindCfg::Quote => EventKind::Quote,
            EventKindCfg::Bar1s => EventKind::Bar(BarInterval::OneSecond),
            EventKindCfg::Bar1m => EventKind::Bar(BarInterval::OneMinute),
            EventKindCfg::Bar5m => EventKind::Bar(BarInterval::FiveMinute),
            EventKindCfg::Bar15m => EventKind::Bar(BarInterval::FifteenMinute),
            EventKindCfg::Bar1h => EventKind::Bar(BarInterval::OneHour),
            EventKindCfg::Bar1d => EventKind::Bar(BarInterval::OneDay),
        }
    }
}

/// Startup-session scope selector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeCfg {
    #[default]
    Live,
    LiveBackfill,
}

/// Persistence-preset selector for a startup session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceCfg {
    #[default]
    None,
    Cached,
    CachedWithTap,
    ReadOnly,
    Refresh,
    TapOnly,
}

/// One target `(instrument, kind)` plus per-request scope/persistence
/// preferences. Used by both `subscribe` and the `open-client` seed list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionSpec {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
    /// Scope preference. On conflict with an existing authoritative scope the
    /// reply returns the *actual* scope rather than erroring (handled server
    /// side); client subscriptions are pure-live today.
    #[serde(default)]
    pub scope: ScopeCfg,
    #[serde(default)]
    pub persistence: PersistenceCfg,
}

/// The `(provider, asset_class, symbol, kind)` tuple an `unsubscribe` names.
/// Flattened into the request frame, so the wire shape is identical to the
/// historical inline fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsubscribeSpec {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
}

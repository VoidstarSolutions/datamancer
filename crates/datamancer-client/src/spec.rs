//! The subscription vocabulary shared by every control surface: the wire-level
//! `Cfg` selectors and the `(instrument, kind)` + scope/persistence spec that
//! `subscribe`/`unsubscribe` carry.

use datamancer_core::{AssetClass, BarInterval, EventKind, Instrument};
use serde::{Deserialize, Serialize};

/// The core value has no wire selector: the daemon's control vocabulary is a
/// closed set that can lag `datamancer-core`'s `#[non_exhaustive]` enums
/// (e.g. [`AssetClass::Etf`] today). Extending the vocabulary is a daemon
/// contract change, not a client-side conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("asset class {0} has no wire selector in the control vocabulary")]
pub struct UnsupportedAssetClass(pub AssetClass);

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

impl TryFrom<AssetClass> for AssetClassCfg {
    type Error = UnsupportedAssetClass;

    /// Fallible because `AssetClass` is `#[non_exhaustive]` and already wider
    /// than the wire vocabulary (`Etf` has no selector).
    fn try_from(value: AssetClass) -> Result<Self, Self::Error> {
        match value {
            AssetClass::Equity => Ok(AssetClassCfg::Equity),
            AssetClass::Crypto => Ok(AssetClassCfg::Crypto),
            other => Err(UnsupportedAssetClass(other)),
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

impl From<EventKind> for EventKindCfg {
    /// Total: `EventKind`/`BarInterval` are closed enums, and the wire
    /// vocabulary covers the full kind space ([`EventKind::enumerate`]).
    fn from(value: EventKind) -> Self {
        match value {
            EventKind::Trade => EventKindCfg::Trade,
            EventKind::Quote => EventKindCfg::Quote,
            EventKind::Bar(BarInterval::OneSecond) => EventKindCfg::Bar1s,
            EventKind::Bar(BarInterval::OneMinute) => EventKindCfg::Bar1m,
            EventKind::Bar(BarInterval::FiveMinute) => EventKindCfg::Bar5m,
            EventKind::Bar(BarInterval::FifteenMinute) => EventKindCfg::Bar15m,
            EventKind::Bar(BarInterval::OneHour) => EventKindCfg::Bar1h,
            EventKind::Bar(BarInterval::OneDay) => EventKindCfg::Bar1d,
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
///
/// No `deny_unknown_fields`: these structs are `#[serde(flatten)]`-ed into
/// the request frames, and serde documents that combination as unsupported
/// (the attribute was silently inert via `flatten`). Unknown-key rejection
/// is the daemon's job — the UDS surface enforces it explicitly server-side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl SubscriptionSpec {
    /// Build a spec for `(instrument, kind)` with default scope/persistence —
    /// closing the discovery loop: this is exactly the vocabulary
    /// [`crate::Client::instruments`] hands back.
    ///
    /// # Errors
    ///
    /// [`UnsupportedAssetClass`] when the instrument's asset class has no
    /// wire selector (see [`AssetClassCfg`]).
    pub fn new(instrument: &Instrument, kind: EventKind) -> Result<Self, UnsupportedAssetClass> {
        Ok(Self {
            provider: instrument.provider().as_str().to_string(),
            asset_class: instrument.asset_class().try_into()?,
            symbol: instrument.symbol().to_string(),
            kind: kind.into(),
            scope: ScopeCfg::default(),
            persistence: PersistenceCfg::default(),
        })
    }
}

/// The `(provider, asset_class, symbol, kind)` tuple an `unsubscribe` names.
/// Flattened into the request frame, so the wire shape is identical to the
/// historical inline fields. No `deny_unknown_fields` for the same reason as
/// [`SubscriptionSpec`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsubscribeSpec {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
}

impl UnsubscribeSpec {
    /// Name the `(instrument, kind)` to unsubscribe — the counterpart of
    /// [`SubscriptionSpec::new`].
    ///
    /// # Errors
    ///
    /// [`UnsupportedAssetClass`] when the instrument's asset class has no
    /// wire selector (see [`AssetClassCfg`]).
    pub fn new(instrument: &Instrument, kind: EventKind) -> Result<Self, UnsupportedAssetClass> {
        Ok(Self {
            provider: instrument.provider().as_str().to_string(),
            asset_class: instrument.asset_class().try_into()?,
            symbol: instrument.symbol().to_string(),
            kind: kind.into(),
        })
    }
}

impl From<SubscriptionSpec> for UnsubscribeSpec {
    /// An unsubscribe names the same tuple a subscribe did; scope and
    /// persistence do not participate in identity.
    fn from(spec: SubscriptionSpec) -> Self {
        Self {
            provider: spec.provider,
            asset_class: spec.asset_class,
            symbol: spec.symbol,
            kind: spec.kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AssetClassCfg, EventKindCfg, PersistenceCfg, ScopeCfg, SubscriptionSpec, UnsubscribeSpec,
    };
    use datamancer_core::{AssetClass, EventKind, Instrument, ProviderId};

    /// The Cfg→core→Cfg round trip is the identity over the full kind space:
    /// the wire vocabulary covers exactly `EventKind::enumerate`.
    #[test]
    fn event_kind_round_trips_over_the_full_kind_space() {
        for kind in EventKind::enumerate() {
            assert_eq!(EventKind::from(EventKindCfg::from(kind)), kind);
        }
    }

    #[test]
    fn asset_class_conversion_covers_the_wire_vocabulary_and_rejects_the_rest() {
        assert_eq!(
            AssetClassCfg::try_from(AssetClass::Equity),
            Ok(AssetClassCfg::Equity)
        );
        assert_eq!(
            AssetClassCfg::try_from(AssetClass::Crypto),
            Ok(AssetClassCfg::Crypto)
        );
        assert!(AssetClassCfg::try_from(AssetClass::Etf).is_err());
    }

    /// `SubscriptionSpec::new` from discovery vocabulary must serialize to
    /// the exact wire shape a hand-written JSON spec produces.
    #[test]
    fn subscription_spec_new_matches_the_wire_shape() {
        let instrument = Instrument::new(
            ProviderId::from_static("alpaca-crypto"),
            AssetClass::Crypto,
            "BTC/USD",
        );
        let spec = SubscriptionSpec::new(&instrument, EventKind::Trade).unwrap();
        let from_json: SubscriptionSpec = serde_json::from_str(
            r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        assert_eq!(spec, from_json);
        assert_eq!(spec.scope, ScopeCfg::Live);
        assert_eq!(spec.persistence, PersistenceCfg::None);

        let unsub = UnsubscribeSpec::new(&instrument, EventKind::Trade).unwrap();
        assert_eq!(UnsubscribeSpec::from(spec), unsub);
    }
}

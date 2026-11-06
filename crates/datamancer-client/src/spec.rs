//! The subscription vocabulary shared by every control surface: the wire-level
//! `Cfg` selectors and the `(instrument, kind)` + scope/persistence spec that
//! `subscribe`/`unsubscribe` carry.

use datamancer_core::{
    AssetClass, BarInterval, EventKind, Instrument, LiveScope, Subscribed, Timestamp,
};

use crate::error::ClientError;
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
/// preferences. Used by both `subscribe` and the `open-client` subscription list.
///
/// The daemon refuses an unknown key on every control and WS request frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SubscriptionSpec {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
    /// The scope to open. A `scope` key absent from the wire means live, and
    /// under live a `seed` absent or `null` means `true`; a `scope`,
    /// `backfill_from` and non-null `seed` that disagree fail to parse. A live
    /// scope writes `seed` only when it is `false`.
    #[serde(flatten)]
    pub scope: LiveScope,
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
            scope: LiveScope::default(),
            persistence: PersistenceCfg::default(),
        })
    }

    /// Ask for history from `from`: see [`LiveScope::LiveBackfill`].
    #[must_use]
    pub fn with_backfill_from(mut self, from: Timestamp) -> Self {
        self.scope = LiveScope::backfill(from);
        self
    }

    /// Decline the latest-value seed: see [`LiveScope::Live`]. A no-op on a
    /// [`LiveScope::LiveBackfill`], which never seeds.
    #[must_use]
    pub fn without_seed(mut self) -> Self {
        if matches!(self.scope, LiveScope::Live { .. }) {
            self.scope = LiveScope::unseeded();
        }
        self
    }

    /// Check a subscribe's reply against this request. A
    /// [`Subscribed::Created`] carries the scope asked for; a
    /// [`Subscribed::Joined`] takes the session as it is.
    ///
    /// # Errors
    ///
    /// [`ClientError::ScopeNotHonored`] for a session created with another
    /// scope.
    pub(crate) fn check_subscribed<E: std::error::Error>(
        &self,
        subscribed: &Subscribed,
    ) -> Result<(), ClientError<E>> {
        let got = subscribed.session_scope();
        if matches!(subscribed, Subscribed::Created { .. }) && got != self.scope {
            return Err(ClientError::ScopeNotHonored {
                asked: self.scope,
                got,
            });
        }
        Ok(())
    }
}

/// The `(provider, asset_class, symbol, kind)` tuple an `unsubscribe` names,
/// flattened into the request frame. The daemon refuses an unknown key on every
/// control and WS request frame.
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

/// An opaque handle to an in-flight historical query, minted by the daemon.
///
/// A plain alias rather than a newtype: the client only ever echoes it back to
/// the daemon, so a wrapper would buy no invariant while adding a conversion at
/// every call site.
pub type QueryId = u64;

/// One bounded historical query: the `(provider, asset_class, symbol, kind)`
/// tuple plus a source-time range.
///
/// Deliberately carries **no** persistence selector — cache policy for queries
/// is the daemon's, configured under `[session] query_persistence`. Flattened
/// into the request frame, so (like [`SubscriptionSpec`]) it cannot use
/// `deny_unknown_fields`; the daemon enforces unknown-key rejection server-side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySpec {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
    /// Inclusive start of the source-time range.
    pub from: Timestamp,
    /// End of the source-time range. Must be `>= from`.
    pub to: Timestamp,
}

impl QuerySpec {
    /// Build a query spec for `(instrument, kind)` over `from..to`.
    ///
    /// # Errors
    ///
    /// [`UnsupportedAssetClass`] when the instrument's asset class has no wire
    /// selector (see [`AssetClassCfg`]).
    pub fn new(
        instrument: &Instrument,
        kind: EventKind,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Self, UnsupportedAssetClass> {
        Ok(Self {
            provider: instrument.provider().as_str().to_string(),
            asset_class: instrument.asset_class().try_into()?,
            symbol: instrument.symbol().to_string(),
            kind: kind.into(),
            from,
            to,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AssetClassCfg, EventKindCfg, PersistenceCfg, SubscriptionSpec, UnsubscribeSpec};
    use datamancer_core::{AssetClass, EventKind, Instrument, LiveScope, ProviderId, Timestamp};

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
        assert_eq!(spec.scope, LiveScope::default());
        assert_eq!(spec.persistence, PersistenceCfg::None);

        let unsub = UnsubscribeSpec::new(&instrument, EventKind::Trade).unwrap();
        assert_eq!(UnsubscribeSpec::from(spec), unsub);
    }

    /// `backfill_from` is absent from a pure-live frame and rides as a bare
    /// integer timestamp under `live_backfill`.
    #[test]
    fn backfill_from_is_omitted_when_none_and_carried_under_live_backfill() {
        let instrument = Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        );
        let live = SubscriptionSpec::new(&instrument, EventKind::Trade).unwrap();
        let json = serde_json::to_string(&live).unwrap();
        assert!(!json.contains("backfill_from"), "{json}");

        let backfill = live.clone().with_backfill_from(Timestamp(1_000));
        assert_eq!(backfill.scope, LiveScope::backfill(Timestamp(1_000)));
        let json = serde_json::to_string(&backfill).unwrap();
        assert_eq!(
            json,
            r#"{"provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"trade","scope":"live_backfill","backfill_from":1000,"persistence":"none"}"#
        );
        let back: SubscriptionSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, backfill);
    }

    fn parse_scope(fields: &str) -> Result<LiveScope, serde_json::Error> {
        let frame = format!(
            r#"{{"provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"trade"{fields}}}"#
        );
        serde_json::from_str::<SubscriptionSpec>(&frame).map(|spec| spec.scope)
    }

    #[test]
    fn scope_fields_parse_to_one_live_scope() {
        let seeded = LiveScope::default();
        let unseeded = LiveScope::unseeded();
        assert_eq!(parse_scope("").unwrap(), seeded);
        assert_eq!(parse_scope(r#","scope":"live""#).unwrap(), seeded);
        assert_eq!(
            parse_scope(r#","scope":"live","backfill_from":null"#).unwrap(),
            seeded
        );
        assert_eq!(parse_scope(r#","seed":null"#).unwrap(), seeded);
        assert_eq!(
            parse_scope(r#","scope":"live","seed":true"#).unwrap(),
            seeded
        );
        assert_eq!(parse_scope(r#","seed":false"#).unwrap(), unseeded);
        assert_eq!(
            parse_scope(r#","scope":"live","seed":false"#).unwrap(),
            unseeded
        );
        assert_eq!(
            parse_scope(r#","scope":"live_backfill","backfill_from":7"#).unwrap(),
            LiveScope::backfill(Timestamp(7))
        );
        assert_eq!(
            parse_scope(r#","scope":"live_backfill","backfill_from":7,"seed":null"#).unwrap(),
            LiveScope::backfill(Timestamp(7))
        );
    }

    #[test]
    fn seed_is_written_only_when_false() {
        let instrument = Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        );
        let spec = SubscriptionSpec::new(&instrument, EventKind::Trade).unwrap();
        let wire = serde_json::to_string(&spec).unwrap();
        assert!(wire.contains(r#""scope":"live","persistence""#), "{wire}");
        let wire = serde_json::to_string(&spec.clone().without_seed()).unwrap();
        assert!(wire.contains(r#""scope":"live","seed":false"#), "{wire}");
        let wire = serde_json::to_string(&spec.with_backfill_from(Timestamp(7))).unwrap();
        assert!(!wire.contains("seed"), "{wire}");
    }

    #[test]
    fn with_backfill_from_and_without_seed_commute_on_a_backfill() {
        let instrument = Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        );
        let spec = SubscriptionSpec::new(&instrument, EventKind::Trade).unwrap();
        let backfill = LiveScope::backfill(Timestamp(7));
        assert_eq!(
            spec.clone()
                .without_seed()
                .with_backfill_from(Timestamp(7))
                .scope,
            backfill
        );
        assert_eq!(
            spec.clone()
                .with_backfill_from(Timestamp(7))
                .without_seed()
                .scope,
            backfill
        );
        assert_eq!(spec.without_seed().scope, LiveScope::unseeded());
    }

    #[test]
    fn a_disagreeing_scope_pair_does_not_parse() {
        for fields in [
            r#","scope":"live_backfill""#,
            r#","scope":"live_backfill","backfill_from":null"#,
        ] {
            let err = parse_scope(fields).unwrap_err().to_string();
            assert!(
                err.contains("scope live_backfill requires backfill_from"),
                "{err}"
            );
        }
        for fields in [
            r#","scope":"live","backfill_from":7"#,
            r#","backfill_from":7"#,
        ] {
            let err = parse_scope(fields).unwrap_err().to_string();
            assert!(
                err.contains("backfill_from requires scope live_backfill"),
                "{err}"
            );
        }
        for fields in [
            r#","scope":"live_backfill","backfill_from":7,"seed":true"#,
            r#","scope":"live_backfill","backfill_from":7,"seed":false"#,
        ] {
            let err = parse_scope(fields).unwrap_err().to_string();
            assert!(err.contains("seed requires scope live"), "{err}");
        }
    }

    mod query_spec_tests {
        use super::super::{AssetClassCfg, EventKindCfg, QuerySpec};
        use datamancer_core::{AssetClass, EventKind, Instrument, ProviderId, Timestamp};

        #[test]
        fn query_spec_round_trips_with_bare_integer_timestamps() {
            let spec = QuerySpec {
                provider: "alpaca".to_string(),
                asset_class: AssetClassCfg::Equity,
                symbol: "AAPL".to_string(),
                kind: EventKindCfg::Bar1d,
                from: Timestamp(1_000),
                to: Timestamp(2_000),
            };
            let json = serde_json::to_string(&spec).expect("serialize");
            assert_eq!(
                json,
                r#"{"provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1d","from":1000,"to":2000}"#
            );
            let back: QuerySpec = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, spec);
        }

        #[test]
        fn new_builds_from_an_instrument_and_kind() {
            let instrument = Instrument::new(
                ProviderId::new("alpaca"),
                AssetClass::Equity,
                "MSFT".to_string(),
            );
            let spec = QuerySpec::new(&instrument, EventKind::Trade, Timestamp(5), Timestamp(9))
                .expect("equity has a wire selector");
            assert_eq!(spec.provider, "alpaca");
            assert_eq!(spec.symbol, "MSFT");
            assert_eq!(spec.kind, EventKindCfg::Trade);
            assert_eq!(spec.from, Timestamp(5));
            assert_eq!(spec.to, Timestamp(9));
        }

        #[test]
        fn new_rejects_an_asset_class_with_no_wire_selector() {
            let instrument = Instrument::new(
                ProviderId::new("alpaca"),
                AssetClass::Etf,
                "SPY".to_string(),
            );
            assert!(
                QuerySpec::new(&instrument, EventKind::Trade, Timestamp(0), Timestamp(1)).is_err()
            );
        }
    }
}

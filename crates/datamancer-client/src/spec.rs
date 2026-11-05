//! The subscription vocabulary shared by every control surface: the wire-level
//! `Cfg` selectors and the `(instrument, kind)` + scope/persistence spec that
//! `subscribe`/`unsubscribe` carry.

use datamancer_core::{AssetClass, BarInterval, EventKind, Instrument, Timestamp};
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

/// Live-scope selector, shared by startup sessions and `subscribe`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeCfg {
    /// Purely from "now".
    #[default]
    Live,
    /// History from a `backfill_from` timestamp up to the live edge, then the
    /// live tail.
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
    /// Scope preference. `live` starts purely from "now"; `live_backfill`
    /// fetches history from [`Self::backfill_from`] up to the live edge and
    /// seams it into the live tail (the daemon owns the seam: live arrivals
    /// are buffered during the backfill and flushed after it, with an in-band
    /// `Gap` only for real loss).
    ///
    /// The `(instrument, kind)` pair is backed by one shared authoritative
    /// session whose scope is fixed when it is created, so a later subscriber
    /// joins that session whatever it asked for. On such a conflict the reply
    /// returns the *actual* scope rather than erroring — read `scope` and
    /// `backfill_from` on the reply, not the request.
    #[serde(default)]
    pub scope: ScopeCfg,
    /// Inclusive source-time start of the backfill. Required when `scope` is
    /// `live_backfill` and rejected (`bad_request`) otherwise, so a request can
    /// never be silently downgraded to pure-live. Absent on the wire when
    /// `None`, which keeps older clients' frames valid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill_from: Option<Timestamp>,
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
            backfill_from: None,
            persistence: PersistenceCfg::default(),
        })
    }

    /// Ask for history from `from` up to the live edge, seamed into the live
    /// tail: sets `scope` to [`ScopeCfg::LiveBackfill`] and `backfill_from`
    /// together, so the pair can never disagree.
    #[must_use]
    pub fn with_backfill_from(mut self, from: Timestamp) -> Self {
        self.scope = ScopeCfg::LiveBackfill;
        self.backfill_from = Some(from);
        self
    }
}

/// The scope a `subscribe` actually opened, echoed in the reply: the shared
/// authoritative session's creation-time scope, which is the requested one
/// only when this subscriber created the session. Read this, not the request,
/// to learn whether history will precede the live tail — and read `joined`
/// to learn whether it will precede *this* stream at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Subscribed {
    /// The actual scope.
    pub scope: ScopeCfg,
    /// The actual backfill start; `Some` exactly when `scope` is
    /// [`ScopeCfg::LiveBackfill`].
    pub backfill_from: Option<Timestamp>,
    /// `true` when this subscribe joined a session another subscriber had
    /// already opened. A joiner's stream begins where it joined: no history
    /// is replayed for it and no `Gap` marks what it missed, so under
    /// `live_backfill` the `backfill_from` describes the session creator's
    /// coverage, not this subscriber's. A consumer that needs its own
    /// history on a joined pair fetches it itself (`open-query`).
    pub joined: bool,
}

impl Subscribed {
    /// Pure-live, opened by this subscriber.
    #[must_use]
    pub const fn live() -> Self {
        Self {
            scope: ScopeCfg::Live,
            backfill_from: None,
            joined: false,
        }
    }

    /// Backfill from `from`, opened by this subscriber.
    #[must_use]
    pub const fn live_backfill(from: Timestamp) -> Self {
        Self {
            scope: ScopeCfg::LiveBackfill,
            backfill_from: Some(from),
            joined: false,
        }
    }

    /// Mark whether the session was joined rather than created.
    #[must_use]
    pub const fn with_joined(mut self, joined: bool) -> Self {
        self.joined = joined;
        self
    }

    /// Rebuild from reply fields. `None` when the wire is inconsistent
    /// (`live_backfill` without a `backfill_from`), which a client reports
    /// as a protocol error rather than guessing.
    #[must_use]
    pub fn from_wire(
        scope: ScopeCfg,
        backfill_from: Option<Timestamp>,
        joined: bool,
    ) -> Option<Self> {
        match (scope, backfill_from) {
            (ScopeCfg::Live, _) => Some(Self::live().with_joined(joined)),
            (ScopeCfg::LiveBackfill, Some(from)) => {
                Some(Self::live_backfill(from).with_joined(joined))
            }
            (ScopeCfg::LiveBackfill, None) => None,
        }
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
    use super::{
        AssetClassCfg, EventKindCfg, PersistenceCfg, ScopeCfg, Subscribed, SubscriptionSpec,
        UnsubscribeSpec,
    };
    use datamancer_core::{AssetClass, EventKind, Instrument, ProviderId, Timestamp};

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
        assert_eq!(spec.backfill_from, None);
        assert_eq!(spec.persistence, PersistenceCfg::None);

        let unsub = UnsubscribeSpec::new(&instrument, EventKind::Trade).unwrap();
        assert_eq!(UnsubscribeSpec::from(spec), unsub);
    }

    /// `backfill_from` is absent from a pure-live frame (an older daemon's
    /// unknown-key rejection never sees it) and rides as a bare integer
    /// timestamp under `live_backfill`.
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
        assert_eq!(backfill.scope, ScopeCfg::LiveBackfill);
        let json = serde_json::to_string(&backfill).unwrap();
        assert_eq!(
            json,
            r#"{"provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"trade","scope":"live_backfill","backfill_from":1000,"persistence":"none"}"#
        );
        let back: SubscriptionSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, backfill);
    }

    /// The reply-side pair stays consistent: a stray `backfill_from` under
    /// `live` is dropped, and `live_backfill` without one is not a value at
    /// all.
    #[test]
    fn subscribed_from_wire_keeps_the_pair_consistent() {
        let live = Subscribed::from_wire(ScopeCfg::Live, Some(Timestamp(5)), false).unwrap();
        assert_eq!(live, Subscribed::live());
        let backfill =
            Subscribed::from_wire(ScopeCfg::LiveBackfill, Some(Timestamp(5)), true).unwrap();
        assert_eq!(
            backfill,
            Subscribed::live_backfill(Timestamp(5)).with_joined(true)
        );
        assert!(backfill.joined);
        assert_eq!(
            Subscribed::from_wire(ScopeCfg::LiveBackfill, None, false),
            None
        );
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

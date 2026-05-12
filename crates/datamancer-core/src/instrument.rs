//! Instrument identity.
//!
//! An `Instrument` is the qualifying tuple `(provider, asset_class, symbol)`.
//! Symbol grammar is provider-specific (`"AAPL"` on Alpaca equities,
//! `"BTC/USD"` on Alpaca crypto), so the provider and asset class are needed
//! to make the identifier unique across the union of all sources.

use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable identifier for a market-data provider.
///
/// Matches the value returned by [`crate::Provider::id`]. The `Cow`
/// representation keeps the common case — a static provider constant such as
/// `"alpaca"` — zero-allocation while still supporting runtime-constructed
/// ids for multi-tenant deployments and test fakes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderId(Cow<'static, str>);

impl ProviderId {
    /// Construct from a `'static` string. `const`-callable for top-level
    /// provider constants.
    #[must_use]
    pub const fn from_static(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }

    /// Construct from an owned string. Use this for ids produced at runtime
    /// (e.g. parsed configuration).
    pub fn new(id: impl Into<String>) -> Self {
        Self(Cow::Owned(id.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&'static str> for ProviderId {
    fn from(s: &'static str) -> Self {
        Self::from_static(s)
    }
}

impl From<String> for ProviderId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Broad classification of what an instrument trades. v0 covers the three
/// asset classes currently served by registered providers; the enum is
/// `#[non_exhaustive]` so adding `Forex`, `Future`, `Option`, etc. later is
/// a non-breaking extension for downstream `match` arms that opt in.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AssetClass {
    Equity,
    Etf,
    Crypto,
}

impl fmt::Display for AssetClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Equity => f.write_str("equity"),
            Self::Etf => f.write_str("etf"),
            Self::Crypto => f.write_str("crypto"),
        }
    }
}

/// Identifies one instrument as `(provider, asset_class, symbol)`.
///
/// The qualifying tuple is what makes the id unique: the same `symbol` can
/// appear under different providers or asset classes (an equity and an ETF
/// that share a ticker; the same crypto pair on multiple venues), and engine
/// code that holds an `Instrument` should be able to round-trip back to the
/// right provider without an external lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Instrument {
    provider: ProviderId,
    asset_class: AssetClass,
    symbol: String,
}

impl Instrument {
    /// Construct from the qualifying tuple.
    pub fn new(
        provider: impl Into<ProviderId>,
        asset_class: AssetClass,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            asset_class,
            symbol: symbol.into(),
        }
    }

    /// The provider that serves this instrument. Stable for the lifetime of
    /// the value; matches the `id()` of some registered [`crate::Provider`].
    #[must_use]
    pub fn provider(&self) -> &ProviderId {
        &self.provider
    }

    /// The instrument's broad asset class.
    #[must_use]
    pub fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    /// The provider-specific symbol string.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl fmt::Display for Instrument {
    /// `"AAPL (alpaca/equity)"` — symbol-first for log readability, with the
    /// qualifying tuple in parentheses so collisions are visible at a glance.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}/{})",
            self.symbol, self.provider, self.asset_class
        )
    }
}

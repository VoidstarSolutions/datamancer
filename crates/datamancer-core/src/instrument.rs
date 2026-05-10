//! Instrument identity.
//!
//! `Instrument` is intentionally opaque — a newtype around a symbol string —
//! so callers don't need to be revised when asset class, exchange, contract
//! specification, or other structured fields are added later.

use std::fmt;

/// Identifies one instrument in a provider-agnostic form.
///
/// The internal representation is a symbol string (e.g. `"AAPL"`, `"BTC/USD"`,
/// `"ESM6"`). Callers should treat this as opaque and construct via
/// [`Instrument::from`] / [`Instrument::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Instrument(String);

impl Instrument {
    /// Construct from any string-like value.
    pub fn new(symbol: impl Into<String>) -> Self {
        Self(symbol.into())
    }

    /// The underlying symbol string. Stable for as long as the instrument
    /// exists; do not assume any particular grammar across providers.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.0
    }
}

impl<S: Into<String>> From<S> for Instrument {
    fn from(symbol: S) -> Self {
        Self::new(symbol)
    }
}

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

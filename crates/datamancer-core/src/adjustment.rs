//! Corporate-action adjustment mode for historical bar data.

use serde::{Deserialize, Serialize};

/// How corporate actions (splits, dividends, spin-offs) are folded into
/// historical bar prices.
///
/// This is a single source of truth carried by both the provider request and
/// the cache key so adjusted data can never be stored under a raw key (or vice
/// versa). The default is [`Adjustment::All`]: fully adjusted bars, so charts
/// built downstream do not fabricate phantom reversals at split boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Adjustment {
    /// No adjustment; bars carry raw, as-traded prices.
    Raw,
    /// Split-adjusted only.
    Split,
    /// Dividend-adjusted only.
    Dividend,
    /// Spin-off-adjusted only.
    SpinOff,
    /// Fully adjusted: splits, dividends, and spin-offs.
    #[default]
    All,
}

impl Adjustment {
    /// Stable lowercase token used in cache row keys, coverage ids, and SQL
    /// binds. Changing these strings re-segregates existing cache rows.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Adjustment::Raw => "raw",
            Adjustment::Split => "split",
            Adjustment::Dividend => "dividend",
            Adjustment::SpinOff => "spinoff",
            Adjustment::All => "all",
        }
    }

    /// Inverse of [`as_str`](Self::as_str): parse a stable token back into an
    /// [`Adjustment`]. Returns `None` for an unrecognized token.
    ///
    /// Used to reconstruct a cache catalog entry's adjustment mode from the
    /// `coverage` record id, where the mode is stored as its `as_str` token.
    #[must_use]
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "raw" => Some(Adjustment::Raw),
            "split" => Some(Adjustment::Split),
            "dividend" => Some(Adjustment::Dividend),
            "spinoff" => Some(Adjustment::SpinOff),
            "all" => Some(Adjustment::All),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Adjustment;

    #[test]
    fn default_is_all() {
        assert_eq!(Adjustment::default(), Adjustment::All);
    }

    #[test]
    fn tokens_are_distinct_and_stable() {
        let all = [
            Adjustment::Raw,
            Adjustment::Split,
            Adjustment::Dividend,
            Adjustment::SpinOff,
            Adjustment::All,
        ];
        let mut tokens: Vec<&str> = all.iter().map(|a| a.as_str()).collect();
        tokens.sort_unstable();
        tokens.dedup();
        assert_eq!(tokens.len(), all.len(), "tokens must be unique");
        assert_eq!(Adjustment::All.as_str(), "all");
        assert_eq!(Adjustment::Raw.as_str(), "raw");
    }

    #[test]
    fn from_token_inverts_as_str() {
        for a in [
            Adjustment::Raw,
            Adjustment::Split,
            Adjustment::Dividend,
            Adjustment::SpinOff,
            Adjustment::All,
        ] {
            assert_eq!(Adjustment::from_token(a.as_str()), Some(a));
        }
        assert_eq!(Adjustment::from_token("nonsense"), None);
        assert_eq!(Adjustment::from_token(""), None);
    }
}

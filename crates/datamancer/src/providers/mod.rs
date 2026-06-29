//! Built-in provider implementations.
//!
//! Each provider is feature-gated so the dependency footprint follows what
//! the consumer actually wires up.

#[cfg(feature = "provider-alpaca")]
pub mod alpaca;
#[cfg(feature = "provider-alpaca")]
pub mod alpaca_crypto;

#[cfg(feature = "provider-alpaca")]
pub use alpaca::{AlpacaProvider, AlpacaProviderConfig, AlpacaStreamFeed};
#[cfg(feature = "provider-alpaca")]
pub use alpaca_crypto::{AlpacaCryptoProvider, AlpacaCryptoProviderConfig, AlpacaCryptoVenue};
/// Re-exported provider account selector (`Paper`/`Live`); the actual
/// credentials are resolved from the environment by `oxidized_alpaca` keyed on
/// this. Surfaced so embedders (e.g. `datamancerd`) can build provider configs
/// without depending on `oxidized_alpaca` directly.
#[cfg(feature = "provider-alpaca")]
pub use oxidized_alpaca::AccountType;

/// Saturating `f64 → u64` for provider wire-format sizes / volumes.
///
/// Alpaca's WebSocket payloads carry trade and quote sizes as `f64`; our
/// canonical events use `u64`. This is the boundary conversion: NaN, ±∞, and
/// negative inputs collapse to `0`; values at or above `u64::MAX` saturate to
/// `u64::MAX`; everything else truncates toward zero. Lossy by contract — the
/// entire point is to make the out-of-range behavior explicit (Rust's
/// `as`-cast for `f64 → u64` saturates rather than producing UB, but the
/// silent saturation is itself a hazard for an event-pipeline boundary).
#[cfg(feature = "provider-alpaca")]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "provider boundary: Alpaca wire format is f64; saturating conversion is intentionally lossy"
)]
pub(crate) fn f64_to_u64_saturating(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    if value >= u64::MAX as f64 {
        return u64::MAX;
    }
    value as u64
}

//! Built-in provider implementations.
//!
//! Each provider is feature-gated so the dependency footprint follows what
//! the consumer actually wires up.

#[cfg(feature = "provider-alpaca")]
pub mod alpaca;
#[cfg(feature = "provider-alpaca")]
pub mod alpaca_crypto;
#[cfg(feature = "provider-alpaca")]
pub mod credentials;
#[cfg(feature = "provider-alpaca")]
pub mod runtime;

#[cfg(feature = "provider-alpaca")]
pub use alpaca::{AlpacaProvider, AlpacaProviderConfig, AlpacaStreamFeed};
#[cfg(feature = "provider-alpaca")]
pub use alpaca_crypto::{AlpacaCryptoProvider, AlpacaCryptoProviderConfig, AlpacaCryptoVenue};
#[cfg(feature = "provider-alpaca")]
pub use credentials::{AlpacaCredentials, CredentialsSource};
/// Re-exported provider account selector (`Paper`/`Live`); the actual
/// credentials are resolved from the environment by `oxidized_alpaca` keyed on
/// this. Surfaced so embedders (e.g. `datamancerd`) can build provider configs
/// without depending on `oxidized_alpaca` directly.
#[cfg(feature = "provider-alpaca")]
pub use oxidized_alpaca::AccountType;
#[cfg(feature = "provider-alpaca")]
pub use runtime::SettingsSource;

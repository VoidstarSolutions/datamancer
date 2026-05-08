//! Built-in provider implementations.
//!
//! Each provider is feature-gated so the dependency footprint follows what
//! the consumer actually wires up.

#[cfg(feature = "provider-alpaca")]
pub mod alpaca;

#[cfg(feature = "provider-alpaca")]
pub use alpaca::{AlpacaProvider, AlpacaProviderConfig, AlpacaStreamFeed};

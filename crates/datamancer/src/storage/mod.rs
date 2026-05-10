//! Built-in storage / cache implementations.
//!
//! Each backend is feature-gated so the dependency footprint follows what
//! the consumer actually wires up.

#[cfg(feature = "storage-surreal")]
pub mod surreal;

#[cfg(feature = "storage-surreal")]
pub use surreal::{SurrealCache, SurrealCacheConfig};

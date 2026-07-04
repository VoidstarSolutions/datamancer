//! Built-in storage / cache implementations.
//!
//! Each backend is feature-gated so the dependency footprint follows what
//! the consumer actually wires up.

#[cfg(feature = "storage-surreal")]
pub mod surreal;

#[cfg(feature = "storage-surreal")]
pub use surreal::{SurrealCache, SurrealCacheConfig};

#[cfg(feature = "storage-surreal")]
pub mod surreal_tap_log;

#[cfg(feature = "storage-surreal")]
pub use surreal_tap_log::{SurrealTapLog, SurrealTapLogConfig};

#[cfg(feature = "storage-turso")]
pub(crate) mod turso_common;

#[cfg(feature = "storage-turso")]
pub(crate) mod coverage;

#[cfg(feature = "storage-turso")]
pub mod turso;

#[cfg(feature = "storage-turso")]
pub use turso::{TursoCache, TursoCacheConfig};

#[cfg(feature = "storage-turso")]
pub mod turso_tap_log;

#[cfg(feature = "storage-turso")]
pub use turso_tap_log::{TursoTapLog, TursoTapLogConfig};

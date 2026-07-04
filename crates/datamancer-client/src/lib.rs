//! Consumer-side surface for datamancerd: the control **vocabulary** shared
//! by every transport (subscription specs, stable error codes, request/reply
//! types) and, behind features, concrete clients (`ws`, `iceoryx2`)
//! implementing one generic [`Client`] trait.
//!
//! The vocabulary is the operator-facing contract extracted from the daemon:
//! the JSON shapes and stable code strings here must not change without a
//! breaking-change review — they are regression-guarded by tests.
#![forbid(unsafe_code)]

mod client;
mod error;

pub mod codes;
#[cfg(feature = "iceoryx2")]
pub mod iceoryx2;
pub mod paths;
pub mod protocol;
pub mod spec;
#[cfg(feature = "ws")]
pub mod ws;

pub use client::Client;
pub use error::ClientError;
pub use paths::default_control_socket;

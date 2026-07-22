//! Consumer-side surface for datamancerd: the control **vocabulary** shared
//! by every transport (subscription specs, stable error codes, request/reply
//! types) and, behind features, concrete clients (`ws`, `iceoryx2`)
//! implementing one generic [`Client`] trait.
//!
//! The vocabulary is the operator-facing contract extracted from the daemon:
//! the JSON shapes and stable code strings here must not change without a
//! breaking-change review — they are regression-guarded by tests.
//!
//! `#![forbid(unsafe_code)]` on every platform: the Windows named-pipe
//! client's owner-SID + integrity checks (review B1) delegate all Win32 FFI
//! to the shared, audited `datamancer-winsec` crate.
#![forbid(unsafe_code)]

mod client;
mod error;

#[cfg(feature = "app")]
pub mod app;
pub mod codes;
#[cfg(feature = "iceoryx2")]
pub mod iceoryx2;
pub mod paths;
pub mod protocol;
pub mod spec;
// Windows named-pipe server-identity check, shared by the iceoryx2 control
// connection and the app-facade ping (both named-pipe clients).
#[cfg(all(windows, feature = "iceoryx2"))]
mod win_pipe;
// Standalone named-pipe control client for the Windows hybrid admin plane
// (Phase 4): admin ops over the pipe, data over WS. `pub` so `PipeControlError`
// (the admin-plane transport error surfaced by the Windows `AppHandle`) is a
// publicly reachable type; `PipeControlClient` itself stays `pub(crate)`.
#[cfg(all(windows, feature = "iceoryx2"))]
pub mod pipe_control;
#[cfg(feature = "ws")]
pub mod ws;

pub use client::Client;
pub use error::ClientError;
pub use paths::default_control_socket;

/// This crate's version — the client side of the daemon's ping version gate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

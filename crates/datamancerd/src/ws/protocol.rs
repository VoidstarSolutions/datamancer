//! The WebSocket control protocol: JSON frames over the one WS connection.
//!
//! The wire vocabulary (`WsRequest`/`WsReply`) lives in `datamancer-client`
//! alongside the UDS vocabulary it shares (`SubscriptionSpec`, the stable
//! `codes` table); this module re-exports it under its historical path plus
//! the daemon-side glue that needs the orchestrator's `datamancer` crate.

pub use datamancer_client::protocol::ws::{WsReply, WsRequest};

use crate::control::error_code;

/// An error reply derived from a library error (stable code + display),
/// echoing the request `id`.
#[must_use]
pub fn ws_reply_from_library_error(id: u64, err: &datamancer::Error) -> WsReply {
    WsReply::error(id, error_code(err), err.to_string())
}

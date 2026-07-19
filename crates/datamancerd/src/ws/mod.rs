//! The remote WebSocket client surface (single bidirectional connection = one
//! client). Owns the listener, per-connection bridge, and the WS control
//! protocol. The event wire format + sink + writer live in the
//! `datamancer-transport-ws` crate; this module owns the part that touches the
//! orchestrator (`ClientSession`).

mod conn;
mod listener;
mod protocol;

pub use listener::serve;
pub use protocol::{WsHealthPush, WsReply, WsRequest};

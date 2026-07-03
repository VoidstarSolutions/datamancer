//! The remote WebSocket client surface (single bidirectional connection = one
//! client). Owns the listener, per-connection bridge, and the WS control
//! protocol. The event wire format + sink + writer live in the
//! `datamancer-transport-ws` crate; this module owns the part that touches the
//! orchestrator (`ClientSession`).

mod protocol;

// Not yet consumed outside tests; the listener/bridge task (Task 6) wires
// these into `ClientSession`.
#[allow(unused_imports)]
pub use protocol::{WsReply, WsRequest};

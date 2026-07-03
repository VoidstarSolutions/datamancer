//! WebSocket client transport for datamancer.
//!
//! One bidirectional WebSocket connection is one client: inbound JSON control
//! frames drive the client's `ClientSession`; the client's multiplexed
//! `EventStream` is serialized outbound as [`wire::EventFrame`]s. The instrument
//! is carried inline on every event frame (no interning). This crate owns the
//! wire format, the channel-backed [`WsDataSink`], and the single-writer socket
//! task; `datamancerd` owns the listener and the per-connection glue that
//! touches the orchestrator.
#![forbid(unsafe_code)]

mod error;
mod sink;
mod wire;
mod writer;

pub use error::{Result, WsTransportError};
pub use wire::{EventFrame, from_wire, to_wire};

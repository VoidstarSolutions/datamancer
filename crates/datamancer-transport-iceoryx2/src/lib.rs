//! iceoryx2 zero-copy transport for datamancer.
//!
//! Two planes over one logical client connection:
//!
//! - **Data plane** — one iceoryx2 pub-sub service per client carrying that
//!   client's multiplexed substreams as a flat `#[repr(C)]` `Copy`
//!   [`DataPayload`] that holds a compact [`SymbolId`] instead of the
//!   heap-backed `Instrument`. A [`SymbolTable`] interns the mapping and a
//!   low-rate announcement service publishes [`SymbolAnnouncement`]s so
//!   subscribers resolve `SymbolId -> Instrument`.
//! - **Diagnostics plane** — a separate service carrying the serialized
//!   Phase-3 `SystemSnapshot` (not the zero-copy hot path).
//!
//! `SymbolId`/interning are sink-local and **not** a global-identity concept.
//! The forbid-unsafe gate (EXT-1) holds: every payload uses the
//! `ZeroCopySend` derive plus fixed-size containers only, so this crate keeps
//! `#![forbid(unsafe_code)]` like the two core crates.
#![forbid(unsafe_code)]

mod payload;
mod symbol_table;

pub use payload::{ControlTag, DataPayload, FromPodError, PayloadKind, from_pod, to_pod};
pub use symbol_table::{
    InstrumentTooLong, SYMBOL_STRING_CAPACITY, SymbolAnnouncement, SymbolDecodeError, SymbolId,
    SymbolResolver, SymbolTable,
};

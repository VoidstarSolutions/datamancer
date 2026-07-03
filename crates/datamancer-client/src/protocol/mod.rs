//! Request/reply framings per control surface. One vocabulary
//! ([`crate::spec`], [`crate::codes`]), two framings: newline-JSON over UDS
//! and correlated JSON frames over WS.

pub mod uds;
pub mod ws;

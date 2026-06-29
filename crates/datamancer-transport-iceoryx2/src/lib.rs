//! iceoryx2 zero-copy transport for datamancer.
//!
//! EXT-1 spike: confirm a `#[repr(C)] Copy` POD payload deriving
//! [`iceoryx2::prelude::ZeroCopySend`] compiles under `#![forbid(unsafe_code)]`
//! with no caller-side `unsafe`.
#![forbid(unsafe_code)]

use iceoryx2::prelude::ZeroCopySend;

/// EXT-1 spike payload: a flat `#[repr(C)]` `Copy` POD deriving `ZeroCopySend`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ZeroCopySend)]
#[repr(C)]
pub struct SpikePayload {
    pub tag: u8,
    pub seq: u64,
    pub source_ts: i64,
}

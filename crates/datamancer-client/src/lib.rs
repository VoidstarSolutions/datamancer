//! Consumer-side surface for datamancerd: the control **vocabulary** shared
//! by every transport (subscription specs, stable error codes, request/reply
//! types) and, behind features, concrete clients (`ws`, `iceoryx2`)
//! implementing one generic [`Client`] trait (added in a later task).
//!
//! The vocabulary is the operator-facing contract extracted from the daemon:
//! the JSON shapes and stable code strings here must not change without a
//! breaking-change review — they are regression-guarded by tests.
#![forbid(unsafe_code)]

pub mod codes;
pub mod protocol;
pub mod spec;

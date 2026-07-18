//! `datamancer-winsec` — Windows security primitives shared by the datamancerd
//! control channel's two ends (the client `win_pipe` and the daemon
//! `win_control`).
//!
//! Two surfaces:
//! - a **pure, cross-platform** integrity-level classifier (`classify`,
//!   `integrity_ok`, `IntegrityClass`), unit-tested on every platform because
//!   CI cannot elevate a process; and
//! - **Windows-only Win32 readers** for token/handle identity and integrity.
//!
//! EXT-1: this crate is the workspace's single audited `unsafe` surface for
//! these primitives. It is `#![forbid(unsafe_code)]` off Windows and
//! `#![deny(unsafe_code)]` on Windows with one scoped `#[allow(unsafe_code)]`
//! confined to the `ffi` module. `datamancer-core` and `datamancer-credentials`
//! never depend on it and stay `forbid`.
#![cfg_attr(not(windows), forbid(unsafe_code))]
#![cfg_attr(windows, deny(unsafe_code))]

mod integrity;
pub use integrity::{IntegrityClass, classify, integrity_ok};

#[cfg(windows)]
mod ffi;
#[cfg(windows)]
pub use ffi::{
    client_process_integrity, current_process_integrity, current_process_token_sid, owner_sid_of,
};

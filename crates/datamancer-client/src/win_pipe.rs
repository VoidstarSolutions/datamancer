//! Windows named-pipe client helpers: connect to the daemon's control pipe and,
//! before trusting it, (1) verify **this process** is at Medium integrity and
//! (2) verify the connected pipe's **owner SID** equals this process's token SID
//! (review B1).
//!
//! All Win32 FFI lives in `datamancer-winsec`; this module is safe Rust (the
//! crate is `#![forbid(unsafe_code)]`).
//!
//! # Owner check
//!
//! The daemon restricts its control pipe with an owner-only DACL, so a
//! *different* user cannot open it. The owner check is defense in depth: before
//! sending anything, the client requires the connected pipe's owner SID to equal
//! its own token SID, so a bug that weakened the server DACL cannot cause a
//! client to stream credentials to a foreign-owner endpoint. Any mismatch or
//! failure is fail-closed.
//!
//! # Integrity self-check
//!
//! Mandatory Integrity Control gates the connect independently of the DACL: a
//! below-Medium client is refused by the OS at connect (opaque access-denied).
//! Checking our own integrity first turns that into a clear message, and also
//! refuses an elevated client (which the daemon would reject anyway).
//! `DATAMANCER_ALLOW_ANY_INTEGRITY` overrides this local check.
//!
//! **Asymmetry.** This override relaxes only *this* client's own pre-connect
//! self-check — it has no effect on what the daemon accepts. The daemon is
//! always the authority: it independently re-reads every connecting client's
//! integrity off the raw pipe handle and rejects a non-Medium client in-band
//! (`integrity_rejected`) unless the *daemon* has separately set
//! `[server].allow_any_integrity = true` (see `datamancerd::win_control`).
//! Setting `DATAMANCER_ALLOW_ANY_INTEGRITY=1` on an elevated client without
//! also relaxing the daemon does not let that client through.

use std::io;
use std::os::windows::io::AsRawHandle as _;
use std::path::Path;
use std::time::Duration;

use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};

/// Bounded `ERROR_PIPE_BUSY` retry: all instances busy is transient (the daemon
/// pre-creates the next instance on each accept), so back off briefly and retry.
/// ~20 × 50 ms ≈ 1 s ceiling.
const CONNECT_ATTEMPTS: u32 = 20;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(50);
/// Win32 `ERROR_PIPE_BUSY` (winerror.h). Kept as a literal so this crate needs
/// no `windows-sys` dependency.
const ERROR_PIPE_BUSY: i32 = 231;

/// Operator override: `DATAMANCER_ALLOW_ANY_INTEGRITY` set to a truthy value
/// relaxes the client's own integrity self-check (the daemon remains the
/// authority for what it accepts).
fn integrity_override() -> bool {
    match std::env::var("DATAMANCER_ALLOW_ANY_INTEGRITY") {
        Ok(v) => !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false"),
        Err(_) => false,
    }
}

/// Fail-closed identity gate: the connected pipe's owner SID must equal this
/// process's own token SID (review B1).
fn verify_owner_is_self(handle: std::os::windows::io::RawHandle) -> io::Result<()> {
    let expected = datamancer_winsec::current_process_token_sid()?;
    let actual = datamancer_winsec::owner_sid_of(handle)?;
    if expected == actual {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "control pipe owner SID ({actual}) does not match this user \
                 ({expected}); refusing to use it"
            ),
        ))
    }
}

/// Connect to the daemon's control pipe and verify integrity + owner before
/// returning. Any failure is a hard connect error — the caller must send
/// nothing.
pub(crate) async fn connect_verified(path: &Path) -> io::Result<NamedPipeClient> {
    // Integrity self-check first: a clear message beats an opaque access-denied.
    let rid = datamancer_winsec::current_process_integrity()?;
    if !datamancer_winsec::integrity_ok(rid, integrity_override()) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "this process is running at {} integrity; the datamancer control \
                 pipe requires Medium integrity to reach a same-user daemon. \
                 Re-launch without elevation (or below-Medium sandboxing), or set \
                 DATAMANCER_ALLOW_ANY_INTEGRITY=1 to override.",
                datamancer_winsec::classify(rid).describe()
            ),
        ));
    }

    let mut attempts = 0u32;
    let client = loop {
        match ClientOptions::new().open(path) {
            Ok(client) => break client,
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) && attempts < CONNECT_ATTEMPTS => {
                attempts += 1;
                tokio::time::sleep(CONNECT_RETRY_DELAY).await;
            }
            Err(e) => return Err(e),
        }
    };
    verify_owner_is_self(client.as_raw_handle())?;
    Ok(client)
}

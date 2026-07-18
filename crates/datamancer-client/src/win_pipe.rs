//! Windows named-pipe client helpers: connect to the daemon's control pipe
//! and **verify its owner** before trusting it (review B1).
//!
//! # Why the owner check
//!
//! The daemon restricts its control pipe with an owner-only DACL, so a
//! *different* user cannot open it at all (the OS denies the connect). But a
//! same-user process could squat the well-known name, and — defense in depth —
//! a bug that weakened the server DACL must never cause a client to stream
//! credentials to a foreign endpoint. So before sending anything, the client
//! reads the connected pipe's owner SID and requires it to equal this
//! process's own token SID. Any mismatch or failure is fail-closed: the
//! connection is rejected.
//!
//! # EXT-1 unsafe policy
//!
//! The crate is `#![deny(unsafe_code)]` on Windows (it stays `forbid` on
//! Unix/macOS). This module is the crate's *single* `#[allow(unsafe_code)]`
//! exception; all Win32 FFI is confined here, each `unsafe` block carries a
//! `// SAFETY:` proof, and the public API is entirely safe Rust. The
//! token-SID helper is intentionally duplicated from `datamancerd`'s
//! `win_control` rather than shared through a third crate — keeping each
//! crate's Win32 surface to one audited module (the `datamancer-credentials`
//! crate stays `forbid(unsafe_code)`).
#![allow(unsafe_code)]

use std::io;
use std::os::windows::io::{AsRawHandle as _, RawHandle};
use std::path::Path;
use std::time::Duration;

use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use windows_sys::Win32::Foundation::{CloseHandle, ERROR_PIPE_BUSY, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, GetSecurityInfo, SE_KERNEL_OBJECT,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, TOKEN_QUERY,
    TOKEN_USER, TokenUser,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// Bounded `ERROR_PIPE_BUSY` retry: all instances busy is transient (the daemon
/// pre-creates the next instance on each accept), so back off briefly and
/// retry rather than fail (review N6). ~20 × 50 ms ≈ 1 s ceiling.
const CONNECT_ATTEMPTS: u32 = 20;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(50);

fn last_os_error(context: &str) -> io::Error {
    let e = io::Error::last_os_error();
    io::Error::new(e.kind(), format!("{context}: {e}"))
}

/// Read a NUL-terminated wide string allocated by a `*W` API into a `String`.
fn u16_ptr_to_string(p: *const u16) -> String {
    // SAFETY: `p` is a NUL-terminated wide string produced by a Win32 `*W`
    // allocator (`ConvertSidToStringSidW`); we walk to the terminator to
    // measure its length, reading only within the allocation.
    let len = unsafe {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        n
    };
    // SAFETY: `p..p+len` is exactly the wide string just measured (excludes the
    // terminator), valid for reads for `len` `u16`s.
    let slice = unsafe { std::slice::from_raw_parts(p, len) };
    String::from_utf16_lossy(slice)
}

/// This process token's **user SID** as an `S-1-…` string (review S1: the
/// authoritative running-user identity, not a spoofable name lookup).
fn own_token_sid() -> io::Result<String> {
    let mut token = std::ptr::null_mut();
    // SAFETY: `GetCurrentProcess` returns a pseudo-handle; `OpenProcessToken`
    // writes a real, owned token handle into `token` on success (return != 0),
    // closed below.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(last_os_error("OpenProcessToken"));
    }
    let mut len = 0u32;
    // SAFETY: documented size-probe form — null buffer + 0 length makes
    // `GetTokenInformation` report the needed size in `len` and fail; the
    // (unwritten) buffer is not read.
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &raw mut len) };
    let mut buf = vec![0u8; len as usize];
    // SAFETY: `buf` is exactly `len` bytes — the size the probe reported — into
    // which `TokenUser` writes a `TOKEN_USER` (SID trailing).
    let ok = unsafe {
        GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &raw mut len)
    };
    // SAFETY: `token` is the handle we opened and still own; close it.
    unsafe { CloseHandle(token) };
    if ok == 0 {
        return Err(last_os_error("GetTokenInformation"));
    }
    // SAFETY: on success `buf` holds a `TOKEN_USER`. Read it out *unaligned* —
    // a `Vec<u8>` buffer is not guaranteed to meet `TOKEN_USER`'s alignment, so
    // forming a `&TOKEN_USER` reference would be UB. The copied `User.Sid`
    // still points into `buf`, which outlives the `ConvertSidToStringSidW`
    // call below.
    let token_user = unsafe { buf.as_ptr().cast::<TOKEN_USER>().read_unaligned() };
    let mut sid_str = std::ptr::null_mut();
    // SAFETY: `token_user.User.Sid` is a valid SID within `buf`;
    // `ConvertSidToStringSidW` allocates a wide string into `sid_str` on
    // success (return != 0), freed below.
    if unsafe { ConvertSidToStringSidW(token_user.User.Sid, &raw mut sid_str) } == 0 {
        return Err(last_os_error("ConvertSidToStringSid"));
    }
    let s = u16_ptr_to_string(sid_str);
    // SAFETY: `sid_str` was allocated by `ConvertSidToStringSidW`; free it.
    unsafe { LocalFree(sid_str.cast()) };
    Ok(s)
}

/// The **owner SID** of a connected pipe handle, as an `S-1-…` string.
fn pipe_owner_sid(handle: RawHandle) -> io::Result<String> {
    let mut owner: PSID = std::ptr::null_mut();
    let mut psd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `handle` is a live kernel object (a connected named pipe).
    // `GetSecurityInfo` writes the owner SID (pointing within the SD it
    // allocates into `psd`) and returns `ERROR_SUCCESS` (0) on success; `psd`
    // is freed below. `owner` aliases into `psd`, so it must be read before the
    // free.
    let rc = unsafe {
        GetSecurityInfo(
            handle,
            SE_KERNEL_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &raw mut owner,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &raw mut psd,
        )
    };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc.cast_signed()));
    }
    let mut sid_str = std::ptr::null_mut();
    // SAFETY: `owner` is a valid SID within the still-live `psd`;
    // `ConvertSidToStringSidW` allocates a wide string into `sid_str` on
    // success (return != 0).
    let ok = unsafe { ConvertSidToStringSidW(owner, &raw mut sid_str) };
    let converted = if ok == 0 {
        Err(last_os_error("ConvertSidToStringSid"))
    } else {
        Ok(u16_ptr_to_string(sid_str))
    };
    // SAFETY: free the SD allocated by `GetSecurityInfo` and, if allocated, the
    // string from `ConvertSidToStringSidW`. `LocalFree(NULL)` is a no-op.
    unsafe {
        LocalFree(sid_str.cast());
        LocalFree(psd);
    }
    converted
}

/// Fail-closed identity gate: the connected pipe's owner SID must equal this
/// process's own token SID (review B1).
fn verify_owner_is_self(handle: RawHandle) -> io::Result<()> {
    let expected = own_token_sid()?;
    let actual = pipe_owner_sid(handle)?;
    if expected == actual {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "control pipe owner SID ({actual}) does not match this user ({expected}); \
                 refusing to use it"
            ),
        ))
    }
}

/// Connect to the daemon's control pipe and verify its owner before returning.
///
/// Retries transient `ERROR_PIPE_BUSY` (all instances momentarily busy), then
/// runs the B1 owner-identity check. Any owner mismatch or Win32 failure is
/// returned as an `io::Error` — the caller must treat it as a hard connect
/// failure and send nothing.
pub(crate) async fn connect_verified(path: &Path) -> io::Result<NamedPipeClient> {
    let mut attempts = 0u32;
    let client = loop {
        match ClientOptions::new().open(path) {
            Ok(client) => break client,
            Err(e)
                if e.raw_os_error() == Some(ERROR_PIPE_BUSY.cast_signed())
                    && attempts < CONNECT_ATTEMPTS =>
            {
                attempts += 1;
                tokio::time::sleep(CONNECT_RETRY_DELAY).await;
            }
            Err(e) => return Err(e),
        }
    };
    verify_owner_is_self(client.as_raw_handle())?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_token_sid_is_a_sid_string() {
        let sid = own_token_sid().expect("token SID");
        assert!(sid.starts_with("S-1-"), "not a SID: {sid}");
    }
}

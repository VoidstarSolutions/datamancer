//! Windows Win32 FFI for reading token/handle identity and integrity. This is
//! the workspace's single audited `unsafe` surface for these primitives; every
//! `unsafe` block carries a `// SAFETY:` proof and the public API is safe Rust.
#![allow(unsafe_code)]

use std::io;
use std::os::windows::io::RawHandle;

use windows_sys::Win32::Foundation::{CloseHandle, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, GetSecurityInfo, SE_KERNEL_OBJECT,
};
use windows_sys::Win32::Security::{
    GetSidSubAuthority, GetSidSubAuthorityCount, GetTokenInformation, OWNER_SECURITY_INFORMATION,
    PSECURITY_DESCRIPTOR, PSID, TOKEN_MANDATORY_LABEL, TOKEN_QUERY, TOKEN_USER,
    TokenIntegrityLevel, TokenUser,
};
use windows_sys::Win32::System::Pipes::GetNamedPipeClientProcessId;
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
};

fn last_os_error(context: &str) -> io::Error {
    let e = io::Error::last_os_error();
    io::Error::new(e.kind(), format!("{context}: {e}"))
}

/// Read a NUL-terminated wide string allocated by a `*W` API into a `String`.
fn u16_ptr_to_string(p: *const u16) -> String {
    // SAFETY: `p` is a NUL-terminated wide string produced by a Win32 `*W`
    // allocator; we walk to the terminator to measure its length, reading only
    // within the allocation.
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

/// This process token's **user SID** as an `S-1-…` string. The authoritative
/// running-user identity (not a spoofable name lookup).
///
/// # Errors
///
/// Returns the underlying OS error if opening the process token, querying its
/// `TokenUser`, or converting the SID to a string fails.
pub fn current_process_token_sid() -> io::Result<String> {
    let mut token = std::ptr::null_mut();
    // SAFETY: GetCurrentProcess returns a pseudo-handle; OpenProcessToken writes
    // a real, owned token handle into `token` on success (return != 0), closed
    // below.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(last_os_error("OpenProcessToken"));
    }
    let mut len = 0u32;
    // SAFETY: documented size-probe form — null buffer + 0 length makes
    // GetTokenInformation report the needed size in `len` and fail.
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &raw mut len) };
    let mut buf = vec![0u8; len as usize];
    // SAFETY: `buf` is exactly `len` bytes; TokenUser writes a TOKEN_USER (SID
    // trailing) into it.
    let ok = unsafe {
        GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &raw mut len)
    };
    // Capture the failure error *before* CloseHandle, which can overwrite the
    // thread-local last-error and mask GetTokenInformation's.
    let query_err = (ok == 0).then(|| last_os_error("GetTokenInformation"));
    // SAFETY: `token` is the handle we opened and own; close it regardless.
    unsafe { CloseHandle(token) };
    if let Some(err) = query_err {
        return Err(err);
    }
    // SAFETY: on success `buf` holds a TOKEN_USER. Read it out *unaligned* — a
    // Vec<u8> is not guaranteed to meet the struct alignment. The copied
    // User.Sid still points into `buf`, which outlives the conversion below.
    let token_user = unsafe { buf.as_ptr().cast::<TOKEN_USER>().read_unaligned() };
    let mut sid_str = std::ptr::null_mut();
    // SAFETY: `token_user.User.Sid` is a valid SID within `buf`;
    // ConvertSidToStringSidW allocates a wide string into `sid_str` on success.
    if unsafe { ConvertSidToStringSidW(token_user.User.Sid, &raw mut sid_str) } == 0 {
        return Err(last_os_error("ConvertSidToStringSid"));
    }
    let s = u16_ptr_to_string(sid_str);
    // SAFETY: `sid_str` was allocated by ConvertSidToStringSidW; free it.
    unsafe { LocalFree(sid_str.cast()) };
    Ok(s)
}

/// Read the integrity-level RID (last sub-authority of the mandatory-label SID)
/// from an already-opened token. The caller owns and closes `token`.
fn token_integrity_rid(token: *mut core::ffi::c_void) -> io::Result<u32> {
    let mut len = 0u32;
    // SAFETY: documented size-probe form — null buffer + 0 length makes
    // GetTokenInformation report the needed size in `len` and fail.
    unsafe {
        GetTokenInformation(
            token,
            TokenIntegrityLevel,
            std::ptr::null_mut(),
            0,
            &raw mut len,
        )
    };
    let mut buf = vec![0u8; len as usize];
    // SAFETY: `buf` is exactly `len` bytes; TokenIntegrityLevel writes a
    // TOKEN_MANDATORY_LABEL (SID trailing) into it.
    let ok = unsafe {
        GetTokenInformation(
            token,
            TokenIntegrityLevel,
            buf.as_mut_ptr().cast(),
            len,
            &raw mut len,
        )
    };
    if ok == 0 {
        return Err(last_os_error("GetTokenInformation(integrity)"));
    }
    // SAFETY: on success `buf` holds a TOKEN_MANDATORY_LABEL. Read it out
    // *unaligned* — a Vec<u8> is not guaranteed to meet the struct alignment.
    // Label.Sid points into `buf`, which outlives the sub-authority reads.
    let label = unsafe {
        buf.as_ptr()
            .cast::<TOKEN_MANDATORY_LABEL>()
            .read_unaligned()
    };
    let sid = label.Label.Sid;
    // SAFETY: `sid` is a valid label SID within `buf`; `GetSidSubAuthorityCount`
    // returns a pointer to its sub-authority count.
    let count = unsafe { *GetSidSubAuthorityCount(sid) };
    // A mandatory-label SID always carries its integrity RID as the last
    // sub-authority, so a count of 0 is malformed — refuse rather than read out
    // of bounds.
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "integrity-label SID has no sub-authorities",
        ));
    }
    // SAFETY: `count >= 1`, so `count - 1` is a valid sub-authority index; the
    // integrity RID is that last sub-authority.
    let rid = unsafe { *GetSidSubAuthority(sid, u32::from(count - 1)) };
    Ok(rid)
}

/// This process's integrity-level RID.
///
/// # Errors
///
/// Returns the underlying OS error if opening the process token or reading its
/// integrity-level label fails.
pub fn current_process_integrity() -> io::Result<u32> {
    let mut token = std::ptr::null_mut();
    // SAFETY: GetCurrentProcess returns a pseudo-handle; OpenProcessToken writes
    // an owned token handle into `token` on success (return != 0).
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(last_os_error("OpenProcessToken(integrity)"));
    }
    let rid = token_integrity_rid(token);
    // SAFETY: `token` is the handle we opened and own; close it regardless.
    unsafe { CloseHandle(token) };
    rid
}

/// The **owner SID** of a connected kernel handle (e.g. a named pipe), as an
/// `S-1-…` string.
///
/// # Errors
///
/// Returns the underlying OS error if querying the handle's security info or
/// converting the owner SID to a string fails.
// `handle` is an opaque Windows kernel-object `HANDLE`, not a dereferenceable
// Rust pointer: it is passed by value to `GetSecurityInfo`, which resolves it
// kernel-side — nothing here reads through it. A safe public API is the crate's
// contract, so the function stays safe.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn owner_sid_of(handle: RawHandle) -> io::Result<String> {
    let mut owner: PSID = std::ptr::null_mut();
    let mut psd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `handle` is a live kernel object. GetSecurityInfo writes the owner
    // SID (pointing within the SD it allocates into `psd`) and returns 0 on
    // success; `psd` is freed below. `owner` aliases into `psd`, so it is read
    // before the free.
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
    // ConvertSidToStringSidW allocates a wide string into `sid_str` on success.
    let ok = unsafe { ConvertSidToStringSidW(owner, &raw mut sid_str) };
    let converted = if ok == 0 {
        Err(last_os_error("ConvertSidToStringSid"))
    } else {
        Ok(u16_ptr_to_string(sid_str))
    };
    // SAFETY: free the SD from GetSecurityInfo and, if allocated, the string
    // from ConvertSidToStringSidW. LocalFree(NULL) is a no-op.
    unsafe {
        LocalFree(sid_str.cast());
        LocalFree(psd);
    }
    converted
}

/// The integrity-level RID of the process on the other end of a connected pipe.
/// Resolves the client PID from the pipe, opens a query-only handle, and reads
/// its token integrity. No impersonation (no thread-local identity to leak).
/// Small race: if the client exits between connect and `OpenProcess`, this
/// fails (fail-closed reject upstream).
///
/// # Errors
///
/// Returns the underlying OS error if the client PID cannot be read from the
/// pipe, the client process cannot be opened, or its integrity label cannot be
/// read.
// `handle` is an opaque Windows kernel-object `HANDLE`, not a dereferenceable
// Rust pointer: it is passed by value to `GetNamedPipeClientProcessId`, which
// resolves it kernel-side. A safe public API is the crate's contract.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn client_process_integrity(handle: RawHandle) -> io::Result<u32> {
    let mut pid = 0u32;
    // SAFETY: `handle` is a live connected named-pipe server endpoint;
    // GetNamedPipeClientProcessId writes the client PID on success (!= 0).
    if unsafe { GetNamedPipeClientProcessId(handle, &raw mut pid) } == 0 {
        return Err(last_os_error("GetNamedPipeClientProcessId"));
    }
    // SAFETY: opens a query-only handle to the client process; returns null on
    // failure. Closed below before returning.
    let proc = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if proc.is_null() {
        return Err(last_os_error("OpenProcess(client)"));
    }
    let mut token = std::ptr::null_mut();
    // SAFETY: `proc` is a valid process handle; OpenProcessToken writes an owned
    // token handle into `token` on success (return != 0).
    let opened = unsafe { OpenProcessToken(proc, TOKEN_QUERY, &raw mut token) };
    // Capture the failure error before CloseHandle can overwrite the last-error.
    let open_err = (opened == 0).then(|| last_os_error("OpenProcessToken(client)"));
    // SAFETY: done with the process handle regardless of the result.
    unsafe { CloseHandle(proc) };
    if let Some(err) = open_err {
        return Err(err);
    }
    let rid = token_integrity_rid(token);
    // SAFETY: `token` is the client token handle we own; close it.
    unsafe { CloseHandle(token) };
    rid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_sid_is_a_sid_string() {
        let sid = current_process_token_sid().expect("token SID");
        assert!(sid.starts_with("S-1-"), "not a SID: {sid}");
    }

    #[test]
    fn current_integrity_reads_medium_or_elevated() {
        // A normal user process is Medium; an elevated one (e.g. the GitHub
        // windows-latest runner) is High/System — never below Medium. Assert the
        // FFI read lands in one of those bands via the shared classifier rather
        // than a fixed level, so the test holds on both a developer machine and
        // an elevated CI runner.
        let rid = current_process_integrity().expect("integrity rid");
        let class = crate::classify(rid);
        assert!(
            matches!(
                class,
                crate::IntegrityClass::Medium | crate::IntegrityClass::Elevated
            ),
            "expected Medium or elevated integrity, got {rid:#x} ({})",
            class.describe()
        );
    }
}

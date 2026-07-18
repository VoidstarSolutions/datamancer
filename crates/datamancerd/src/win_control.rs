//! Windows named-pipe control transport with an **owner-only DACL** — the
//! Windows counterpart of the Unix UDS control socket (`server::bind_socket`).
//!
//! # Access-control model (why this differs from Unix, review S4)
//!
//! On Unix the control socket is world-openable, so every privileged op is
//! gated per-request on the peer's uid (`SO_PEERCRED`, see
//! [`crate::credentials::privileged_op_permitted`]). On Windows we instead
//! restrict the *pipe object itself* with a discretionary ACL that grants
//! access to exactly one principal: the daemon's own process-token user SID
//! (SDDL `O:<sid>D:P(A;;GA;;;<sid>)` — `P` = protected, so no inherited
//! `Everyone` ACE; no `SYSTEM` ACE either, review S3; `O:` stamps the owner as
//! the token *user* SID so an elevated daemon's owner is not the Administrators
//! group). The OS therefore enforces
//! same-user **at connect time**: a different user cannot open the pipe at
//! all. Every connection the daemon accepts is already the owner's, so the
//! per-op gate collapses to "always permitted" on Windows (the caller passes
//! `privileged = true` to `server::serve_connection`).
//!
//! Defense in depth: the client independently verifies the connected pipe's
//! owner SID equals its own token SID before sending anything privileged
//! (`datamancer_client`'s `win_pipe`, review B1), so a weakened server DACL
//! cannot silently leak credentials.
//!
//! **Integrity-level caveat.** The DACL keys on the user SID, not the
//! integrity level. Windows' mandatory "no-write-up" policy still applies: a
//! same-user client at a *lower* integrity level than an elevated daemon may
//! be denied write access to the (Medium-IL default) pipe. Run the daemon and
//! its clients at the same integrity level (both elevated or both not).
//!
//! # EXT-1 unsafe policy
//!
//! The crate is `#![deny(unsafe_code)]` on Windows (it stays `forbid` on
//! Unix/macOS). This module is the crate's *single* `#[allow(unsafe_code)]`
//! exception, and all Win32 FFI is confined here. Every `unsafe` block carries
//! a `// SAFETY:` proof. No `unsafe` leaks past this module's public API,
//! which is entirely safe Rust (`&str` in, `NamedPipeServer`/`String` out).
#![allow(unsafe_code)]

use std::io;

use tokio::net::windows::named_pipe::NamedPipeServer;
use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, PIPE_ACCESS_DUPLEX,
};
use windows_sys::Win32::System::Pipes::{
    CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// In/out buffer advice for the pipe (bytes). Control traffic is small
/// newline-JSON; this is a hint, not a hard cap.
const PIPE_BUFFER_BYTES: u32 = 4096;

/// Encode a Rust string as a NUL-terminated UTF-16 buffer for the `*W` APIs.
fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

fn last_os_error(context: &str) -> io::Error {
    let e = io::Error::last_os_error();
    io::Error::new(e.kind(), format!("{context}: {e}"))
}

/// Read a NUL-terminated wide string allocated by a `*W` API into a `String`.
fn u16_ptr_to_string(p: *const u16) -> String {
    // SAFETY: `p` is a NUL-terminated wide string produced by a Win32 `*W`
    // allocator (e.g. `ConvertSidToStringSidW`); we walk to the terminator to
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

/// The current process token's **user SID** as an `S-1-…` string.
///
/// This is the authoritative running-user identity (review S1): unlike a
/// `USERNAME`→SID name lookup it cannot be spoofed via the environment and is
/// unambiguous across domains.
fn process_token_sid() -> io::Result<String> {
    let mut token = std::ptr::null_mut();
    // SAFETY: `GetCurrentProcess` returns a pseudo-handle (no lifetime to
    // manage). `OpenProcessToken` writes a real, owned token handle into
    // `token` on success (return != 0), which we close below.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(last_os_error("OpenProcessToken"));
    }

    // Size query: null buffer + 0 length asks for the required byte count.
    let mut len = 0u32;
    // SAFETY: documented size-probe form — a null buffer with 0 length makes
    // `GetTokenInformation` write the needed size into `len` and fail; we do
    // not read the (unwritten) buffer.
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &raw mut len) };

    let mut buf = vec![0u8; len as usize];
    // SAFETY: `buf` is exactly `len` bytes — the size the probe just reported —
    // and `TokenUser` writes a `TOKEN_USER` (with its SID trailing) into it.
    let ok = unsafe {
        GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &raw mut len)
    };
    // SAFETY: `token` is the handle we opened above and still own; close it
    // regardless of the query result.
    unsafe { CloseHandle(token) };
    if ok == 0 {
        return Err(last_os_error("GetTokenInformation"));
    }

    // SAFETY: on success `buf` holds a `TOKEN_USER`. Read it out *unaligned* —
    // a `Vec<u8>` buffer is not guaranteed to meet `TOKEN_USER`'s alignment, so
    // forming a `&TOKEN_USER` reference would be UB. The copied `User.Sid`
    // still points into `buf`, which outlives the `ConvertSidToStringSidW` call
    // below.
    let token_user = unsafe { buf.as_ptr().cast::<TOKEN_USER>().read_unaligned() };
    let mut sid_str = std::ptr::null_mut();
    // SAFETY: `token_user.User.Sid` is a valid SID within `buf`;
    // `ConvertSidToStringSidW` allocates a wide string into `sid_str` on
    // success (return != 0), freed below.
    if unsafe { ConvertSidToStringSidW(token_user.User.Sid, &raw mut sid_str) } == 0 {
        return Err(last_os_error("ConvertSidToStringSid"));
    }
    let s = u16_ptr_to_string(sid_str);
    // SAFETY: `sid_str` was allocated by `ConvertSidToStringSidW`; free it with
    // `LocalFree` per its contract.
    unsafe { LocalFree(sid_str.cast()) };
    Ok(s)
}

/// A self-relative security descriptor allocated by
/// `ConvertStringSecurityDescriptorToSecurityDescriptorW`, freed on drop.
struct OwnerSecurityDescriptor(PSECURITY_DESCRIPTOR);

impl Drop for OwnerSecurityDescriptor {
    fn drop(&mut self) {
        // SAFETY: `self.0` was allocated by
        // `ConvertStringSecurityDescriptorToSecurityDescriptorW` and is freed
        // exactly once (here); `LocalFree(NULL)` is a documented no-op.
        unsafe { LocalFree(self.0) };
    }
}

/// Build the owner-only security descriptor for `owner_sid`
/// (SDDL `O:<sid>D:P(A;;GA;;;<sid>)`).
///
/// The `O:` component **explicitly stamps the owner** as the token *user* SID.
/// Without it, Windows fills the owner from the token's default owner, which for
/// an elevated process is the *Administrators* group SID — and the client's
/// owner-SID check (`win_pipe`, review B1) compares against `TokenUser`, so a
/// defaulted owner would make an elevated daemon's own client reject the pipe.
fn build_owner_sd(owner_sid: &str) -> io::Result<OwnerSecurityDescriptor> {
    build_sd(&format!("O:{owner_sid}D:P(A;;GA;;;{owner_sid})"))
}

/// Parse an SDDL string into a self-relative security descriptor.
fn build_sd(sddl: &str) -> io::Result<OwnerSecurityDescriptor> {
    let sddl = wide(sddl);
    let mut psd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: `sddl` is a valid NUL-terminated wide SDDL string. On success
    // (return != 0) a self-relative SD is allocated into `psd`; ownership
    // transfers to the returned `OwnerSecurityDescriptor`, which frees it.
    let ok = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &raw mut psd,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(last_os_error("ConvertStringSecurityDescriptor"));
    }
    Ok(OwnerSecurityDescriptor(psd))
}

/// A bound control pipe: its wide name plus the owner SID from which the
/// owner-only security descriptor is (re)built per instance. Analogous to a
/// bound `UnixListener`, but named pipes have no single listener object — each
/// waiting instance is a separate handle (see [`Self::create_instance`]).
///
/// Holds only owned, thread-safe data (`Vec<u16>`, `String`) so the accept
/// loop can move it across an `.await` on the multi-threaded runtime; the
/// raw-pointer security descriptor is built and freed *within* each
/// `create_instance` call, never held across a yield.
pub(crate) struct ControlPipe {
    name: Vec<u16>,
    owner_sid: String,
}

impl ControlPipe {
    /// Resolve the daemon's own token SID and validate the owner-only security
    /// descriptor for `pipe_name` (e.g. `\\.\pipe\datamancer\<user>\control`).
    ///
    /// Fail-closed (review B2): any SID or SD failure returns `Err` so the
    /// caller can refuse to start rather than fall back to a default
    /// (world-openable) pipe DACL. The SD is built once here to surface a
    /// malformed-SDDL failure at bind time, then rebuilt per instance.
    pub(crate) fn bind(pipe_name: &str) -> io::Result<Self> {
        let owner_sid = process_token_sid()?;
        let _validate = build_owner_sd(&owner_sid)?;
        Ok(Self {
            name: wide(pipe_name),
            owner_sid,
        })
    }

    /// Create one server instance of the pipe, wrapped for tokio.
    ///
    /// `first` sets `FILE_FLAG_FIRST_PIPE_INSTANCE`, which makes creation
    /// **fail if the name already exists** — i.e. if another process squatted
    /// the control name before the daemon (fail-closed, review S5). Only the
    /// very first instance may set it; subsequent instances (created to keep
    /// accepting while an earlier one is serving) must not.
    ///
    /// `FILE_FLAG_OVERLAPPED` is required by tokio's async pipe wrapper. The
    /// owner-only DACL is applied to every instance via `SECURITY_ATTRIBUTES`;
    /// the descriptor is built here and lives until the end of the call — long
    /// enough for `CreateNamedPipeW` to copy it into the new pipe object.
    pub(crate) fn create_instance(&self, first: bool) -> io::Result<NamedPipeServer> {
        let sd = build_owner_sd(&self.owner_sid)?;
        create_named_pipe_instance(&self.name, &sd, first)
    }
}

/// Create one overlapped byte-mode named-pipe server instance carrying the
/// given security descriptor, wrapped for tokio. `first` sets
/// `FILE_FLAG_FIRST_PIPE_INSTANCE` (fail if the name already exists). The SD is
/// borrowed for the duration of the call — long enough for `CreateNamedPipeW`
/// to copy it into the new pipe object.
fn create_named_pipe_instance(
    name: &[u16],
    sd: &OwnerSecurityDescriptor,
    first: bool,
) -> io::Result<NamedPipeServer> {
    let mut open_mode = PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED;
    if first {
        open_mode |= FILE_FLAG_FIRST_PIPE_INSTANCE;
    }
    let mut sa = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>())
            .expect("SECURITY_ATTRIBUTES size fits u32"),
        lpSecurityDescriptor: sd.0,
        bInheritHandle: 0,
    };
    // SAFETY: `name` is a valid NUL-terminated wide pipe name; `sa` carries the
    // SD and is valid for the duration of the call. `CreateNamedPipeW` copies
    // the SD, so it may outlive/be reused across instances. Returns
    // `INVALID_HANDLE_VALUE` on failure.
    let handle = unsafe {
        CreateNamedPipeW(
            name.as_ptr(),
            open_mode,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
            PIPE_UNLIMITED_INSTANCES,
            PIPE_BUFFER_BYTES,
            PIPE_BUFFER_BYTES,
            0,
            &raw mut sa,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(last_os_error("CreateNamedPipe"));
    }
    // SAFETY: `handle` is a valid, overlapped named-pipe instance we own and do
    // not otherwise touch; `NamedPipeServer` takes ownership (closes on drop).
    unsafe { NamedPipeServer::from_raw_handle(handle.cast()) }
}

#[cfg(test)]
mod tests {
    use std::os::windows::io::AsRawHandle as _;

    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::windows::named_pipe::ClientOptions;

    use super::*;

    /// The owner SID of a live pipe handle (test-only; mirrors the client's
    /// `win_pipe::pipe_owner_sid`). Panics on any Win32 failure.
    fn owner_sid_of(handle: std::os::windows::io::RawHandle) -> String {
        use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_KERNEL_OBJECT};
        use windows_sys::Win32::Security::{OWNER_SECURITY_INFORMATION, PSID};

        let mut owner: PSID = std::ptr::null_mut();
        let mut psd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: `handle` is a live pipe; `GetSecurityInfo` writes the owner SID
        // (into the SD it allocates at `psd`) and returns 0 on success.
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
        assert_eq!(rc, 0, "GetSecurityInfo failed: {rc}");
        let mut sid_str = std::ptr::null_mut();
        // SAFETY: `owner` is valid within the still-live `psd`.
        unsafe { ConvertSidToStringSidW(owner, &raw mut sid_str) };
        let s = u16_ptr_to_string(sid_str);
        // SAFETY: free both allocations.
        unsafe {
            LocalFree(sid_str.cast());
            LocalFree(psd);
        }
        s
    }

    #[test]
    fn process_token_sid_is_a_sid_string() {
        let sid = process_token_sid().expect("token SID");
        assert!(sid.starts_with("S-1-"), "not a SID: {sid}");
    }

    // `#[tokio::test]`: `create_instance` wraps the pipe with tokio's
    // `NamedPipeServer::from_raw_handle`, which requires a running reactor.
    #[tokio::test]
    async fn first_instance_rejects_a_squatted_name() {
        // Two `bind`s of the same name each minting a *first* instance: the
        // second must fail (FILE_FLAG_FIRST_PIPE_INSTANCE), proving the
        // fail-closed anti-squat guarantee (review S5).
        let name = r"\\.\pipe\datamancer-test-squat-guard";
        let a = ControlPipe::bind(name).expect("bind a");
        let _first = a.create_instance(true).expect("first instance");
        let b = ControlPipe::bind(name).expect("bind b");
        let err = b
            .create_instance(true)
            .expect_err("second first-instance must fail");
        // Windows rejects a duplicate FILE_FLAG_FIRST_PIPE_INSTANCE with
        // ERROR_ACCESS_DENIED → PermissionDenied.
        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "expected access-denied on squat, got {err:?}"
        );
    }

    #[tokio::test]
    async fn owner_dacl_pipe_round_trips_same_user() {
        let name = r"\\.\pipe\datamancer-test-owner-roundtrip";
        let pipe = ControlPipe::bind(name).expect("bind");
        let server = pipe.create_instance(true).expect("first instance");

        let server_task = tokio::spawn(async move {
            server.connect().await.expect("accept");
            let (read, mut write) = tokio::io::split(server);
            let mut lines = BufReader::new(read).lines();
            if let Ok(Some(_line)) = lines.next_line().await {
                let _ = write.write_all(b"{\"ok\":true}\n").await;
            }
        });

        // Same-user client (this test process is the owner) must connect.
        let client = ClientOptions::new().open(name).expect("client connect");
        let (read, mut write) = tokio::io::split(client);
        write
            .write_all(b"{\"op\":\"ping\"}\n")
            .await
            .expect("write");
        let mut lines = BufReader::new(read).lines();
        let reply = lines.next_line().await.expect("read").expect("reply line");
        assert!(reply.contains(r#""ok":true"#), "unexpected reply: {reply}");
        server_task.await.expect("server task");
    }

    /// The pipe's owner is explicitly stamped as the token *user* SID — not
    /// left to Windows' default (which is the Administrators group for an
    /// elevated process, and would break the client's owner-SID check).
    #[tokio::test]
    async fn owner_is_stamped_as_token_user() {
        let name = r"\\.\pipe\datamancer-test-owner-stamp";
        let pipe = ControlPipe::bind(name).expect("bind");
        let server = pipe.create_instance(true).expect("first instance");
        let owner = owner_sid_of(server.as_raw_handle());
        let me = process_token_sid().expect("token SID");
        assert_eq!(
            owner, me,
            "pipe owner must be the token user SID (elevated-safe), not defaulted"
        );
    }

    /// A principal that is **not** in the DACL is denied at connect. The DACL
    /// grants only `LocalSystem` (S-1-5-18) while the owner stays this user (so
    /// creation succeeds); this non-SYSTEM test process must then be refused —
    /// proving the owner-only DACL is the authorization boundary, without
    /// needing a second user account (review B3).
    #[tokio::test]
    async fn owner_dacl_denies_a_non_granted_principal() {
        let name = r"\\.\pipe\datamancer-test-foreign-dacl";
        let me = process_token_sid().expect("token SID");
        let sd = build_sd(&format!("O:{me}D:P(A;;GA;;;S-1-5-18)")).expect("build sd");
        let _server = create_named_pipe_instance(&wide(name), &sd, true).expect("create");
        let err = ClientOptions::new()
            .open(name)
            .expect_err("a principal not in the DACL must be denied");
        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "expected access-denied for a non-granted principal, got {err:?}"
        );
    }
}

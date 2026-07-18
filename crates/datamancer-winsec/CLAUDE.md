# datamancer-winsec

Shared Windows security primitives for the control channel's two ends: the
daemon's `win_control` and the client's `win_pipe`. Token/handle identity
(process token user SID, pipe owner SID) plus process integrity-level
reading, and a pure classifier on top of it.

## Invariants / stance

- **EXT-1: the workspace's single audited `unsafe` crate for these
  primitives.** `#![forbid(unsafe_code)]` off Windows; `#![deny(unsafe_code)]`
  on Windows with one scoped `#[allow(unsafe_code)]` confined to the `ffi`
  module. `datamancerd::win_control` remains its own separate audited allow
  site (pipe creation + SDDL) — it does not move here.
- **`datamancer-core` and `datamancer-credentials` never depend on this
  crate** and stay `forbid(unsafe_code)` on every platform.
- **The integrity classifier (`classify`, `integrity_ok`, `IntegrityClass`) is
  pure and cross-platform**, unit-tested on every OS — CI cannot elevate or
  sandbox a process, so this is where the High/System/Low reject-path
  coverage actually lives. Do not weaken these tests.
- **Windows-only readers** (`current_process_integrity`,
  `client_process_integrity`, `current_process_token_sid`, `owner_sid_of`)
  are thin wrappers around Win32 calls in `ffi`, each carrying a `// SAFETY:`
  proof. `RawHandle` and windows-sys `HANDLE` are the same transparent alias,
  so no cast is needed at the call sites.
- `[lints] workspace = true`.

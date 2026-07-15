# Native Windows Support — Design & Architectural Path

*Design spec · 2026-07-15 · tracking issue #29 · status: DRAFT for review*

## 1. Goal & motivation

Run the **entire** Datamancer stack natively on Windows — including `datamancerd`
(the daemon), its control surface, and the iceoryx2 transport — not just the
ws-portable subset.

This **supersedes** the "Windows daemon support" non-goal recorded in
[`2026-07-03-open-sourcing-design.md`](2026-07-03-open-sourcing-design.md), which
established the current boundary: Windows CI builds only
`datamancer-transport-ws` + `datamancer-client --features ws`, and the daemon +
iceoryx2 are POSIX-only. That boundary is now a **limitation to overcome**, not a
permanent stance.

### Guiding constraint (non-negotiable)

Cross-platform support must **never degrade the macOS/Linux experience**. The
litmus test: *a macOS build must be byte-for-byte unaffected by any change here.*
Concretely:

- Windows-specific code lives behind `#[cfg(windows)]` / `[target.'cfg(windows)'.dependencies]`.
- New **platform modules** implement existing internal seams; we never widen a
  shared state machine to special-case Windows.
- `#![forbid(unsafe_code)]` holds in all seven crates. Any unavoidable Win32
  unsafe is isolated to a single implementation crate and justified with a
  `// SAFETY:` proof, per the iceoryx2 **EXT-1** precedent — **never** relaxed in
  the core crates.

## 2. Executive summary

**The dependency situation is far better than the old non-goal implied.** Every
heavy dependency is already Windows-capable:

- **iceoryx2 0.9.2** officially supports Windows (Tier 2); its PAL already depends
  on `windows-sys`. The earlier full-build failure was **solely a missing
  `libclang`** (bindgen), a *toolchain* gap, not a portability wall.
- **turso** is pure Rust — no `libsqlite3-sys` anywhere.
- **The Windows keychain backend is already wired** — `windows-native-keyring-store`
  is already a `[target.'cfg(windows)'.dependencies]` with a `#[cfg(windows)]`
  arm in `keychain.rs`.
- **TLS** on the portable path is rustls/schannel; no OpenSSL.

**The real work is a focused POSIX-abstraction port,** concentrated in the
daemon's control plane. It falls into five buckets, in rough order of size:

1. **Control socket** (largest) — UDS → Windows **named pipe**. The daemon
   `server.rs` has **no seam** here (the biggest surface); the client `app`
   facade is **already seamed** and needs only a platform module + one `cfg`
   switch.
2. **Peer-credential auth** (security-critical) — `SO_PEERCRED`/`geteuid` →
   `ImpersonateNamedPipeClient` + **token SID** comparison (never PID).
3. **Process lifecycle** — SIGTERM → `tokio::signal::windows`; `rustix::flock`
   single-instance lock → `LockFileEx`; `process_group(0)` detach →
   `DETACHED_PROCESS`.
4. **Filesystem permissions** — the `0600` credential-file guard is `#[cfg(unix)]`;
   Windows needs an **owner-only ACL**, fail-closed.
5. **Build & CI** — install LLVM (`LIBCLANG_PATH`) and grow the Windows CI job
   from the ws subset to the full stack.

**The single highest-risk item is the peer-credential auth port** — it is a
security boundary, and a naive port silently reopens every privileged op.

**The former "biggest unknown" — iceoryx2 shared memory on Windows — has been
spiked and resolved: it does not work on Windows and won't in a production-viable
way.** Windows therefore needs its **own data-transport path** to the same
service. See §2.5.

## 2.5 Spike result & Windows transport decision (2026-07-15)

A build+runtime spike settled the biggest open question. **iceoryx2 0.9.2
compiles on Windows** (once LLVM/`libclang` is installed — build succeeds in
~20s) **but cannot create a node at runtime**: all six `#[ignore]`d
cross-process tests fail with `create iceoryx2 node: InternalError`, root-caused
to iceoryx2's PAL reading the POSIX user database `/etc/passwd` on Windows.

Deep research confirms this is not a version/config nit but a standing limitation:

- Windows is **Tier 2** ("restricted security and safety feature set") — never
  Tier 1 ([README](https://github.com/eclipse-iceoryx/iceoryx2)).
- Windows cross-process runtime has **multiple open, unresolved issues** — a
  service-creation **hang** ([#149](https://github.com/eclipse-iceoryx/iceoryx2/issues/149)),
  and the user-DB permission model ([#460](https://github.com/eclipse-iceoryx/iceoryx2/issues/460)).
- The only bypass, the `dev_permissions` flag, is **explicitly not for production**
  and grants all-process access — it would **break our same-uid security model**
  ([FAQ](https://github.com/eclipse-iceoryx/iceoryx2/blob/main/FAQ.md)).
- The **roadmap does not mention Windows** at all — no path to Tier 1
  ([ROADMAP](https://github.com/eclipse-iceoryx/iceoryx2/blob/main/ROADMAP.md));
  latest is the v0.9 line, v1.0 targeted end-2026, no Windows runtime fixes.

**Decision.** iceoryx2 stays the Linux/macOS zero-copy transport (nothing is
removed). Windows gets its **own transport path to the same service** — which the
architecture already anticipates (WS is a second worked transport; a unified
client-transport trait is the stated direction). iceoryx2 delivers *no* new
semantics, so swapping the transport on Windows loses only *zero-copy* (a
performance property), never a capability.

| Option | Windows data transport | Effort | Trade-off |
|---|---|---|---|
| **C1 (recommended, now)** | Reuse the existing **WS transport over loopback** | ~zero new transport code (already built + portable) | Loses zero-copy on Windows; correct and functional |
| **C2 (optional, later)** | **Windows-native shared-memory** transport — named-pipe control + memory-mapped shm ([`interprocess`](https://crates.io/crates/interprocess), [`memmap2`](https://crates.io/crates/mmap-io)/`winmmf`) | new transport crate | Restores zero-copy on Windows |

**Recommendation for review:** ship **C1** to make Windows functional, treat **C2**
as a later optimization only if Windows perf demands zero-copy. The control-plane
port (§4.1–4.4) is required either way — only the *data transport* diverges.

## 3. Current-state inventory (source-anchored)

### 3.1 What is already Windows-ready

| Area | Evidence |
|---|---|
| iceoryx2 transport crate | No Unix-only code; delegates all shm to iceoryx2 (Windows Tier 2). `datamancer-transport-iceoryx2/**` |
| WS transport crate | Pure `tokio-tungstenite`; Windows CI already builds it |
| turso storage | Pure Rust; no C sqlite in `Cargo.lock` |
| Keychain credentials | `windows-native-keyring-store` dep + `#[cfg(windows)]` arm already present (`keychain.rs:41`, `Cargo.toml:29`) |
| Client `app` find-or-spawn seams | `ControlEndpoint`/`DaemonSpawner`/`SpawnedDaemon` traits, fully fake-tested (`app/lifecycle.rs`) |
| Shutdown drain logic | Platform-agnostic async over trait objects (`shutdown.rs`) |
| Atomic writes | `std::fs::rename` replaces-existing on Windows (`paths.rs`, `file.rs`) |
| Path construction | `PathBuf::join`-clean throughout; already has Windows path arms in some tests |
| `default_daemon_log` | Already Windows-aware, test-pinned (`paths.rs:34`, test `:65-70`) |

### 3.2 The porting surface (un-gated POSIX code)

| # | Item | Location | Current | Windows target |
|---|---|---|---|---|
| **Control socket** ||||
| C1 | Daemon bind/accept | `datamancerd/server.rs:28,477-493,800-826` | `UnixListener` | Named-pipe server (byte mode) |
| C2 | Stale-socket cleanup | `server.rs:505-538` (`FileTypeExt::is_socket`, `UnixStream::connect` probe) | filesystem unlink/probe | **Dropped** — pipes vanish on last-handle-close |
| C3 | Client `ControlConn` | `datamancer-client/iceoryx2.rs:20,98-99,104` | hardcodes `tokio::net::unix::{OwnedReadHalf,OwnedWriteHalf}` in struct fields | needs its own cfg abstraction / shared control-transport trait |
| C4 | Client `app` endpoint | `datamancer-client/app/platform.rs:13,28` | `UnixStream::connect` (behind `ControlEndpoint` seam ✅) | named-pipe `ClientOptions` in a Windows `platform` module |
| C5 | Default socket identity | `paths.rs:24-28` `default_control_socket` | `<dir>/control.sock` | `\\.\pipe\datamancer\control-<user>` |
| C6 | Admin-socket config fallback | `datamancerd/config.rs:263-266` | literal `/run/datamancer/control.sock` | Windows pipe-name fallback |
| **Auth** ||||
| A1 | Gate logic | `datamancerd/credentials.rs:17-21` `privileged_op_permitted` | `peer_uid == Some(own_euid)` (portable) | reuse — feed it a SID-equality bool |
| A2 | Read peer identity | `server.rs:905` `stream.peer_cred().uid()` | `SO_PEERCRED` | `ImpersonateNamedPipeClient` → token user SID |
| A3 | Own identity | `server.rs:288` `rustix::process::geteuid()` | euid | own-process token user SID |
| A4 | Enforcement sites | `server.rs:933` (creds), `:949` (config/shutdown) | uid compare | unchanged logic, SID inputs |
| **Lifecycle** ||||
| L1 | SIGTERM | `server.rs:318,1077-1082` `tokio::signal::unix` | `SignalKind::terminate` | `tokio::signal::windows::{ctrl_close,ctrl_shutdown}`; `ctrl_c` already portable |
| L2 | Single-instance lock | `single_instance.rs:14,67` `rustix::fs::flock` | advisory flock + PID file | `LockFileEx`/`fs4` (keep PID diagnostics, per-user scope) |
| L3 | Spawn detach | `app/platform.rs:102-106` `process_group(0)` (already `#[cfg(unix)]`) | setsid-style detach | `DETACHED_PROCESS`/`CREATE_NEW_PROCESS_GROUP` |
| **Filesystem** ||||
| F1 | Credential file perms | `credentials/file.rs:48-52,58-62` (already `#[cfg(unix)]`) | `0o600` create + re-establish | owner-only **protected DACL**, strip inheritance, **fail-closed** |
| F2 | Path-shape tests missing Windows arm | `datamancer-client/paths.rs:74-87`, `datamancerd/paths.rs:300-311` | macOS/linux only | add `#[cfg(windows)]` arms (account for nested `data\`) |
| F3 | POSIX literal paths | `config.rs:265` `/run/...`, scaffold `/tmp/...` | POSIX absolute | OS-neutral / Windows fallback |
| **Build/CI** ||||
| B1 | Toolchain | iceoryx2 → bindgen 0.72 → libclang | not installed on Windows CI | install LLVM, set `LIBCLANG_PATH` |
| B2 | CI job | `.github/workflows/ci.yml:78-91` | ws subset only, 15-min | full build+test, higher timeout |
| B3 | e2e harness | `daemon_e2e.rs:18`, `config_service_e2e.rs:23`, `ws_e2e.rs:215` | `std::os::unix::net::UnixStream`, `libc::kill` SIGTERM | named-pipe client + `GenerateConsoleCtrlEvent` |
| B4 | Keychain name label | `credentials/keychain.rs:54-58` | returns `"secret-service"` on Windows (wrong) | add `#[cfg(windows)]` arm → `"credential-manager"` |
| B5 | openssl-sys edge | via `tokio-tungstenite` `native-tls` on `--all-features` only | not on portable path | pin `rustls-tls` or ensure Perl/C on CI |

## 4. Per-item porting strategy

### 4.1 Control socket → named pipe (C1–C6)

Use **Windows named pipes** (`tokio::net::windows::named_pipe`), **byte mode**, so
`BufReader::lines()` / `write_all(line + "\n")` framing is unchanged from UDS. The
newline-JSON protocol (`protocol/uds.rs`) is transport-neutral and needs no change.

Introduce a **server-side control-transport seam** in `datamancerd` (it has none
today) — an internal trait abstracting *bind → accept → (stream, peer-identity)*.
Two impls: the existing UDS path (`#[cfg(unix)]`) and a named-pipe path
(`#[cfg(windows)]`). The named-pipe path pre-creates the next instance before
accept (Windows has no `UnixListener::accept` equivalent), sets the pipe DACL
(§4.2), and uses `FILE_FLAG_FIRST_PIPE_INSTANCE` to refuse a hijacked pre-existing
instance — the analog of the existing "don't adopt a foreign socket" logic.

Client side: the `app` facade is already seamed — add a Windows `platform` module
(named-pipe `ControlEndpoint` + `DETACHED_PROCESS` `DaemonSpawner`) and a single
`cfg` switch at `app/mod.rs:134-140`. The `iceoryx2.rs` `ControlConn` is **not**
seamed (it names `tokio::net::unix` half-types in its struct); refactor it onto a
shared control-transport abstraction rather than duplicating a second cfg-split.

`default_control_socket` / `admin_socket` return a `\\.\pipe\...` identity on
Windows; the `Path`-based `ensure_daemon` state machine already accepts this.

### 4.2 Peer-credential auth (A1–A4) — SECURITY CRITICAL

Windows has no `SO_PEERCRED`. Port the *transport identity read*, not the gate
logic (`privileged_op_permitted` is a portable bool compare and stays).

**Recommended:** on the server pipe handle, `ImpersonateNamedPipeClient` → open
the impersonation token (`OpenThreadToken`) → read its user SID
(`GetTokenInformation`/`TokenUser`) → `RevertToSelf` → compare (`EqualSid`)
against the daemon's own token user SID. This is the SID-equality analog of
`peer_uid == own_euid`.

**Must-not-get-wrong:**
- **Fail-closed.** The Unix gate denies on unreadable peer (`None`). Any
  impersonation/token error on Windows must likewise **deny**, never allow.
- **Not PID.** `GetNamedPipeClientProcessId` alone is spoofable (Project Zero);
  use it only as an auxiliary signal, never the authorization decision.
- **Pipe DACL** granting only the current user's SID (and denying
  `FILE_CREATE_PIPE_INSTANCE` to others) as defense-in-depth.

Isolate the Win32 token calls behind a vetted crate (`windows`/`windows-sys`); if
any `unsafe` is unavoidable it is confined to `datamancerd` (or a tiny helper
crate) with a `// SAFETY:` proof, per EXT-1 — the core crates keep `forbid`.

### 4.3 Lifecycle (L1–L3)

- **Signals:** a `cfg`-split helper returns a selectable "terminate" future —
  `SignalKind::terminate` on unix, `ctrl_close`+`ctrl_shutdown` on Windows — so
  the `select!` loop body is identical. `ctrl_c` is already cross-platform. Note
  Windows `CTRL_CLOSE/SHUTDOWN` impose an OS kill deadline (~5–10s) the drain must
  finish inside (§6 spike).
- **Single-instance lock:** `cfg`-split `InstanceLock` — `rustix::flock` on unix,
  `fs4`/`fd-lock` `LockFileEx` on Windows — same `<data dir>\datamancerd.lock`
  path and PID-write diagnostics, **per-user** scope (avoid a machine-global
  mutex, which would change semantics).
- **Spawn detach:** Windows `DaemonSpawner` impl using
  `std::os::windows::process::CommandExt::creation_flags(DETACHED_PROCESS |
  CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW)`; log-file stdio redirect is
  unchanged. Both are safe-Rust APIs.

### 4.4 Filesystem permissions (F1–F3)

Replace the compiled-out `0o600` logic with a Windows **owner-only protected
DACL** (single ACE for the current user SID, inheritance stripped) applied at
create and re-established on each save — preserving the existing "mode not
trusted" invariant. **Fail-closed:** the Windows file backend must refuse to write
a credential file it cannot lock down, rather than emit inheritable-readable
plaintext. Add a Windows ACL assertion test (the `0o600` tests are `#[cfg(unix)]`).
Fix the `/run` and `/tmp` literals; add Windows arms to the two path-shape tests.

### 4.5 Build & CI (B1–B5)

Install LLVM and export `LIBCLANG_PATH` in the Windows CI job (and document it for
devs — `winget install LLVM.LLVM` / `choco install llvm` / `KyleMayes/install-llvm-action`).
Grow the Windows job to build `datamancerd` + `transport-iceoryx2` +
`datamancer-client --features app`, raise the timeout, and — once the port lands —
run the `#[ignore]`d daemon e2e (needs a live iceoryx2 runtime on Windows + the
named-pipe/ctrl-event test-harness rewrite). Keep the full Windows job **non-required
until it is proven stable** (a flaky required check blocks everyone's merges). Fix
the keychain `NAME` label; pin `rustls-tls` (or provision Perl/C) for
`--all-features`.

## 5. Phased plan (dependency-ordered, each phase independently mergeable & non-degrading)

> Each phase is a separate issue/PR off an up-to-date `main`. macOS/Linux behavior
> is unchanged at every step.

**Phase 1 — Zero-risk portability cleanups.** Keychain `NAME` Windows arm (B4);
Windows arms on the two path-shape tests (F2); OS-neutral fallback literals (F3);
a docs page on the LLVM/`LIBCLANG_PATH` requirement (B1). No IPC changes; mergeable
immediately.

**Phase 2 — Compile-enabling lifecycle port (no IPC yet).** `cfg`-split
single-instance lock (L2), signals (L1), credential-file ACL fail-closed (F1),
Windows `DaemonSpawner` detach (L3). After this, most of the daemon compiles on
Windows except the control transport.

**Phase 3 — Control-transport seam + named pipe (the core).** Introduce the
server-side control-transport seam; implement the Windows named-pipe path (C1–C2,
C5–C6); refactor client `ControlConn` (C3) and wire the client `app` platform
switch (C4). **Ships with §4.2 auth (A1–A4)** — the security-critical piece,
reviewed as a unit. This is the largest PR and the one to spike first.

**Phase 4 — Windows data transport (WS-loopback).** *Spike complete — iceoryx2 is
not viable on Windows (§2.5).* Instead, wire the existing WS transport as the
Windows same-host data/diagnostics/health path (option C1), behind the unified
client-transport seam, `#[cfg(windows)]`-selected. iceoryx2 remains the
Linux/macOS transport, unchanged. (Optional later: a Windows-native shared-memory
transport, C2, if zero-copy on Windows is required.)

**Phase 5 — Full Windows CI + e2e.** Promote the Windows CI job to the full stack;
port the e2e harnesses (B3); resolve the openssl-sys `--all-features` edge (B5);
decide required-vs-optional once green and stable.

## 6. Risk / uncertainty register (de-risking spikes, ranked)

1. **iceoryx2 shm on Windows — RESOLVED (§2.5).** Spiked: iceoryx2 0.9.2 compiles
   but node creation fails at runtime (`/etc/passwd`), Windows is Tier 2 with
   unresolved runtime issues and no roadmap fix. **Decision: do not use iceoryx2
   for the Windows data transport;** use WS-loopback now (C1), native shm later
   (C2). No longer an open risk.
2. **Peer-identity auth gate (Phase 3).** Highest *risk* (security boundary).
   *Spike: `ImpersonateNamedPipeClient` + token-SID equality, fail-closed, DACL.*
3. **Named-pipe newline-JSON semantics (Phase 3).** *Spike: byte-mode
   `BufReader::lines()` round-trip + clean EOF-on-disconnect + multi-instance
   accept with no race.*
4. **Windows shutdown-signal timing (Phase 2).** *Spike: confirm the drain
   (`shutdown_timeout`) completes inside the OS `CTRL_CLOSE/SHUTDOWN` kill
   deadline.*
5. **Detached child lifetime (Phase 2).** *Spike: `DETACHED_PROCESS` child
   outlives a spawning GUI/console app; `try_wait` polling still works.*
6. **`#![forbid(unsafe_code)]` policy.** Decide up front: which crate (if any)
   relaxes to a single scoped `#[allow]` for Win32 token/lock calls, or whether a
   vetted safe wrapper crate covers everything. Default: stay fully safe; EXT-1
   fallback only in the one implementation crate.

## 7. Reference — what the port must ultimately serve (web-template gap analysis)

The native-Windows daemon exists to back the **Tauri desktop prototype**
(`VSS_Datamancer_Prototype_002`: Status / Event Log / Config screens). A companion
gap analysis of that prototype vs. current capabilities is tracked separately; two
findings intersect this port directly and should inform it:

- **The Tauri app is an out-of-process client.** Connection/auth/error `Control`
  events are **suppressed over the wire** today (`datamancer-transport-ws/wire.rs`,
  and the iceoryx2 transport) — a remote UI reads connectivity only from
  `snapshot`. Any Windows control-transport work should keep this contract in view;
  the design's Event Log needs either un-suppression (with care) or a new
  event-log surface, independent of this port.
- **The Config screen shows per-provider credential source (keychain vs file).**
  Today the backend is **global and auto-selected**, and the file fallback's
  owner-only guarantee is `#[cfg(unix)]` — directly the F1 work above. A
  per-provider credential-source UI is a separate design question, but the Windows
  ACL fail-closed behavior is a prerequisite for the file backend being safe to
  surface at all.

The full prototype gap analysis (data we have but don't expose, data we lack, and
design elements not feasible through current feeds — notably **no futures
provider**) is out of scope for this spec and lives with the design-review effort.

## 8. Out of scope

- Implementation (this spec defines the path; code lands per §5 as follow-up
  issues/PRs).
- New providers / futures feeds, the operational event-log store, and other
  prototype-driven feature work (separate design effort).
- Native Windows *GUI* packaging of the Tauri app itself.

## Appendix — primary source anchors

Control plane: `datamancerd/src/server.rs:28,288,477-538,800-826,895-906,1077-1082`;
`datamancer-client/src/iceoryx2.rs:20,98-104`;
`datamancer-client/src/app/{lifecycle.rs,platform.rs,mod.rs:134-140}`;
`datamancer-client/src/protocol/uds.rs`.
Auth: `datamancerd/src/credentials.rs:17-21`.
Lifecycle: `datamancerd/src/{single_instance.rs:14,67, shutdown.rs}`;
`app/platform.rs:102-106`.
Filesystem: `datamancer-credentials/src/file.rs:48-62`, `lib.rs:230-278`;
`datamancer-client/src/paths.rs:24-28,74-87`; `datamancerd/src/paths.rs:300-311`;
`config.rs:263-266`.
Deps/toolchain: root `Cargo.toml`; `datamancer-credentials/Cargo.toml:23-30`;
`datamancerd/Cargo.toml:44-47,73`; `.github/workflows/ci.yml:78-91`, `e2e.yml`.
Keychain: `datamancer-credentials/src/keychain.rs:34-58`.

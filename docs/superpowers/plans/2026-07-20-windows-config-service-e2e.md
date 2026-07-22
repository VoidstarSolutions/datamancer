# Windows config-service admin-plane e2e (Phase 5, B3 targeted) — Design + Plan

> Targeted slice of parent-spec **Phase 5 / B3** (`docs/superpowers/specs/2026-07-15-native-windows-support-design.md`): port the daemon control-plane e2e harness off `UnixStream`/POSIX so `config_service_boots_disabled_and_shuts_down_cleanly` runs on Windows CI. Continues on branch `feat/36-windows-3-named-pipe` / PR #37. Follows the Phase-4 daemon-boot work (the daemon now boots on Windows).

## Goal
Prove the **named-pipe admin plane end-to-end on Windows**: spawn the real `datamancerd`, and over the control pipe drive `ping` / `get-config` / `configure-provider` (incl. the `unknown_config_field` error path) / `shutdown`. This complements the Phase-4 `windows_ws_only_boot_smoke` (WS data/health plane) with the **control** plane.

## Key facts that shape the design
- The boot test is **pure control plane** — no data, no WS. So it needs **no `ws` feature**: on Windows the daemon boots control-plane-only (no iceoryx2 node — Phase 4; a no-data-plane warning is expected), which is exactly enough. It runs in the existing `native clippy + control-transport` job's default (no-ws) context.
- **A Windows named pipe supports `std::fs::File` duplex I/O** — open `\\.\pipe\…` with `OpenOptions::read(true).write(true)`, then `write_all(line+"\n")` / `BufRead::read_line`. The newline-JSON round-trip is byte-identical to the UDS path, so `#![forbid(unsafe_code)]` holds (pure std I/O, no FFI).
- CI runners are **elevated integrity** → the config needs `[server].allow_any_integrity = true` (same as the Phase-4 smoke; [[reference-ci-elevated-integrity]]).

## Design (contained to `config_service_e2e.rs`)
Remove `#![cfg(unix)]`; cfg-split the harness helpers so both tests **compile** on Windows and the boot test **runs** there:

1. **`CtrlStream` type alias** — `#[cfg(unix)] UnixStream` / `#[cfg(windows)] std::fs::File`. Both `Read + Write + try_clone`.
2. **`connect(socket) -> io::Result<CtrlStream>`** — unix `UnixStream::connect`; windows `OpenOptions::new().read(true).write(true).open(socket)` (retry `ERROR_PIPE_BUSY`).
3. **`round_trip`** — unchanged body over `CtrlStream` (write line, read reply line).
4. **`wait_ready(socket)`** — unix `socket.exists()`; windows: a pipe is not a filesystem object, so poll `connect()` until it succeeds (or times out).
5. **`control_socket_path(dir)`** — unix `dir/control.sock`; windows a unique `\\.\pipe\config-service-e2e-…`.
6. **`write_config_no_providers`** — cfg-split the config string: windows adds `allow_any_integrity = true` under `[server]`.
7. **`stop_daemon`** (fallback cleanup) — unix `kill <pid>`; windows `taskkill /F /PID <pid>` (pid from the single-instance lockfile).

The live test (`…enables_and_disables_a_provider_live`) stays `#[ignore]` (needs Alpaca creds); it compiles on Windows via the same helpers but won't run there (and its config carries no `[ws]`, so it's unix/iceoryx2-oriented regardless).

## Non-goals
- Porting `daemon_e2e.rs` and the other Unix-coupled e2e files (a broader shared-harness extraction — deferred; this is the *targeted* slice).
- Running the live provider test on Windows (needs creds + the WS data plane).

## Plan — ✅ DONE (verified on Windows)
- [x] Cfg-split the helpers; removed `#![cfg(unix)]`; the boot test **passes on Windows** (`1 passed`).
- [x] Added to the native-Windows CI job (no `ws` feature; runs alongside `win_control`).
- [x] Unix arms are behavior-preserving (UDS path, file-exists readiness, `kill` — unchanged).

### Three Windows-pipe-client gotchas found while porting `round_trip` (a bare `std::fs::File` is not a drop-in for tokio's pipe client)
1. **Impersonation QoS required.** The daemon reads the client's integrity by *impersonating* it. A bare `File` open defaults to anonymous/identification and the daemon's read **stalls silently** (connect ok, no reply). Fix: `OpenOptionsExt::security_qos_flags(SECURITY_IMPERSONATION)` (`0x0002_0000`; sets `SECURITY_SQOS_PRESENT`). Safe std API — `forbid(unsafe_code)` holds.
2. **`ERROR_PIPE_BUSY` between round-trips.** Each `round_trip` opens a fresh connection; the daemon's accept loop may not have re-created a free instance yet → open fails (code 231). Fix: brief retry loop (what `WaitNamedPipe` does).
3. **No `try_clone`.** A cloned pipe handle is a separate client end; use one handle for write-then-read (the pipe is duplex), matching how a normal client behaves.

Also: TOML **literal strings** (single quotes) for the Windows `admin_socket` pipe name — no backslash-doubling. And leftover daemons from killed test runs hold the global instance lock + pipe, which masquerades as a hang — kill `datamancerd` between manual runs.

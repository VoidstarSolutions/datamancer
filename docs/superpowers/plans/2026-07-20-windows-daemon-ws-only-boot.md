# Windows WS-only daemon boot — Implementation Plan

> Design: `docs/superpowers/specs/2026-07-20-windows-daemon-ws-only-boot-design.md`. Parent: Phase 4 (`docs/superpowers/plans/2026-07-19-windows-phase4-ws-loopback.md`) — this is the daemon half of Task 5/6 the Phase-4 plan under-scoped. Continues on branch `feat/36-windows-3-named-pipe`, PR #37. TDD; per-crate Windows gates after each task (`LIBCLANG_PATH=C:/Program Files/LLVM/bin`).

## Global constraints
- Every change `#[cfg(not(windows))]`/`#[cfg(windows)]`-additive; unix/macOS byte-for-byte unchanged. iceoryx2 node/publishers/data-sink are the unix build verbatim.
- `#![forbid/deny(unsafe_code)]` unaffected (pure cfg-gating, no FFI).
- Breaking changes covered by the in-range `feat(windows)!:` marker (PR #37); do **not** bump `[workspace.package] version`.
- Windows gates per task: `cargo clippy -p datamancerd --features ws --all-targets -- -D warnings`, `-p datamancer-client`, `cargo test`, `cargo fmt --check`.

---

> **STATUS (2026-07-20): D1–D4 DONE and verified on Windows; D5 (CI) remaining.** The daemon **boots WS-only on Windows** — proven by `windows_ws_only_boot_smoke` (WS `snapshot` ok + a pushed `HealthView` after `watch-health`). All Windows gates green: `datamancerd --features ws` and no-ws clippy `-D warnings`, 101 unit tests, `datamancer-client --features app` 63 tests, fmt.

## Task D1 — `UNSUPPORTED_ON_WINDOWS` stable code (vocabulary) — ✅ DONE
- [x] Added `UNSUPPORTED_ON_WINDOWS` to `datamancer-client/src/codes.rs` + `unsupported_on_windows_code_is_stable` regression test in `protocol/uds.rs`.

## Task D2 — cfg-split the iceoryx2 node stack in `server.rs` — ✅ DONE
> As built: gated the `node` field, `NodeBuilder::create()`, the diagnostics+health publishers + `spawn_diagnostics` (+ its drain-abort slot → no-op on Windows), the iceoryx2 imports, `Iceoryx2DataSink`, and `open_client` (Windows arm returns `UNSUPPORTED_ON_WINDOWS`). Cascade also gated: `service_prefix`/`max_clients`/`next_client_id` fields, `spawn_pump`, `DaemonError::Transport`, `PublishOutcome`/`StreamExt` imports; `diag_interval` gets `#[cfg_attr(all(windows, not(feature="ws")), allow(dead_code))]`. `bootstrap` returns `Ok` on Windows (live-verified boot).
- [x] **Field**: `#[cfg(not(windows))] node: Node` + its init in the `Ok(Self { … })`.
- [ ] **`bootstrap`**: gate the `NodeBuilder::create()` block (`:235-237`) `#[cfg(not(windows))]`.
- [ ] **`run`**: gate the two publishers + `spawn_diagnostics` (`:302-312`) and the `diagnostics.abort()` closure in `drain(...)` (`:389`) `#[cfg(not(windows))]`. On Windows, `drain` gets a no-op for that abort slot (or a cfg-split `drain` call).
- [ ] **imports**: gate `NodeBuilder`, `ipc_threadsafe`, `Node`, `Iceoryx2DiagnosticsPublisher`, `Iceoryx2HealthPublisher`, `Iceoryx2DataSink` so Windows sees no unused-import under `-D warnings`.
- [ ] **`open_client`**: cfg-split — `#[cfg(not(windows))]` keeps today's body; `#[cfg(windows)]` returns `Reply::error(codes::UNSUPPORTED_ON_WINDOWS, …)`. Watch `max_clients`/`service_prefix`/`next_client_id` for Windows-dead warnings; gate as needed.
- [ ] **TDD**: a `#[cfg(windows)]` test that `Server::bootstrap(...)` returns `Ok` (no node-create error) with a minimal `[ws]` config.
- [ ] Gate: `cargo clippy -p datamancerd --features ws --all-targets -- -D warnings` on Windows; `cargo test`; also `cargo clippy -p datamancerd --all-targets` (no-ws) still green.
- [ ] Commit `feat(windows): boot datamancerd WS-only (skip the iceoryx2 node on Windows)`.

## Task D3 — no-data-plane warn on Windows + ws_e2e portability — ✅ DONE
- [x] `Server::warn_if_no_data_plane` (`#[cfg(windows)]`, called from `run`): boot-time `tracing::warn!` when no WS data plane serves. **Not** a `compile_error!` (adversarial-review finding — the no-ws control-transport job must keep compiling).
- [x] `ws_e2e.rs`: gated `graceful_shutdown_closes_live_connection` `#[cfg(unix)]` (the `libc::kill(SIGTERM)` test); the file now compiles on Windows with `--features ws`.
- [ ] Gate: `cargo test -p datamancerd --features ws --no-run` on Windows (compiles all test targets); `-- --ignored` where a live boot is available.
- [ ] Commit `feat(windows): require ws on the Windows daemon; make ws_e2e compile on Windows`.

## Task D4 — Boot smoke test (the real proof) — ✅ DONE
- [x] `windows_ws_only_boot_smoke` (`#[cfg(windows)]`, `#[ignore]`) in `ws_e2e.rs`: spawn `datamancerd --features ws` (unique pipe admin_socket + port, no provider), then over WS assert `snapshot` ok + a pushed `HealthView` (`{"view":…}`) after `watch-health`. **Passes on Windows.**
- [x] Client `EnsureConfig.ws_data_url` default (`ws://127.0.0.1:9001`) matches the daemon `[ws]` scaffold port (9001).
- Note: the smoke drives the daemon over **raw WS** (proves boot + data + health without an `app` dev-dep in `datamancerd`). An `AppHandle::ensure`-based integration test (Tasks 3–4 already unit-tested in the client crate) is a possible follow-up.

## Task D5 — Phase-4 Task 6 CI (Zach-blessed)
- [ ] New native-Windows job: build `datamancerd --features ws`, boot it, run the D4 smoke (`--ignored`). Rewrite the `ci.yml:78-87` support-boundary comment to record the lifted scope + Zach's blessing.
- [ ] Keep the existing `native clippy + control-transport tests` job (control-transport subset) and `ws-portable subset` job.
- [ ] Commit `ci(windows): run the WS-loopback daemon + app smoke on Windows`.

---

## Self-review before PR update
- [ ] Unix/macOS byte-for-byte: `git diff` shows only cfg-additive changes; iceoryx2 node/publishers/data-sink path is the unix build verbatim.
- [ ] Windows daemon **boots** (D4 smoke passes locally), serves WS data + health + snapshot.
- [ ] `-D warnings` green on Windows for `datamancerd --features ws`, no-ws, and `datamancer-client --features app`.
- [ ] `cargo deny check`; `.github/scripts/semver-checks.sh origin/main` (marker gate satisfied by the in-range `feat(windows)!:`).
